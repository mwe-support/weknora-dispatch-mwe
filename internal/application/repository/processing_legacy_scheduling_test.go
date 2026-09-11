package repository

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingLegacySchedulingUsesDrainAndActiveLedger(t *testing.T) {
	checkProcessingLegacyScheduling(t, processingTestStore(t))
}

func checkProcessingLegacyScheduling(t *testing.T, r *ProcessingRepository) {
	t.Helper()
	ctx := context.Background()
	source := types.DataSource{ID: "scheduler-source", TenantID: 1, KnowledgeBaseID: "kb", Name: "synthetic", Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive}
	require.NoError(t, r.db.Create(&source).Error)
	old := types.SyncLog{ID: "scheduler-old-run", TenantID: 1, DataSourceID: source.ID, Status: "running", StartedAt: time.Now().Add(-time.Hour)}
	require.NoError(t, r.db.Create(&old).Error)
	logs := NewSyncLogRepository(r.db)
	busy, err := logs.HasRunningSync(ctx, source.ID)
	require.NoError(t, err)
	require.True(t, busy, "unverified old execution remains conservative")
	var queues []map[string]any
	for _, queue := range types.QueueDefinitions() {
		queues = append(queues, map[string]any{"queue": queue.Name, "inventory_digest": strings.Repeat("a", 64), "tasks": []any{}})
	}
	inventory, _ := json.Marshal(map[string]any{"complete": true, "workers": []map[string]any{{"old_instance_id": "old", "replacement_id": "guarded", "image_digest": strings.Repeat("b", 64), "exit_confirmed": true, "guard_protocol": 2}}, "queues": queues})
	scope, _, err := ProcessingSourceRevisions(&source)
	require.NoError(t, err)
	configuration, err := r.ConfigurationRevision(ctx, 1, "kb")
	require.NoError(t, err)
	drain, err := r.RecordLegacyDrain(ctx, types.ProcessingLegacyDrain{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, ScopeRevision: scope, ConfigurationRevision: configuration, Inventory: inventory, CheckedAt: time.Now().UTC(), Actor: "synthetic-operator", EvidenceReference: "synthetic-queue-inventory", EvidenceDigest: strings.Repeat("c", 64), OperationRequestID: "scheduler-drain"})
	require.NoError(t, err)
	busy, err = logs.HasRunningSync(ctx, source.ID)
	require.NoError(t, err)
	require.False(t, busy, "a verified old running record must not permanently stop cron")
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, ExternalID: "scheduler-document", SourceRevision: "v1", PipelineFingerprint: "fixture"})
	require.NoError(t, err)
	busy, err = logs.HasRunningSync(ctx, source.ID)
	require.NoError(t, err)
	require.True(t, busy, "a new active job still prevents overlapping dispatch")
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
		{Stage: "fetch", UnitKey: "one", Phase: types.ProcessingPhasePrepare, InputFingerprint: "one", RequiredForCompletion: true},
		{Stage: "fetch", UnitKey: "two", Phase: types.ProcessingPhasePrepare, InputFingerprint: "two", RequiredForCompletion: true},
	}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for i, step := range steps {
		lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		out := types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "capability", ErrorCode: "SYNTHETIC_BLOCK"}
		if i == 1 {
			out = types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/sibling", OutputDigest: strings.Repeat("a", 64)}
		}
		require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
		if i == 0 {
			busy, err = logs.HasRunningSync(ctx, source.ID)
			require.NoError(t, err)
			require.True(t, busy, "a blocked job can still have an active sibling")
		}
	}
	busy, err = logs.HasRunningSync(ctx, source.ID)
	require.NoError(t, err)
	require.False(t, busy)
	pending := types.SyncLog{ID: "scheduler-new-delivery", TenantID: 1, DataSourceID: source.ID, Status: "running", StartedAt: drain.CheckedAt.Add(time.Minute)}
	require.NoError(t, logs.Create(ctx, &pending))
	busy, err = logs.HasRunningSync(ctx, source.ID)
	require.NoError(t, err)
	require.True(t, busy, "a delivery created after the drain has not been proven ended")
	pending.Status = "canceled"
	require.NoError(t, logs.Update(ctx, &pending))
	busy, err = logs.HasRunningSync(ctx, source.ID)
	require.NoError(t, err)
	require.False(t, busy)
	retained, err := logs.FindByID(ctx, old.ID)
	require.NoError(t, err)
	require.Equal(t, "running", retained.Status)
	require.Nil(t, retained.FinishedAt)
}
