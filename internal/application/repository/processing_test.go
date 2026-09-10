package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func processingTestStore(t *testing.T) *ProcessingRepository {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.DataSource{}, &types.ProcessingJob{}, &types.ProcessingStep{}, &types.ProcessingEvent{}, &types.SyncRunItem{}, &types.TaskPendingOp{}, &types.ProcessingArtifactReference{}, &types.ProcessingStorageReservation{}, &types.ProcessingGraphWrite{}, &types.ProcessingWikiWrite{}, &types.WikiPage{}, &types.StoredResource{}, &types.ResourceBinding{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}, &types.FAQIndexWrite{}))
	installProcessingHistoryTestSchema(t, db)
	require.NoError(t, db.Create(&types.DataSource{
		ID: "source", TenantID: 1, KnowledgeBaseID: "kb", Name: "synthetic",
		Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive,
	}).Error)
	return NewProcessingRepository(db)
}

func TestProcessingPlanDeliveryLeaseAndDependencyCommit(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	require.NoError(t, r.VerifySchema(ctx))
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1,
		KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	specs := []types.ProcessingStepSpec{
		{Stage: "fetch", UnitKey: "source", Phase: types.ProcessingPhasePrepare, InputFingerprint: "source-v1", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "parse", UnitKey: "source", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parse-v1", DependsOn: []string{"fetch/source"}, RequiredForReady: true, RequiredForCompletion: true},
	}
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, specs))
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, specs)) // Producer crash after commit.
	var ops []types.TaskPendingOp
	require.NoError(t, r.db.Find(&ops).Error)
	require.Len(t, ops, 1)
	var ref types.ProcessingRef
	require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
	bad := ref
	bad.InputFingerprint = "stale"
	_, err = r.ClaimStep(ctx, 1, bad, time.Minute)
	require.ErrorIs(t, err, ErrProcessingConflict)
	_, err = r.ClaimStep(ctx, 2, ref, time.Minute)
	require.Error(t, err)
	lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
	require.NoError(t, err)
	started, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingRunning, started.Status)
	_, err = r.ClaimStep(ctx, 1, ref, time.Minute)
	require.ErrorIs(t, err, ErrProcessingConflict)
	outcome := types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "objects/source", OutputDigest: "digest", Completeness: "complete"}
	require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
	require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome)) // Lost ACK must not duplicate transitions.
	require.NoError(t, r.db.Find(&ops).Error)
	require.Len(t, ops, 2)
	var next types.ProcessingRef
	require.NoError(t, json.Unmarshal(ops[1].Payload, &next))
	require.NotEqual(t, ref.StepID, next.StepID)
	_, err = r.ClaimStep(ctx, 1, next, time.Minute)
	require.NoError(t, err)
	events, err := r.ListEvents(ctx, 1, job.ID, 0, 100)
	require.NoError(t, err)
	succeeded := 0
	for _, event := range events {
		if event.Type == "step_committed" && event.ToState == types.ProcessingSucceeded {
			succeeded++
		}
	}
	require.Equal(t, 1, succeeded)
}

func TestProcessingJobReusesGenerationAndPreservesOldHistory(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb",
		DataSourceID: "source", ExternalID: "file", SourceRevision: "revision-1", PipelineFingerprint: "pipeline"}
	first, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Generation)
	require.True(t, first.IsCurrent)
	require.False(t, first.IsPublished)
	same, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.Equal(t, first.ID, same.ID)
	input.SourceRevision = "revision-2"
	next, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Generation)
	require.NotEqual(t, first.ID, next.ID)
	old, err := r.GetJob(ctx, 1, first.ID)
	require.NoError(t, err)
	require.False(t, old.IsCurrent)
	require.Equal(t, types.ProcessingSuperseded, old.Status)
	events, err := r.ListEvents(ctx, 1, first.ID, 0, 20)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "job_created", events[0].Type)
	require.Equal(t, "job_superseded", events[1].Type)
	_, err = r.GetJob(ctx, 2, first.ID)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestProcessingRecoveryRetriesOnlyFailedStageAndFencesOldWorkers(t *testing.T) {
	for _, stage := range []string{"parse", "export_start"} {
		t.Run(stage, func(t *testing.T) {
			r := processingTestStore(t)
			ctx := context.Background()
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1,
				KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"})
			require.NoError(t, err)
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: stage, UnitKey: "source", Phase: types.ProcessingPhasePrepare,
				InputFingerprint: "input", RequiredForReady: true, RequiredForCompletion: true}}))
			ops, err := r.PendingDeliveries(ctx, 100)
			require.NoError(t, err)
			require.Len(t, ops, 1)
			var ref types.ProcessingRef
			require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
			require.NoError(t, r.ConfirmDelivery(ctx, 1, ref, "queue-synthetic-1"))
			lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
			require.NoError(t, err)
			require.NoError(t, r.Heartbeat(ctx, 1, *lease, time.Minute, "verified/page-1"))
			expired := time.Now().Add(-time.Minute)
			require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("id = ?", ref.StepID).Update("lease_expires_at", expired).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			err = r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "old", OutputDigest: "old"})
			require.ErrorIs(t, err, ErrProcessingConflict)
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			require.Equal(t, "verified/page-1", steps[0].CheckpointRef)
			if stage == "export_start" {
				require.Equal(t, types.ProcessingBlocked, steps[0].Status)
				require.Equal(t, "EXPORT_START_UNCERTAIN", steps[0].ErrorCode)
				require.Zero(t, steps[0].RetryCount)
				return
			}
			require.Equal(t, types.ProcessingRetryWait, steps[0].Status)
			require.Equal(t, 1, steps[0].RetryCount)
			require.Equal(t, 2, steps[0].Attempt)
			require.GreaterOrEqual(t, time.Until(*steps[0].NextRunAt), 119*time.Second)
			require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("id = ?", ref.StepID).Update("next_run_at", expired).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			ops, err = r.PendingDeliveries(ctx, 100)
			require.NoError(t, err)
			require.Len(t, ops, 1)
			var next types.ProcessingRef
			require.NoError(t, json.Unmarshal(ops[0].Payload, &next))
			_, err = r.ClaimStep(ctx, 1, ref, time.Minute)
			require.ErrorIs(t, err, ErrProcessingConflict)
			recovered, err := r.ClaimStep(ctx, 1, next, time.Minute)
			require.NoError(t, err)
			require.NoError(t, r.FinishStep(ctx, 1, *recovered, types.ProcessingOutcome{Status: types.ProcessingSucceeded,
				OutputManifestRef: "new", OutputDigest: "new", Completeness: "complete"}))
			events, err := r.ListEvents(ctx, 1, job.ID, 0, 100)
			require.NoError(t, err)
			resolved := 0
			for _, event := range events {
				if event.ResolvesEventID != nil {
					resolved++
				}
			}
			require.Equal(t, 1, resolved)
		})
	}
}

func TestProcessingSealedFanoutAndManualRetryPreserveEarlierSuccess(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1,
		KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "index", UnitKey: "all", Kind: "barrier", Phase: types.ProcessingPhasePrepare,
		InputFingerprint: "input", RequiredForReady: true, RequiredForCompletion: true}}))
	claimNext := func() *types.ProcessingLease {
		t.Helper()
		ops, err := r.PendingDeliveries(ctx, 100)
		require.NoError(t, err)
		require.NotEmpty(t, ops)
		var ref types.ProcessingRef
		require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
		lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
		require.NoError(t, err)
		return lease
	}
	parent := claimNext()
	wake := time.Now().Add(time.Minute)
	require.NoError(t, r.FinishStep(ctx, 1, *parent, types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &wake, SealPlan: true,
		ChildSteps: []types.ProcessingStepSpec{
			{Stage: "batch", UnitKey: "a", Phase: types.ProcessingPhasePrepare, InputFingerprint: "a"},
			{Stage: "batch", UnitKey: "b", Phase: types.ProcessingPhasePrepare, InputFingerprint: "b"},
		}}))
	first := claimNext()
	require.NoError(t, r.FinishStep(ctx, 1, *first, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "first", OutputDigest: "first", Completeness: "complete"}))
	second := claimNext()
	require.NoError(t, r.FinishStep(ctx, 1, *second, types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "MODEL_UNAVAILABLE"}))
	fresh, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, "pending", fresh.Readiness)
	require.ErrorIs(t, r.RetryStep(ctx, 1, job.ID, second.Ref.StepID, fresh.Revision-1, "operation", "operator"), ErrProcessingConflict)
	require.NoError(t, r.RetryStep(ctx, 1, job.ID, second.Ref.StepID, fresh.Revision, "operation", "operator"))
	require.NoError(t, r.RetryStep(ctx, 1, job.ID, second.Ref.StepID, fresh.Revision, "operation", "operator"))
	retry := claimNext()
	require.Equal(t, second.Ref.StepID, retry.Ref.StepID)
	require.NoError(t, r.FinishStep(ctx, 1, *retry, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "second", OutputDigest: "second"}))
	require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("id = ?", parent.Ref.StepID).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
	require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
	barrier := claimNext()
	require.Equal(t, parent.Ref.StepID, barrier.Ref.StepID)
	require.NoError(t, r.FinishStep(ctx, 1, *barrier, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "index-manifest", OutputDigest: "both"}))
	fresh, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, "ready", fresh.Readiness)
	require.NotEqual(t, types.ProcessingSucceeded, fresh.Status) // Publication is still required.
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		if step.ID == first.Ref.StepID {
			require.Equal(t, 1, step.Attempt)
		}
	}
}
