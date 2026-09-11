package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingTenantParserConfigurationFencesCommit(t *testing.T) {
	checkProcessingTenantParser(t, processingTestStore(t))
}

func checkProcessingTenantParser(t *testing.T, r *ProcessingRepository) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "parser-fence", SourceRevision: "v1", PipelineFingerprint: "p1"}
	job, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parse", RequiredForReady: true, RequiredForCompletion: true}}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	step := steps[0]
	lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	tenants := NewTenantRepository(r.db)
	require.NoError(t, r.db.Model(&types.Tenant{}).Where("id = ?", 1).UpdateColumn("storage_used", 10).Error)
	owner, err := tenants.GetTenantByID(ctx, 1)
	require.NoError(t, err)
	owner.ParserEngineConfig = &types.ParserEngineConfig{MinerUEndpoint: "http://synthetic-parser:8000"}
	owner.MarkParserEngineConfigExplicit()
	if r.db.Dialector.Name() == "postgres" {
		tx := r.db.WithContext(ctx).Begin()
		require.NoError(t, tx.Error)
		defer tx.Rollback()
		_, err := lockProcessingConfiguration(tx, 1, "kb")
		require.NoError(t, err)
		finished := make(chan error, 1)
		go func() { finished <- tenants.UpdateTenant(ctx, owner) }()
		select {
		case err := <-finished:
			t.Fatalf("settings writer bypassed the configuration fence: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		// A queued settings writer owns no tenant row lock. Quota can still
		// finish without waiting for the reader or upgrading SHARE to UPDATE.
		require.NoError(t, tenants.AdjustStorageUsed(ctx, 1, 1))
		require.NoError(t, tx.Commit().Error)
		require.NoError(t, <-finished)
		current, err := tenants.GetTenantByID(ctx, 1)
		require.NoError(t, err)
		require.EqualValues(t, 11, current.StorageUsed, "settings must not overwrite an increment completed while waiting")
	} else {
		require.NoError(t, tenants.UpdateTenant(ctx, owner))
	}
	require.ErrorIs(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""), ErrProcessingScope)
	require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "stale-parser", OutputDigest: "stale"}), ErrProcessingScope)
	next, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.NotEqual(t, job.ConfigurationRevision, next.ConfigurationRevision)
	require.Equal(t, job.Generation+1, next.Generation)
	before := next.ConfigurationRevision
	require.NoError(t, tenants.AdjustStorageUsed(ctx, 1, 1))
	after, err := r.ConfigurationRevision(ctx, 1, "kb")
	require.NoError(t, err)
	require.Equal(t, before, after, "quota changes do not invalidate processing inputs")
}
