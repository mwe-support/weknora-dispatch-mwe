package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingRetirementSurvivesScopeChangesAndExcludesRollback(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"}
	job, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "body", RequiredForReady: true, RequiredForCompletion: true}}))
	input.SourceRevision = "v2"
	_, err = r.EnsureJob(ctx, input)
	require.NoError(t, err)
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "retire-1", "operator"))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	var cleanup types.ProcessingStep
	for _, step := range steps {
		if step.Phase == types.ProcessingPhaseRetire {
			cleanup = step
		}
	}
	require.NotEmpty(t, cleanup.ID)
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, true, "pin-1", "operator"))
	_, err = r.ClaimStep(ctx, 1, processingStepRef(job, &cleanup), time.Minute)
	require.ErrorIs(t, err, ErrProcessingConflict)
	job, _ = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, false, "unpin-1", "operator"))
	use := types.ProcessingArtifactReference{ID: "consumer-reference", TenantID: 1, ProducerJobID: job.ID, ProducerStepID: "retained-output", ConsumerJobID: "consumer", ConsumerStepID: "consumer-step", Attempt: 1, Digest: "verified"}
	require.NoError(t, r.db.Create(&use).Error)
	_, err = r.ClaimStep(ctx, 1, processingStepRef(job, &cleanup), time.Minute)
	require.ErrorIs(t, err, ErrProcessingConflict)
	require.NoError(t, r.db.Delete(&use).Error)
	// Pausing/deleting the source must stop imports but retain cleanup authority.
	require.NoError(t, NewDataSourceRepository(r.db).Delete(ctx, "source"))
	lease, err := r.ClaimStep(ctx, 1, processingStepRef(job, &cleanup), time.Minute)
	require.NoError(t, err)
	require.Equal(t, "deleting", lease.Job.RetirementState)
	job, _ = r.GetJob(ctx, 1, job.ID)
	require.ErrorIs(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, true, "pin-late", "operator"), ErrProcessingConflict)
	require.NoError(t, r.Heartbeat(ctx, 1, *lease, time.Minute, "confirmed-cleanup-page"))
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "transient", ErrorCode: "DELETE_ACK_LOST", Retryable: true}))
	cleanupPtr, err := r.GetStep(ctx, 1, job.ID, cleanup.ID)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingRetryWait, cleanupPtr.Status)
	require.NoError(t, r.db.Model(cleanupPtr).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
	require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
	cleanupPtr, _ = r.GetStep(ctx, 1, job.ID, cleanup.ID)
	lease, err = r.ClaimStep(ctx, 1, processingStepRef(job, cleanupPtr), time.Minute)
	require.NoError(t, err)
	require.Equal(t, "confirmed-cleanup-page", lease.Step.CheckpointRef)
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "ledger:retirement", OutputDigest: "verified"}))
	job, _ = r.GetJob(ctx, 1, job.ID)
	require.Equal(t, "deleted", job.RetirementState)
	require.Equal(t, types.ProcessingSuperseded, job.Status)
}
