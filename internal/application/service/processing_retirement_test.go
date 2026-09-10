package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingRetirementPinConcurrentClaimsAndRollbackAtomicity(t *testing.T) {
	db := processingServiceTestDatabase(t)
	if db.Dialector.Name() != "postgres" {
		t.Skip("requires isolated PostgreSQL concurrency")
	}
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(8)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncRunItem{}, &types.SyncLog{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	firstLease := processingControlCandidate(t, db, "file", true)
	secondLease := processingControlCandidate(t, db, "file", true, "v2")
	first, err := r.GetJob(ctx, 1, firstLease.Job.ID)
	require.NoError(t, err)
	require.NoError(t, r.PlanRetirement(ctx, 1, first.ID, first.Revision, "retire-1", "operator"))
	first, _ = r.GetJob(ctx, 1, first.ID)
	steps, err := r.ListSteps(ctx, 1, first.ID)
	require.NoError(t, err)
	var cleanup types.ProcessingStep
	for _, step := range steps {
		if step.Phase == types.ProcessingPhaseRetire {
			cleanup = step
		}
	}
	require.NotEmpty(t, cleanup.ID)
	var wg sync.WaitGroup
	start := make(chan struct{})
	errorsCh := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				errorsCh <- r.SetRollbackPin(ctx, 1, first.ID, first.Revision, true, fmt.Sprint("pin-", i), "operator")
				return
			}
			_, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: first.ID, Generation: first.Generation, StepID: cleanup.ID, Attempt: cleanup.Attempt, DispatchSeq: cleanup.DispatchSeq, InputFingerprint: cleanup.InputFingerprint}, time.Minute)
			errorsCh <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	winners := 0
	for err := range errorsCh {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, repository.ErrProcessingConflict)
		}
	}
	require.Equal(t, 1, winners)
	first, _ = r.GetJob(ctx, 1, first.ID)
	require.False(t, first.RollbackPin && first.RetirementState == "deleting")
	// Use a separate logical source for a deterministic rollback transaction.
	a := processingControlCandidate(t, db, "rollback-file", true)
	b := processingControlCandidate(t, db, "rollback-file", true, "v2")
	target, _ := r.GetJob(ctx, 1, a.Job.ID)
	require.NoError(t, r.SetRollbackPin(ctx, 1, target.ID, target.Revision, true, "pin-atomic", "operator"))
	target, _ = r.GetJob(ctx, 1, target.ID)
	require.NoError(t, db.Exec("ALTER TABLE processing_events ADD CONSTRAINT injected_rollback_failure CHECK (event_type <> 'version_rolled_back')").Error)
	require.Error(t, r.RollbackVersion(ctx, 1, target.ID, target.Revision, target.ActiveIndexManifest, "rollback-atomic", "operator"))
	still, _ := r.GetJob(ctx, 1, b.Job.ID)
	require.True(t, still.IsPublished)
	require.EqualValues(t, 2, still.PublicationEpoch)
	unchanged, _ := r.GetJob(ctx, 1, target.ID)
	require.Equal(t, target.Revision, unchanged.Revision)
	require.NoError(t, r.Heartbeat(ctx, 1, *b, time.Minute, ""))
	require.NoError(t, db.Exec("ALTER TABLE processing_events DROP CONSTRAINT injected_rollback_failure").Error)
	require.NoError(t, r.RollbackVersion(ctx, 1, target.ID, target.Revision, target.ActiveIndexManifest, "rollback-atomic", "operator"))
	require.ErrorIs(t, r.Heartbeat(ctx, 1, *b, time.Minute, ""), repository.ErrProcessingConflict)
	require.NoError(t, r.Heartbeat(ctx, 1, *secondLease, time.Minute, "")) // Other logical source unaffected.
}

func TestProcessingArtifactReferencesHoldProducerUntilConsumerRetires(t *testing.T) {
	db := processingServiceTestDatabase(t)
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	makeJob := func(revision string) (*types.ProcessingJob, types.ProcessingStep) {
		t.Helper()
		job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: revision, PipelineFingerprint: "p1"})
		require.NoError(t, err)
		require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "same-immutable-input", RequiredForReady: true, RequiredForCompletion: true}}))
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		return job, steps[0]
	}
	claim := func(job *types.ProcessingJob, step types.ProcessingStep) (*types.ProcessingLease, error) {
		return r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	}
	producer, output := makeJob("v1")
	lease, err := claim(producer, output)
	require.NoError(t, err)
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/confirmed-body", OutputDigest: "verified", Completeness: "complete"}))
	consumer, input := makeJob("v2")
	require.Error(t, r.ReferenceArtifact(ctx, 2, producer.ID, output.ID, consumer.ID, input.ID))
	require.Error(t, r.ReferenceArtifact(ctx, 1, producer.ID, output.ID, consumer.ID, "wrong-step"))
	require.NoError(t, r.ReferenceArtifact(ctx, 1, producer.ID, output.ID, consumer.ID, input.ID))
	require.NoError(t, r.ReferenceArtifact(ctx, 1, producer.ID, output.ID, consumer.ID, input.ID))
	var count int64
	require.NoError(t, db.Model(&types.ProcessingArtifactReference{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	planRetire := func(job *types.ProcessingJob) types.ProcessingStep {
		t.Helper()
		current, err := r.GetJob(ctx, 1, job.ID)
		require.NoError(t, err)
		require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, current.Revision, "retire-"+job.ID, "operator"))
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		for _, step := range steps {
			if step.Phase == types.ProcessingPhaseRetire {
				return step
			}
		}
		t.Fatal("cleanup missing")
		return types.ProcessingStep{}
	}
	producerCleanup := planRetire(producer)
	_, err = claim(producer, producerCleanup)
	require.ErrorIs(t, err, repository.ErrProcessingConflict)
	makeJob("v3")
	consumerCleanup := planRetire(consumer)
	lease, err = claim(consumer, consumerCleanup)
	require.NoError(t, err)
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "ledger:retirement", OutputDigest: "verified"}))
	require.NoError(t, db.Model(&types.ProcessingArtifactReference{}).Count(&count).Error)
	require.Zero(t, count)
	lease, err = claim(producer, producerCleanup)
	require.NoError(t, err)
	require.Equal(t, "deleting", lease.Job.RetirementState)
	require.Error(t, r.ReferenceArtifact(ctx, 1, producer.ID, output.ID, consumer.ID, input.ID))
}
