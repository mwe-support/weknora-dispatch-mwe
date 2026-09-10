package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func processingControlCandidate(t *testing.T, db *gorm.DB, file string, published bool, revisions ...string) *types.ProcessingLease {
	t.Helper()
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	revision := "v1"
	if len(revisions) > 0 {
		revision = revisions[0]
	}
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: file, SourceRevision: revision, PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
		{Stage: "normalize", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "normalize", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "publish", UnitKey: "body", Phase: types.ProcessingPhasePublish, InputFingerprint: "publish", DependsOn: []string{"normalize/body"}, RequiredForCompletion: true},
		{Stage: "summary", UnitKey: "body", Phase: types.ProcessingPhaseProjection, InputFingerprint: "summary", DependsOn: []string{"publish/body"}, RequiredForCompletion: true},
	}))
	claim := func(stage string) *types.ProcessingLease {
		t.Helper()
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		for _, step := range steps {
			if step.Stage != stage {
				continue
			}
			lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
			require.NoError(t, err)
			return lease
		}
		t.Fatal("missing control stage")
		return nil
	}
	lease := claim("normalize")
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/body", OutputDigest: "verified", Completeness: "complete", Candidate: &types.Knowledge{Title: "synthetic"}}))
	lease = claim("publish")
	if published {
		require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/publication", OutputDigest: "verified"}))
		lease = claim("summary")
	}
	return lease
}

func TestProcessingKnowledgeCancelFencesPublishedAndCandidateWork(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	s := &knowledgeService{repo: repository.NewKnowledgeRepository(db)}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "operator")
	for _, published := range []bool{false, true} {
		file := "candidate"
		if published {
			file = "published"
		}
		lease := processingControlCandidate(t, db, file, published)
		k, err := s.CancelKnowledgeParse(ctx, lease.Job.KnowledgeID)
		require.NoError(t, err)
		job, err := r.GetJob(ctx, 1, lease.Job.ID)
		require.NoError(t, err)
		require.Equal(t, types.ProcessingCanceled, job.Status)
		require.Equal(t, published, job.IsPublished)
		if published {
			require.Equal(t, types.ParseStatusCompleted, k.ParseStatus)
			require.Equal(t, "enabled", k.EnableStatus)
			require.NotEmpty(t, job.ActiveIndexManifest)
		} else {
			require.Equal(t, types.ParseStatusCancelled, k.ParseStatus)
		}
		require.Error(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""))
		require.Error(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "late", OutputDigest: "late"}))
		_, err = s.CancelKnowledgeParse(ctx, lease.Job.KnowledgeID)
		require.NoError(t, err)
		again, err := r.GetJob(ctx, 1, lease.Job.ID)
		require.NoError(t, err)
		require.Equal(t, job.Revision, again.Revision)
		events, err := r.ListEvents(ctx, 1, job.ID, 0, 100)
		require.NoError(t, err)
		encoded, err := json.Marshal(events)
		require.NoError(t, err)
		require.Contains(t, string(encoded), "operator")
	}
}

func TestProcessingKnowledgeBaseDeleteInvalidatesJobsAtomically(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	first := processingControlCandidate(t, db, "first", true)
	second := processingControlCandidate(t, db, "second", false)
	ctx := context.Background()
	kbs := repository.NewKnowledgeBaseRepository(db)
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("ALTER TABLE processing_events ADD CONSTRAINT injected_kb_cancel_failure CHECK (event_type <> 'job_invalidated')").Error)
	} else {
		require.NoError(t, db.Exec("CREATE TRIGGER injected_kb_cancel_failure BEFORE INSERT ON processing_events WHEN NEW.event_type = 'job_invalidated' BEGIN SELECT RAISE(ABORT, 'test cancellation failure'); END").Error)
	}
	require.Error(t, kbs.DeleteKnowledgeBase(ctx, "kb"))
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id = ?", "kb").Take(&kb).Error)
	require.NoError(t, r.Heartbeat(ctx, 1, *first, time.Minute, ""))
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("ALTER TABLE processing_events DROP CONSTRAINT injected_kb_cancel_failure").Error)
	} else {
		require.NoError(t, db.Exec("DROP TRIGGER injected_kb_cancel_failure").Error)
	}
	require.NoError(t, kbs.DeleteKnowledgeBase(ctx, "kb"))
	for _, lease := range []*types.ProcessingLease{first, second} {
		job, err := r.GetJob(ctx, 1, lease.Job.ID)
		require.NoError(t, err)
		require.Equal(t, types.ProcessingCanceled, job.Status)
		require.False(t, job.IsPublished)
		require.False(t, job.IsCurrent)
		step, err := r.GetStep(ctx, 1, job.ID, lease.Step.ID)
		require.NoError(t, err)
		require.Equal(t, types.ProcessingCanceled, step.Status)
		require.Empty(t, step.LeaseToken)
		require.Error(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "late", OutputDigest: "late"}))
	}
	require.NoError(t, kbs.DeleteKnowledgeBase(ctx, "kb"))
}
