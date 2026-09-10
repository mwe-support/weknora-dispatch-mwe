package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func TestProcessingKnowledgeDeleteFencesBeforeRetirement(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	lease := processingControlCandidate(t, db, "delete-me", true)
	other := processingControlCandidate(t, db, "keep-me", true)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "operator")
	kr := repository.NewKnowledgeRepository(db)
	require.NoError(t, kr.DeleteKnowledge(ctx, 1, lease.Job.KnowledgeID))
	job, err := r.GetJob(ctx, 1, lease.Job.ID)
	require.NoError(t, err)
	require.False(t, job.IsCurrent)
	require.False(t, job.IsPublished)
	require.Equal(t, "retained", job.RetirementState)
	require.Error(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""))
	require.Error(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "late", OutputDigest: "late"}))
	require.NoError(t, r.Heartbeat(ctx, 1, *other, time.Minute, ""))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	found := false
	for _, step := range steps {
		if step.Stage == "retire" {
			found = true
			require.Equal(t, types.ProcessingEnqueuePending, step.Status)
		}
	}
	require.True(t, found, "deletion must leave durable cleanup")
	require.NoError(t, kr.DeleteKnowledge(ctx, 1, lease.Job.KnowledgeID))
	again, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, job.Revision, again.Revision, "duplicate delete must reuse its receipt")
	// Missing legacy services deliberately prove this path never does broad I/O.
	s := &knowledgeService{repo: kr}
	_, err = s.ReparseKnowledge(ctx, other.Job.KnowledgeID, nil)
	require.ErrorContains(t, err, "processing history")
	k, err := kr.GetKnowledgeByID(ctx, 1, other.Job.KnowledgeID)
	require.NoError(t, err)
	require.Error(t, s.cloneKnowledge(ctx, k, &types.KnowledgeBase{ID: "target"}))
	require.Error(t, s.CloneChunk(ctx, k, &types.Knowledge{}))
	require.Error(t, s.moveOneKnowledge(ctx, k.ID, &types.KnowledgeBase{ID: "kb"}, &types.KnowledgeBase{ID: "target"}, "reuse_vectors"))
	require.NoError(t, r.Heartbeat(ctx, 1, *other, time.Minute, ""), "rejected legacy mutations must not revoke or modify the job")
	require.NoError(t, s.DeleteKnowledge(ctx, other.Job.KnowledgeID))
}

func TestProcessingKnowledgeDeleteKeepsUncertainExternalEvidence(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	lease := processingControlCandidate(t, db, "uncertain", false)
	require.NoError(t, db.Create(&types.ProcessingStep{ID: "external-unknown", JobID: lease.Job.ID, Stage: "export_start", UnitKey: "external", Phase: types.ProcessingPhasePrepare, Status: types.ProcessingBlocked, ErrorClass: "uncertain", ErrorCode: "EXPORT_START_UNCERTAIN"}).Error)
	ctx := context.Background()
	require.NoError(t, repository.NewKnowledgeRepository(db).DeleteKnowledge(ctx, 1, lease.Job.KnowledgeID))
	steps, err := r.ListSteps(ctx, 1, lease.Job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		if step.Stage != "retire" {
			continue
		}
		cleanup, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: lease.Job.ID, Generation: lease.Job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		outcome, err := (&knowledgeService{}).retireProcessingVersion(ctx, r, *cleanup, &types.Tenant{ID: 1})
		require.NoError(t, err)
		require.Equal(t, "RETIREMENT_EXTERNAL_OUTCOME_UNCERTAIN", outcome.ErrorCode)
		require.NoError(t, r.FinishStep(ctx, 1, *cleanup, outcome))
	}
	job, err := r.GetJob(ctx, 1, lease.Job.ID)
	require.NoError(t, err)
	require.Equal(t, "deleting", job.RetirementState)
	unknown, err := r.GetStep(ctx, 1, job.ID, "external-unknown")
	require.NoError(t, err)
	require.Equal(t, "EXPORT_START_UNCERTAIN", unknown.ErrorCode)
}

func TestProcessingKnowledgeDeletePreservesPinsAndKBRetiresWithoutLegacyIO(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	lease := processingControlCandidate(t, db, "pinned", true)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	job, err := r.GetJob(ctx, 1, lease.Job.ID)
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, true, "hold", "operator"))
	kr, kbr := repository.NewKnowledgeRepository(db), repository.NewKnowledgeBaseRepository(db)
	s := &knowledgeService{repo: kr}
	require.ErrorIs(t, s.CheckKnowledgeDeletion(ctx, 1, []string{job.KnowledgeID}), repository.ErrProcessingConflict)
	require.NoError(t, s.CheckKnowledgeDeletion(ctx, 2, []string{job.KnowledgeID}))
	require.ErrorIs(t, kr.DeleteKnowledge(ctx, 1, job.KnowledgeID), repository.ErrProcessingConflict)
	require.ErrorIs(t, kbr.DeleteKnowledgeBase(ctx, "kb"), repository.ErrProcessingConflict)
	require.NoError(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""))
	_, err = kr.GetKnowledgeByID(ctx, 1, job.KnowledgeID)
	require.NoError(t, err)
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, false, "release", "operator"))
	require.NoError(t, s.CheckKnowledgeDeletion(ctx, 1, []string{job.KnowledgeID}))
	require.NoError(t, kbr.DeleteKnowledgeBase(ctx, "kb"))
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 1, KnowledgeBaseID: "kb"})
	require.NoError(t, err)
	// The legacy heavy cleanup sees no managed objects; its services are absent.
	kbs := &knowledgeBaseService{kgRepo: kr}
	require.NoError(t, kbs.ProcessKBDelete(ctx, asynq.NewTask(types.TypeKBDelete, payload)))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		if step.Stage != "retire" {
			continue
		}
		cleanup, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		outcome, err := (&knowledgeService{}).retireProcessingVersion(ctx, r, *cleanup, &types.Tenant{ID: 1})
		require.NoError(t, err)
		require.Equal(t, types.ProcessingSucceeded, outcome.Status)
		require.NoError(t, r.FinishStep(ctx, 1, *cleanup, outcome))
	}
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, "deleted", job.RetirementState)
}
