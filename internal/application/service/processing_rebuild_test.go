package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingExplicitRebuildKeepsPublishedVersionAndIdempotentGeneration(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.SyncRunItem{}, &types.SyncLog{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	lease := processingControlCandidate(t, db, "file", true)
	metadata, _ := json.Marshal(ProcessingDocumentSpec{FileID: "file", Kind: "smartcanvas", Title: "synthetic"})
	require.NoError(t, db.Model(&types.ProcessingJob{}).Where("id = ?", lease.Job.ID).Update("metadata", types.JSON(metadata)).Error)
	job, err := r.GetJob(ctx, 1, lease.Job.ID)
	require.NoError(t, err)
	s := &knowledgeService{config: &config.Config{}, kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}}
	control := types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: "rebuild-one", Actor: "operator", Reason: "verify the new pipeline"}
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("ALTER TABLE processing_events ADD CONSTRAINT injected_rebuild_failure CHECK (event_type <> 'version_rebuild_requested')").Error)
	} else {
		require.NoError(t, db.Exec("CREATE TRIGGER injected_rebuild_failure BEFORE INSERT ON processing_events WHEN NEW.event_type = 'version_rebuild_requested' BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END").Error)
	}
	_, err = RebuildProcessingVersion(ctx, s, r, 1, job.ID, control)
	require.Error(t, err)
	unchanged, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.True(t, unchanged.IsCurrent)
	require.True(t, unchanged.IsPublished)
	require.Equal(t, job.Revision, unchanged.Revision)
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("ALTER TABLE processing_events DROP CONSTRAINT injected_rebuild_failure").Error)
	} else {
		require.NoError(t, db.Exec("DROP TRIGGER injected_rebuild_failure").Error)
	}
	var wg sync.WaitGroup
	var results [2]*types.ProcessingJob
	var errs [2]error
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = RebuildProcessingVersion(ctx, s, r, 1, job.ID, control)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, results[0].ID, results[1].ID)
	require.EqualValues(t, 2, results[0].Generation)
	require.True(t, results[0].IsCurrent)
	require.False(t, results[0].IsPublished)
	old, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.True(t, old.IsPublished)
	require.False(t, old.IsCurrent)
	_, err = repository.NewKnowledgeRepository(db).GetKnowledgeByID(ctx, 1, old.KnowledgeID)
	require.NoError(t, err)
	steps, err := r.ListSteps(ctx, 1, results[0].ID)
	require.NoError(t, err)
	for _, step := range steps {
		require.NotEqual(t, types.ProcessingPhaseScan, step.Phase)
		require.Equal(t, 1, step.Attempt)
	}
	cancel := types.ProcessingControlRequest{ExpectedRevision: results[0].Revision, OperationRequestID: "cancel-one", Actor: "operator", Reason: "stop the candidate only"}
	require.NoError(t, r.CancelJob(ctx, 1, results[0].ID, cancel))
	require.NoError(t, r.CancelJob(ctx, 1, results[0].ID, cancel))
	old, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.True(t, old.IsPublished)
	// Receipt survives subsequent configuration changes and requires its actor.
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("embedding_model_id", "missing-new-model").Error)
	duplicate, err := RebuildProcessingVersion(ctx, nil, r, 1, job.ID, control)
	require.NoError(t, err)
	require.Equal(t, results[0].ID, duplicate.ID)
	control.Actor = "another-operator"
	_, err = RebuildProcessingVersion(ctx, nil, r, 1, job.ID, control)
	require.ErrorIs(t, err, repository.ErrProcessingConflict)
}

func TestProcessingExportOperatorResolutionReusesVerifiedTaskAndPreservesCancellation(t *testing.T) {
	for _, tc := range []struct{ canceled, notStarted bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("canceled-%t-not_started-%t", tc.canceled, tc.notStarted), func(t *testing.T) {
			canceled := tc.canceled
			t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
			db := processingServiceTestDatabase(t)
			r := repository.NewProcessingRepository(db)
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			metadata, _ := json.Marshal(ProcessingDocumentSpec{FileID: "file", Kind: "doc"})
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "fixed", PipelineFingerprint: "fixed", Metadata: metadata})
			require.NoError(t, err)
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "export_start", UnitKey: "body", Phase: types.ProcessingPhasePrepare, RequiredForReady: true, RequiredForCompletion: true, InputFingerprint: "fixed"}}))
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			ref := types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: steps[0].ID, Attempt: steps[0].Attempt, DispatchSeq: steps[0].DispatchSeq, InputFingerprint: steps[0].InputFingerprint}
			lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
			require.NoError(t, err)
			client := &processingExportClient{startError: context.DeadlineExceeded}
			e := processingDocumentExecution{repo: r, lease: *lease, artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil), document: ProcessingDocumentSpec{FileID: "file", Kind: "doc"}}
			outcome, err := e.exportStage(ctx, client, func(context.Context) error { return nil })
			require.Error(t, err)
			require.NoError(t, r.FinishStep(ctx, 1, *lease, tencentdocs.ProcessingFailure("export_start", err)))
			job, _ = r.GetJob(ctx, 1, job.ID)
			if canceled {
				require.NoError(t, r.CancelJob(ctx, 1, job.ID, types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: "cancel-unknown", Actor: "operator", Reason: "stop import"}))
				require.NoError(t, repository.NewKnowledgeBaseRepository(db).DeleteKnowledgeBase(ctx, "kb"))
				cleanupSteps, err := r.ListSteps(ctx, 1, job.ID)
				require.NoError(t, err)
				for _, cleanupStep := range cleanupSteps {
					if cleanupStep.Stage != "retire" {
						continue
					}
					cleanup, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: cleanupStep.ID, Attempt: cleanupStep.Attempt, DispatchSeq: cleanupStep.DispatchSeq, InputFingerprint: cleanupStep.InputFingerprint}, time.Minute)
					require.NoError(t, err)
					blocked, err := (&knowledgeService{}).retireProcessingVersion(ctx, r, *cleanup, &types.Tenant{ID: 1})
					require.NoError(t, err)
					require.Equal(t, "RETIREMENT_EXTERNAL_OUTCOME_UNCERTAIN", blocked.ErrorCode)
					require.NoError(t, r.FinishStep(ctx, 1, *cleanup, blocked))
				}
				job, _ = r.GetJob(ctx, 1, job.ID)
			}
			request := types.ProcessingExportResolution{ProcessingControlRequest: types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: "external-proof", Actor: "operator", Reason: "provider log associates the task with this exact request"}, StepID: lease.Step.ID, FileID: "file", SourceRevision: "fixed", TaskID: "verified-provider-task", EvidenceReference: "provider-trace-7391", RequestDigest: fmt.Sprintf("%x", sha256.Sum256([]byte(job.ID+"/file/fixed")))}
			if tc.notStarted {
				request.TaskID, request.NotStarted = "", true
			}
			bad := request
			bad.FileID = "another-file"
			require.ErrorIs(t, r.ResolveExport(ctx, 1, job.ID, bad), repository.ErrProcessingConflict)
			bad = request
			bad.RequestDigest = "wrong-intent"
			require.ErrorIs(t, r.ResolveExport(ctx, 1, job.ID, bad), repository.ErrProcessingConflict)
			require.NoError(t, r.ResolveExport(ctx, 1, job.ID, request))
			require.NoError(t, r.ResolveExport(ctx, 1, job.ID, request))
			step, err := r.GetStep(ctx, 1, job.ID, lease.Step.ID)
			require.NoError(t, err)
			if canceled {
				require.Equal(t, types.ProcessingCanceled, step.Status)
				job, _ = r.GetJob(ctx, 1, job.ID)
				require.Equal(t, types.ProcessingCanceled, job.Status)
				cleanupSteps, err := r.ListSteps(ctx, 1, job.ID)
				require.NoError(t, err)
				for _, cleanupStep := range cleanupSteps {
					if cleanupStep.Stage == "retire" {
						require.Equal(t, types.ProcessingEnqueuePending, cleanupStep.Status, "verified outcome releases the cleanup dependency")
					}
				}
			} else {
				ref.Attempt, ref.DispatchSeq = step.Attempt, step.DispatchSeq
				lease, err = r.ClaimStep(ctx, 1, ref, time.Minute)
				require.NoError(t, err)
				e.lease = *lease
				outcome, err = e.exportStage(ctx, client, func(context.Context) error { return nil })
				require.NoError(t, err)
				require.Equal(t, types.ProcessingSucceeded, outcome.Status)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
			}
			expectedStarts := 1
			if tc.notStarted && !canceled {
				expectedStarts++
			}
			require.Equal(t, expectedStarts, client.starts, "only explicit not-started proof permits another StartExport")
			var events []types.ProcessingEvent
			require.NoError(t, db.Where("job_id = ? AND event_type = ?", job.ID, "export_manually_verified").Find(&events).Error)
			require.Len(t, events, 1)
			require.Equal(t, "operator", events[0].Actor)
			require.NotNil(t, events[0].ResolvesEventID)
		})
	}
}
