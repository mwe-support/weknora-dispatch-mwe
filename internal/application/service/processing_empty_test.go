package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingVerifiedEmptySkipsOnlyFirstSourceVersion(t *testing.T) {
	require.NoError(t, validateProcessingDOCXText(map[string]int{}, 0, &types.ReadResult{}), "a verified empty DOCX must reach the shared empty-source policy")
	for _, previous := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "published"}[previous], func(t *testing.T) {
			t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
			db := processingServiceTestDatabase(t)
			require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.SyncRunItem{}, &types.Knowledge{}))
			r := repository.NewProcessingRepository(db)
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			if previous {
				old, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "empty", SourceRevision: "v1", PipelineFingerprint: "p1"})
				require.NoError(t, err)
				require.NoError(t, db.Model(old).Updates(map[string]any{"is_published": true, "status": types.ProcessingSucceeded}).Error)
			}
			run := types.SyncLog{ID: "empty-run", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusRunning}
			require.NoError(t, db.Create(&run).Error)
			scan, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "scan", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "run:" + run.ID, OriginRunID: run.ID, SourceRevision: run.ID, PipelineFingerprint: "p1"})
			require.NoError(t, err)
			require.NoError(t, db.Model(scan).Update("status", types.ProcessingSucceeded).Error)
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "empty", SourceRevision: "v2", PipelineFingerprint: "p1", OriginRunID: run.ID})
			require.NoError(t, err)
			require.NoError(t, db.Create(&types.SyncRunItem{RunID: run.ID, TenantID: 1, ItemKey: "empty", Kind: "document", ExternalID: "empty", JobID: job.ID}).Error)
			plan := []types.ProcessingStepSpec{
				{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parsed", RequiredForReady: true, RequiredForCompletion: true},
				{Stage: "assets", UnitKey: "body", Kind: "barrier", Phase: types.ProcessingPhasePrepare, InputFingerprint: "assets", DependsOn: []string{"parse/body"}, RequiredForReady: true, RequiredForCompletion: true},
				{Stage: "publish", UnitKey: "body", Phase: types.ProcessingPhasePublish, InputFingerprint: "publish", DependsOn: []string{"assets/body"}, RequiredForCompletion: true}}
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
			claim := func(stage string) *types.ProcessingLease {
				steps, err := r.ListSteps(ctx, 1, job.ID)
				require.NoError(t, err)
				for _, step := range steps {
					if step.Stage != stage {
						continue
					}
					lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
					require.NoError(t, err)
					return lease
				}
				t.Fatal("stage missing")
				return nil
			}
			artifacts := NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil)
			parse := claim("parse")
			data, _ := json.Marshal(processingParsed{ReadResult: types.ReadResult{MarkdownContent: " \n\t"}})
			path, digest, err := artifacts.Save(ctx, parse.Job, parse.Step, "parse", data)
			require.NoError(t, err)
			require.NoError(t, r.FinishStep(ctx, 1, *parse, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: path, OutputDigest: digest}))
			lease := claim("assets")
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			e := processingDocumentExecution{lease: *lease, kb: &types.KnowledgeBase{ID: "kb"}, artifacts: artifacts, steps: steps}
			out, err := e.assetBarrier(ctx)
			require.NoError(t, err)
			require.Equal(t, "verified_empty", out.Completeness)
			require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
			require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			actual, err := r.GetJob(ctx, 1, job.ID)
			require.NoError(t, err)
			require.False(t, actual.IsPublished)
			require.Equal(t, "verified_empty", actual.Completeness)
			snapshot, err := r.RunSnapshot(ctx, 1, run.ID)
			require.NoError(t, err)
			require.Equal(t, 1, snapshot.Documents)
			require.Zero(t, snapshot.Active)
			require.NoError(t, db.First(&run, "id = ?", run.ID).Error)
			if previous {
				require.Equal(t, types.ProcessingBlocked, actual.Status)
				require.Equal(t, 1, snapshot.Blocked)
				require.Equal(t, types.SyncLogStatusFailed, run.Status)
				var published int64
				require.NoError(t, db.Model(&types.ProcessingJob{}).Where("is_published = ?", true).Count(&published).Error)
				require.EqualValues(t, 1, published)
			} else {
				require.Equal(t, types.ProcessingSkipped, actual.Status)
				require.Equal(t, 1, snapshot.Skipped)
				require.Equal(t, types.SyncLogStatusSuccess, run.Status)
				pending, err := r.PendingDeliveries(ctx, 100)
				require.NoError(t, err)
				require.Empty(t, pending)
			}
		})
	}
}
