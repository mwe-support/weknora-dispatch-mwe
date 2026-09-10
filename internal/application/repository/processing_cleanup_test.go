package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingCleanupWaitsForOldVersionAndRecordsRetention(t *testing.T) {
	for _, hold := range []string{"", "pin", "reference", "failure"} {
		t.Run(hold, func(t *testing.T) {
			r := processingTestStore(t)
			require.NoError(t, r.db.AutoMigrate(&types.Knowledge{}, &types.SyncLog{}))
			ctx := context.Background()
			success := types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/confirmed", OutputDigest: "verified"}
			claim := func(job *types.ProcessingJob, stage string) *types.ProcessingLease {
				t.Helper()
				steps, err := r.ListSteps(ctx, 1, job.ID)
				require.NoError(t, err)
				for _, step := range steps {
					if step.Stage == stage {
						if step.NextRunAt != nil {
							require.NoError(t, r.db.Model(&step).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
							require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
							stepPtr, err := r.GetStep(ctx, 1, job.ID, step.ID)
							require.NoError(t, err)
							step = *stepPtr
						}
						lease, err := r.ClaimStep(ctx, 1, processingStepRef(job, &step), time.Minute)
						require.NoError(t, err)
						return lease
					}
				}
				t.Fatal("missing stage", stage)
				return nil
			}
			makeJob := func(rev string) *types.ProcessingJob {
				t.Helper()
				job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: rev, PipelineFingerprint: "p1"})
				require.NoError(t, err)
				require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
					{Stage: "index", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "index", RequiredForReady: true, RequiredForCompletion: true},
					{Stage: "publish", UnitKey: "body", Phase: types.ProcessingPhasePublish, InputFingerprint: "publish", DependsOn: []string{"index/body"}, RequiredForCompletion: true},
					{Stage: "summary", UnitKey: "body", Phase: types.ProcessingPhaseProjection, InputFingerprint: "summary", DependsOn: []string{"publish/body"}, RequiredForCompletion: true},
					{Stage: "retire_previous", UnitKey: "body", Phase: types.ProcessingPhaseProjection, InputFingerprint: "cleanup", DependsOn: []string{"summary/body"}, RequiredForCompletion: true},
				}))
				out := success
				out.Completeness = "complete"
				out.Candidate = &types.Knowledge{Title: "synthetic"}
				require.NoError(t, r.FinishStep(ctx, 1, *claim(job, "index"), out))
				require.NoError(t, r.FinishStep(ctx, 1, *claim(job, "publish"), success))
				return job
			}
			first := makeJob("v1")
			require.NoError(t, r.FinishStep(ctx, 1, *claim(first, "summary"), success))
			require.NoError(t, r.FinishStep(ctx, 1, *claim(first, "retire_previous"), success))
			second := makeJob("v2")
			var oldCleanup int64
			require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id = ? AND phase = ?", first.ID, types.ProcessingPhaseRetire).Count(&oldCleanup).Error)
			require.Zero(t, oldCleanup, "enhancement must finish before cleanup starts")
			if hold == "pin" {
				first, _ = r.GetJob(ctx, 1, first.ID)
				require.NoError(t, r.SetRollbackPin(ctx, 1, first.ID, first.Revision, true, "hold-old", "operator"))
			}
			if hold == "reference" {
				require.NoError(t, r.db.Create(&types.ProcessingArtifactReference{ID: "hold", TenantID: 1, ProducerJobID: first.ID, ProducerStepID: "output", ConsumerJobID: second.ID, ConsumerStepID: "input", Attempt: 1, Digest: "verified"}).Error)
			}
			require.NoError(t, r.FinishStep(ctx, 1, *claim(second, "summary"), success))
			before, err := r.GetJob(ctx, 1, second.ID)
			require.NoError(t, err)
			require.NotEqual(t, types.ProcessingSucceeded, before.Status)
			require.Equal(t, "ready", before.Readiness)
			check := claim(second, "retire_previous")
			require.NoError(t, r.FinishStep(ctx, 1, *check, success))
			require.NoError(t, r.FinishStep(ctx, 1, *check, success))
			step, err := r.GetStep(ctx, 1, second.ID, check.Step.ID)
			require.NoError(t, err)
			if hold == "pin" || hold == "reference" {
				require.Equal(t, types.ProcessingSucceeded, step.Status)
				var receipt map[string]any
				require.NoError(t, json.Unmarshal(step.Result, &receipt))
				require.EqualValues(t, 1, receipt["retained"])
				first, _ = r.GetJob(ctx, 1, first.ID)
				require.Equal(t, "retained", first.RetirementState)
				return
			}
			require.Equal(t, types.ProcessingWaitingExternal, step.Status, "publication must not claim completion while old cleanup remains")
			oldLease := claim(first, "retire")
			if hold == "failure" {
				require.NoError(t, r.FinishStep(ctx, 1, *oldLease, types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "uncertain", ErrorCode: "RETIREMENT_STORAGE_OUTCOME_UNCERTAIN"}))
				require.NoError(t, r.FinishStep(ctx, 1, *claim(second, "retire_previous"), success))
				current, _ := r.GetJob(ctx, 1, second.ID)
				require.NotEqual(t, types.ProcessingSucceeded, current.Status)
				require.True(t, current.IsPublished)
				require.Equal(t, "ready", current.Readiness)
				step, _ = r.GetStep(ctx, 1, second.ID, check.Step.ID)
				require.Equal(t, "PREVIOUS_RETIREMENT_BLOCKED", step.ErrorCode)
				first, err = r.GetJob(ctx, 1, first.ID)
				require.NoError(t, err)
				require.NoError(t, r.RetryStep(ctx, 1, first.ID, oldLease.Step.ID, first.Revision, "repair-cleanup", "operator"))
				oldLease = claim(first, "retire")
			}
			require.NoError(t, r.FinishStep(ctx, 1, *oldLease, success))
			require.NoError(t, r.FinishStep(ctx, 1, *claim(second, "retire_previous"), success))
			current, err := r.GetJob(ctx, 1, second.ID)
			require.NoError(t, err)
			require.Equal(t, types.ProcessingSucceeded, current.Status)
		})
	}
}
