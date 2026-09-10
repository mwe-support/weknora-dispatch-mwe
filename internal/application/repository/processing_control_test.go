package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingSourceChangesCloseWorkAndPreservePublishedArtifacts(t *testing.T) {
	for _, action := range []string{"pause", "scope", "delete", "supersede"} {
		t.Run(action, func(t *testing.T) {
			r := processingTestStore(t)
			ctx := context.Background()
			input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "pipeline"}
			job, err := r.EnsureJob(ctx, input)
			require.NoError(t, err)
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
				{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parse", RequiredForReady: true, RequiredForCompletion: true},
				{Stage: "index", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "index", DependsOn: []string{"parse/body"}, RequiredForReady: true, RequiredForCompletion: true},
			}))
			ops, err := r.PendingDeliveries(ctx, 10)
			require.NoError(t, err)
			require.Len(t, ops, 1)
			var ref types.ProcessingRef
			require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
			lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
			require.NoError(t, err)
			// This fixture represents a separate already published version. No
			// lifecycle control operation may remove its accepted artifact pointer.
			published := types.ProcessingJob{ID: "published", Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "previous-file", Generation: 1, Status: types.ProcessingSucceeded, IsPublished: true, ActiveIndexManifest: `{"text":"accepted"}`}
			require.NoError(t, r.db.Create(&published).Error)
			sources := NewDataSourceRepository(r.db)
			source, err := sources.FindByID(ctx, "source")
			require.NoError(t, err)
			switch action {
			case "pause":
				source.Status = types.DataSourceStatusPaused
				require.NoError(t, sources.Update(ctx, source))
				// Resuming cannot revive the old delivery or lease.
				source.Status = types.DataSourceStatusActive
				require.NoError(t, sources.Update(ctx, source))
			case "scope":
				source.Config = types.JSON(`{"type":"tencent_docs","resource_ids":["changed"]}`)
				require.NoError(t, sources.Update(ctx, source))
			case "delete":
				require.NoError(t, sources.Delete(ctx, source.ID))
			case "supersede":
				input.SourceRevision = "v2"
				_, err = r.EnsureJob(ctx, input)
				require.NoError(t, err)
			}
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			for _, step := range steps {
				require.Contains(t, []string{types.ProcessingCanceled, types.ProcessingSuperseded}, step.Status)
				require.Empty(t, step.LeaseToken)
				require.Nil(t, step.LeaseExpiresAt)
			}
			ops, err = r.PendingDeliveries(ctx, 10)
			require.NoError(t, err)
			require.Empty(t, ops)
			require.Error(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "late", OutputDigest: "late"}))
			retained, err := r.GetJob(ctx, 1, published.ID)
			require.NoError(t, err)
			require.True(t, retained.IsPublished)
			require.JSONEq(t, string(published.ActiveIndexManifest), string(retained.ActiveIndexManifest))
			if action != "delete" {
				next, err := r.EnsureJob(ctx, input)
				require.NoError(t, err)
				require.EqualValues(t, 2, next.Generation)
				require.NotEqual(t, job.ID, next.ID)
			}
		})
	}
}

func TestProcessingConfigurationReconcileClosesFencedSteps(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "pipeline"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parse", RequiredForReady: true, RequiredForCompletion: true}}))
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("chunking_config", types.ChunkingConfig{ChunkSize: 321}).Error)
	require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingSuperseded, steps[0].Status)
	jobs, err := r.RecoveryJobs(ctx, "", 100)
	require.NoError(t, err)
	require.Empty(t, jobs)
}
