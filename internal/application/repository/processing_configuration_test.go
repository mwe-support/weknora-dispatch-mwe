package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingConfigurationChangeFencesInFlightCommit(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb",
		DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "pipeline"}
	job, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body",
		Phase: types.ProcessingPhasePrepare, InputFingerprint: "input", RequiredForReady: true, RequiredForCompletion: true}}))
	ops, err := r.PendingDeliveries(ctx, 1)
	require.NoError(t, err)
	var ref types.ProcessingRef
	require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
	lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
	require.NoError(t, err)
	// Renaming is display-only, so it must not throw away valid processing.
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("name", "renamed").Error)
	reused, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.Equal(t, job.ID, reused.ID)
	// Changing the actual chunking input invalidates both heartbeat and commit.
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("chunking_config", types.ChunkingConfig{ChunkSize: 321}).Error)
	require.ErrorIs(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""), ErrProcessingScope)
	require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded,
		OutputManifestRef: "stale", OutputDigest: "stale"}), ErrProcessingScope)
	next, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Generation)
	require.NotEqual(t, job.ID, next.ID)
}

func TestProcessingConfigurationIncludesInheritedModelParameters(t *testing.T) {
	r := processingTestStore(t)
	require.NoError(t, r.db.AutoMigrate(&types.Model{}))
	model := &types.Model{ID: "synthetic-embedding", TenantID: 1, Name: "model", Type: types.ModelTypeEmbedding, Status: types.ModelStatusActive,
		Parameters: types.ModelParameters{EmbeddingParameters: types.EmbeddingParameters{Dimension: 3}}}
	require.NoError(t, r.db.Create(model).Error)
	t.Setenv(types.DefaultEmbeddingModelIDEnv, model.ID)
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "pipeline"}
	first, err := r.EnsureJob(context.Background(), input)
	require.NoError(t, err)
	require.NoError(t, r.db.Model(model).Update("display_name", "renamed").Error)
	same, err := r.EnsureJob(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, first.ID, same.ID)
	model.Parameters.EmbeddingParameters.Dimension = 4
	require.NoError(t, r.db.Model(model).Update("parameters", model.Parameters).Error)
	second, err := r.EnsureJob(context.Background(), input)
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Generation)
	require.NotEqual(t, first.ConfigurationRevision, second.ConfigurationRevision)
}
