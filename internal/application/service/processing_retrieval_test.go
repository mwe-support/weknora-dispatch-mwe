package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProcessingRetrievalFiltersBothSingleAndMultipleStorePaths(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "1", PipelineFingerprint: "1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
		{Stage: "index", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "index", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "publish", UnitKey: "version", Phase: types.ProcessingPhasePublish, InputFingerprint: "publish", DependsOn: []string{"index/body"}, RequiredForCompletion: true},
	}))
	indexID := ""
	for range 2 {
		ops, err := r.PendingDeliveries(ctx, 10)
		require.NoError(t, err)
		require.Len(t, ops, 1)
		var ref types.ProcessingRef
		require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
		lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
		require.NoError(t, err)
		outcome := types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic", OutputDigest: "verified"}
		if lease.Step.Stage == "index" {
			indexID = lease.Step.ID
			outcome.Candidate = &types.Knowledge{Title: "synthetic"}
			outcome.Completeness = "complete"
		}
		require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
	}
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	accepted, err := types.ProcessingIndexSourceID(indexID, 1, uuid.NewString())
	require.NoError(t, err)
	orphan, err := types.ProcessingIndexSourceID(indexID, 2, uuid.NewString())
	require.NoError(t, err)
	engine := &fakeRetrieveEngineService{engineType: types.PostgresRetrieverEngineType, support: []types.RetrieverType{types.VectorRetrieverType}, canned: []*types.IndexWithScore{
		{KnowledgeID: job.KnowledgeID, KnowledgeBaseID: "kb", ChunkID: "accepted", SourceID: accepted},
		{KnowledgeID: job.KnowledgeID, KnowledgeBaseID: "kb", ChunkID: "orphan", SourceID: orphan},
	}}
	group := &storeGroup{OwnerTenantID: 1, KBIDs: []string{"kb"}, Engine: buildBoundComposite(t, engine), BaseParams: vectorParams("query"), TopK: 50}
	svc := &knowledgeBaseService{kgRepo: repository.NewKnowledgeRepository(db)}
	for _, groups := range [][]*storeGroup{{group}, {group, group}} {
		results, err := svc.retrieveFromStores(ctx, groups, retriever.EngineAwareNormalizer{})
		require.NoError(t, err)
		for _, result := range results {
			require.Len(t, result.Results, 1)
			require.Equal(t, "accepted", result.Results[0].ChunkID)
		}
	}
}
