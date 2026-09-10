package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type faqPublishChunks struct {
	interfaces.ChunkRepository
	row       *types.Chunk
	saves     int
	writes    []types.FAQIndexWrite
	confirmed bool
}

func (r *faqPublishChunks) RegisterFAQIndexWrites(_ context.Context, writes []types.FAQIndexWrite) error {
	r.writes = writes
	r.confirmed = false
	return nil
}
func (r *faqPublishChunks) ConfirmFAQIndexWrites(_ context.Context, _ uint64, ids []string) error {
	r.confirmed = true
	return nil
}

func (r *faqPublishChunks) ListChunksByID(context.Context, uint64, []string) ([]*types.Chunk, error) {
	return []*types.Chunk{r.row}, nil
}
func (r *faqPublishChunks) SaveChunks(_ context.Context, rows []*types.Chunk) error {
	r.saves++
	r.row = rows[0]
	return nil
}

type faqPublishEngine struct {
	interfaces.RetrieveEngineService
	fail   bool
	before func()
}

func (*faqPublishEngine) EstimateStorageSize(context.Context, embedding.Embedder, []*types.IndexInfo, []types.RetrieverType) int64 {
	return 64
}

func (*faqPublishEngine) Support() []types.RetrieverType {
	return []types.RetrieverType{types.VectorRetrieverType, types.KeywordsRetrieverType}
}
func (*faqPublishEngine) EngineType() types.RetrieverEngineType {
	return types.PostgresRetrieverEngineType
}
func (e *faqPublishEngine) BatchIndex(context.Context, embedding.Embedder, []*types.IndexInfo, []types.RetrieverType) error {
	if e.before != nil {
		e.before()
	}
	if e.fail {
		return errors.New("index unavailable")
	}
	return nil
}

func TestTencentFAQIndexFailureKeepsPublishedAnswer(t *testing.T) {
	old := &types.FAQChunkMetadata{StandardQuestion: "Q", Answers: []string{"old answer"}}
	row := &types.Chunk{ID: "faq", TenantID: 1, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeFAQ, IsEnabled: true, ContentHash: types.CalculateFAQContentHash(old)}
	require.NoError(t, row.SetFAQMetadata(old))
	r := &faqPublishChunks{row: row}
	engine := &faqPublishEngine{fail: true}
	engine.before = func() { require.Len(t, r.writes, 1); require.False(t, r.confirmed) }
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(engine))
	s := &knowledgeService{chunkRepo: r, repo: &sourcePathRepo{}, retrieveEngine: registry}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 1, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}, {RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.KeywordsRetrieverType}}}})
	newMeta := &types.FAQChunkMetadata{StandardQuestion: "Q", Answers: []string{"new answer"}}
	ops := []faqMergeOperation{{ExistingChunk: row, MergedMeta: newMeta}}
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 1, Type: types.KnowledgeBaseTypeFAQ, IndexingStrategy: types.IndexingStrategy{VectorEnabled: true, KeywordEnabled: true}}
	_, err := s.executeFAQMergeOperations(ctx, "test", kb, &types.Knowledge{ID: "knowledge"}, &processingPipelineEmbedder{}, types.FAQIndexModeQuestionAnswer, ops, &types.FAQImportProgress{})
	require.Error(t, err)
	require.Zero(t, r.saves)
	meta, err := r.row.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{"old answer"}, meta.Answers)
	engine.fail = false
	_, err = s.executeFAQMergeOperations(ctx, "retry", kb, &types.Knowledge{ID: "knowledge"}, &processingPipelineEmbedder{}, types.FAQIndexModeQuestionAnswer, ops, &types.FAQImportProgress{})
	require.NoError(t, err)
	require.Equal(t, 1, r.saves)
	require.True(t, r.confirmed)
	meta, err = r.row.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{"new answer"}, meta.Answers)
}
