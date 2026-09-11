package service

import (
	"context"
	"os"
	"testing"

	pgindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/postgres"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestProcessingPostgresKeywordOnlyIndexesUseNullVector(t *testing.T) {
	if os.Getenv("PROCESSING_TEST_POSTGRES") == "" {
		t.Skip("requires isolated PostgreSQL with pgvector")
	}
	db := processingServiceTestDatabase(t)
	repo := processingTestPostgresIndexes(t, db)
	engine := retriever.NewKVHybridRetrieveEngine(repo, types.PostgresRetrieverEngineType)
	ctx := context.Background()
	item := func(id string) *types.IndexInfo {
		return &types.IndexInfo{SourceID: id, ChunkID: id, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", Content: "SYNTHETIC-KEYWORD-7391", IsEnabled: true}
	}
	require.NoError(t, engine.Index(ctx, nil, item("single"), []types.RetrieverType{types.KeywordsRetrieverType}))
	require.NoError(t, engine.BatchIndex(ctx, nil, []*types.IndexInfo{item("batch-one"), item("batch-two")}, []types.RetrieverType{types.KeywordsRetrieverType}))
	vector := item("vector")
	vector.PreparedEmbedding = []float32{0.25, 0.5}
	require.NoError(t, engine.BatchIndex(ctx, nil, []*types.IndexInfo{vector}, []types.RetrieverType{types.VectorRetrieverType}))
	var keywordCount, vectorCount int64
	require.NoError(t, db.Table("embeddings").Where("embedding IS NULL AND dimension = 0").Count(&keywordCount).Error)
	require.NoError(t, db.Table("embeddings").Where("embedding IS NOT NULL AND dimension = 2").Count(&vectorCount).Error)
	require.EqualValues(t, 3, keywordCount)
	require.EqualValues(t, 1, vectorCount)
	reader := repo.(interfaces.ProcessingIndexReader)
	saved, err := reader.ReadProcessingIndexes(ctx, "kb", "knowledge", 2, []string{"vector"})
	require.NoError(t, err)
	require.Len(t, saved, 1)
	require.Equal(t, vector.Content, saved[0].Content)
	require.Equal(t, vector.PreparedEmbedding, saved[0].PreparedEmbedding)
	saved, err = reader.ReadProcessingIndexes(ctx, "other-kb", "knowledge", 2, []string{"vector"})
	require.NoError(t, err)
	require.Empty(t, saved)
}

func processingTestPostgresIndexes(t *testing.T, db *gorm.DB) interfaces.RetrieveEngineRepository {
	t.Helper()
	require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public").Error)
	require.NoError(t, db.Exec(`CREATE TABLE embeddings (
		id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
		source_id VARCHAR(64) NOT NULL, source_type INTEGER NOT NULL, chunk_id VARCHAR(64),
		knowledge_id VARCHAR(64), knowledge_base_id VARCHAR(64), tag_id VARCHAR(64), content TEXT,
		dimension INTEGER NOT NULL, embedding public.halfvec, is_enabled BOOLEAN DEFAULT TRUE,
		UNIQUE(source_id, source_type))`).Error)
	return pgindex.NewPostgresRetrieveEngineRepository(db)
}
