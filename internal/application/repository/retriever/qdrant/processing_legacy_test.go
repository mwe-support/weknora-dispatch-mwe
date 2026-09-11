package qdrant

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestProcessingLegacyQdrantReadsExactStoredVectors(t *testing.T) {
	host := os.Getenv("PROCESSING_TEST_QDRANT")
	if host == "" {
		t.Skip("requires isolated Qdrant")
	}
	require.Equal(t, "lifecycle-qdrant", host)
	client, err := qdrant.NewClient(&qdrant.Config{Host: host, Port: 6334})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	r := NewQdrantRetrieveEngineRepository(client, nil).(*qdrantRepository)
	r.collectionBaseName = "processing_legacy_test_" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, r.ensureCollection(ctx, 2))
	t.Cleanup(func() { require.NoError(t, client.DeleteCollection(context.Background(), r.getCollectionName(2))) })
	item := &types.IndexInfo{SourceID: "legacy-source", ChunkID: "legacy-chunk", KnowledgeBaseID: "kb", KnowledgeID: "legacy", Content: "synthetic legacy vector", IsEnabled: true}
	params := map[string]any{"embedding": map[string][]float32{item.SourceID: {0, 1}}}
	require.NoError(t, r.Save(ctx, item, params))
	rows, err := r.ReadProcessingIndexes(ctx, "kb", "legacy", 2, []string{item.SourceID})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, item.Content, rows[0].Content)
	require.Equal(t, []float32{0, 1}, rows[0].PreparedEmbedding)
	rows, err = r.ReadProcessingIndexes(ctx, "other-kb", "legacy", 2, []string{item.SourceID})
	require.NoError(t, err)
	require.Empty(t, rows)
	p2, err := types.ProcessingIndexSourceID(uuid.NewString(), 1, uuid.NewString())
	require.NoError(t, err)
	for _, id := range []string{p2, "fq-" + strings.Repeat("a", 60)} {
		owned := *item
		owned.SourceID = id
		params := map[string]any{"embedding": map[string][]float32{id: {0, 1}}}
		require.NoError(t, r.Save(ctx, &owned, params))
		require.NoError(t, r.BatchSave(ctx, []*types.IndexInfo{&owned}, params))
		require.NoError(t, r.BatchSave(ctx, []*types.IndexInfo{&owned}, params))
		rows, err = r.ReadProcessingIndexes(ctx, "kb", "legacy", 2, []string{id})
		require.NoError(t, err)
		require.Len(t, rows, 1, "lost acknowledgements must not multiply a staged index")
		require.NoError(t, r.DeleteBySourceIDList(ctx, []string{id}, 2, "document"))
		rows, err = r.ReadProcessingIndexes(ctx, "kb", "legacy", 2, []string{id})
		require.NoError(t, err)
		require.Empty(t, rows, "cleanup must wait for the deletion to finish")
	}
	rows, err = r.ReadProcessingIndexes(ctx, "kb", "legacy", 2, []string{item.SourceID})
	require.NoError(t, err)
	require.Len(t, rows, 1, "staged cleanup must preserve the old index")
}
