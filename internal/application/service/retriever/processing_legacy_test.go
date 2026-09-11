package retriever

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type processingLegacyReader struct {
	interfaces.RetrieveEngineService
	items  map[string]*types.IndexInfo
	calls  int
	change func([]*types.IndexInfo) []*types.IndexInfo
}

func (r *processingLegacyReader) ReadProcessingIndexes(_ context.Context, kbID, knowledgeID string, dimension int, ids []string) ([]*types.IndexInfo, error) {
	r.calls++
	var rows []*types.IndexInfo
	for i := len(ids) - 1; i >= 0; i-- {
		copy := *r.items[ids[i]]
		copy.PreparedEmbedding = make([]float32, dimension)
		for j := range copy.PreparedEmbedding {
			copy.PreparedEmbedding[j] = 0.25
		}
		rows = append(rows, &copy)
	}
	if r.change != nil {
		rows = r.change(rows)
	}
	return rows, nil
}

func TestProcessingLegacyIndexReadRequiresExactCompleteBoundedCoverage(t *testing.T) {
	reader := &processingLegacyReader{items: map[string]*types.IndexInfo{}}
	var expected []*types.IndexInfo
	for i := 0; i < 129; i++ {
		item := &types.IndexInfo{SourceID: fmt.Sprint(i), ChunkID: fmt.Sprint(i), KnowledgeID: "legacy", KnowledgeBaseID: "kb", Content: "synthetic"}
		expected = append(expected, item)
		reader.items[item.SourceID] = item
	}
	engine := &CompositeRetrieveEngine{engineInfos: []*engineInfo{{retrieveEngine: reader, retrieverType: []types.RetrieverType{types.VectorRetrieverType}}}}
	rows, err := engine.ReadProcessingIndexes(context.Background(), "kb", "legacy", 2, expected)
	require.NoError(t, err)
	require.Len(t, rows, len(expected))
	require.Equal(t, 2, reader.calls)
	for i, row := range rows {
		require.Equal(t, expected[i].SourceID, row.SourceID)
		require.Len(t, row.PreparedEmbedding, 2)
	}
	for _, bad := range []string{"missing", "duplicate", "content", "knowledge", "dimension", "nan"} {
		t.Run(bad, func(t *testing.T) {
			reader.change = func(rows []*types.IndexInfo) []*types.IndexInfo {
				switch bad {
				case "missing":
					return rows[1:]
				case "duplicate":
					return append(rows, rows[0])
				case "content":
					rows[0].Content = "changed"
				case "knowledge":
					rows[0].KnowledgeID = "other"
				case "dimension":
					rows[0].PreparedEmbedding = nil
				case "nan":
					rows[0].PreparedEmbedding[0] = float32(math.NaN())
				}
				return rows
			}
			_, err := engine.ReadProcessingIndexes(context.Background(), "kb", "legacy", 2, expected)
			require.Error(t, err)
		})
	}
	reader.change = nil
	engine.engineInfos[0].retrieverType = append(engine.engineInfos[0].retrieverType, types.KeywordsRetrieverType)
	require.NoError(t, engine.VerifyProcessingKeywordIndexes(context.Background(), "kb", "legacy", 2, expected))
	engine.engineInfos[0].retrieverType = []types.RetrieverType{types.KeywordsRetrieverType}
	require.NoError(t, engine.VerifyProcessingKeywordIndexes(context.Background(), "kb", "legacy", 2, expected))
	engine.engineInfos[0].retrieverType = []types.RetrieverType{types.VectorRetrieverType}
	engine.engineInfos = append(engine.engineInfos, engine.engineInfos[0])
	_, err = engine.ReadProcessingIndexes(context.Background(), "kb", "legacy", 2, expected)
	require.EqualError(t, err, "LEGACY_INDEX_DESTINATION_AMBIGUOUS")
}
