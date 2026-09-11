package qdrant

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/qdrant/go-client/qdrant"
)

func (q *qdrantRepository) ReadProcessingIndexes(ctx context.Context, kbID, knowledgeID string, dimension int, sourceIDs []string) ([]*types.IndexInfo, error) {
	if kbID == "" || knowledgeID == "" || dimension <= 0 || len(sourceIDs) == 0 || len(sourceIDs) > 128 {
		return nil, errors.New("LEGACY_INDEX_SCOPE_INVALID")
	}
	// 129 detects duplicate physical points for the bounded set of at most 128
	// requested identities. Ambiguous old points must be reconciled explicitly.
	limit := uint32(129)
	points, err := q.client.Scroll(ctx, &qdrant.ScrollPoints{CollectionName: q.getCollectionName(dimension), Limit: &limit,
		Filter: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatch(fieldKnowledgeBaseID, kbID),
			qdrant.NewMatch(fieldKnowledgeID, knowledgeID), qdrant.NewMatchKeywords(fieldSourceID, sourceIDs...)}},
		WithPayload: qdrant.NewWithPayload(true), WithVectors: qdrant.NewWithVectors(true)})
	if err != nil {
		return nil, err
	}
	if len(points) > 128 {
		return nil, errors.New("LEGACY_INDEX_DUPLICATE")
	}
	result := make([]*types.IndexInfo, 0, len(points))
	for _, point := range points {
		p := point.Payload
		item := &types.IndexInfo{SourceID: p[fieldSourceID].GetStringValue(), SourceType: types.SourceType(p[fieldSourceType].GetIntegerValue()),
			ChunkID: p[fieldChunkID].GetStringValue(), KnowledgeID: p[fieldKnowledgeID].GetStringValue(), KnowledgeBaseID: p[fieldKnowledgeBaseID].GetStringValue(),
			Content: p[fieldContent].GetStringValue(), IsEnabled: p[fieldIsEnabled].GetBoolValue(), TagID: p[fieldTagID].GetStringValue()}
		if point.Vectors != nil && point.Vectors.GetVector() != nil {
			vector := point.Vectors.GetVector()
			if dense := vector.GetDenseVector(); dense != nil {
				item.PreparedEmbedding = dense.Data
			} else {
				item.PreparedEmbedding = vector.GetData()
			}
		}
		result = append(result, item)
	}
	return result, nil
}
