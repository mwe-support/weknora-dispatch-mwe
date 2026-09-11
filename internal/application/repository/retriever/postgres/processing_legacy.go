package postgres

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
)

func (r *pgRepository) ReadProcessingIndexes(ctx context.Context, kbID, knowledgeID string, dimension int, sourceIDs []string) ([]*types.IndexInfo, error) {
	if kbID == "" || knowledgeID == "" || dimension < 0 || len(sourceIDs) == 0 || len(sourceIDs) > 128 {
		return nil, errors.New("LEGACY_INDEX_SCOPE_INVALID")
	}
	var rows []pgVector
	if err := r.db.WithContext(ctx).Where("knowledge_base_id = ? AND knowledge_id = ? AND dimension = ? AND source_id IN ?", kbID, knowledgeID, dimension, sourceIDs).
		Order("id").Limit(129).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) > 128 {
		return nil, errors.New("LEGACY_INDEX_DUPLICATE")
	}
	result := make([]*types.IndexInfo, 0, len(rows))
	for _, row := range rows {
		item := &types.IndexInfo{SourceID: row.SourceID, SourceType: types.SourceType(row.SourceType), ChunkID: row.ChunkID,
			KnowledgeID: row.KnowledgeID, KnowledgeBaseID: row.KnowledgeBaseID, Content: row.Content, IsEnabled: row.IsEnabled, TagID: row.TagID}
		if row.Embedding != nil {
			item.PreparedEmbedding = row.Embedding.Slice()
		}
		result = append(result, item)
	}
	return result, nil
}
