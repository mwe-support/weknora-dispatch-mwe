package retriever

import (
	"context"
	"errors"
	"math"
	"slices"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func (v *KeywordsVectorHybridRetrieveEngineService) ReadProcessingIndexes(ctx context.Context, kbID, knowledgeID string, dimension int, sourceIDs []string) ([]*types.IndexInfo, error) {
	reader, ok := v.indexRepository.(interfaces.ProcessingIndexReader)
	if !ok {
		return nil, errors.New("LEGACY_INDEX_READER_UNAVAILABLE")
	}
	return reader.ReadProcessingIndexes(ctx, kbID, knowledgeID, dimension, sourceIDs)
}

func (c *CompositeRetrieveEngine) VerifyProcessingKeywordIndexes(ctx context.Context, kbID, knowledgeID string, vectorDimension int, expected []*types.IndexInfo) error {
	var selected *engineInfo
	for _, info := range c.engineInfos {
		if !slices.Contains(info.retrieverType, types.KeywordsRetrieverType) {
			continue
		}
		if selected != nil {
			return errors.New("LEGACY_INDEX_DESTINATION_AMBIGUOUS")
		}
		selected = info
	}
	if selected == nil {
		return errors.New("LEGACY_INDEX_READER_UNAVAILABLE")
	}
	// A hybrid backend stores keyword text on its vector row; a separate
	// keyword-only backend writes dimension zero in BatchIndex.
	dimension := 0
	if slices.Contains(selected.retrieverType, types.VectorRetrieverType) {
		dimension = vectorDimension
	}
	engine := CompositeRetrieveEngine{engineInfos: []*engineInfo{selected}}
	_, err := engine.ReadProcessingIndexes(ctx, kbID, knowledgeID, dimension, expected)
	return err
}

// A migration reads the one selected vector destination (or the sole keyword
// destination when vectors are disabled). It never guesses between stores.
func (c *CompositeRetrieveEngine) ReadProcessingIndexes(ctx context.Context, kbID, knowledgeID string, dimension int, expected []*types.IndexInfo) ([]*types.IndexInfo, error) {
	if kbID == "" || knowledgeID == "" || dimension < 0 || len(expected) == 0 || len(expected) > 100000 {
		return nil, errors.New("LEGACY_INDEX_SCOPE_INVALID")
	}
	kind := types.KeywordsRetrieverType
	if dimension > 0 {
		kind = types.VectorRetrieverType
	}
	var reader interfaces.ProcessingIndexReader
	for _, info := range c.engineInfos {
		if !slices.Contains(info.retrieverType, kind) {
			continue
		}
		if reader != nil {
			return nil, errors.New("LEGACY_INDEX_DESTINATION_AMBIGUOUS")
		}
		var ok bool
		reader, ok = info.retrieveEngine.(interfaces.ProcessingIndexReader)
		if !ok {
			return nil, errors.New("LEGACY_INDEX_READER_UNAVAILABLE")
		}
	}
	if reader == nil {
		return nil, errors.New("LEGACY_INDEX_READER_UNAVAILABLE")
	}
	seen := map[string]bool{}
	for _, item := range expected {
		if item == nil || item.SourceID == "" || item.ChunkID == "" || seen[item.SourceID] || item.KnowledgeID != knowledgeID || item.KnowledgeBaseID != kbID {
			return nil, errors.New("LEGACY_INDEX_SCOPE_INVALID")
		}
		seen[item.SourceID] = true
	}
	result := make([]*types.IndexInfo, 0, len(expected))
	for start := 0; start < len(expected); start += 128 {
		batch := expected[start:min(start+128, len(expected))]
		ids := make([]string, len(batch))
		wanted := map[string]*types.IndexInfo{}
		for i, item := range batch {
			ids[i], wanted[item.SourceID] = item.SourceID, item
		}
		rows, err := reader.ReadProcessingIndexes(ctx, kbID, knowledgeID, dimension, ids)
		if err != nil {
			return nil, err
		}
		found := map[string]*types.IndexInfo{}
		for _, row := range rows {
			if row == nil {
				return nil, errors.New("LEGACY_INDEX_INVALID")
			}
			item := wanted[row.SourceID]
			if item == nil || found[row.SourceID] != nil || row.KnowledgeID != knowledgeID || row.KnowledgeBaseID != kbID || row.ChunkID != item.ChunkID || row.SourceType != item.SourceType || row.IsEnabled != item.IsEnabled || row.Content != item.Content || len(row.PreparedEmbedding) != dimension {
				return nil, errors.New("LEGACY_INDEX_COVERAGE_INVALID")
			}
			for _, value := range row.PreparedEmbedding {
				if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
					return nil, errors.New("LEGACY_INDEX_VECTOR_INVALID")
				}
			}
			found[row.SourceID] = row
		}
		for _, item := range batch {
			row := found[item.SourceID]
			if row == nil {
				return nil, errors.New("LEGACY_INDEX_MISSING")
			}
			result = append(result, row)
		}
	}
	return result, nil
}
