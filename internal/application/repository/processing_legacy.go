package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// LegacyKnowledge excludes rows whose state is owned by the leased ledger.
// Apply this to the UPDATE as well as discovery queries: a callback is not a lease.
func LegacyKnowledge(db *gorm.DB) *gorm.DB {
	return db.Where("COALESCE(CAST(metadata->>'processing_protocol' AS TEXT), '') <> '2'")
}

func (r *knowledgeRepository) HasProcessingKnowledge(ctx context.Context, tenant uint64, kb string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&types.Knowledge{}).Where("tenant_id = ? AND knowledge_base_id = ? AND metadata->>'processing_protocol' = '2'", tenant, kb).Count(&count).Error
	return count > 0, err
}

func (r *ProcessingRepository) LegacyKnowledgeTaskAllowed(ctx context.Context, knowledgeID, chunkID string) (bool, error) {
	if knowledgeID == "" && chunkID == "" {
		return true, nil
	}
	query := r.db.WithContext(ctx).Unscoped().Model(&types.Knowledge{}).
		Where("CAST(metadata->>'processing_protocol' AS TEXT) = '2'")
	if chunkID == "" {
		query = query.Where("id = ?", knowledgeID)
	} else {
		query = query.Where("id = ? OR id IN (SELECT knowledge_id FROM chunks WHERE id = ?)", knowledgeID, chunkID)
	}
	// IDs are globally unique. A wrong tenant in an old payload must not make
	// a ledger-owned row look like legacy data; the handler validates legacy scope.
	var count int64
	err := query.Count(&count).Error
	return count == 0, err
}
