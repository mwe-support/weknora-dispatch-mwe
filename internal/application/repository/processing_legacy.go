package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// LegacyKnowledge excludes rows whose state is owned by the leased ledger.
// Apply this to the UPDATE as well as discovery queries: a callback is not a lease.
func LegacyKnowledge(db *gorm.DB) *gorm.DB {
	return db.Where("COALESCE(CAST(metadata->>'processing_protocol' AS TEXT), '') <> '2'")
}

// Ledger runs and original records supporting recovery evidence are audit
// roots. Legacy reset, cancellation and expiry must not rewrite or remove them.
func LegacySyncLog(db *gorm.DB) *gorm.DB {
	return db.Where("NOT EXISTS (SELECT 1 FROM processing_jobs WHERE origin_run_id = sync_logs.id)").
		Where("NOT EXISTS (SELECT 1 FROM processing_legacy_evidence WHERE run_id = sync_logs.id)")
}

// Ownership and legacy-evidence admission take the same source lock. Check
// the write predicate in a new statement after that lock, not a stale snapshot.
func lockLegacySyncSource(tx *gorm.DB, runID string) error {
	var run types.SyncLog
	err := tx.Select("data_source_id", "tenant_id").Where("id = ?", runID).Take(&run).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if run.DataSourceID == "" {
		return nil
	}
	q := tx.Unscoped().Select("id").Where("id = ? AND tenant_id = ?", run.DataSourceID, run.TenantID)
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var source types.DataSource
	err = q.Take(&source).Error
	// No new owner can be admitted without the original source row.
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	return err
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
