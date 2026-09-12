package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// DataSourceRepository provides data access for data sources
type DataSourceRepository struct {
	db *gorm.DB
}

// NewDataSourceRepository creates a new data source repository
func NewDataSourceRepository(db *gorm.DB) interfaces.DataSourceRepository {
	return &DataSourceRepository{db: db}
}

// Create inserts a new data source record
func (r *DataSourceRepository) Create(ctx context.Context, ds *types.DataSource) error {
	if ds == nil {
		return errors.New("data source is nil")
	}
	syncDeletions := ds.SyncDeletions
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(ds).Error; err != nil {
			return err
		}
		// GORM applies default:true to a false bool during Create.
		ds.SyncDeletions = syncDeletions
		if !syncDeletions {
			return tx.Model(ds).Update("sync_deletions", false).Error
		}
		return nil
	})
}

// FindByID retrieves a data source by ID
func (r *DataSourceRepository) FindByID(ctx context.Context, id string) (*types.DataSource, error) {
	if id == "" {
		return nil, errors.New("id is empty")
	}
	var ds types.DataSource
	if err := r.db.WithContext(ctx).
		Where("id = ?", id).
		Where("deleted_at IS NULL").
		First(&ds).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("data source not found")
		}
		return nil, err
	}
	ds.SettingsFingerprint = dataSourceSettingsFingerprint(&ds)
	return &ds, nil
}

func dataSourceSettingsFingerprint(ds *types.DataSource) string {
	// Execution cursors, counters and timestamps do not invalidate settings.
	values, _ := json.Marshal([]any{ds.Name, ds.Type, string(ds.Config), ds.SyncSchedule, ds.SyncMode, ds.Status, ds.ConflictStrategy, ds.SyncDeletions, ds.SyncLogRetentionDays})
	digest := sha256.Sum256(values)
	return hex.EncodeToString(digest[:])
}

// FindByKnowledgeBase lists all data sources for a knowledge base
func (r *DataSourceRepository) FindByKnowledgeBase(ctx context.Context, kbID string) ([]*types.DataSource, error) {
	if kbID == "" {
		return nil, errors.New("knowledge base id is empty")
	}
	var dataSources []*types.DataSource
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ?", kbID).
		Where("deleted_at IS NULL").
		Order("created_at DESC").
		Find(&dataSources).Error; err != nil {
		return nil, err
	}
	return dataSources, nil
}

// Update updates an existing data source
func (r *DataSourceRepository) Update(ctx context.Context, ds *types.DataSource) error {
	if ds == nil {
		return errors.New("data source is nil")
	}
	if ds.ID == "" {
		return errors.New("data source id is empty")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		before, err := lockProcessingSource(tx, ds.ID)
		if err != nil {
			return err
		}
		if before.DeletedAt.Valid || (ds.TenantID != 0 && ds.TenantID != before.TenantID) || (ds.KnowledgeBaseID != "" && ds.KnowledgeBaseID != before.KnowledgeBaseID) {
			return ErrProcessingScope
		}
		if ds.SettingsFingerprint != "" && ds.SettingsFingerprint != dataSourceSettingsFingerprint(before) {
			return errors.New("data source settings changed; reload and retry")
		}
		// Execution state is server-managed; settings cannot overwrite a newer
		// cursor. Scope edits and revocation of execution commit atomically.
		if err := tx.Model(ds).Omit("last_sync_cursor", "last_sync_result", "last_sync_at", "sync_schedule", "sync_deletions").Updates(ds).Error; err != nil {
			return err
		}
		if err := tx.Model(ds).Updates(map[string]any{"sync_schedule": ds.SyncSchedule, "sync_deletions": ds.SyncDeletions}).Error; err != nil {
			return err
		}
		var after types.DataSource
		if err := tx.Where("id = ?", ds.ID).Take(&after).Error; err != nil {
			return err
		}
		status, reason, err := processingSourceEditReason(before, &after)
		if err != nil {
			return err
		}
		if status != "" {
			if err := invalidateProcessingSource(tx, &after, status, reason); err != nil {
				return err
			}
		}
		ds.SettingsFingerprint = dataSourceSettingsFingerprint(&after)
		return nil
	})
}

// UpdateSyncState updates only fields managed by sync execution. GORM's
// Updates(struct) skips zero values, so use a map here to persist cleared error
// messages without broadening the generic Update method.
func (r *DataSourceRepository) UpdateSyncState(ctx context.Context, ds *types.DataSource) error {
	if ds == nil {
		return errors.New("data source is nil")
	}
	if ds.ID == "" {
		return errors.New("data source id is empty")
	}
	query := r.db.WithContext(ctx).
		Model(&types.DataSource{}).
		Where("id = ?", ds.ID)
	if ds.Type == types.ConnectorTypeTencentDocs {
		query = query.Where("tencent_file_sync = ? OR NOT EXISTS (?)", true, r.db.Model(&types.ProcessingJob{}).Select("1").Where("datasource_id = ? AND tenant_id = ?", ds.ID, ds.TenantID))
		// A worker from an older scope/credential configuration cannot replace
		// the new execution cursor. Keep legacy connectors' update contract.
		if len(ds.Config) == 0 {
			query = query.Where("config IS NULL")
		} else {
			query = query.Where("config = ?", ds.Config)
		}
	}
	result := query.Updates(map[string]interface{}{
		"status":           gorm.Expr("CASE WHEN status IN (?, ?) THEN status ELSE ? END", types.DataSourceStatusPaused, types.DataSourceStatusDeleted, ds.Status),
		"last_sync_at":     ds.LastSyncAt,
		"last_sync_cursor": ds.LastSyncCursor,
		"last_sync_result": ds.LastSyncResult,
		"error_message":    ds.ErrorMessage,
		"updated_at":       time.Now().UTC(),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("data source changed or was deleted while syncing")
	}
	return nil
}

// Delete performs a soft delete
func (r *DataSourceRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("id is empty")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		source, err := lockProcessingSource(tx, id)
		if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && source.DeletedAt.Valid) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := tx.Where("id = ?", id).Delete(&types.DataSource{}).Error; err != nil {
			return err
		}
		return invalidateProcessingSource(tx, source, types.ProcessingCanceled, "SOURCE_DELETED")
	})
}

// FindActive retrieves all active data sources (used for scheduling)
func (r *DataSourceRepository) FindActive(ctx context.Context) ([]*types.DataSource, error) {
	var dataSources []*types.DataSource
	if err := r.db.WithContext(ctx).
		Where("status = ?", types.DataSourceStatusActive).
		Where("deleted_at IS NULL").
		Where("sync_schedule != ''").
		Order("created_at DESC").
		Find(&dataSources).Error; err != nil {
		return nil, err
	}
	return dataSources, nil
}

// SyncLogRepository provides data access for sync logs
type SyncLogRepository struct {
	db *gorm.DB
}

// NewSyncLogRepository creates a new sync log repository
func NewSyncLogRepository(db *gorm.DB) interfaces.SyncLogRepository {
	return &SyncLogRepository{db: db}
}

// Create inserts a new sync log entry
func (r *SyncLogRepository) Create(ctx context.Context, log *types.SyncLog) error {
	if log == nil {
		return errors.New("sync log is nil")
	}
	if err := r.db.WithContext(ctx).Create(log).Error; err != nil {
		return err
	}
	return nil
}

// FindByID retrieves a sync log by ID
func (r *SyncLogRepository) FindByID(ctx context.Context, id string) (*types.SyncLog, error) {
	if id == "" {
		return nil, errors.New("id is empty")
	}
	var log types.SyncLog
	if err := r.db.WithContext(ctx).
		Where("id = ?", id).
		First(&log).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("sync log not found")
		}
		return nil, err
	}
	return &log, nil
}

// FindByDataSource lists sync logs for a data source with pagination
func (r *SyncLogRepository) FindByDataSource(ctx context.Context, dsID string, limit int, offset int) ([]*types.SyncLog, error) {
	if dsID == "" {
		return nil, errors.New("data source id is empty")
	}
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}
	var logs []*types.SyncLog
	if err := r.db.WithContext(ctx).
		Where("data_source_id = ?", dsID).
		Order("started_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&logs).Error; err != nil {
		return nil, err
	}
	return logs, nil
}

// FindLatest retrieves the most recent sync log for a data source
func (r *SyncLogRepository) FindLatest(ctx context.Context, dsID string) (*types.SyncLog, error) {
	if dsID == "" {
		return nil, errors.New("data source id is empty")
	}
	var log types.SyncLog
	if err := r.db.WithContext(ctx).
		Where("data_source_id = ?", dsID).
		Order("started_at DESC").
		Limit(1).
		First(&log).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &log, nil
}

// HasRunningSync distinguishes execution from immutable historical status.
func (r *SyncLogRepository) HasRunningSync(ctx context.Context, dsID string) (bool, error) {
	if dsID == "" {
		return false, errors.New("data source id is empty")
	}
	var count int64
	active := []string{types.ProcessingEnqueuePending, types.ProcessingQueued, types.ProcessingRunning, types.ProcessingWaitingExternal, types.ProcessingRetryWait}
	if err := r.db.WithContext(ctx).Model(&types.ProcessingJob{}).
		Where("datasource_id = ? AND retirement_state <> ?", dsID, "deleted").
		Where(`status = ? OR status IN ? OR EXISTS (SELECT 1 FROM processing_steps s WHERE s.job_id=processing_jobs.id AND s.phase<>? AND s.status IN ?)`, types.ProcessingPlanned, active, types.ProcessingPhaseRetire, active).Count(&count).Error; err != nil {
		return false, err
	}
	if count > 0 {
		return true, nil
	}
	if err := r.db.WithContext(ctx).
		Model(&types.SyncLog{}).
		Where("data_source_id = ?", dsID).
		Where("status = ?", types.SyncLogStatusRunning).
		Where("NOT EXISTS (SELECT 1 FROM processing_jobs WHERE origin_run_id = sync_logs.id)").
		// A validated, immutable drain is evidence that the old workers and
		// deliveries ended. Its admission expiry does not restart those workers.
		// Later pending dispatches still block until they acquire a ledger job.
		Where(`NOT EXISTS (SELECT 1 FROM processing_legacy_drains d
		 WHERE d.tenant_id=sync_logs.tenant_id AND d.datasource_id=sync_logs.data_source_id AND d.checked_at>=sync_logs.started_at)`).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// Update updates an existing sync log entry
func (r *SyncLogRepository) Update(ctx context.Context, log *types.SyncLog) error {
	if log == nil {
		return errors.New("sync log is nil")
	}
	if log.ID == "" {
		return errors.New("sync log id is empty")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockLegacySyncSource(tx, log.ID); err != nil {
			return err
		}
		return tx.Model(log).Scopes(LegacySyncLog).Updates(log).Error
	})
}

// UpdateResult updates only fields produced by sync execution. Use an explicit
// map so empty error messages are written when a later sync succeeds.
func (r *SyncLogRepository) UpdateResult(ctx context.Context, log *types.SyncLog) error {
	if log == nil {
		return errors.New("sync log is nil")
	}
	if log.ID == "" {
		return errors.New("sync log id is empty")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockLegacySyncSource(tx, log.ID); err != nil {
			return err
		}
		return tx.
			Model(&types.SyncLog{}).
			Where("id = ?", log.ID).
			Scopes(LegacySyncLog).
			Updates(map[string]interface{}{
				"status":        log.Status,
				"finished_at":   log.FinishedAt,
				"items_total":   log.ItemsTotal,
				"items_created": log.ItemsCreated,
				"items_updated": log.ItemsUpdated,
				"items_deleted": log.ItemsDeleted,
				"items_skipped": log.ItemsSkipped,
				"items_failed":  log.ItemsFailed,
				"error_message": log.ErrorMessage,
				"result":        log.Result,
				"updated_at":    time.Now().UTC(),
			}).Error
	})
}

// CancelPendingByDataSource marks all non-terminal sync logs for a data source as canceled.
func (r *SyncLogRepository) CancelPendingByDataSource(ctx context.Context, dsID string) error {
	if dsID == "" {
		return errors.New("data source id is empty")
	}
	now := time.Now().UTC()
	return r.db.WithContext(ctx).
		Model(&types.SyncLog{}).
		Where("data_source_id = ?", dsID).
		Where("status IN ?", []string{types.SyncLogStatusRunning, "pending"}).
		Scopes(LegacySyncLog).
		Updates(map[string]interface{}{
			"status":        types.SyncLogStatusCanceled,
			"finished_at":   &now,
			"error_message": "data source deleted",
		}).Error
}

// CleanupOldLogs deletes sync logs older than the retention period
func (r *SyncLogRepository) CleanupOldLogs(ctx context.Context, retentionDays int) error {
	if retentionDays < 90 {
		retentionDays = 90
	}
	if retentionDays > 36500 {
		return errors.New("sync log retention exceeds 100 years")
	}
	now, err := processingDBTime(r.db.WithContext(ctx))
	if err != nil {
		return err
	}
	// Referenced lifecycle runs are audit roots. Unresolved legacy failures and
	// unfinished runs have no automatic expiry; only safe terminal logs qualify.
	return r.db.WithContext(ctx).Where("finished_at < ? AND status IN ?", now.AddDate(0, 0, -retentionDays), []string{types.SyncLogStatusSuccess, types.SyncLogStatusCanceled}).
		Where("COALESCE(items_failed,0) = 0 AND COALESCE(error_message,'') = ''").
		Where("result IS NULL OR result->'errors' IS NULL OR result->'errors' = ?", "[]").
		Scopes(LegacySyncLog).
		Where("NOT EXISTS (?)", r.db.Model(&types.SyncRunItem{}).Select("1").Where("run_id = sync_logs.id")).
		Where("NOT EXISTS (?)", r.db.Model(&types.ProcessingEvent{}).Select("1").Where("run_id = sync_logs.id")).Delete(&types.SyncLog{}).Error
}
