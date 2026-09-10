package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type ProcessingRunSnapshot struct {
	Protocol          int    `json:"protocol"`
	ScanJobID         string `json:"scan_job_id"`
	ScanStatus        string `json:"scan_status"`
	DiscoveryComplete bool   `json:"discovery_complete"`
	Documents         int    `json:"documents"`
	Succeeded         int    `json:"succeeded"`
	Created           int    `json:"created"`
	Updated           int    `json:"updated"`
	Reused            int    `json:"reused"`
	Blocked           int    `json:"blocked"`
	Failed            int    `json:"failed"`
	Canceled          int    `json:"canceled"`
	Superseded        int    `json:"superseded"`
	Skipped           int    `json:"skipped"`
	Active            int    `json:"active"`
	Unadmitted        int    `json:"unadmitted"`
	Containers        int    `json:"containers"`
	Links             int    `json:"links"`
}

func (r *ProcessingRepository) RunSnapshot(ctx context.Context, tenant uint64, runID string) (snapshot ProcessingRunSnapshot, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var run types.SyncLog
		if err := tx.Where("id = ? AND tenant_id = ?", runID, tenant).Take(&run).Error; err != nil {
			return err
		}
		var scan types.ProcessingJob
		if err := tx.Where("tenant_id = ? AND datasource_id = ? AND origin_run_id = ? AND kind = ?", tenant, run.DataSourceID, runID, types.ProcessingJobScan).Order("generation DESC").Take(&scan).Error; err != nil {
			return err
		}
		var err error
		snapshot, err = processingRunSnapshot(tx, &scan)
		return err
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return
}

func processingRunSnapshot(tx *gorm.DB, scan *types.ProcessingJob) (ProcessingRunSnapshot, error) {
	snapshot := ProcessingRunSnapshot{Protocol: types.ProcessingProtocol, ScanJobID: scan.ID, ScanStatus: scan.Status, DiscoveryComplete: scan.Status == types.ProcessingSucceeded}
	var groups []struct {
		Kind, JobID, Status, Disposition string
		Count                            int
	}
	if err := tx.Table("sync_run_items AS item").Select("item.kind, item.disposition, CASE WHEN item.job_id IS NULL OR item.job_id = '' THEN '' ELSE 'attached' END AS job_id, COALESCE(job.status, '') AS status, COUNT(*) AS count").
		Joins("LEFT JOIN processing_jobs AS job ON job.id = item.job_id AND job.tenant_id = item.tenant_id AND job.datasource_id = ? AND job.knowledge_base_id = ?", scan.DataSourceID, scan.KnowledgeBaseID).
		Where("item.run_id = ? AND item.tenant_id = ?", scan.OriginRunID, scan.TenantID).
		Group("item.kind, item.disposition, CASE WHEN item.job_id IS NULL OR item.job_id = '' THEN '' ELSE 'attached' END, job.status").Scan(&groups).Error; err != nil {
		return snapshot, err
	}
	for _, group := range groups {
		switch group.Kind {
		case "container":
			snapshot.Containers += group.Count
		case "link":
			snapshot.Links += group.Count
		case "document":
			snapshot.Documents += group.Count
			switch group.Status {
			case types.ProcessingSucceeded:
				snapshot.Succeeded += group.Count
				switch group.Disposition {
				case "created":
					snapshot.Created += group.Count
				case "updated":
					snapshot.Updated += group.Count
				case "reused":
					snapshot.Reused += group.Count
				}
			case types.ProcessingBlocked:
				snapshot.Blocked += group.Count
			case types.ProcessingFailed:
				snapshot.Failed += group.Count
			case types.ProcessingCanceled:
				snapshot.Canceled += group.Count
			case types.ProcessingSuperseded:
				snapshot.Superseded += group.Count
			case types.ProcessingSkipped:
				snapshot.Skipped += group.Count
			case "":
				if group.JobID != "" {
					return snapshot, errors.New("RUN_MEMBERSHIP_JOB_MISSING")
				}
				snapshot.Unadmitted += group.Count
			default:
				snapshot.Active += group.Count
			}
		default:
			return snapshot, errors.New("RUN_MEMBERSHIP_KIND_INVALID")
		}
	}
	return snapshot, nil
}

func refreshProcessingRuns(tx *gorm.DB, job *types.ProcessingJob) error {
	if job.OriginRunID == "" {
		return nil
	}
	runs := []string{job.OriginRunID}
	if job.Kind == types.ProcessingJobDocument {
		if err := tx.Model(&types.SyncRunItem{}).Where("job_id = ? AND tenant_id = ?", job.ID, job.TenantID).Pluck("run_id", &runs).Error; err != nil {
			return err
		}
	}
	slices.Sort(runs)
	for _, runID := range slices.Compact(runs) {
		var run types.SyncLog
		if err := tx.Where("id = ? AND tenant_id = ? AND data_source_id = ?", runID, job.TenantID, job.DataSourceID).Take(&run).Error; err != nil {
			return err
		}
		// Historical results are immutable. Current recovery is always read
		// from membership and the ledger, never by rewriting this snapshot.
		if run.FinishedAt != nil {
			continue
		}
		scan := job
		if job.Kind != types.ProcessingJobScan {
			scan = &types.ProcessingJob{}
			if err := tx.Where("tenant_id = ? AND datasource_id = ? AND origin_run_id = ? AND kind = ?", job.TenantID, job.DataSourceID, runID, types.ProcessingJobScan).Order("generation DESC").Take(scan).Error; err != nil {
				return err
			}
		}
		snapshot, err := processingRunSnapshot(tx, scan)
		if err != nil {
			return err
		}
		status := types.SyncLogStatusRunning
		scanStopped := scan.Status == types.ProcessingSucceeded || scan.Status == types.ProcessingBlocked || processingTerminal(scan.Status)
		if scanStopped && snapshot.Active == 0 && (snapshot.Unadmitted == 0 || scan.Status != types.ProcessingSucceeded) {
			status = types.SyncLogStatusFailed
			if snapshot.Succeeded > 0 {
				status = types.SyncLogStatusPartial
			}
			if snapshot.DiscoveryComplete && snapshot.Succeeded == snapshot.Documents {
				status = types.SyncLogStatusSuccess
			}
			if scan.Status == types.ProcessingCanceled || scan.Status == types.ProcessingSuperseded || (snapshot.Failed+snapshot.Blocked == 0 && snapshot.Canceled+snapshot.Superseded > 0) {
				status = types.SyncLogStatusCanceled
			}
		}
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		message := ""
		if status == types.SyncLogStatusFailed || status == types.SyncLogStatusPartial {
			message = "PROCESSING_INCOMPLETE"
		}
		changes := map[string]any{"status": status, "result": types.JSON(encoded), "items_total": snapshot.Documents, "items_created": snapshot.Created, "items_updated": snapshot.Updated, "items_skipped": snapshot.Reused + snapshot.Skipped, "items_failed": snapshot.Failed + snapshot.Blocked, "error_message": message}
		var finished *time.Time
		if status != types.SyncLogStatusRunning {
			now, err := processingDBTime(tx)
			if err != nil {
				return err
			}
			changes["finished_at"] = now
			finished = &now
			if err := appendProcessingEvent(tx, scan, types.ProcessingEvent{Type: "run_finished", RunID: runID, ToState: status, Detail: encoded}); err != nil {
				return err
			}
		}
		if err := tx.Model(&types.SyncLog{}).Where("id = ? AND finished_at IS NULL", runID).Updates(changes).Error; err != nil {
			return err
		}
		if err := projectProcessingSource(tx, scan, &run, status, encoded, message, finished); err != nil {
			return err
		}
	}
	return nil
}

// The source card is a projection of the latest run in the same configuration.
// An older run finishing late cannot replace a newer run's result or status.
func projectProcessingSource(tx *gorm.DB, scan *types.ProcessingJob, run *types.SyncLog, status string, result []byte, message string, finished *time.Time) error {
	var source types.DataSource
	err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", scan.DataSourceID, scan.TenantID, scan.KnowledgeBaseID).Take(&source).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	scope, auth, err := ProcessingSourceRevisions(&source)
	if err != nil {
		return err
	}
	if scope != scan.ScopeRevision || auth != scan.AuthRevision {
		return nil
	}
	updates := map[string]any{"last_sync_result": types.JSON(result)}
	if finished != nil {
		updates["last_sync_at"], updates["error_message"] = *finished, message
		if source.Status != types.DataSourceStatusPaused && source.Status != types.DataSourceStatusDeleted {
			updates["status"] = types.DataSourceStatusActive
			if status == types.SyncLogStatusFailed || status == types.SyncLogStatusPartial {
				updates["status"] = types.DataSourceStatusError
			}
		}
	}
	return tx.Model(&types.DataSource{}).Where("id = ? AND tenant_id = ?", source.ID, source.TenantID).
		Where("NOT EXISTS (?)", tx.Model(&types.SyncLog{}).Select("1").Where("data_source_id = ? AND tenant_id = ? AND (started_at > ? OR (started_at = ? AND created_at > ?))", source.ID, source.TenantID, run.StartedAt, run.StartedAt, run.CreatedAt)).Updates(updates).Error
}
