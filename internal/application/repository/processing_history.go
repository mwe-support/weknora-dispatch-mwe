package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

var ErrProcessingHistoryExpired = errors.New("history snapshot expired; refresh the list")
var ErrProcessingHistoryLimit = errors.New("history exceeds the snapshot limit; narrow the filters")
var ErrProcessingHistoryFilter = errors.New("invalid history filter")

var processingHistoryViews = map[string]bool{
	"current_run_progress": true, "current_document_lifecycle": true, "stage_retry_queue": true,
	"unresolved_incidents": true, "attempt_timeline": true, "lifecycle_inconsistencies": true,
}

type ProcessingHistoryFilter struct {
	View     string     `json:"view"`
	SourceID string     `json:"source_id,omitempty"`
	RunID    string     `json:"run_id,omitempty"`
	JobID    string     `json:"job_id,omitempty"`
	Status   string     `json:"status,omitempty"`
	Stage    string     `json:"stage,omitempty"`
	Search   string     `json:"search,omitempty"`
	From     *time.Time `json:"from,omitempty"`
	To       *time.Time `json:"to,omitempty"`
}

// Only allowlisted SQL projections are cached; no source credentials, signed
// URLs, artifact payloads or document bodies enter the snapshot tables.
type ProcessingHistorySnapshot struct {
	ID           string    `json:"snapshot_id" gorm:"primaryKey"`
	TenantID     uint64    `json:"-"`
	Principal    string    `json:"-"`
	KBScope      string    `json:"-"`
	ViewName     string    `json:"view"`
	FilterDigest string    `json:"filter_digest"`
	Total        int       `json:"total"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type ProcessingHistoryRow struct {
	SnapshotID string `gorm:"primaryKey"`
	Rank       int    `gorm:"primaryKey"`
	Payload    string
}

type ProcessingHistoryPage struct {
	ProcessingHistorySnapshot
	Rows      []json.RawMessage `json:"rows"`
	NextAfter int               `json:"next_after"`
	HasMore   bool              `json:"has_more"`
}

func (r *ProcessingRepository) CreateHistorySnapshot(ctx context.Context, tenant uint64, principal, kb string, filter ProcessingHistoryFilter, limit int) (*ProcessingHistoryPage, error) {
	if tenant == 0 || principal == "" || len(principal) > 160 || len(kb) > 64 || limit < 1 || limit > 100 || !processingHistoryViews[filter.View] {
		return nil, ErrProcessingHistoryFilter
	}
	for _, value := range []string{filter.SourceID, filter.RunID, filter.JobID, filter.Status, filter.Stage, filter.Search} {
		if len(value) > 256 {
			return nil, ErrProcessingHistoryFilter
		}
	}
	if filter.From != nil && filter.To != nil && !filter.To.After(*filter.From) {
		return nil, ErrProcessingHistoryFilter
	}
	var snapshot ProcessingHistorySnapshot
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serializes only one user's short cache insert/prune, never worker I/O.
		// The projection below is one MVCC SELECT; count and every page derive from
		// exactly its rows, even while lifecycle writers keep running.
		if tx.Dialector.Name() == "postgres" {
			key := fmt.Sprintf("processing-history/%d/%s", tenant, principal)
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", key).Error; err != nil {
				return err
			}
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if filter.View == "attempt_timeline" && filter.From == nil {
			since := now.Add(-7 * 24 * time.Hour)
			filter.From = &since
		}
		encoded, err := json.Marshal(filter)
		if err != nil {
			return err
		}
		snapshot = ProcessingHistorySnapshot{ID: uuid.NewString(), TenantID: tenant, Principal: principal, KBScope: kb, ViewName: filter.View,
			FilterDigest: fmt.Sprintf("%x", sha256.Sum256(encoded)), CreatedAt: now, ExpiresAt: now.Add(15 * time.Minute)}
		query := tx.Table("mwe_processing_"+filter.View).Where("tenant_id = ?", tenant)
		if kb != "" {
			query = query.Where("knowledge_base_id = ?", kb)
		}
		for _, item := range []struct{ column, value string }{{"datasource_id", filter.SourceID}, {"run_id", filter.RunID}, {"job_id", filter.JobID}, {"status", filter.Status}, {"stage", filter.Stage}} {
			if item.value != "" {
				query = query.Where(item.column+" = ?", item.value)
			}
		}
		if filter.Search != "" {
			value := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(strings.ToLower(filter.Search)) + "%"
			query = query.Where("(LOWER(title) LIKE ? ESCAPE '!' OR LOWER(external_id) LIKE ? ESCAPE '!' OR LOWER(job_id) LIKE ? ESCAPE '!')", value, value, value)
		}
		if filter.From != nil {
			query = query.Where("observed_at >= ?", *filter.From)
		}
		if filter.To != nil {
			query = query.Where("observed_at < ?", *filter.To)
		}
		order := "observed_at DESC, row_id DESC"
		var rows []map[string]any
		if err := query.Order(order).Limit(10001).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) > 10000 {
			return ErrProcessingHistoryLimit
		}
		frozen := make([]ProcessingHistoryRow, 0, len(rows))
		size := 0
		for i, row := range rows {
			payload, err := json.Marshal(row)
			if err != nil {
				return err
			}
			size += len(payload)
			if size > 32<<20 {
				return ErrProcessingHistoryLimit
			}
			frozen = append(frozen, ProcessingHistoryRow{SnapshotID: snapshot.ID, Rank: i + 1, Payload: string(payload)})
		}
		snapshot.Total = len(frozen)
		var old []ProcessingHistorySnapshot
		if err := tx.Where("tenant_id = ? AND principal = ?", tenant, principal).Order("created_at DESC, id DESC").Find(&old).Error; err != nil {
			return err
		}
		for i, row := range old {
			if i >= 7 || !row.ExpiresAt.After(now) {
				if err := deleteHistorySnapshot(tx, row.ID); err != nil {
					return err
				}
			}
		}
		if err := tx.Create(&snapshot).Error; err != nil {
			return err
		}
		if len(frozen) > 0 {
			return tx.CreateInBatches(frozen, 100).Error
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.HistoryPage(ctx, tenant, principal, kb, snapshot.ID, snapshot.FilterDigest, 0, limit)
}

func (r *ProcessingRepository) HistoryPage(ctx context.Context, tenant uint64, principal, kb, id, digest string, after, limit int) (*ProcessingHistoryPage, error) {
	if tenant == 0 || principal == "" || id == "" || digest == "" || after < 0 || limit < 1 || limit > 100 {
		return nil, ErrProcessingHistoryFilter
	}
	page := &ProcessingHistoryPage{Rows: []json.RawMessage{}, NextAfter: after}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Owner/scope mismatches deliberately reveal no existence or record count.
		if err := tx.Where("id = ? AND tenant_id = ? AND principal = ? AND kb_scope = ? AND filter_digest = ?", id, tenant, principal, kb, digest).Take(&page.ProcessingHistorySnapshot).Error; err != nil {
			return err
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if !page.ExpiresAt.After(now) {
			return ErrProcessingHistoryExpired
		}
		if after > page.Total {
			return ErrProcessingHistoryFilter
		}
		var rows []ProcessingHistoryRow
		if err := tx.Where("snapshot_id = ? AND rank > ?", id, after).Order("rank").Limit(limit).Find(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			page.Rows = append(page.Rows, json.RawMessage(row.Payload))
			page.NextAfter = row.Rank
		}
		if len(rows) == 0 && after < page.Total {
			return ErrProcessingHistoryExpired
		}
		page.HasMore = page.NextAfter < page.Total
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return page, err
}

func deleteHistorySnapshot(tx *gorm.DB, id string) error {
	if err := tx.Where("snapshot_id = ?", id).Delete(&ProcessingHistoryRow{}).Error; err != nil {
		return err
	}
	return tx.Where("id = ?", id).Delete(&ProcessingHistorySnapshot{}).Error
}

func (r *ProcessingRepository) CollectHistorySnapshots(ctx context.Context) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		var expired []ProcessingHistorySnapshot
		if err := tx.Where("expires_at <= ?", now).Order("expires_at, id").Limit(100).Find(&expired).Error; err != nil {
			return err
		}
		for _, row := range expired {
			if err := deleteHistorySnapshot(tx, row.ID); err != nil {
				return err
			}
		}
		return nil
	})
}
