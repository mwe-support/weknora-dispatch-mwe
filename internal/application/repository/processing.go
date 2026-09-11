package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ProcessingRepository is the durable lifecycle boundary. The same store is
// used by sources, workers, recovery and status queries.
type ProcessingRepository struct{ db *gorm.DB }

func NewProcessingRepository(db *gorm.DB) *ProcessingRepository { return &ProcessingRepository{db: db} }

func (r *ProcessingRepository) JobDetail(ctx context.Context, tenant uint64, id string) (job *types.ProcessingJob, steps []types.ProcessingStep, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		repo := NewProcessingRepository(tx)
		var err error
		job, err = repo.GetJob(ctx, tenant, id)
		if err != nil {
			return err
		}
		steps, err = repo.ListSteps(ctx, tenant, id)
		return err
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return
}

// A successful HTTP liveness check must not hide a missing lifecycle schema
// after a migration/configuration error. This runs before workers are started.
func (r *ProcessingRepository) VerifySchema(ctx context.Context) error {
	for _, query := range []string{
		"SELECT original_error_digest, linked_job_id FROM mwe_processing_attempt_timeline WHERE 1=0",
		"SELECT id, revision, active_index_manifest, index_destination FROM processing_jobs WHERE 1=0",
		"SELECT id, step_attempt, dispatch_seq, input_fingerprint FROM processing_steps WHERE 1=0",
		"SELECT job_id, job_revision, event_type FROM processing_events WHERE 1=0",
		"SELECT run_id, item_key, job_id FROM sync_run_items WHERE 1=0",
		"SELECT step_id, step_attempt, dispatch_seq, delivered_at FROM task_pending_ops WHERE 1=0",
		"SELECT producer_job_id, consumer_step_id, digest FROM processing_artifact_references WHERE 1=0",
		"SELECT id, creation_job_id, state FROM resources WHERE 1=0",
		"SELECT id, physical_path, bytes, kind FROM processing_storage_reservations WHERE 1=0",
		"SELECT id, publication_epoch, destination_digest FROM processing_graph_writes WHERE 1=0",
		"SELECT id, publication_epoch, artifact_ref FROM processing_wiki_writes WHERE 1=0",
		"SELECT id, mutation_revision FROM wiki_pages WHERE 1=0",
		"SELECT id, faq_index_manifest FROM chunks WHERE 1=0",
		"SELECT id, chunk_id, state, source_ids, destination, estimated_bytes, storage_released FROM faq_index_writes WHERE 1=0",
		"SELECT id, principal, kb_scope, filter_digest, expires_at FROM processing_history_snapshots WHERE 1=0",
		"SELECT snapshot_id, rank, payload FROM processing_history_rows WHERE 1=0",
		"SELECT id, error_digest, snapshot_digest, configuration_revision, job_id FROM processing_legacy_evidence WHERE 1=0",
		"SELECT id, scope_revision, inventory, expires_at FROM processing_legacy_drains WHERE 1=0",
	} {
		if err := r.db.WithContext(ctx).Exec(query).Error; err != nil {
			return fmt.Errorf("lifecycle schema is unavailable: %w", err)
		}
	}
	for view := range processingHistoryViews {
		if err := r.db.WithContext(ctx).Exec("SELECT row_id, tenant_id, knowledge_base_id FROM mwe_processing_" + view + " WHERE 1=0").Error; err != nil {
			return fmt.Errorf("lifecycle history schema is unavailable: %w", err)
		}
	}
	return nil
}

var (
	ErrProcessingScope    = errors.New("processing source scope is no longer valid")
	ErrProcessingConflict = errors.New("processing state changed")
)

// ProcessingSourceRevisions compares semantic settings, not AES ciphertext or
// timestamps. A credentials-only rotation does not change the content scope.
func ProcessingSourceRevisions(source *types.DataSource) (scope, auth string, err error) {
	if source == nil {
		return "", "", ErrProcessingScope
	}
	config := &types.DataSourceConfig{}
	if len(source.Config) != 0 {
		config, err = source.ParseConfig()
		if err != nil {
			return "", "", err
		}
	}
	ids := slices.Clone(config.ResourceIDs)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	scopeBytes, err := json.Marshal([]any{source.ID, source.TenantID, source.KnowledgeBaseID, source.Type, ids, config.Settings})
	if err != nil {
		return "", "", err
	}
	authBytes, err := json.Marshal(config.Credentials)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(scopeBytes)), fmt.Sprintf("%x", sha256.Sum256(authBytes)), nil
}

func (r *ProcessingRepository) EnsureJob(ctx context.Context, input types.ProcessingJob) (*types.ProcessingJob, error) {
	return r.ensureJob(ctx, input, false)
}

func (r *ProcessingRepository) ensureJob(ctx context.Context, input types.ProcessingJob, rebuild bool) (*types.ProcessingJob, error) {
	if input.TenantID == 0 || input.KnowledgeBaseID == "" || input.DataSourceID == "" ||
		strings.TrimSpace(input.ExternalID) == "" || input.SourceRevision == "" || input.PipelineFingerprint == "" ||
		(input.Kind != types.ProcessingJobDocument && input.Kind != types.ProcessingJobScan) {
		return nil, errors.New("processing job requires scoped identity, kind, source revision and pipeline fingerprint")
	}
	var result *types.ProcessingJob
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		configuration, err := lockProcessingConfiguration(tx, input.TenantID, input.KnowledgeBaseID)
		if err != nil {
			return err
		}
		if input.ConfigurationRevision != "" && input.ConfigurationRevision != configuration {
			return ErrProcessingScope
		}
		// Lock an existing source row, including for the first document generation.
		// There must be no network I/O while holding this short transaction lock.
		query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", input.DataSourceID, input.TenantID, input.KnowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var source types.DataSource
		if err := query.Take(&source).Error; err != nil {
			return err
		}
		if source.Status == types.DataSourceStatusPaused || source.Status == types.DataSourceStatusDeleted {
			return ErrProcessingScope
		}
		scope, auth, err := ProcessingSourceRevisions(&source)
		if err != nil {
			return err
		}
		if (input.ScopeRevision != "" && input.ScopeRevision != scope) || (input.AuthRevision != "" && input.AuthRevision != auth) {
			return ErrProcessingScope
		}
		input.ScopeRevision, input.AuthRevision = scope, auth
		var current types.ProcessingJob
		currentQuery := processingLogicalQuery(tx, input)
		if input.Kind == types.ProcessingJobScan {
			currentQuery = currentQuery.Order("generation DESC")
		} else {
			currentQuery = currentQuery.Where("is_current = ?", true)
		}
		err = currentQuery.Take(&current).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// A run ID is an immutable discovery identity, including after its
		// temporary artifacts retire. Replaying it must never rescan that run.
		if input.Kind == types.ProcessingJobScan && err == nil {
			result = &current
			return nil
		}
		if !rebuild && err == nil && current.Status != types.ProcessingCanceled && current.Status != types.ProcessingSuperseded && current.SourceRevision == input.SourceRevision && current.PipelineFingerprint == input.PipelineFingerprint && current.ConfigurationRevision == configuration &&
			current.ScopeRevision == scope && current.AuthRevision == auth && (input.SourceDigest == "" || current.SourceDigest == input.SourceDigest) {
			result = &current
			return nil
		}
		if current.ID != "" {
			from := current.Status
			current.IsCurrent = false
			changes := map[string]any{"is_current": false}
			if !current.IsPublished && !processingTerminal(current.Status) {
				current.Status = types.ProcessingSuperseded
				changes["status"] = current.Status
			}
			if err := tx.Model(&types.ProcessingJob{}).Where("id = ?", current.ID).Updates(changes).Error; err != nil {
				return err
			}
			if !current.IsPublished {
				now, err := processingDBTime(tx)
				if err != nil {
					return err
				}
				if err := stopProcessingSteps(tx, &current, types.ProcessingSuperseded, "NEW_GENERATION", now); err != nil {
					return err
				}
			}
			if err := appendProcessingEvent(tx, &current, types.ProcessingEvent{Type: "job_superseded", FromState: from, ToState: current.Status}); err != nil {
				return err
			}
			if err := refreshProcessingRuns(tx, &current); err != nil {
				return err
			}
		}
		var generation int64
		if err := processingLogicalQuery(tx, input).Select("COALESCE(MAX(generation), 0)").Scan(&generation).Error; err != nil {
			return err
		}
		now := tx.NowFunc().UTC()
		job := &types.ProcessingJob{ID: uuid.NewString(), Kind: input.Kind, TenantID: input.TenantID,
			KnowledgeBaseID: input.KnowledgeBaseID, DataSourceID: input.DataSourceID, ExternalID: input.ExternalID,
			OriginRunID: input.OriginRunID, Generation: generation + 1, ScopeRevision: scope, AuthRevision: auth,
			SourceRevision: input.SourceRevision, SourceDigest: input.SourceDigest, PipelineFingerprint: input.PipelineFingerprint, ConfigurationRevision: configuration,
			IsCurrent: true, Status: types.ProcessingPlanned, Completeness: "unknown", Readiness: "pending",
			RetirementState: "retained", Metadata: input.Metadata, CreatedAt: now, UpdatedAt: now}
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "job_created", ToState: job.Status}); err != nil {
			return err
		}
		result = job
		return nil
	})
	return result, err
}

func processingLogicalQuery(tx *gorm.DB, job types.ProcessingJob) *gorm.DB {
	return tx.Model(&types.ProcessingJob{}).Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ? AND kind = ?",
		job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID, job.Kind)
}

func processingTerminal(status string) bool {
	switch status {
	case types.ProcessingSucceeded, types.ProcessingFailed, types.ProcessingCanceled, types.ProcessingSuperseded, types.ProcessingSkipped:
		return true
	default:
		return false
	}
}

func appendProcessingEvent(tx *gorm.DB, job *types.ProcessingJob, event types.ProcessingEvent) error {
	previous := job.Revision
	updated := tx.Model(&types.ProcessingJob{}).Where("id = ? AND revision = ?", job.ID, previous).
		Updates(map[string]any{"revision": previous + 1, "updated_at": tx.NowFunc().UTC()})
	if updated.Error != nil {
		return updated.Error
	}
	if updated.RowsAffected != 1 {
		return ErrProcessingConflict
	}
	job.Revision++
	event.ID, event.TenantID, event.JobID, event.JobRevision, event.Generation = 0, job.TenantID, job.ID, job.Revision, job.Generation
	if event.RunID == "" {
		event.RunID = job.OriginRunID
	}
	if event.Actor == "" {
		event.Actor = types.TaskInitiatorFromContext(tx.Statement.Context).UserID
	}
	return tx.Create(&event).Error
}

// Leases use the database's clock; application clock drift cannot extend a
// worker's authority. SQLite supports the same semantics in local tests/Lite.
func processingDBTime(tx *gorm.DB) (time.Time, error) {
	if tx.Dialector.Name() == "postgres" {
		var now time.Time
		err := tx.Raw("SELECT clock_timestamp()").Scan(&now).Error
		return now.UTC(), err
	}
	var value string
	if err := tx.Raw("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')").Scan(&value).Error; err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, value)
}

func (r *ProcessingRepository) GetJob(ctx context.Context, tenant uint64, id string) (*types.ProcessingJob, error) {
	var job types.ProcessingJob
	if err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ?", id, tenant).Take(&job).Error; err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *ProcessingRepository) ListEvents(ctx context.Context, tenant uint64, jobID string, after int64, limit int) ([]types.ProcessingEvent, error) {
	if _, err := r.GetJob(ctx, tenant, jobID); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		limit = 100
	}
	var events []types.ProcessingEvent
	err := r.db.WithContext(ctx).Where("tenant_id = ? AND job_id = ? AND id > ?", tenant, jobID, after).Order("id ASC").Limit(limit).Find(&events).Error
	return events, err
}
