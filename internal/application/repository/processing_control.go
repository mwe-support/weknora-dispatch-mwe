package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func lockProcessingSource(tx *gorm.DB, id string) (*types.DataSource, error) {
	query := tx.Unscoped().Where("id = ?", id)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var source types.DataSource
	err := query.Take(&source).Error
	return &source, err
}

// The caller holds the source lock. Source edits and invalidation commit
// together, so a pause followed by resume cannot revive an old delivery.
func invalidateProcessingSource(tx *gorm.DB, source *types.DataSource, status, reason string) error {
	var jobs []types.ProcessingJob
	if err := tx.Where("datasource_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND status NOT IN ?", source.ID, source.TenantID, source.KnowledgeBaseID,
		[]string{types.ProcessingSucceeded, types.ProcessingCanceled, types.ProcessingSuperseded, types.ProcessingSkipped}).Order("id").Find(&jobs).Error; err != nil {
		return err
	}
	for i := range jobs {
		if err := stopProcessingJob(tx, &jobs[i], status, reason); err != nil {
			return err
		}
	}
	// Project only after every member has been stopped; otherwise a run could
	// freeze an intermediate snapshot halfway through the same source edit.
	for i := range jobs {
		if err := refreshProcessingRuns(tx, &jobs[i]); err != nil {
			return err
		}
	}
	return nil
}

func stopProcessingJob(tx *gorm.DB, job *types.ProcessingJob, status, reason string) error {
	if job.Status == types.ProcessingCanceled || job.Status == types.ProcessingSuperseded || job.Status == types.ProcessingSucceeded {
		return nil
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return err
	}
	if err := stopProcessingSteps(tx, job, status, reason, now); err != nil {
		return err
	}
	from := job.Status
	job.Status, job.FinishedAt = status, &now
	if err := tx.Model(job).Updates(map[string]any{"status": status, "finished_at": now}).Error; err != nil {
		return err
	}
	if job.KnowledgeID != "" {
		parseStatus := types.ParseStatusCancelled
		if job.IsPublished {
			parseStatus = types.ParseStatusCompleted
		}
		if err := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ? AND metadata->>'processing_job_id' = ?", job.KnowledgeID, job.TenantID, job.ID).
			Updates(map[string]any{"parse_status": parseStatus, "pending_subtasks_count": 0}).Error; err != nil {
			return err
		}
	}
	return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "job_invalidated", FromState: from, ToState: status, Message: reason})
}

// CancelProcessingKnowledge bridges the existing per-document cancel API. It
// stops only this version, preserving any published retrieval artifacts. Source
// serialization makes cancellation and every lease commit mutually exclusive.
func (r *knowledgeRepository) CancelProcessingKnowledge(ctx context.Context, tenant uint64, id string) (*types.Knowledge, error) {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job types.ProcessingJob
		if err := tx.Where("knowledge_id = ? AND tenant_id = ?", id, tenant).Take(&job).Error; err != nil {
			return err
		}
		if _, err := lockProcessingSource(tx, job.DataSourceID); err != nil {
			return err
		}
		if err := tx.Where("id = ? AND tenant_id = ?", job.ID, tenant).Take(&job).Error; err != nil {
			return err
		}
		if job.Status == types.ProcessingCanceled {
			return nil
		}
		if job.Status == types.ProcessingSucceeded || job.Status == types.ProcessingSuperseded {
			return ErrProcessingConflict
		}
		if err := stopProcessingJob(tx, &job, types.ProcessingCanceled, "USER_CANCELED"); err != nil {
			return err
		}
		return refreshProcessingRuns(tx, &job)
	})
	if err != nil {
		return nil, err
	}
	return r.GetKnowledgeByID(ctx, tenant, id)
}

func stopProcessingSteps(tx *gorm.DB, job *types.ProcessingJob, status, reason string, now time.Time) error {
	var steps []types.ProcessingStep
	if err := tx.Where("job_id = ? AND phase <> ? AND status NOT IN ?", job.ID, types.ProcessingPhaseRetire,
		[]string{types.ProcessingSucceeded, types.ProcessingCanceled, types.ProcessingSuperseded, types.ProcessingSkipped}).Order("id").Find(&steps).Error; err != nil {
		return err
	}
	for i := range steps {
		step := &steps[i]
		event := stepEvent(step, "step_invalidated", step.Status)
		event.ToState, event.Message = status, reason
		step.Status, step.LeaseToken, step.LeaseExpiresAt, step.NextRunAt, step.FinishedAt = status, "", nil, nil, &now
		// Cancellation cannot prove that an in-flight export was not accepted.
		// Keep this external obligation open even though its job is stopped.
		uncertain := step.ErrorCode == "EXPORT_START_UNCERTAIN" || (step.Stage == "export_start" && event.FromState == types.ProcessingRunning)
		if uncertain {
			step.Status, step.ErrorClass, step.ErrorCode = types.ProcessingBlocked, "uncertain", "EXPORT_START_UNCERTAIN"
			event.ToState, event.ErrorClass, event.ErrorCode = step.Status, step.ErrorClass, step.ErrorCode
		}
		if err := appendProcessingEvent(tx, job, event); err != nil {
			return err
		}
		if uncertain {
			var id int64
			if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND job_revision = ?", job.ID, job.Revision).Pluck("id", &id).Error; err != nil {
				return err
			}
			step.LastErrorEventID = &id
		}
		if err := tx.Save(step).Error; err != nil {
			return err
		}
		if !uncertain {
			var incidents []int64
			if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND step_id = ? AND error_class <> '' AND resolves_event_id IS NULL", job.ID, step.ID).
				Where("id NOT IN (?)", tx.Model(&types.ProcessingEvent{}).Select("resolves_event_id").Where("resolves_event_id IS NOT NULL")).Order("id").Pluck("id", &incidents).Error; err != nil {
				return err
			}
			for _, incident := range incidents {
				if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "incident_resolved", StepID: step.ID, Attempt: step.Attempt, ResolvesEventID: &incident, ResolutionType: "scope_canceled", Message: reason}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Called only after validation reports a missing or changed scope, never for
// database/transport errors. Tombstones remain readable for closing the ledger.
func reconcileInvalidProcessingScope(tx *gorm.DB, tenant uint64, id string) error {
	var job types.ProcessingJob
	if err := tx.Where("id = ? AND tenant_id = ?", id, tenant).Take(&job).Error; err != nil {
		return err
	}
	source, err := lockProcessingSource(tx, job.DataSourceID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	// Source serialization also protects event revisions from other workers.
	if err := tx.Where("id = ? AND tenant_id = ?", id, tenant).Take(&job).Error; err != nil {
		return err
	}
	status, reason := types.ProcessingSuperseded, "PROCESSING_CONFIGURATION_CHANGED"
	if source.ID == "" || source.DeletedAt.Valid || source.Status == types.DataSourceStatusDeleted {
		status, reason = types.ProcessingCanceled, "SOURCE_DELETED"
	} else if source.Status == types.DataSourceStatusPaused {
		status, reason = types.ProcessingCanceled, "SOURCE_PAUSED"
	}
	if err := stopProcessingJob(tx, &job, status, reason); err != nil {
		return err
	}
	return refreshProcessingRuns(tx, &job)
}

func processingSourceEditReason(before, after *types.DataSource) (string, string, error) {
	if after.DeletedAt.Valid || after.Status == types.DataSourceStatusDeleted {
		return types.ProcessingCanceled, "SOURCE_DELETED", nil
	}
	if after.Status == types.DataSourceStatusPaused {
		return types.ProcessingCanceled, "SOURCE_PAUSED", nil
	}
	// Skip harmless display/counter changes without parsing legacy credentials.
	if before.Type == after.Type && string(before.Config) == string(after.Config) {
		return "", "", nil
	}
	scope, auth, err := ProcessingSourceRevisions(after)
	if err != nil {
		return "", "", err
	}
	oldScope, oldAuth, err := ProcessingSourceRevisions(before)
	if err != nil {
		// Repairing an unreadable old configuration must still be possible.
		return types.ProcessingSuperseded, "SOURCE_CONFIGURATION_CHANGED", nil
	}
	if scope != oldScope || auth != oldAuth {
		return types.ProcessingSuperseded, "SOURCE_CONFIGURATION_CHANGED", nil
	}
	return "", "", nil
}
