package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// Deleted is monotonic: neither a pin nor a new publication can resurrect this
// immutable knowledge ID. Periodic deletion catches provider writes whose ACK
// arrived after retirement and after the original worker disappeared.
func (r *ProcessingRepository) RetiredIndexGarbage(ctx context.Context, after string, limit int) ([]types.ProcessingJob, string, error) {
	if limit < 1 || limit > 100 {
		return nil, after, ErrProcessingConflict
	}
	now, err := processingDBTime(r.db.WithContext(ctx))
	if err != nil {
		return nil, after, err
	}
	var jobs []types.ProcessingJob
	err = r.db.WithContext(ctx).Where("id > ? AND kind = ? AND retirement_state = ? AND is_current = ? AND is_published = ? AND rollback_pin = ? AND updated_at < ?", after, types.ProcessingJobDocument, "deleted", false, false, false, now.Add(-7*24*time.Hour)).
		Where("(index_destination IS NOT NULL AND index_destination->>'knowledge_type' <> ?) OR EXISTS (SELECT 1 FROM processing_graph_writes w WHERE w.job_id = processing_jobs.id AND w.tenant_id = processing_jobs.tenant_id)", types.KnowledgeBaseTypeFAQ).Order("id").Limit(limit).Find(&jobs).Error
	next := ""
	if len(jobs) == limit {
		next = jobs[len(jobs)-1].ID
	}
	return jobs, next, err
}

func (r *ProcessingRepository) RetiredGraphWrites(ctx context.Context, tenant uint64, id string) (writes []types.ProcessingGraphWrite, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, id)
		if err != nil {
			return err
		}
		if job.RetirementState != "deleted" || job.IsCurrent || job.IsPublished || job.RollbackPin {
			return ErrProcessingConflict
		}
		return tx.Where("job_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", job.ID, tenant, job.KnowledgeBaseID, job.KnowledgeID).Order("id").Find(&writes).Error
	})
	return
}

func (r *ProcessingRepository) RetiredIndexGarbageDeleted(ctx context.Context, tenant uint64, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, id)
		if err != nil {
			return err
		}
		if job.RetirementState != "deleted" || job.IsCurrent || job.IsPublished || job.RollbackPin {
			return ErrProcessingConflict
		}
		return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "retired_indexes_reconciled", Actor: "system:lifecycle"})
	})
}
