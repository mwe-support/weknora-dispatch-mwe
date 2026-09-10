package repository

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func (r *knowledgeRepository) CheckKnowledgeDeletion(ctx context.Context, tenant uint64, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	var blocked int64
	err := r.db.WithContext(ctx).Table("knowledges AS k").Joins("LEFT JOIN processing_jobs j ON j.knowledge_id=k.id AND j.tenant_id=k.tenant_id AND j.knowledge_base_id=k.knowledge_base_id AND j.id=k.metadata->>'processing_job_id'").
		Where("k.tenant_id=? AND k.id IN ? AND k.deleted_at IS NULL AND k.metadata->>'processing_protocol'='2' AND (j.id IS NULL OR j.rollback_pin=?)", tenant, ids, true).Count(&blocked).Error
	if err != nil {
		return err
	}
	if blocked > 0 {
		return ErrProcessingConflict
	}
	return nil
}

// The existing delete API targets immutable knowledge IDs. Revoke their ledger
// authority and create cleanup in the same transaction, before any storage I/O.
func (r *knowledgeRepository) deleteKnowledgeRows(ctx context.Context, tenant uint64, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var managed []types.Knowledge
		if err := tx.Unscoped().Where("tenant_id = ? AND id IN ? AND metadata->>'processing_protocol' = '2'", tenant, ids).
			Order("knowledge_base_id, id").Find(&managed).Error; err != nil {
			return err
		}
		// Acquire shared KB/source locks in the same order across batch requests.
		var jobs []types.ProcessingJob
		if len(managed) > 0 {
			if err := tx.Where("tenant_id = ? AND knowledge_id IN ?", tenant, ids).Order("knowledge_base_id, datasource_id, id").Find(&jobs).Error; err != nil {
				return err
			}
			if len(jobs) != len(managed) {
				return ErrProcessingConflict
			}
		}
		for _, candidate := range jobs {
			job, err := lockProcessingRetirement(tx, tenant, candidate.ID)
			if err != nil {
				return err
			}
			if err := deleteProcessingKnowledge(tx, job, "KNOWLEDGE_DELETED"); err != nil {
				return err
			}
		}
		return tx.Scopes(LegacyKnowledge).Where("tenant_id = ? AND id IN ?", tenant, ids).Delete(&types.Knowledge{}).Error
	})
}

// Caller holds KB -> source -> job locks. A rollback pin is an explicit hold;
// deleting a document or its KB cannot silently discard it. Artifact consumers
// can keep the hidden version alive until their own retirement releases it.
func deleteProcessingKnowledge(tx *gorm.DB, job *types.ProcessingJob, reason string) error {
	if job.RollbackPin {
		return ErrProcessingConflict
	}
	requestID := fmt.Sprintf("delete-%s-%d", job.ID, job.PublicationEpoch)
	var receipts int64
	if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND action = ? AND operation_request_id = ?", job.ID, "delete", requestID).Count(&receipts).Error; err != nil {
		return err
	}
	if receipts > 0 || job.RetirementState == "deleted" {
		return nil
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return err
	}
	job.IsCurrent, job.IsPublished = false, false
	if err := tx.Model(job).Updates(map[string]any{"is_current": false, "is_published": false}).Error; err != nil {
		return err
	}
	if processingTerminal(job.Status) {
		if err := stopProcessingSteps(tx, job, types.ProcessingCanceled, reason, now); err != nil {
			return err
		}
	} else if err := stopProcessingJob(tx, job, types.ProcessingCanceled, reason); err != nil {
		return err
	}
	if job.KnowledgeID != "" {
		if err := tx.Unscoped().Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ? AND metadata->>'processing_job_id' = ?", job.KnowledgeID, job.TenantID, job.ID).
			Updates(map[string]any{"deleted_at": now, "enable_status": "disabled", "parse_status": types.ParseStatusDeleting}).Error; err != nil {
			return err
		}
	}
	actor := types.TaskInitiatorFromContext(tx.Statement.Context).UserID
	if actor == "" {
		actor = "system:lifecycle"
	}
	if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "knowledge_delete_requested", Action: "delete", OperationRequestID: requestID, Actor: actor, Message: reason}); err != nil {
		return err
	}
	if job.Kind == types.ProcessingJobDocument && job.RetirementState == "retained" {
		if err := planProcessingRetirement(tx, job, job.Revision, "retire-"+requestID, actor); err != nil {
			return err
		}
	}
	return refreshProcessingRuns(tx, job)
}
