package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func (r *ProcessingRepository) HasRollbackReceipt(ctx context.Context, tenant uint64, jobID, requestID, actor string) (bool, error) {
	var event types.ProcessingEvent
	err := r.db.WithContext(ctx).Where("tenant_id = ? AND action = ? AND operation_request_id = ?", tenant, "rollback", requestID).Take(&event).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if event.JobID != jobID || event.Actor != actor {
		return false, ErrProcessingConflict
	}
	return true, nil
}

func (r *ProcessingRepository) PinnedRollbackSnapshot(ctx context.Context, tenant uint64, jobID string) (job *types.ProcessingJob, kb *types.KnowledgeBase, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err = lockProcessingJob(tx, tenant, jobID)
		if err != nil {
			return err
		}
		if !job.RollbackPin || job.RetirementState != "retained" {
			return ErrProcessingConflict
		}
		kb = &types.KnowledgeBase{}
		return tx.Where("id = ? AND tenant_id = ?", job.KnowledgeBaseID, tenant).Take(kb).Error
	})
	if kb != nil {
		kb.EnsureDefaults()
		kb.ApplyDeploymentModelDefaults()
	}
	return
}

// The service pins first, verifies stored outputs outside the transaction, then
// passes the exact verified manifest and revision. Pins and cleanup ownership
// share the source lock; rollback never resurrects a partially deleted version.
func (r *ProcessingRepository) RollbackVersion(ctx context.Context, tenant uint64, jobID string, revision int64, verifiedManifest, requestID, actor string, reasons ...string) error {
	reason, err := processingControlReason(reasons)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, jobID)
		if err != nil {
			return err
		}
		if done, err := processingControlReceipt(tx, job, revision, "rollback", requestID, actor); err != nil || done {
			return err
		}
		if !job.RollbackPin || job.RetirementState != "retained" || job.IsPublished || job.PublishedAt == nil || job.Readiness != "ready" || job.Completeness != "complete" || !job.PlanSealed || job.KnowledgeID == "" || verifiedManifest != job.ActiveIndexManifest {
			return ErrProcessingConflict
		}
		var manifest map[string]types.ProcessingArtifactVersion
		if json.Unmarshal([]byte(verifiedManifest), &manifest) != nil || len(manifest) == 0 {
			return ErrProcessingConflict
		}
		var steps []types.ProcessingStep
		if err := tx.Where("job_id = ?", job.ID).Find(&steps).Error; err != nil {
			return err
		}
		confirmed := map[string]types.ProcessingStep{}
		for _, step := range steps {
			confirmed[step.ID] = step
		}
		for id, version := range manifest {
			step, ok := confirmed[id]
			if !ok || step.Status != types.ProcessingSucceeded || step.Attempt != version.Attempt || step.OutputDigest != version.Digest || step.OutputManifestRef == "" {
				return ErrProcessingConflict
			}
		}
		var candidate types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", job.KnowledgeID, tenant, job.KnowledgeBaseID).Take(&candidate).Error; err != nil {
			return err
		}
		if candidate.GetMetadata()["processing_job_id"] != job.ID {
			return ErrProcessingConflict
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", job.KnowledgeBaseID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		if kb.Type == types.KnowledgeBaseTypeFAQ {
			return rollbackProcessingFAQ(tx, job, now, requestID, actor, reason)
		}
		var epoch int64
		if err := processingLogicalQuery(tx, *job).Select("COALESCE(MAX(publication_epoch), 0)").Scan(&epoch).Error; err != nil {
			return err
		}
		var displaced []types.ProcessingJob
		if err := processingLogicalQuery(tx, *job).Where("id <> ? AND (is_current = ? OR is_published = ?)", job.ID, true, true).Order("id").Find(&displaced).Error; err != nil {
			return err
		}
		for i := range displaced {
			old := &displaced[i]
			old.IsCurrent, old.IsPublished = false, false
			if err := tx.Model(old).Updates(map[string]any{"is_current": false, "is_published": false}).Error; err != nil {
				return err
			}
			if old.KnowledgeID != "" {
				if err := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", old.KnowledgeID, tenant).Update("enable_status", "disabled").Error; err != nil {
					return err
				}
			}
			if err := stopProcessingJob(tx, old, types.ProcessingSuperseded, "VERSION_ROLLBACK"); err != nil {
				return err
			}
			if err := appendProcessingEvent(tx, old, types.ProcessingEvent{Type: "publication_replaced", Actor: actor, Message: "VERSION_ROLLBACK"}); err != nil {
				return err
			}
			if err := refreshProcessingRuns(tx, old); err != nil {
				return err
			}
		}
		deadline := now.Add(24 * time.Hour)
		for i := range steps {
			step := &steps[i]
			if step.Phase == types.ProcessingPhaseRetire {
				step.Status, step.LeaseToken, step.LeaseExpiresAt, step.NextRunAt, step.FinishedAt = types.ProcessingSuperseded, "", nil, nil, &now
			} else if step.Phase == types.ProcessingPhaseProjection {
				step.ExpectedPublicationEpoch = epoch + 1
				// Pure confirmed outputs remain reusable. Shared page/graph applies
				// must be restored under the new epoch, including successful applies.
				shared := step.Stage == "retire_previous" || step.Stage == "wiki" || step.Stage == "wiki_pages" || step.Stage == "wiki_page" || step.Stage == "wiki_links" || step.Stage == "graph" || step.Stage == "graph_apply"
				if step.Status != types.ProcessingSucceeded || shared {
					step.Attempt++
					step.Status, step.LeaseToken, step.LeaseExpiresAt, step.NextRunAt, step.FinishedAt = types.ProcessingPlanned, "", nil, nil, nil
					step.MaxRetries, step.DeadlineAt = step.RetryCount+4, &deadline
					if step.Stage == "retire_previous" {
						step.Result = nil
						step.OutputManifestRef = ""
						step.OutputDigest = ""
					}
					delete(manifest, step.ID)
				}
			} else {
				continue
			}
			if err := tx.Save(step).Error; err != nil {
				return err
			}
		}
		encoded, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		job.IsCurrent, job.IsPublished, job.PublicationEpoch, job.ActiveIndexManifest = true, true, epoch+1, string(encoded)
		job.Status, job.FinishedAt, job.PublishedAt = types.ProcessingRunning, nil, &now
		if err := tx.Model(job).Updates(map[string]any{"is_current": true, "is_published": true, "publication_epoch": job.PublicationEpoch,
			"active_index_manifest": job.ActiveIndexManifest, "status": job.Status, "finished_at": nil, "published_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&candidate).Updates(map[string]any{"enable_status": "enabled", "parse_status": types.ParseStatusFinalizing, "error_message": ""}).Error; err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "version_rolled_back", Action: "rollback", OperationRequestID: requestID, Actor: actor, Message: reason}); err != nil {
			return err
		}
		var incidents []int64
		if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND error_code = ? AND resolves_event_id IS NULL", job.ID, "ROLLBACK_ARTIFACT_UNAVAILABLE").
			Where("id NOT IN (?)", tx.Model(&types.ProcessingEvent{}).Select("resolves_event_id").Where("job_id = ? AND resolves_event_id IS NOT NULL", job.ID)).Pluck("id", &incidents).Error; err != nil {
			return err
		}
		for _, id := range incidents {
			if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "incident_resolved", ResolvesEventID: &id, ResolutionType: "restored_artifact", Actor: actor}); err != nil {
				return err
			}
		}
		if err := refreshProcessingJob(tx, job); err != nil {
			return err
		}
		return scheduleProcessingSteps(tx, job)
	})
}

func (r *ProcessingRepository) RollbackVerificationFailed(ctx context.Context, tenant uint64, jobID, requestID, actor string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, jobID)
		if err != nil {
			return err
		}
		if done, err := processingControlReceipt(tx, job, job.Revision, "rollback_verification_failed", requestID, actor); err != nil || done {
			return err
		}
		return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "rollback_verification_failed", Action: "rollback_verification_failed", OperationRequestID: requestID, Actor: actor,
			ErrorClass: "integrity", ErrorCode: "ROLLBACK_ARTIFACT_UNAVAILABLE", Message: "Retained outputs or index restoration could not be verified; the published version was preserved"})
	})
}
