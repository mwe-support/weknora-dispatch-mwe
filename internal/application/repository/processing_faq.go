package repository

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type processingFAQConflict struct{ Entries []string }

func (*processingFAQConflict) Error() string { return "FAQ entries changed before source publication" }

// No canonical row changes until every target revision and every immutable
// index receipt in the source batch has passed under the shared FAQ lock.
func commitProcessingFAQ(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, mutations []types.ProcessingFAQMutation) error {
	if step.Stage != "publish" || step.Phase != types.ProcessingPhasePublish {
		return ErrProcessingConflict
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", job.KnowledgeBaseID, job.TenantID).Take(&kb).Error; err != nil {
		return err
	}
	if kb.Type != types.KnowledgeBaseTypeFAQ {
		if len(mutations) > 0 {
			return ErrProcessingScope
		}
		return nil
	}
	if len(mutations) == 0 {
		return errors.New("FAQ publication requires complete entry coverage")
	}
	if err := lockFAQKnowledgeBase(tx, job.TenantID, job.KnowledgeBaseID); err != nil {
		return err
	}
	var roots []types.ProcessingStep
	if err := tx.Where("job_id = ? AND stage = ? AND status = ?", job.ID, "faq_entry", types.ProcessingSucceeded).Find(&roots).Error; err != nil {
		return err
	}
	if len(roots) != len(mutations) {
		return ErrProcessingConflict
	}
	expected := map[string]int{}
	for _, root := range roots {
		if !root.PlanSealed {
			return ErrProcessingConflict
		}
		expected[root.ID] = root.Attempt
	}
	conflicts := map[string]bool{}
	questions := map[string]string{}
	chunks := map[string]bool{}
	for _, mutation := range mutations {
		chunk := mutation.Chunk
		if chunk == nil || chunk.ID == "" || chunk.TenantID != job.TenantID || chunk.KnowledgeBaseID != job.KnowledgeBaseID || chunk.ChunkType != types.ChunkTypeFAQ ||
			expected[mutation.EntryStepID] != mutation.Attempt || mutation.Attempt < 1 || chunks[chunk.ID] || mutation.Manifest == "" {
			return ErrProcessingScope
		}
		delete(expected, mutation.EntryStepID)
		chunks[chunk.ID] = true
		var canonical types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?", chunk.KnowledgeID, job.TenantID, job.KnowledgeBaseID, types.KnowledgeTypeFAQ).Take(&canonical).Error; err != nil {
			return err
		}
		var actual types.Chunk
		err := tx.Unscoped().Where("id = ?", chunk.ID).Take(&actual).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if (mutation.NewEntry && !errors.Is(err, gorm.ErrRecordNotFound)) || (!mutation.NewEntry && (err != nil || actual.DeletedAt.Valid || actual.TenantID != job.TenantID ||
			actual.KnowledgeBaseID != job.KnowledgeBaseID || actual.KnowledgeID != chunk.KnowledgeID || actual.ContentRevision != chunk.ContentRevision || actual.ChunkType != types.ChunkTypeFAQ)) {
			conflicts[mutation.EntryStepID] = true
		}
		meta, err := chunk.FAQMetadata()
		if err != nil || meta == nil || meta.StandardQuestion == "" || len(meta.Answers) == 0 {
			return ErrFAQQuestionConflict
		}
		positives := append([]string{meta.StandardQuestion}, meta.SimilarQuestions...)
		for _, negative := range meta.NegativeQuestions {
			if slices.Contains(positives, negative) {
				return ErrFAQQuestionConflict
			}
		}
		for _, q := range positives {
			if other := questions[q]; other != "" {
				conflicts[mutation.EntryStepID], conflicts[other] = true, true
			}
			questions[q] = mutation.EntryStepID
		}
		if err := checkStoredFAQQuestions(tx, chunk); err != nil {
			if !errors.Is(err, ErrFAQQuestionConflict) {
				return err
			}
			conflicts[mutation.EntryStepID] = true
		}
		chunk.FAQIndexManifest = mutation.Manifest
		if err := validateFAQIndexManifest(tx, chunk); err != nil {
			return err
		}
	}
	if len(conflicts) > 0 {
		ids := make([]string, 0, len(conflicts))
		for id := range conflicts {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		return &processingFAQConflict{Entries: ids}
	}
	for _, mutation := range mutations {
		chunk := *mutation.Chunk
		var err error
		if mutation.NewEntry {
			err = NewChunkRepository(tx).CreateChunks(tx.Statement.Context, []*types.Chunk{&chunk})
		} else {
			// Public Chunk JSON omits immutable source fields. A proposal changes
			// only FAQ-owned fields, preserving source data and manual edit history.
			err = (&chunkRepository{db: tx}).saveChunkSnapshots(tx.Statement.Context, []*types.Chunk{&chunk},
				[]string{"content", "metadata", "content_hash", "is_enabled", "flags", "tag_id", "status", "index_status", "updated_at"})
		}
		if err != nil {
			return err
		}
		refs := slices.Clone(mutation.ResourceRefs)
		slices.Sort(refs)
		refs = slices.Compact(refs)
		for _, ref := range refs {
			handle, ok := types.ParseResourcePath(ref)
			if !ok {
				return ErrProcessingScope
			}
			var resource types.StoredResource
			if err := tx.Where("tenant_id = ? AND handle = ?", job.TenantID, handle).Take(&resource).Error; err != nil {
				return err
			}
			var owned int64
			if err := tx.Model(&types.ResourceBinding{}).Where("tenant_id = ? AND resource_id = ? AND owner_type = ? AND owner_id = ?", job.TenantID, resource.ID, "processing_job", job.ID).Count(&owned).Error; err != nil {
				return err
			}
			if owned == 0 {
				return ErrProcessingScope
			}
			// The shared chunk writer has atomically bound the current answer.
		}
	}
	return nil
}

// Preserve receipts from retired children for content-addressed embedding
// reuse. Only the conflicting entry and its aggregate lose active coverage.
func resetProcessingFAQEntries(tx *gorm.DB, job *types.ProcessingJob, ids []string, now time.Time, conflict bool) error {
	if len(ids) == 0 {
		return ErrProcessingConflict
	}
	var aggregate types.ProcessingStep
	if err := tx.Where("job_id = ? AND stage = ? AND unit_key = ?", job.ID, "faq_index", "body").Take(&aggregate).Error; err != nil {
		return err
	}
	ids = slices.Clone(ids)
	slices.Sort(ids)
	for _, id := range slices.Compact(ids) {
		var root types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ? AND parent_step_id = ? AND stage = ?", id, job.ID, aggregate.ID, "faq_entry").Take(&root).Error; err != nil {
			return err
		}
		var children []types.ProcessingStep
		if err := tx.Where("job_id = ? AND parent_step_id = ?", job.ID, root.ID).Find(&children).Error; err != nil {
			return err
		}
		removed := map[string]bool{}
		for _, child := range children {
			removed[child.ID] = true
			event := stepEvent(&child, "faq_unit_invalidated", child.Status)
			event.ToState = types.ProcessingSuperseded
			if err := appendProcessingEvent(tx, job, event); err != nil {
				return err
			}
			if err := tx.Model(&child).Updates(map[string]any{"status": types.ProcessingSuperseded, "parent_step_id": "", "required_for_ready": false, "required_for_completion": false,
				"lease_token": "", "lease_expires_at": nil, "next_run_at": nil, "finished_at": now}).Error; err != nil {
				return err
			}
		}
		var dependencies []string
		if len(root.Dependencies) > 0 && json.Unmarshal(root.Dependencies, &dependencies) != nil {
			return ErrProcessingConflict
		}
		dependencies = slices.DeleteFunc(dependencies, func(id string) bool { return removed[id] })
		root.Dependencies, _ = json.Marshal(dependencies)
		event := stepEvent(&root, "faq_entry_changed", root.Status)
		previousRetries := root.RetryCount
		root.Status = types.ProcessingRetryWait
		due := now.Add(processingRetryDelay(root.ID, root.RetryCount))
		root.NextRunAt = &due
		if root.RetryCount >= root.MaxRetries || (root.DeadlineAt != nil && !due.Before(*root.DeadlineAt)) {
			root.Status = types.ProcessingFailed
			root.NextRunAt = nil
			root.FinishedAt = &now
		} else {
			root.RetryCount++
			root.FinishedAt = nil
		}
		root.Attempt++
		root.PlanSealed, root.PlanDigest, root.ExpectedUnits = false, "", 0
		root.CheckpointRef, root.OutputManifestRef, root.OutputDigest = "", "", ""
		root.Result = nil
		root.LeaseToken, root.LeaseExpiresAt = "", nil
		root.ErrorClass, root.ErrorCode, root.ErrorMessage = "conflict", "FAQ_ENTRY_CHANGED", "The FAQ changed; retry merges this entry with its current revision"
		if !conflict {
			deadline := now.Add(24 * time.Hour)
			root.Status, root.NextRunAt, root.FinishedAt = types.ProcessingPlanned, nil, nil
			root.RetryCount, root.DeadlineAt, root.MaxRetries = previousRetries, &deadline, previousRetries+4
			root.LastErrorEventID = nil
			root.ErrorClass, root.ErrorCode, root.ErrorMessage = "", "", ""
			event.Type = "faq_entry_remerge"
		}
		event.ToState, event.ErrorClass, event.ErrorCode, event.Message = root.Status, root.ErrorClass, root.ErrorCode, root.ErrorMessage
		if err := appendProcessingEvent(tx, job, event); err != nil {
			return err
		}
		if conflict {
			var incident types.ProcessingEvent
			if err := tx.Where("job_id = ? AND job_revision = ?", job.ID, job.Revision).Take(&incident).Error; err != nil {
				return err
			}
			root.LastErrorEventID = &incident.ID
		}
		if err := tx.Save(&root).Error; err != nil {
			return err
		}
	}
	// Its child identities and sealed plan are unchanged; successful siblings
	// remain dependencies and are never executed again.
	event := stepEvent(&aggregate, "faq_coverage_invalidated", aggregate.Status)
	aggregate.Status, aggregate.OutputManifestRef, aggregate.OutputDigest = types.ProcessingPlanned, "", ""
	aggregate.Attempt++
	aggregate.LeaseToken, aggregate.LeaseExpiresAt, aggregate.NextRunAt, aggregate.FinishedAt = "", nil, nil, nil
	if !conflict {
		deadline := now.Add(24 * time.Hour)
		aggregate.DeadlineAt, aggregate.MaxRetries = &deadline, aggregate.RetryCount+4
	}
	event.ToState = aggregate.Status
	if err := appendProcessingEvent(tx, job, event); err != nil {
		return err
	}
	return tx.Save(&aggregate).Error
}

// Reapply the retained source's validated payload against today's canonical
// entries. The currently published source remains active until that new merge
// and its index receipts commit. Its publication epoch advances at that point.
func rollbackProcessingFAQ(tx *gorm.DB, job *types.ProcessingJob, now time.Time, requestID, actor, reason string) error {
	var displaced []types.ProcessingJob
	if err := processingLogicalQuery(tx, *job).Where("id <> ? AND is_current = ?", job.ID, true).Order("id").Find(&displaced).Error; err != nil {
		return err
	}
	for i := range displaced {
		old := &displaced[i]
		old.IsCurrent = false
		if err := tx.Model(old).Update("is_current", false).Error; err != nil {
			return err
		}
		if !old.IsPublished {
			if err := stopProcessingJob(tx, old, types.ProcessingSuperseded, "FAQ_ROLLBACK_REQUESTED"); err != nil {
				return err
			}
		}
		if err := appendProcessingEvent(tx, old, types.ProcessingEvent{Type: "current_replaced", Actor: actor, Message: "FAQ_ROLLBACK_REQUESTED"}); err != nil {
			return err
		}
		if err := refreshProcessingRuns(tx, old); err != nil {
			return err
		}
	}
	var ids []string
	if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND stage = ?", job.ID, "faq_entry").Pluck("id", &ids).Error; err != nil {
		return err
	}
	if err := resetProcessingFAQEntries(tx, job, ids, now, false); err != nil {
		return err
	}
	var publish types.ProcessingStep
	if err := tx.Where("job_id = ? AND stage = ? AND unit_key = ?", job.ID, "publish", "body").Take(&publish).Error; err != nil {
		return err
	}
	publish.Attempt++
	publish.Status, publish.LeaseToken, publish.OutputManifestRef, publish.OutputDigest = types.ProcessingPlanned, "", "", ""
	publish.LeaseExpiresAt, publish.NextRunAt, publish.FinishedAt = nil, nil, nil
	publish.ErrorClass, publish.ErrorCode, publish.ErrorMessage = "", "", ""
	deadline := now.Add(24 * time.Hour)
	publish.DeadlineAt, publish.MaxRetries = &deadline, publish.RetryCount+4
	if err := tx.Save(&publish).Error; err != nil {
		return err
	}
	if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND stage = ?", job.ID, "retire_previous").Updates(map[string]any{
		"status": types.ProcessingPlanned, "step_attempt": gorm.Expr("step_attempt + 1"), "result": nil, "output_manifest_ref": "", "output_digest": "",
		"lease_token": "", "lease_expires_at": nil, "next_run_at": nil, "finished_at": nil, "deadline_at": deadline, "max_retries": gorm.Expr("retry_count + 4"),
		"error_class": "", "error_code": "", "error_message": "",
	}).Error; err != nil {
		return err
	}
	if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND phase = ?", job.ID, types.ProcessingPhaseRetire).
		Updates(map[string]any{"status": types.ProcessingSuperseded, "lease_token": "", "lease_expires_at": nil, "next_run_at": nil, "finished_at": now}).Error; err != nil {
		return err
	}
	job.IsCurrent, job.Status, job.Readiness, job.FinishedAt = true, types.ProcessingRunning, "pending", nil
	if err := tx.Model(job).Updates(map[string]any{"is_current": true, "status": job.Status, "readiness": job.Readiness, "finished_at": nil}).Error; err != nil {
		return err
	}
	if err := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", job.KnowledgeID, job.TenantID).
		Updates(map[string]any{"enable_status": "disabled", "parse_status": types.ParseStatusProcessing}).Error; err != nil {
		return err
	}
	if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "faq_rollback_requested", Action: "rollback", OperationRequestID: requestID, Actor: actor, Message: reason}); err != nil {
		return err
	}
	return scheduleProcessingSteps(tx, job)
}

// Retirement owns only this source's immutable index attempts. Canonical FAQ
// manifests may retain those attempts after the entire source job is removed.
func (r *ProcessingRepository) RetirementFAQIndexWrites(ctx context.Context, tenant uint64, lease types.ProcessingLease) (writes []types.FAQIndexWrite, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if lease.Step.Phase != types.ProcessingPhaseRetire || job.RetirementState != "deleting" {
			return ErrProcessingConflict
		}
		if err := lockFAQKnowledgeBase(tx.Unscoped(), tenant, job.KnowledgeBaseID); err != nil {
			return err
		}
		var candidates []types.FAQIndexWrite
		if err := tx.Where("tenant_id = ? AND job_id = ? AND state <> ?", tenant, job.ID, "deleted").Order("id").Find(&candidates).Error; err != nil {
			return err
		}
		for _, write := range candidates {
			held, err := faqIndexWriteReferenced(tx, write)
			if err != nil {
				return err
			}
			if held {
				continue
			}
			if err := tx.Model(&write).Update("state", "deleting").Error; err != nil {
				return err
			}
			write.State = "deleting"
			writes = append(writes, write)
		}
		return nil
	})
	return
}

func processingFAQRetired(tx *gorm.DB, job *types.ProcessingJob) error {
	var writes []types.FAQIndexWrite
	if err := tx.Where("tenant_id = ? AND job_id = ? AND state <> ?", job.TenantID, job.ID, "deleted").Find(&writes).Error; err != nil {
		return err
	}
	if len(writes) == 0 {
		return nil
	}
	if err := lockFAQKnowledgeBase(tx.Unscoped(), job.TenantID, job.KnowledgeBaseID); err != nil {
		return err
	}
	for _, write := range writes {
		held, err := faqIndexWriteReferenced(tx, write)
		if err != nil {
			return err
		}
		if !held {
			return ErrProcessingConflict
		}
	}
	return nil
}
