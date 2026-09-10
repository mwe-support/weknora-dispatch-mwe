package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func (r *ProcessingRepository) ClaimStep(ctx context.Context, tenant uint64, ref types.ProcessingRef, duration time.Duration) (*types.ProcessingLease, error) {
	if duration <= 0 || duration > 10*time.Minute {
		return nil, errors.New("invalid processing lease duration")
	}
	var lease *types.ProcessingLease
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, ref.JobID, ref.StepID)
		if err != nil {
			return err
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", ref.StepID, job.ID).Take(&step).Error; err != nil {
			return err
		}
		if !processingRefMatches(job, &step, ref) || !processingStepEligible(job, &step) ||
			(step.Status != types.ProcessingEnqueuePending && step.Status != types.ProcessingQueued) {
			return ErrProcessingConflict
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if step.NextRunAt != nil && step.NextRunAt.After(now) {
			return ErrProcessingConflict
		}
		if step.DeadlineAt != nil && !step.DeadlineAt.After(now) {
			return ErrProcessingConflict
		}
		var steps []types.ProcessingStep
		if err := tx.Where("job_id = ?", job.ID).Find(&steps).Error; err != nil {
			return err
		}
		ready, err := processingDependenciesDone(&step, steps)
		if err != nil {
			return err
		}
		if !ready {
			return ErrProcessingConflict
		}
		if step.Stage == "retire" && step.Phase == types.ProcessingPhaseRetire {
			if err := processingWikiRetired(tx, job); err != nil {
				return err
			}
			if err := processingRetirementUnreferenced(tx, job); err != nil {
				return err
			}
		}
		from := step.Status
		expires := now.Add(duration)
		if step.DeadlineAt != nil && step.DeadlineAt.Before(expires) {
			expires = *step.DeadlineAt
		}
		step.Status, step.LeaseToken = types.ProcessingRunning, uuid.NewString()
		step.LeaseExpiresAt, step.HeartbeatAt, step.ProgressAt, step.StartedAt = &expires, &now, &now, &now
		if err := tx.Save(&step).Error; err != nil {
			return err
		}
		if job.Status == types.ProcessingPlanned {
			job.Status = types.ProcessingRunning
			if err := tx.Model(job).Update("status", job.Status).Error; err != nil {
				return err
			}
		}
		if step.Stage == "retire" && step.Phase == types.ProcessingPhaseRetire {
			job.RetirementState = "deleting"
			if err := tx.Model(job).Update("retirement_state", job.RetirementState).Error; err != nil {
				return err
			}
		}
		if err := appendProcessingEvent(tx, job, stepEvent(&step, "step_started", from)); err != nil {
			return err
		}
		lease = &types.ProcessingLease{Ref: ref, Token: step.LeaseToken, ExpiresAt: expires, Job: *job, Step: step}
		return nil
	})
	return lease, err
}

func (r *ProcessingRepository) FinishStep(ctx context.Context, tenant uint64, lease types.ProcessingLease, outcome types.ProcessingOutcome) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, lease.Ref.JobID, lease.Ref.StepID)
		if err != nil {
			return err
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", lease.Ref.StepID, job.ID).Take(&step).Error; err != nil {
			return err
		}
		// The append-only completion is a receipt for a lost ACK, including when
		// retry scheduling has already advanced the attempt.
		if lease.Token == "" {
			return ErrProcessingConflict
		}
		var receipts int64
		if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND step_id = ? AND step_attempt = ? AND dispatch_seq = ? AND lease_token = ? AND event_type = ?",
			job.ID, step.ID, lease.Ref.Attempt, lease.Ref.DispatchSeq, lease.Token, "step_committed").Count(&receipts).Error; err != nil {
			return err
		}
		if receipts > 0 {
			return nil
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if !processingRefMatches(job, &step, lease.Ref) || !processingStepEligible(job, &step) || step.Status != types.ProcessingRunning ||
			step.LeaseToken != lease.Token || step.LeaseExpiresAt == nil || !step.LeaseExpiresAt.After(now) {
			return ErrProcessingConflict
		}
		return finishProcessingStep(tx, job, &step, outcome, now, "step_committed")
	})
}

func finishProcessingStep(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, outcome types.ProcessingOutcome, now time.Time, eventType string) error {
	if step.Stage == "retire_previous" && outcome.Status == types.ProcessingSucceeded {
		var err error
		outcome, err = finishProcessingPreviousRetirement(tx, job, step, now)
		if err != nil {
			return err
		}
	}
	if len(outcome.FAQConflicts) > 0 {
		if step.Stage != "faq_write" || len(outcome.FAQConflicts) != 1 || outcome.FAQConflicts[0] != step.ParentStepID {
			return ErrProcessingConflict
		}
		if err := resetProcessingFAQEntries(tx, job, outcome.FAQConflicts, now, true); err != nil {
			return err
		}
		event := stepEvent(step, eventType, step.Status)
		event.ToState = types.ProcessingSuperseded
		if err := appendProcessingEvent(tx, job, event); err != nil {
			return err
		}
		if err := refreshProcessingJob(tx, job); err != nil {
			return err
		}
		if err := refreshProcessingRuns(tx, job); err != nil {
			return err
		}
		return scheduleProcessingSteps(tx, job)
	}
	if outcome.Status == types.ProcessingSucceeded && step.Phase == types.ProcessingPhasePublish {
		err := tx.Transaction(func(faqTx *gorm.DB) error { return commitProcessingFAQ(faqTx, job, step, outcome.FAQMutations) })
		var conflict *processingFAQConflict
		if errors.As(err, &conflict) {
			if err := resetProcessingFAQEntries(tx, job, conflict.Entries, now, true); err != nil {
				return err
			}
			outcome = types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "conflict", ErrorCode: "FAQ_ENTRY_CHANGED", Message: "A FAQ entry changed; retry keeps other entries and confirmed embedding batches", Retryable: true}
		} else if err != nil {
			return err
		}
	}
	if outcome.Status == types.ProcessingSucceeded && (step.Stage == "wiki_page" || step.Stage == "wiki_links" || step.Stage == "wiki_retire_page" || outcome.WikiPage != nil) {
		// A savepoint also undoes any page/backlink writes before a late CAS
		// conflict. The retry event and checkpoint then commit together.
		err := tx.Transaction(func(pageTx *gorm.DB) error {
			return commitProcessingWiki(pageTx, job, step, outcome.WikiPage, outcome, now)
		})
		if errors.Is(err, ErrWikiPageConflict) {
			outcome = types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "conflict", ErrorCode: "WIKI_PAGE_CHANGED",
				Message: "The shared page changed; retry merges this page with its current revision", Retryable: true}
		} else if err != nil {
			return err
		}
	}
	event := stepEvent(step, eventType, step.Status)
	if err := commitProcessingDiscovery(tx, job, step, outcome); err != nil {
		return err
	}
	if outcome.CheckpointRef != "" {
		step.CheckpointRef = outcome.CheckpointRef
	}
	if len(outcome.Result) > 0 {
		step.Result = outcome.Result
	}
	step.ErrorClass, step.ErrorCode, step.ErrorMessage = outcome.ErrorClass, outcome.ErrorCode, outcome.Message
	step.NextRunAt = nil
	switch outcome.Status {
	case types.ProcessingSucceeded:
		if outcome.OutputManifestRef == "" || outcome.OutputDigest == "" {
			return errors.New("processing success requires a verified output manifest and digest")
		}
		if outcome.SealPlan {
			if len(outcome.ChildSteps) != 0 {
				return errors.New("a successful barrier cannot register unfinished work")
			}
			if err := sealProcessingChildren(tx, job, step, nil); err != nil {
				return err
			}
		}
		if step.Kind == "barrier" && !step.PlanSealed {
			return errors.New("unsealed processing barrier")
		}
		step.OutputManifestRef, step.OutputDigest = outcome.OutputManifestRef, outcome.OutputDigest
		step.Status, step.FinishedAt = types.ProcessingSucceeded, &now
		if step.Stage == "graph_apply" || outcome.GraphWriteID != "" {
			if err := commitProcessingGraph(tx, job, step, outcome.GraphWriteID); err != nil {
				return err
			}
		}
		if outcome.Candidate != nil {
			if err := prepareProcessingCandidate(tx, job, outcome.Candidate); err != nil {
				return err
			}
		}
		if len(outcome.Chunks) > 0 {
			if job.KnowledgeID == "" {
				return errors.New("processing chunks require a candidate")
			}
			for _, chunk := range outcome.Chunks {
				if chunk == nil || chunk.TenantID != job.TenantID || chunk.KnowledgeBaseID != job.KnowledgeBaseID || chunk.KnowledgeID != job.KnowledgeID {
					return ErrProcessingScope
				}
			}
			if err := NewChunkRepository(tx).CreateChunks(tx.Statement.Context, outcome.Chunks); err != nil {
				return err
			}
		}
		if len(outcome.IndexedChunkIDs) > 0 {
			updated := tx.Model(&types.Chunk{}).Where("id IN ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
				outcome.IndexedChunkIDs, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).
				Updates(map[string]any{"status": int(types.ChunkStatusIndexed), "index_status": "indexed"})
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != int64(len(outcome.IndexedChunkIDs)) {
				return ErrProcessingScope
			}
		}
		if outcome.Description != nil {
			if step.Stage != "summary" || !job.IsPublished || step.ExpectedPublicationEpoch != job.PublicationEpoch {
				return ErrProcessingConflict
			}
			updated := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", job.KnowledgeID, job.TenantID, job.KnowledgeBaseID).
				Update("description", *outcome.Description)
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrProcessingScope
			}
		}
		if outcome.Questions != nil {
			if err := commitProcessingQuestions(tx, job, step, outcome.Questions); err != nil {
				return err
			}
		}
		if outcome.Completeness != "" {
			if outcome.Completeness != "complete" && outcome.Completeness != "verified_empty" {
				return errors.New("invalid successful completeness")
			}
			job.Completeness = outcome.Completeness
			if err := tx.Model(job).Update("completeness", job.Completeness).Error; err != nil {
				return err
			}
		}
		if step.Phase == types.ProcessingPhasePublish {
			if err := publishProcessingJob(tx, job, now); err != nil {
				return err
			}
		} else if step.Phase == types.ProcessingPhaseProjection && step.Stage != "retire_previous" {
			if err := activateProcessingArtifact(tx, job, step); err != nil {
				return err
			}
		} else if step.Phase == types.ProcessingPhaseRetire && step.Stage == "retire" {
			if err := finishProcessingRetirement(tx, job, step, now); err != nil {
				return err
			}
		}
		if step.LastErrorEventID != nil {
			event.ResolvesEventID, event.ResolutionType = step.LastErrorEventID, "recovered_retry"
		}
	case types.ProcessingWaitingExternal:
		if outcome.NextRunAt == nil || !outcome.NextRunAt.After(now) {
			return errors.New("external wait requires a future poll time")
		}
		if outcome.SealPlan {
			if err := sealProcessingChildren(tx, job, step, outcome.ChildSteps); err != nil {
				return err
			}
		}
		step.Status, step.NextRunAt = types.ProcessingWaitingExternal, outcome.NextRunAt
	case types.ProcessingFailed, types.ProcessingBlocked:
		if outcome.ErrorClass == "" || outcome.ErrorCode == "" {
			return errors.New("processing failure requires classification")
		}
		step.Status = outcome.Status
		if step.Stage == "export_start" && outcome.Retryable {
			step.Status, step.ErrorClass, step.ErrorCode = types.ProcessingBlocked, "uncertain", "EXPORT_START_UNCERTAIN"
		} else if outcome.Retryable && step.RetryCount < step.MaxRetries && (step.DeadlineAt == nil || now.Before(*step.DeadlineAt)) {
			delay := processingRetryDelay(step.ID, step.RetryCount)
			if outcome.RetryAfter > delay {
				delay = outcome.RetryAfter
			}
			due := now.Add(delay)
			if step.DeadlineAt == nil || due.Before(*step.DeadlineAt) {
				step.Status, step.NextRunAt = types.ProcessingRetryWait, &due
				step.RetryCount++
			}
		}
		if step.Status == types.ProcessingFailed || step.Status == types.ProcessingBlocked {
			step.FinishedAt = &now
		}
	default:
		return errors.New("invalid processing outcome")
	}
	event.ToState, event.ErrorClass, event.ErrorCode, event.Message = step.Status, step.ErrorClass, step.ErrorCode, step.ErrorMessage
	if err := appendProcessingEvent(tx, job, event); err != nil {
		return err
	}
	if outcome.Status == types.ProcessingFailed || outcome.Status == types.ProcessingBlocked {
		var id int64
		if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND job_revision = ?", job.ID, job.Revision).Pluck("id", &id).Error; err != nil {
			return err
		}
		step.LastErrorEventID = &id
	}
	if step.Status == types.ProcessingRetryWait {
		step.Attempt++
	}
	step.LeaseToken, step.LeaseExpiresAt = "", nil
	if err := tx.Save(step).Error; err != nil {
		return err
	}
	if step.Status == types.ProcessingSucceeded {
		var incidents []int64
		if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND step_id = ? AND error_class <> '' AND resolves_event_id IS NULL", job.ID, step.ID).
			Where("id NOT IN (?)", tx.Model(&types.ProcessingEvent{}).Select("resolves_event_id").Where("job_id = ? AND resolves_event_id IS NOT NULL", job.ID)).
			Order("id").Pluck("id", &incidents).Error; err != nil {
			return err
		}
		for _, incident := range incidents {
			if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "incident_resolved", StepID: step.ID, Attempt: step.Attempt,
				ResolvesEventID: &incident, ResolutionType: "recovered_retry"}); err != nil {
				return err
			}
		}
	}
	if step.Phase != types.ProcessingPhaseRetire {
		if err := refreshProcessingJob(tx, job); err != nil {
			return err
		}
	}
	if err := refreshProcessingRuns(tx, job); err != nil {
		return err
	}
	return scheduleProcessingSteps(tx, job)
}

func processingRetryDelay(stepID string, retries int) time.Duration {
	if retries > 3 {
		retries = 3
	}
	base := (2 * time.Minute) << retries
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", stepID, retries)))
	return base + time.Duration(digest[0])*base/1020 // 0–25%, stable across crash replay.
}

func refreshProcessingJob(tx *gorm.DB, job *types.ProcessingJob) error {
	var steps []types.ProcessingStep
	if err := tx.Where("job_id = ?", job.ID).Find(&steps).Error; err != nil {
		return err
	}
	ready, done, readyCount, doneCount := job.PlanSealed && job.Completeness == "complete", job.PlanSealed, 0, 0
	status := types.ProcessingRunning
	for _, step := range steps {
		succeeded := step.Status == types.ProcessingSucceeded && (step.Kind != "barrier" || step.PlanSealed)
		if step.RequiredForReady {
			readyCount++
			ready = ready && succeeded
		}
		if step.RequiredForCompletion {
			doneCount++
			done = done && succeeded
		}
		if step.RequiredForCompletion || step.RequiredForReady {
			if step.Status == types.ProcessingFailed {
				status = types.ProcessingFailed
			}
			if step.Status == types.ProcessingBlocked && status != types.ProcessingFailed {
				status = types.ProcessingBlocked
			}
		}
	}
	job.Readiness = "pending"
	if ready && readyCount > 0 {
		job.Readiness = "ready"
	}
	if done && doneCount > 0 && (job.Kind == types.ProcessingJobScan || job.IsPublished) {
		status = types.ProcessingSucceeded
	}
	job.Status = status
	updates := map[string]any{"status": status, "readiness": job.Readiness}
	if status == types.ProcessingSucceeded && job.FinishedAt == nil {
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		job.FinishedAt, updates["finished_at"] = &now, now
		if job.Kind == types.ProcessingJobDocument {
			if err := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", job.KnowledgeID, job.TenantID, job.KnowledgeBaseID).
				Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "processed_at": now, "error_message": ""}).Error; err != nil {
				return err
			}
		}
	}
	return tx.Model(job).Updates(updates).Error
}
