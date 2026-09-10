package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// Heartbeat extends execution authority using database time. Progress and
// checkpoints are separate; a live but stalled worker cannot fake progress.
func (r *ProcessingRepository) Heartbeat(ctx context.Context, tenant uint64, lease types.ProcessingLease, duration time.Duration, checkpoint string) error {
	if duration <= 0 || duration > 10*time.Minute {
		return errors.New("invalid processing lease duration")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, lease.Ref.JobID, lease.Ref.StepID)
		if err != nil {
			return err
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", lease.Ref.StepID, job.ID).Take(&step).Error; err != nil {
			return err
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if !processingRefMatches(job, &step, lease.Ref) || !processingStepEligible(job, &step) || step.Status != types.ProcessingRunning ||
			step.LeaseToken != lease.Token || step.LeaseExpiresAt == nil || !step.LeaseExpiresAt.After(now) {
			return ErrProcessingConflict
		}
		expires := now.Add(duration)
		if step.DeadlineAt != nil {
			if !step.DeadlineAt.After(now) {
				return ErrProcessingConflict
			}
			if step.DeadlineAt.Before(expires) {
				expires = *step.DeadlineAt
			}
		}
		changes := map[string]any{"lease_expires_at": expires, "heartbeat_at": now}
		if checkpoint != "" && checkpoint != step.CheckpointRef {
			changes["checkpoint_ref"], changes["progress_at"] = checkpoint, now
		}
		return tx.Model(&step).Updates(changes).Error
	})
}

// ReconcileJob handles only durable execution evidence. Queue inspection and
// missing diagnostic spans cannot cause a failure or spend a business retry.
func (r *ProcessingRepository) ReconcileJob(ctx context.Context, tenant uint64, jobID string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, jobID)
		if errors.Is(err, ErrProcessingScope) || errors.Is(err, gorm.ErrRecordNotFound) {
			job, err = lockProcessingRetirement(tx, tenant, jobID)
			if err != nil {
				return err
			}
			if err := reconcileInvalidProcessingScope(tx, tenant, jobID); err != nil {
				return err
			}
			if err := tx.Where("id = ? AND tenant_id = ?", jobID, tenant).Take(job).Error; err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		var expired []types.ProcessingStep
		if err := tx.Where("job_id = ? AND status = ? AND lease_expires_at <= ?", jobID, types.ProcessingRunning, now).Find(&expired).Error; err != nil {
			return err
		}
		for i := range expired {
			step := &expired[i]
			if !processingStepEligible(job, step) {
				continue
			}
			outcome := types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "transient", ErrorCode: "LEASE_EXPIRED", Retryable: true,
				CheckpointRef: step.CheckpointRef, Result: step.Result, Message: "Worker lease expired before durable completion"}
			if err := finishProcessingStep(tx, job, step, outcome, now, "lease_expired"); err != nil {
				return err
			}
		}
		var timedOut []types.ProcessingStep
		if err := tx.Where("job_id = ? AND status IN ? AND deadline_at <= ?", jobID,
			[]string{types.ProcessingPlanned, types.ProcessingEnqueuePending, types.ProcessingQueued, types.ProcessingRetryWait, types.ProcessingWaitingExternal}, now).Find(&timedOut).Error; err != nil {
			return err
		}
		for i := range timedOut {
			step := &timedOut[i]
			if !processingStepEligible(job, step) {
				continue
			}
			outcome := types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "budget", ErrorCode: "PROCESSING_DEADLINE_EXCEEDED",
				Message: "Automatic execution deadline exceeded"}
			if err := finishProcessingStep(tx, job, step, outcome, now, "deadline_exceeded"); err != nil {
				return err
			}
		}
		return scheduleProcessingSteps(tx, job)
	})
}

// PendingDeliveries returns unconfirmed lifecycle outbox records only. The
// producer uses the deterministic delivery ID; duplicate publication is safe.
func (r *ProcessingRepository) PendingDeliveries(ctx context.Context, limit int) ([]types.TaskPendingOp, error) {
	if limit < 1 || limit > 200 {
		limit = 100
	}
	var result []types.TaskPendingOp
	now, err := processingDBTime(r.db.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	err = r.db.WithContext(ctx).Model(&types.TaskPendingOp{}).Select("task_pending_ops.*").
		Joins("JOIN processing_steps s ON s.id = task_pending_ops.step_id AND s.step_attempt = task_pending_ops.step_attempt AND s.dispatch_seq = task_pending_ops.dispatch_seq").
		Where("task_pending_ops.task_type = ? AND task_pending_ops.op = ? AND task_pending_ops.delivered_at IS NULL AND task_pending_ops.available_at <= ? AND s.status IN ?",
			types.TypeProcessingStep, "deliver", now, []string{types.ProcessingEnqueuePending, types.ProcessingQueued}).
		Order("task_pending_ops.available_at, task_pending_ops.id").Limit(limit).Find(&result).Error
	return result, err
}

func (r *ProcessingRepository) ConfirmDelivery(ctx context.Context, tenant uint64, ref types.ProcessingRef, queueID string) error {
	if queueID == "" {
		return errors.New("queue delivery requires an acknowledged task ID")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, ref.JobID, ref.StepID)
		if err != nil {
			return err
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", ref.StepID, job.ID).Take(&step).Error; err != nil {
			return err
		}
		var op types.TaskPendingOp
		if err := tx.Where("tenant_id = ? AND step_id = ? AND step_attempt = ? AND dispatch_seq = ?", tenant, ref.StepID, ref.Attempt, ref.DispatchSeq).Take(&op).Error; err != nil {
			return err
		}
		if op.DeliveredAt != nil {
			return nil
		}
		if ref.Generation != job.Generation || ref.InputFingerprint != step.InputFingerprint {
			return ErrProcessingConflict
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if err := tx.Model(&op).Updates(map[string]any{"delivered_at": now, "queue_task_id": queueID}).Error; err != nil {
			return err
		}
		if !processingRefMatches(job, &step, ref) {
			return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "delivery_confirmed", StepID: step.ID,
				Attempt: ref.Attempt, DispatchSeq: ref.DispatchSeq, QueueTaskID: queueID})
		}
		from := step.Status
		if step.Status == types.ProcessingEnqueuePending {
			step.Status = types.ProcessingQueued
		}
		step.QueueTaskID, step.QueuedAt = queueID, &now
		if err := tx.Model(&step).Updates(map[string]any{"status": step.Status, "queue_task_id": queueID, "queued_at": now}).Error; err != nil {
			return err
		}
		return appendProcessingEvent(tx, job, stepEvent(&step, "delivery_confirmed", from))
	})
}

func (r *ProcessingRepository) ListSteps(ctx context.Context, tenant uint64, jobID string) ([]types.ProcessingStep, error) {
	if _, err := r.GetJob(ctx, tenant, jobID); err != nil {
		return nil, err
	}
	var result []types.ProcessingStep
	err := r.db.WithContext(ctx).Where("job_id = ?", jobID).Order("created_at, id").Find(&result).Error
	return result, err
}

func (r *ProcessingRepository) GetStep(ctx context.Context, tenant uint64, jobID, stepID string) (*types.ProcessingStep, error) {
	if _, err := r.GetJob(ctx, tenant, jobID); err != nil {
		return nil, err
	}
	var step types.ProcessingStep
	err := r.db.WithContext(ctx).Where("id = ? AND job_id = ?", stepID, jobID).Take(&step).Error
	return &step, err
}

func (r *ProcessingRepository) RecoveryJobs(ctx context.Context, after string, limit int) ([]types.ProcessingJob, error) {
	if limit < 1 || limit > 200 {
		limit = 100
	}
	var jobs []types.ProcessingJob
	err := r.db.WithContext(ctx).Where("id > ? AND retirement_state <> ?", after, "deleted").
		Where("EXISTS (?)", r.db.Model(&types.ProcessingStep{}).Select("1").Where("job_id = processing_jobs.id AND status IN ?", []string{types.ProcessingPlanned, types.ProcessingEnqueuePending, types.ProcessingQueued, types.ProcessingRunning, types.ProcessingRetryWait, types.ProcessingWaitingExternal})).
		Order("id").Limit(limit).Find(&jobs).Error
	return jobs, err
}

// Redeliver is called only after a queue inspection proves no live delivery.
// It changes dispatch identity, never step_attempt or the business retry budget.
func (r *ProcessingRepository) Redeliver(ctx context.Context, tenant uint64, jobID string, observed types.ProcessingStep) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, jobID, observed.ID)
		if err != nil {
			return err
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", observed.ID, jobID).Take(&step).Error; err != nil {
			return err
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if step.Status != types.ProcessingQueued || step.Attempt != observed.Attempt || step.DispatchSeq != observed.DispatchSeq || step.QueueTaskID != observed.QueueTaskID ||
			step.QueuedAt == nil || step.QueuedAt.Add(2*time.Minute).After(now) || !processingStepEligible(job, &step) {
			return ErrProcessingConflict
		}
		event := stepEvent(&step, "delivery_lost", step.Status)
		step.Status = types.ProcessingPlanned
		if err := tx.Model(&step).Update("status", step.Status).Error; err != nil {
			return err
		}
		event.ToState = step.Status
		if err := appendProcessingEvent(tx, job, event); err != nil {
			return err
		}
		return scheduleProcessingSteps(tx, job)
	})
}

// RetryStep is an audited operator action. It keeps successful predecessors and
// reuses the same input; repairing inputs requires a new generation/plan.
func (r *ProcessingRepository) RetryStep(ctx context.Context, tenant uint64, jobID, stepID string, expectedRevision int64, requestID, actor string, reasons ...string) error {
	reason, err := processingControlReason(reasons)
	if err != nil {
		return err
	}
	if requestID == "" || len(requestID) > 128 || actor == "" {
		return errors.New("manual retry requires an actor and bounded operation ID")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, jobID, stepID)
		if err != nil {
			return err
		}
		var prior types.ProcessingEvent
		err = tx.Where("tenant_id = ? AND action = ? AND operation_request_id = ?", tenant, "retry", requestID).Take(&prior).Error
		if err == nil {
			if prior.JobID != jobID || prior.StepID != stepID || prior.Actor != actor {
				return ErrProcessingConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if job.Revision != expectedRevision {
			return ErrProcessingConflict
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", stepID, jobID).Take(&step).Error; err != nil {
			return err
		}
		if step.Status != types.ProcessingFailed && step.Status != types.ProcessingBlocked {
			return ErrProcessingConflict
		}
		if step.ErrorCode == "EXPORT_START_UNCERTAIN" {
			return errors.New("unknown export start requires provider reconciliation before retry")
		}
		if !processingStepEligible(job, &step) {
			return ErrProcessingConflict
		}
		event := stepEvent(&step, "manual_retry", step.Status)
		event.Actor, event.Action, event.OperationRequestID, event.ToState = actor, "retry", requestID, types.ProcessingPlanned
		event.Message = reason
		step.Attempt++
		step.Status, step.MaxRetries = types.ProcessingPlanned, step.RetryCount+4
		step.NextRunAt, step.FinishedAt, step.LeaseExpiresAt, step.LeaseToken = nil, nil, nil, ""
		// A user explicitly grants a new execution budget, preserving cumulative
		// retry_count and the entire preceding attempt/error history.
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		deadline := now.Add(24 * time.Hour)
		step.DeadlineAt = &deadline
		if err := tx.Save(&step).Error; err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, job, event); err != nil {
			return err
		}
		if step.Phase != types.ProcessingPhaseRetire {
			if err := refreshProcessingJob(tx, job); err != nil {
				return err
			}
		}
		return scheduleProcessingSteps(tx, job)
	})
}
