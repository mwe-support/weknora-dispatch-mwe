package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type processingCleanupReceipt struct {
	Cursor   string   `json:"cursor,omitempty"`
	Retired  int      `json:"retired"`
	Retained int      `json:"retained"`
	Pending  []string `json:"pending,omitempty"`
	Blocked  []string `json:"blocked,omitempty"`
}

// Inspect at most one page under the existing source lock. A completed page's
// audit event preserves each retention reason; the result holds only totals.
func finishProcessingPreviousRetirement(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, now time.Time) (types.ProcessingOutcome, error) {
	if step.Phase != types.ProcessingPhaseProjection || !job.IsPublished || step.ExpectedPublicationEpoch != job.PublicationEpoch {
		return types.ProcessingOutcome{}, ErrProcessingConflict
	}
	var unfinished int64
	if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND id <> ? AND required_for_completion = ? AND (status <> ? OR (kind = ? AND plan_sealed = ?))", job.ID, step.ID, true, types.ProcessingSucceeded, "barrier", false).Count(&unfinished).Error; err != nil {
		return types.ProcessingOutcome{}, err
	}
	if unfinished > 0 {
		return types.ProcessingOutcome{}, ErrProcessingConflict
	}
	var receipt processingCleanupReceipt
	if len(step.Result) > 0 && json.Unmarshal(step.Result, &receipt) != nil {
		return types.ProcessingOutcome{}, errors.New("cleanup receipt is invalid")
	}
	receipt.Pending, receipt.Blocked = nil, nil
	var prior []types.ProcessingJob
	if err := processingLogicalQuery(tx, *job).Where("id <> ? AND id > ? AND published_at IS NOT NULL AND publication_epoch < ?", job.ID, receipt.Cursor, job.PublicationEpoch).Order("id").Limit(100).Find(&prior).Error; err != nil {
		return types.ProcessingOutcome{}, err
	}
	retired, retained := 0, 0
	records := make([]map[string]string, 0, len(prior))
	for i := range prior {
		old := &prior[i]
		reason := "deleted"
		switch {
		case old.IsCurrent || old.IsPublished:
			return types.ProcessingOutcome{}, ErrProcessingConflict
		case old.RetirementState == "deleted":
			retired++
		case old.RollbackPin:
			reason = "rollback_pin"
			retained++
		default:
			if err := processingRetirementUnreferenced(tx, old); err != nil {
				if !errors.Is(err, ErrProcessingConflict) {
					return types.ProcessingOutcome{}, err
				}
				reason = "artifact_reference"
				retained++
			} else {
				var cleanup []types.ProcessingStep
				if err := tx.Where("job_id = ? AND phase = ? AND expected_publication_epoch = ?", old.ID, types.ProcessingPhaseRetire, old.PublicationEpoch).Find(&cleanup).Error; err != nil {
					return types.ProcessingOutcome{}, err
				}
				if len(cleanup) == 0 {
					request := fmt.Sprintf("auto-retire-%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s", job.ID, job.PublicationEpoch, old.ID))))
					if err := planProcessingRetirement(tx, old, old.Revision, request, "system:lifecycle"); err != nil {
						return types.ProcessingOutcome{}, err
					}
				}
				for _, unit := range cleanup {
					if unit.Status == types.ProcessingFailed || unit.Status == types.ProcessingBlocked {
						receipt.Blocked = append(receipt.Blocked, old.ID)
						break
					}
				}
				reason = "pending"
				receipt.Pending = append(receipt.Pending, old.ID)
			}
		}
		records = append(records, map[string]string{"job_id": old.ID, "reason": reason})
	}
	status := types.ProcessingWaitingExternal
	if len(receipt.Pending) == 0 {
		receipt.Retired += retired
		receipt.Retained += retained
		if len(prior) > 0 {
			receipt.Cursor = prior[len(prior)-1].ID
		}
		detail, _ := json.Marshal(records)
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "retirement_checked", StepID: step.ID, Attempt: step.Attempt, Detail: detail}); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if len(prior) < 100 {
			status = types.ProcessingSucceeded
		}
	}
	result, err := json.Marshal(receipt)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	out := types.ProcessingOutcome{Status: status, Result: result}
	if len(receipt.Blocked) > 0 {
		out.Status = types.ProcessingBlocked
		out.ErrorClass = "dependency"
		out.ErrorCode = "PREVIOUS_RETIREMENT_BLOCKED"
		out.Message = "An older version could not be retired; repair its cleanup stage and retry this check"
	} else if status == types.ProcessingSucceeded {
		out.OutputManifestRef = "ledger:retirement-check/" + step.ID
		out.OutputDigest = fmt.Sprintf("%x", sha256.Sum256(result))
	} else {
		due := now.Add(5 * time.Second)
		out.NextRunAt = &due
	}
	return out, nil
}

// Seven-day collection only schedules the same fenced retirement pipeline.
// Recheck every predicate after locking; a stale enumeration never authorizes
// deletion of a newly pinned, published or referenced version.
func (r *ProcessingRepository) CollectProcessingGarbage(ctx context.Context, after string, limit int) (string, error) {
	if limit < 1 || limit > 100 {
		return after, ErrProcessingConflict
	}
	now, err := processingDBTime(r.db.WithContext(ctx))
	if err != nil {
		return after, err
	}
	cutoff := now.Add(-7 * 24 * time.Hour)
	eligible := func(tx *gorm.DB) *gorm.DB {
		return tx.Where("id > ? AND is_published = ? AND rollback_pin = ? AND retirement_state = ? AND updated_at < ?", after, false, false, "retained", cutoff).
			Where("(kind = ? AND is_current = ? AND status IN ?) OR (kind = ? AND status IN ?)", types.ProcessingJobDocument, false,
				[]string{types.ProcessingSucceeded, types.ProcessingFailed, types.ProcessingCanceled, types.ProcessingSuperseded, types.ProcessingSkipped}, types.ProcessingJobScan,
				[]string{types.ProcessingSucceeded, types.ProcessingCanceled, types.ProcessingSuperseded})
	}
	var jobs []types.ProcessingJob
	if err := eligible(r.db.WithContext(ctx)).Order("id").Limit(limit).Find(&jobs).Error; err != nil {
		return after, err
	}
	var failures error
	for _, candidate := range jobs {
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			job, err := lockProcessingRetirement(tx, candidate.TenantID, candidate.ID)
			if err != nil {
				return err
			}
			var count int64
			if err := eligible(tx.Model(&types.ProcessingJob{})).Where("id = ?", job.ID).Count(&count).Error; err != nil {
				return err
			}
			if count == 0 {
				return nil
			}
			if err := processingRetirementUnreferenced(tx, job); err != nil {
				if errors.Is(err, ErrProcessingConflict) {
					return nil
				}
				return err
			}
			// Uncertain operations retain their evidence even if a newer source version
			// canceled the surrounding job. They require external reconciliation.
			if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND error_class = ?", job.ID, "uncertain").Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return nil
			}
			if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND phase = ? AND expected_publication_epoch = ?", job.ID, types.ProcessingPhaseRetire, job.PublicationEpoch).Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return nil
			}
			if job.Kind == types.ProcessingJobScan && job.IsCurrent {
				job.IsCurrent = false
				if err := tx.Model(job).Update("is_current", false).Error; err != nil {
					return err
				}
				if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "scan_artifacts_retirement_requested", Actor: "system:lifecycle"}); err != nil {
					return err
				}
			}
			return planProcessingRetirement(tx, job, job.Revision, fmt.Sprintf("gc-%s-%d", job.ID, job.PublicationEpoch), "system:lifecycle")
		})
		// Keep the cursor moving across one damaged scope; report the failure while
		// letting other independent jobs in this page make progress.
		failures = errors.Join(failures, err)
	}
	if len(jobs) < limit {
		return "", failures
	}
	return jobs[len(jobs)-1].ID, failures
}

// Completing the failed dependency reopens only its waiting completion check.
// The published content stages and their retry budgets remain untouched.
func wakeProcessingRetirementChecks(tx *gorm.DB, retired *types.ProcessingJob, now time.Time) error {
	var jobs []types.ProcessingJob
	if err := processingLogicalQuery(tx, *retired).Where("is_published = ? AND id <> ?", true, retired.ID).Find(&jobs).Error; err != nil {
		return err
	}
	for i := range jobs {
		job := &jobs[i]
		var checks []types.ProcessingStep
		if err := tx.Where("job_id = ? AND stage = ? AND status = ? AND error_code = ?", job.ID, "retire_previous", types.ProcessingBlocked, "PREVIOUS_RETIREMENT_BLOCKED").Find(&checks).Error; err != nil {
			return err
		}
		for i := range checks {
			step := &checks[i]
			var receipt processingCleanupReceipt
			if json.Unmarshal(step.Result, &receipt) != nil {
				return errors.New("cleanup receipt is invalid")
			}
			if !slices.Contains(receipt.Blocked, retired.ID) || !processingStepEligible(job, step) {
				continue
			}
			step.Attempt++
			deadline := now.Add(24 * time.Hour)
			step.Status, step.DeadlineAt, step.FinishedAt = types.ProcessingPlanned, &deadline, nil
			if err := tx.Save(step).Error; err != nil {
				return err
			}
			if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "retirement_dependency_recovered", StepID: step.ID, Attempt: step.Attempt, Message: retired.ID}); err != nil {
				return err
			}
			if err := refreshProcessingJob(tx, job); err != nil {
				return err
			}
			if err := refreshProcessingRuns(tx, job); err != nil {
				return err
			}
			if err := scheduleProcessingSteps(tx, job); err != nil {
				return err
			}
		}
	}
	return nil
}
