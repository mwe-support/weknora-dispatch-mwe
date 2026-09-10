package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

var processingEvidenceReference = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_:/.-]{0,255}$`)

// This is an operator attestation, never an inference from HTTP 200 or 404.
// The provider's export-status API does not attest a task's document identity.
// Require the exact recorded intent and an external audit reference, and leave
// the actor/evidence in history. Polling stays in the leased export worker.
func (r *ProcessingRepository) ResolveExport(ctx context.Context, tenant uint64, id string, request types.ProcessingExportResolution) error {
	if !validProcessingControl(request.ProcessingControlRequest) || request.StepID == "" || request.FileID == "" || request.SourceRevision == "" || request.RequestDigest == "" ||
		(request.NotStarted == (request.TaskID != "")) || !processingEvidenceReference.MatchString(request.EvidenceReference) ||
		(request.TaskID != "" && !processingEvidenceReference.MatchString(request.TaskID)) {
		return ErrProcessingConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, id)
		if err != nil {
			return err
		}
		if done, err := processingControlReceipt(tx, job, request.ExpectedRevision, "resolve_export", request.OperationRequestID, request.Actor); err != nil || done {
			return err
		}
		if job.RetirementState == "deleted" || job.Kind != types.ProcessingJobDocument || job.SourceRevision != request.SourceRevision {
			return ErrProcessingConflict
		}
		var document struct {
			FileID string `json:"file_id"`
		}
		if json.Unmarshal(job.Metadata, &document) != nil || document.FileID != request.FileID {
			return ErrProcessingConflict
		}
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(job.ID+"/"+document.FileID+"/"+job.SourceRevision)))
		if digest != request.RequestDigest {
			return ErrProcessingConflict
		}
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", request.StepID, id).Take(&step).Error; err != nil {
			return err
		}
		if step.Stage != "export_start" || step.Status != types.ProcessingBlocked || step.ErrorCode != "EXPORT_START_UNCERTAIN" {
			return ErrProcessingConflict
		}
		var receipt struct {
			TaskID    string    `json:"task_id,omitempty"`
			Request   string    `json:"request"`
			StartedAt time.Time `json:"started_at"`
			NotSent   bool      `json:"not_sent,omitempty"`
		}
		if step.CheckpointRef != "" && (json.Unmarshal([]byte(step.CheckpointRef), &receipt) != nil || receipt.Request != digest) {
			return ErrProcessingConflict
		}
		if receipt.TaskID != "" && (receipt.TaskID != request.TaskID || request.NotStarted) {
			return ErrProcessingConflict
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if receipt.StartedAt.IsZero() {
			if step.StartedAt == nil {
				return ErrProcessingConflict
			}
			receipt.StartedAt = *step.StartedAt
		}
		receipt.Request, receipt.TaskID, receipt.NotSent = digest, request.TaskID, request.NotStarted
		checkpoint, _ := json.Marshal(receipt)
		step.CheckpointRef, step.ErrorClass, step.ErrorCode = string(checkpoint), "", ""
		incident := step.LastErrorEventID
		step.LastErrorEventID = nil
		step.LeaseToken, step.LeaseExpiresAt, step.NextRunAt = "", nil, nil
		if job.IsCurrent && job.Status != types.ProcessingCanceled && job.RetirementState == "retained" {
			step.Attempt++
			step.Status, step.FinishedAt, step.MaxRetries = types.ProcessingPlanned, nil, step.RetryCount+4
			deadline := now.Add(24 * time.Hour)
			step.DeadlineAt = &deadline
		} else {
			step.Status, step.FinishedAt = types.ProcessingCanceled, &now
		}
		if err := tx.Save(&step).Error; err != nil {
			return err
		}
		detail, _ := json.Marshal(map[string]any{"evidence_reference": request.EvidenceReference, "request_digest": digest, "not_started": request.NotStarted, "task_digest": fmt.Sprintf("%x", sha256.Sum256([]byte(request.TaskID)))})
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "export_manually_verified", StepID: step.ID, Attempt: step.Attempt, Action: "resolve_export", Actor: request.Actor, OperationRequestID: request.OperationRequestID, Message: request.Reason, Detail: detail, ResolvesEventID: incident, ResolutionType: "operator_verified"}); err != nil {
			return err
		}
		if step.Status == types.ProcessingPlanned {
			if err := refreshProcessingJob(tx, job); err != nil {
				return err
			}
		}
		if err := refreshProcessingRuns(tx, job); err != nil {
			return err
		}
		if err := scheduleProcessingSteps(tx, job); err != nil {
			return err
		}
		var pending int64
		if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND phase <> ? AND error_class = ?", id, types.ProcessingPhaseRetire, "uncertain").Count(&pending).Error; err != nil {
			return err
		}
		if pending != 0 {
			return nil
		}
		var cleanup []types.ProcessingStep
		if err := tx.Where("job_id = ? AND phase = ? AND status = ? AND error_code = ?", id, types.ProcessingPhaseRetire, types.ProcessingBlocked, "RETIREMENT_EXTERNAL_OUTCOME_UNCERTAIN").Find(&cleanup).Error; err != nil {
			return err
		}
		for _, candidate := range cleanup {
			job, err = NewProcessingRepository(tx).GetJob(ctx, tenant, id)
			if err != nil {
				return err
			}
			op := fmt.Sprintf("%x", sha256.Sum256([]byte(request.OperationRequestID+"/"+candidate.ID)))
			if err := NewProcessingRepository(tx).RetryStep(ctx, tenant, id, candidate.ID, job.Revision, op, request.Actor); err != nil {
				return err
			}
		}
		return nil
	})
}
