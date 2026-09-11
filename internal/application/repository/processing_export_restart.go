package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// RestartExpiredExport grants one audited export attempt after an acknowledged
// provider task became unavailable. No downloaded snapshot may be overwritten.
// This is an explicit operator action, never an automatic response to a 404.
func (r *ProcessingRepository) RestartExpiredExport(ctx context.Context, tenant uint64, id string, control types.ProcessingControlRequest) error {
	if !validProcessingControl(control) {
		return ErrProcessingConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, id)
		if err != nil {
			return err
		}
		if done, err := processingControlReceipt(tx, job, control.ExpectedRevision, "restart_export", control.OperationRequestID, control.Actor); err != nil || done {
			return err
		}
		if !job.IsCurrent || job.IsPublished || job.Kind != types.ProcessingJobDocument {
			return ErrProcessingConflict
		}
		var steps []types.ProcessingStep
		if err := tx.Where("job_id = ?", id).Find(&steps).Error; err != nil {
			return err
		}
		var start, poll, download, normalize *types.ProcessingStep
		for i := range steps {
			s := &steps[i]
			if s.ErrorClass == "uncertain" || s.Status == types.ProcessingRunning {
				return ErrProcessingConflict
			}
			switch s.Stage {
			case "export_start":
				start = s
			case "export_poll":
				poll = s
			case "download":
				download = s
			case "normalize":
				normalize = s
			case "native_read":
			default:
				disabledProjection := s.Phase == types.ProcessingPhaseProjection && s.Status == types.ProcessingSkipped && !s.RequiredForReady && !s.RequiredForCompletion && s.OutputManifestRef == ""
				if s.Phase != types.ProcessingPhaseRetire && s.Status != types.ProcessingPlanned && !disabledProjection {
					return ErrProcessingConflict
				}
			}
		}
		if start == nil || poll == nil || download == nil || normalize == nil || start.Status != types.ProcessingSucceeded || download.OutputManifestRef != "" || download.Status == types.ProcessingSucceeded {
			return ErrProcessingConflict
		}
		var receipt struct {
			TaskID string `json:"task_id"`
		}
		if json.Unmarshal([]byte(start.CheckpointRef), &receipt) != nil || receipt.TaskID == "" {
			return ErrProcessingConflict
		}
		unavailable := poll.Status == types.ProcessingBlocked && poll.ErrorCode == "TENCENT_404"
		expired := download.Status == types.ProcessingBlocked && download.ErrorCode == "EXPORT_DOWNLOAD_URL_EXPIRED"
		if !unavailable && !expired {
			return ErrProcessingConflict
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		deadline := now.Add(24 * time.Hour)
		for _, s := range []*types.ProcessingStep{start, poll, download, normalize} {
			// Retain authenticated artifact references and prior receipt in the
			// ledger before opening a fresh attempt. No signed URLs enter events.
			detail, _ := json.Marshal(map[string]any{"checkpoint_ref": s.CheckpointRef, "output_manifest_ref": s.OutputManifestRef, "output_digest": s.OutputDigest})
			event := stepEvent(s, "export_attempt_restarted", s.Status)
			event.Detail, event.Actor, event.Message, event.ToState = detail, control.Actor, control.Reason, types.ProcessingPlanned
			if err := appendProcessingEvent(tx, job, event); err != nil {
				return err
			}
			s.Attempt++
			s.Status, s.MaxRetries = types.ProcessingPlanned, s.RetryCount+4
			s.CheckpointRef, s.OutputManifestRef, s.OutputDigest, s.LeaseToken, s.QueueTaskID = "", "", "", "", ""
			s.NextRunAt, s.FinishedAt, s.LeaseExpiresAt, s.QueuedAt = nil, nil, nil, nil
			s.DeadlineAt = &deadline
			if err := tx.Save(s).Error; err != nil {
				return err
			}
		}
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "export_restart_requested", Action: "restart_export", OperationRequestID: control.OperationRequestID, Actor: control.Actor, Message: control.Reason}); err != nil {
			return err
		}
		if err := refreshProcessingJob(tx, job); err != nil {
			return err
		}
		return scheduleProcessingSteps(tx, job)
	})
}
