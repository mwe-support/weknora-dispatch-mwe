package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func validProcessingControl(c types.ProcessingControlRequest) bool {
	return c.ExpectedRevision > 0 && c.Actor != "" && len(c.Actor) <= 128 && strings.TrimSpace(c.OperationRequestID) != "" && len(c.OperationRequestID) <= 128 && strings.TrimSpace(c.Reason) != "" && len(c.Reason) <= 512
}

func processingControlReason(reasons []string) (string, error) {
	if len(reasons) == 0 {
		return "", nil
	} // Internal automatic operations have their own event type.
	if len(reasons) != 1 || strings.TrimSpace(reasons[0]) == "" || len(reasons[0]) > 512 {
		return "", ErrProcessingConflict
	}
	return reasons[0], nil
}

func (r *ProcessingRepository) RebuiltJob(ctx context.Context, tenant uint64, id string, control types.ProcessingControlRequest) (*types.ProcessingJob, error) {
	if !validProcessingControl(control) {
		return nil, ErrProcessingConflict
	}
	var prior types.ProcessingEvent
	err := r.db.WithContext(ctx).Where("tenant_id = ? AND action = ? AND operation_request_id = ?", tenant, "rebuild", control.OperationRequestID).Take(&prior).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if prior.JobID != id || prior.Actor != control.Actor {
		return nil, ErrProcessingConflict
	}
	var receipt struct {
		JobID string `json:"job_id"`
	}
	if json.Unmarshal(prior.Detail, &receipt) != nil || receipt.JobID == "" {
		return nil, ErrProcessingConflict
	}
	return r.GetJob(ctx, tenant, receipt.JobID)
}

// Rebuild starts a new attempt at this exact source revision. It never deletes
// the published version or restarts a scan. Changed source contents require a
// new verified source revision; native_read checks that before producing data.
func (r *ProcessingRepository) RebuildJob(ctx context.Context, tenant uint64, id string, control types.ProcessingControlRequest, pipeline, configuration string, plan []types.ProcessingStepSpec) (result *types.ProcessingJob, err error) {
	if !validProcessingControl(control) || pipeline == "" || configuration == "" || len(plan) == 0 {
		return nil, ErrProcessingConflict
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		result, err = NewProcessingRepository(tx).RebuiltJob(ctx, tenant, id, control)
		if err != nil || result != nil {
			return err
		}
		job, err := NewProcessingRepository(tx).GetJob(ctx, tenant, id)
		if err != nil {
			return err
		}
		// Model/configuration locks precede source locks, as in normal planning.
		currentConfiguration, err := lockProcessingConfiguration(tx, tenant, job.KnowledgeBaseID)
		if err != nil {
			return err
		}
		if configuration != currentConfiguration {
			return ErrProcessingScope
		}
		job, err = lockProcessingRetirement(tx, tenant, id)
		if err != nil {
			return err
		}
		result, err = NewProcessingRepository(tx).RebuiltJob(ctx, tenant, id, control)
		if err != nil || result != nil {
			return err
		}
		if job.Revision != control.ExpectedRevision || !job.IsCurrent || job.Kind != types.ProcessingJobDocument || job.RetirementState != "retained" {
			return ErrProcessingConflict
		}
		var uncertain int64
		if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND error_class = ?", job.ID, "uncertain").Count(&uncertain).Error; err != nil {
			return err
		}
		if uncertain > 0 {
			return errors.New("reconcile the outstanding external operation before rebuilding")
		}
		input := types.ProcessingJob{Kind: job.Kind, TenantID: tenant, KnowledgeBaseID: job.KnowledgeBaseID, DataSourceID: job.DataSourceID,
			ExternalID: job.ExternalID, SourceRevision: job.SourceRevision, SourceDigest: job.SourceDigest, ScopeRevision: job.ScopeRevision,
			PipelineFingerprint: pipeline, ConfigurationRevision: configuration, Metadata: job.Metadata}
		repo := NewProcessingRepository(tx)
		result, err = repo.ensureJob(ctx, input, true)
		if err != nil {
			return err
		}
		if err := repo.PlanSteps(ctx, tenant, result.ID, plan); err != nil {
			return err
		}
		// ensureJob appended supersession to the old version; refresh its revision.
		job, err = repo.GetJob(ctx, tenant, id)
		if err != nil {
			return err
		}
		receipt, _ := json.Marshal(map[string]string{"job_id": result.ID})
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "version_rebuild_requested", Action: "rebuild", Actor: control.Actor, OperationRequestID: control.OperationRequestID, Message: control.Reason, Detail: receipt}); err != nil {
			return err
		}
		result, err = repo.GetJob(ctx, tenant, result.ID)
		return err
	})
	return
}

func (r *ProcessingRepository) CancelJob(ctx context.Context, tenant uint64, id string, control types.ProcessingControlRequest) error {
	if !validProcessingControl(control) {
		return ErrProcessingConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, id)
		if err != nil {
			return err
		}
		if done, err := processingControlReceipt(tx, job, control.ExpectedRevision, "cancel", control.OperationRequestID, control.Actor); err != nil || done {
			return err
		}
		if !job.IsCurrent || job.RetirementState != "retained" || job.Status == types.ProcessingSucceeded || job.Status == types.ProcessingSuperseded {
			return ErrProcessingConflict
		}
		if err := stopProcessingJob(tx, job, types.ProcessingCanceled, "USER_CANCELED"); err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "version_cancel_requested", Action: "cancel", Actor: control.Actor, OperationRequestID: control.OperationRequestID, Message: control.Reason}); err != nil {
			return err
		}
		return refreshProcessingRuns(tx, job)
	})
}
