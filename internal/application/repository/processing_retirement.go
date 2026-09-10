package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Cleanup is about an immutable version, not permission to re-read its source.
// Keep the normal KB -> source -> job lock order, including soft-deleted rows.
func lockProcessingRetirement(tx *gorm.DB, tenant uint64, jobID string) (*types.ProcessingJob, error) {
	var job types.ProcessingJob
	if err := tx.Where("id = ? AND tenant_id = ?", jobID, tenant).Take(&job).Error; err != nil {
		return nil, err
	}
	q := tx.Unscoped().Where("id = ? AND tenant_id = ?", job.KnowledgeBaseID, tenant)
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "SHARE"})
	}
	var kb types.KnowledgeBase
	if err := q.Take(&kb).Error; err != nil {
		return nil, err
	}
	source, err := lockProcessingSource(tx, job.DataSourceID)
	if err != nil {
		return nil, err
	}
	if source.TenantID != tenant || source.KnowledgeBaseID != job.KnowledgeBaseID {
		return nil, ErrProcessingScope
	}
	q = tx.Where("id = ? AND tenant_id = ?", jobID, tenant)
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := q.Take(&job).Error; err != nil {
		return nil, err
	}
	return &job, nil
}

func processingControlReceipt(tx *gorm.DB, job *types.ProcessingJob, revision int64, action, requestID, actor string) (bool, error) {
	if requestID == "" || len(requestID) > 128 || actor == "" || len(actor) > 128 {
		return false, errors.New("control requires an actor and bounded operation ID")
	}
	var event types.ProcessingEvent
	err := tx.Where("tenant_id = ? AND action = ? AND operation_request_id = ?", job.TenantID, action, requestID).Take(&event).Error
	if err == nil {
		if event.JobID != job.ID || event.Actor != actor {
			return false, ErrProcessingConflict
		}
		return true, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	if job.Revision != revision {
		return false, ErrProcessingConflict
	}
	return false, nil
}

func (r *ProcessingRepository) PlanRetirement(ctx context.Context, tenant uint64, jobID string, revision int64, requestID, actor string, reasons ...string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, jobID)
		if err != nil {
			return err
		}
		return planProcessingRetirement(tx, job, revision, requestID, actor, reasons...)
	})
}

// Caller holds the source lock shared by publication, pins and references.
func planProcessingRetirement(tx *gorm.DB, job *types.ProcessingJob, revision int64, requestID, actor string, reasons ...string) error {
	reason, err := processingControlReason(reasons)
	if err != nil {
		return err
	}
	if done, err := processingControlReceipt(tx, job, revision, "retire", requestID, actor); err != nil || done {
		return err
	}
	if job.IsCurrent || job.IsPublished || job.RollbackPin || job.RetirementState != "retained" {
		return ErrProcessingConflict
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return err
	}
	var contributions []types.ProcessingWikiWrite
	if err := tx.Where("job_id = ? AND state = ?", job.ID, "active").Order("page_id").Find(&contributions).Error; err != nil {
		return err
	}
	var specs []types.ProcessingStepSpec
	var dependencies []string
	seen := map[string]bool{}
	for _, write := range contributions {
		if seen[write.PageID] {
			continue
		}
		seen[write.PageID] = true
		unit := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%d/%s", job.PublicationEpoch, write.PageID))))
		input, _ := json.Marshal(map[string]string{"page_id": write.PageID})
		specs = append(specs, types.ProcessingStepSpec{Stage: "wiki_retire_page", UnitKey: unit, Phase: types.ProcessingPhaseRetire, InputFingerprint: unit, Input: input})
		dependencies = append(dependencies, "wiki_retire_page/"+unit)
	}
	specs = append(specs, types.ProcessingStepSpec{Stage: "retire", UnitKey: fmt.Sprint(job.PublicationEpoch), Phase: types.ProcessingPhaseRetire,
		InputFingerprint: fmt.Sprintf("retire/%s/%d", job.ID, job.PublicationEpoch), DependsOn: dependencies})
	steps, _, err := processingPlan(job.ID, specs)
	if err != nil {
		return err
	}
	deadline := now.Add(24 * time.Hour)
	for i := range steps {
		steps[i].DeadlineAt = &deadline
		steps[i].ExpectedPublicationEpoch = job.PublicationEpoch
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&steps).Error; err != nil {
		return err
	}
	if !job.PlanSealed {
		job.PlanSealed = true
		if err := tx.Model(job).Update("plan_sealed", true).Error; err != nil {
			return err
		}
	}
	if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "retirement_requested", Action: "retire", OperationRequestID: requestID, Actor: actor, Message: reason}); err != nil {
		return err
	}
	return scheduleProcessingSteps(tx, job)
}

func (r *ProcessingRepository) SetRollbackPin(ctx context.Context, tenant uint64, jobID string, revision int64, pin bool, requestID, actor string, reasons ...string) error {
	reason, err := processingControlReason(reasons)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingRetirement(tx, tenant, jobID)
		if err != nil {
			return err
		}
		action := "rollback_unpin"
		if pin {
			action = "rollback_pin"
		}
		if done, err := processingControlReceipt(tx, job, revision, action, requestID, actor); err != nil || done {
			return err
		}
		if job.RetirementState != "retained" {
			return ErrProcessingConflict
		}
		job.RollbackPin = pin
		if err := tx.Model(job).Update("rollback_pin", pin).Error; err != nil {
			return err
		}
		return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: action, Action: action, OperationRequestID: requestID, Actor: actor, Message: reason})
	})
}

func finishProcessingRetirement(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, now time.Time) error {
	if step.Stage != "retire" || job.RetirementState != "deleting" || job.IsCurrent || job.IsPublished || job.RollbackPin {
		return ErrProcessingConflict
	}
	if err := processingRetirementUnreferenced(tx, job); err != nil {
		return err
	}
	var graphWrites int64
	if err := tx.Model(&types.ProcessingGraphWrite{}).Where("job_id = ? AND state <> ?", job.ID, "deleted").Count(&graphWrites).Error; err != nil {
		return err
	}
	if graphWrites != 0 {
		return ErrProcessingConflict
	}
	if err := processingWikiRetired(tx, job); err != nil {
		return err
	}
	if err := processingFAQRetired(tx, job); err != nil {
		return err
	}
	if err := finishProcessingResourceCleanup(tx, job, now); err != nil {
		return err
	}
	if err := tx.Where("consumer_job_id = ? AND tenant_id = ?", job.ID, job.TenantID).Delete(&types.ProcessingArtifactReference{}).Error; err != nil {
		return err
	}
	job.RetirementState = "deleted"
	if err := tx.Model(job).Update("retirement_state", "deleted").Error; err != nil {
		return err
	}
	if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "version_retired", StepID: step.ID, Attempt: step.Attempt}); err != nil {
		return err
	}
	return wakeProcessingRetirementChecks(tx, job, now)
}

func processingRetirementUnreferenced(tx *gorm.DB, job *types.ProcessingJob) error {
	var count int64
	if err := tx.Model(&types.ProcessingArtifactReference{}).Where("producer_job_id = ?", job.ID).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return ErrProcessingConflict
	}
	return nil
}
