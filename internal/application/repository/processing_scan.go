package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *ProcessingRepository) BeginScan(ctx context.Context, input types.ProcessingJob, specs []types.ProcessingStepSpec) (job *types.ProcessingJob, err error) {
	if input.Kind != types.ProcessingJobScan || input.OriginRunID == "" || input.ExternalID != "run:"+input.OriginRunID || input.SourceRevision != input.OriginRunID {
		return nil, ErrProcessingScope
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var run types.SyncLog
		if err := tx.Where("id = ? AND tenant_id = ? AND data_source_id = ?", input.OriginRunID, input.TenantID, input.DataSourceID).Take(&run).Error; err != nil {
			return err
		}
		repo := NewProcessingRepository(tx)
		var err error
		job, err = repo.EnsureJob(ctx, input)
		if err != nil {
			return err
		}
		if !job.IsCurrent || job.RetirementState != "retained" {
			return nil
		}
		return repo.PlanSteps(ctx, input.TenantID, job.ID, specs)
	})
	return
}

func commitProcessingDiscovery(tx *gorm.DB, scan *types.ProcessingJob, step *types.ProcessingStep, outcome types.ProcessingOutcome) error {
	if len(outcome.DiscoveredItems) == 0 && len(outcome.AdmitDocuments) == 0 {
		return nil
	}
	if scan.Kind != types.ProcessingJobScan || scan.OriginRunID == "" || step.Phase != types.ProcessingPhaseScan ||
		(outcome.Status != types.ProcessingSucceeded && !(outcome.Status == types.ProcessingWaitingExternal && outcome.SealPlan)) {
		return errors.New("discovery requires a confirmed scan result")
	}
	var run types.SyncLog
	if err := tx.Where("id = ? AND tenant_id = ? AND data_source_id = ?", scan.OriginRunID, scan.TenantID, scan.DataSourceID).Take(&run).Error; err != nil {
		return err
	}
	for _, item := range outcome.DiscoveredItems {
		if item.JobID != "" || item.SourceRevision != "" || (item.Kind != "container" && item.Kind != "link" && item.Kind != "document") {
			return ErrProcessingScope
		}
		if item.Kind == "document" {
			item.Disposition = "discovered"
		} else {
			item.Disposition = item.Kind
		}
		if err := saveProcessingRunItem(tx, scan, item); err != nil {
			return err
		}
	}
	for _, admission := range outcome.AdmitDocuments {
		input := admission.Job
		if input.Kind != types.ProcessingJobDocument || input.TenantID != scan.TenantID || input.KnowledgeBaseID != scan.KnowledgeBaseID || input.DataSourceID != scan.DataSourceID ||
			input.PipelineFingerprint != scan.PipelineFingerprint {
			return ErrProcessingScope
		}
		input.ScopeRevision, input.AuthRevision, input.ConfigurationRevision, input.OriginRunID = scan.ScopeRevision, scan.AuthRevision, scan.ConfigurationRevision, scan.OriginRunID
		// A run keeps the first confirmed version of each logical document. A
		// second overlapping selector must not replace that membership silently.
		var existing types.SyncRunItem
		err := tx.Where("run_id = ? AND item_key = ?", scan.OriginRunID, "document:"+input.ExternalID).Take(&existing).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if existing.JobID != "" {
			if existing.SourceRevision != input.SourceRevision {
				return errors.New("SCAN_DOCUMENT_CHANGED")
			}
			continue
		}
		repo := NewProcessingRepository(tx)
		var previous types.ProcessingJob
		if err := processingLogicalQuery(tx, input).Where("is_current = ?", true).Find(&previous).Error; err != nil {
			return err
		}
		job, err := repo.EnsureJob(tx.Statement.Context, input)
		if err != nil {
			return err
		}
		if err := repo.PlanSteps(tx.Statement.Context, scan.TenantID, job.ID, admission.Steps); err != nil {
			return err
		}
		disposition := "created"
		if previous.ID == job.ID {
			disposition = "reused"
		} else if job.Generation > 1 {
			disposition = "updated"
		}
		if err := saveProcessingRunItem(tx, scan, types.SyncRunItem{Kind: "document", ExternalID: input.ExternalID, JobID: job.ID, SourceRevision: job.SourceRevision, Disposition: disposition}); err != nil {
			return err
		}
		if err := commitLegacyRetryAdmission(tx, scan, job, input); err != nil {
			return err
		}
		detail, _ := json.Marshal(map[string]any{"document_job_id": job.ID, "generation": job.Generation})
		if err := appendProcessingEvent(tx, scan, types.ProcessingEvent{Type: "document_admitted", StepID: step.ID, RunID: scan.OriginRunID, Detail: detail}); err != nil {
			return err
		}
	}
	return nil
}

func saveProcessingRunItem(tx *gorm.DB, scan *types.ProcessingJob, item types.SyncRunItem) error {
	if strings.TrimSpace(item.ExternalID) == "" || len(item.ExternalID)+len(item.Kind)+1 > 512 {
		return errors.New("invalid scan item identity")
	}
	item.RunID, item.TenantID, item.ItemKey = scan.OriginRunID, scan.TenantID, item.Kind+":"+item.ExternalID
	var previous types.SyncRunItem
	err := tx.Where("run_id = ? AND external_id = ?", item.RunID, item.ExternalID).Take(&previous).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if previous.ItemKey != "" {
		if previous.Kind != item.Kind || previous.TenantID != item.TenantID {
			return errors.New("SCAN_ITEM_CHANGED")
		}
		if item.JobID == "" {
			return nil
		}
		if previous.JobID != "" {
			if previous.JobID != item.JobID || previous.SourceRevision != item.SourceRevision {
				return ErrProcessingConflict
			}
			return nil
		}
		return tx.Model(&types.SyncRunItem{}).Where("run_id = ? AND item_key = ?", item.RunID, item.ItemKey).
			Updates(map[string]any{"job_id": item.JobID, "source_revision": item.SourceRevision, "disposition": item.Disposition}).Error
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error
}
