package repository

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func lockProcessingLease(tx *gorm.DB, tenant uint64, lease types.ProcessingLease) (*types.ProcessingJob, error) {
	job, err := lockProcessingJob(tx, tenant, lease.Ref.JobID, lease.Ref.StepID)
	if err != nil {
		return nil, err
	}
	var step types.ProcessingStep
	if err := tx.Where("id = ? AND job_id = ?", lease.Ref.StepID, job.ID).Take(&step).Error; err != nil {
		return nil, err
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return nil, err
	}
	if !processingRefMatches(job, &step, lease.Ref) || !processingStepEligible(job, &step) || step.Status != types.ProcessingRunning ||
		lease.Token == "" || step.LeaseToken != lease.Token || step.LeaseExpiresAt == nil || !step.LeaseExpiresAt.After(now) {
		return nil, ErrProcessingConflict
	}
	return job, nil
}

func (r *ProcessingRepository) ValidateLease(ctx context.Context, tenant uint64, lease types.ProcessingLease) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		_, err := lockProcessingLease(tx, tenant, lease)
		return err
	})
}

func (r *ProcessingRepository) RecordIndexDestination(ctx context.Context, tenant uint64, lease types.ProcessingLease, destination types.ProcessingIndexDestination) error {
	if (lease.Step.Stage != "index" && lease.Step.Stage != "faq_write" && lease.Step.Stage != "legacy_snapshot") || len(destination.Kinds) == 0 || destination.KnowledgeType == "" || destination.Dimension < 0 ||
		(slices.Contains(destination.Kinds, types.VectorRetrieverType) && destination.Dimension == 0) {
		return ErrProcessingConflict
	}
	encoded, err := json.Marshal(destination)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if len(job.IndexDestination) > 0 {
			var existing types.ProcessingIndexDestination
			if json.Unmarshal(job.IndexDestination, &existing) != nil {
				return ErrProcessingConflict
			}
			prior, _ := json.Marshal(existing)
			if string(prior) != string(encoded) {
				return ErrProcessingConflict
			}
			return nil
		}
		if err := lockProcessingVectorStores(tx, tenant, destination); err != nil {
			return err
		}
		if err := tx.Model(job).Update("index_destination", types.JSON(encoded)).Error; err != nil {
			return err
		}
		return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "index_destination_recorded", StepID: lease.Step.ID, Attempt: lease.Ref.Attempt})
	})
}

// Deletion takes UPDATE on the same rows before checking retained jobs.
// Lock both routes in order when a legacy adoption moves between stores.
func lockProcessingVectorStores(tx *gorm.DB, tenant uint64, destinations ...types.ProcessingIndexDestination) error {
	var ids []string
	for _, destination := range destinations {
		if destination.VectorStoreID != nil && *destination.VectorStoreID != "" {
			ids = append(ids, *destination.VectorStoreID)
		}
	}
	slices.Sort(ids)
	for _, id := range slices.Compact(ids) {
		q := tx.Where("id = ? AND tenant_id = ?", id, tenant)
		if tx.Dialector.Name() == "postgres" {
			q = q.Clauses(clause.Locking{Strength: "SHARE"})
		}
		var store types.VectorStore
		if err := q.Take(&store).Error; err != nil {
			return err
		}
	}
	return nil
}

// The resource row is the shared exclusion point for every binding, including
// legacy knowledge attachments. Processing work locks its source first.
func lockStoredResource(tx *gorm.DB, tenant uint64, id string) (*types.StoredResource, error) {
	q := tx.Unscoped().Where("id = ? AND tenant_id = ?", id, tenant)
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var resource types.StoredResource
	if err := q.Take(&resource).Error; err != nil {
		return nil, err
	}
	return &resource, nil
}

func processingOwnedResources(tx *gorm.DB, job *types.ProcessingJob) *gorm.DB {
	bindings := tx.Model(&types.ResourceBinding{}).Select("resource_id").Where("tenant_id = ? AND owner_type = ? AND owner_id = ?", job.TenantID, "processing_job", job.ID)
	return tx.Unscoped().Model(&types.StoredResource{}).Where("tenant_id = ? AND (creation_job_id = ? OR id IN (?))", job.TenantID, job.ID, bindings)
}

// Claim each unreferenced file before network deletion. A crash leaves a
// deleting row and its physical destination available to the next attempt.
func (r *ProcessingRepository) RetirementResources(ctx context.Context, tenant uint64, lease types.ProcessingLease) (resources []types.StoredResource, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if lease.Step.Stage != "retire" || job.RetirementState != "deleting" {
			return ErrProcessingConflict
		}
		var ids []string
		if err := processingOwnedResources(tx, job).Where("state <> ?", types.ResourceStateDeleted).Order("id").Pluck("id", &ids).Error; err != nil {
			return err
		}
		for _, id := range ids {
			resource, err := lockStoredResource(tx, tenant, id)
			if err != nil {
				return err
			}
			var others int64
			if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ? AND NOT (tenant_id = ? AND owner_type = ? AND owner_id = ?)", id, tenant, "processing_job", job.ID).Count(&others).Error; err != nil {
				return err
			}
			if others > 0 {
				if resource.State != types.ResourceStateActive {
					return ErrProcessingConflict
				}
				if err := tx.Where("resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ?", id, tenant, "processing_job", job.ID).Delete(&types.ResourceBinding{}).Error; err != nil {
					return err
				}
				continue
			}
			if err := tx.Unscoped().Model(resource).Update("state", "deleting").Error; err != nil {
				return err
			}
			resource.State = "deleting"
			resources = append(resources, *resource)
		}
		return nil
	})
	return
}

func (r *ProcessingRepository) RetirementResourceDeleted(ctx context.Context, tenant uint64, lease types.ProcessingLease, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if lease.Step.Stage != "retire" || job.RetirementState != "deleting" {
			return ErrProcessingConflict
		}
		var owned int64
		if err := processingOwnedResources(tx, job).Where("id = ?", id).Count(&owned).Error; err != nil {
			return err
		}
		if owned != 1 {
			return ErrProcessingScope
		}
		resource, err := lockStoredResource(tx, tenant, id)
		if err != nil {
			return err
		}
		if resource.State == types.ResourceStateDeleted {
			return nil
		}
		if resource.State != "deleting" {
			return ErrProcessingConflict
		}
		if err := releaseProcessingResourceStorage(tx, resource); err != nil {
			return err
		}
		if err := tx.Unscoped().Model(resource).Updates(map[string]any{"state": types.ResourceStateDeleted, "deleted_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		return tx.Where("resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ?", id, tenant, "processing_job", job.ID).Delete(&types.ResourceBinding{}).Error
	})
}

func finishProcessingResourceCleanup(tx *gorm.DB, job *types.ProcessingJob, now time.Time) error {
	var count int64
	if err := tx.Model(&types.ProcessingStorageReservation{}).Where("tenant_id = ? AND job_id = ? AND state = ?", job.TenantID, job.ID, "reserved").Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("RETIREMENT_STORAGE_RESERVATIONS_PENDING")
	}
	others := tx.Model(&types.ResourceBinding{}).Select("1").Where("resource_id = resources.id AND NOT (tenant_id = ? AND owner_type = ? AND owner_id = ?)", job.TenantID, "processing_job", job.ID)
	if err := processingOwnedResources(tx, job).Where("state = ? OR (state <> ? AND NOT EXISTS (?))", "deleting", types.ResourceStateDeleted, others).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("processing resource deletion is incomplete")
	}
	if err := tx.Model(&types.ResourceBinding{}).Where("tenant_id = ? AND owner_type = ? AND owner_id = ?", job.TenantID, "processing_job", job.ID).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("processing resource bindings have not been released")
	}
	if job.KnowledgeID == "" {
		return nil
	}
	var knowledge types.Knowledge
	err := tx.Unscoped().Where("id = ? AND tenant_id = ? AND metadata->>'processing_job_id' = ?", job.KnowledgeID, job.TenantID, job.ID).Take(&knowledge).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := tx.Unscoped().Where("knowledge_id = ? AND tenant_id = ?", job.KnowledgeID, job.TenantID).Delete(&types.Chunk{}).Error; err != nil {
		return err
	}
	return tx.Unscoped().Model(&knowledge).Updates(map[string]any{"deleted_at": now, "enable_status": "disabled"}).Error
}
