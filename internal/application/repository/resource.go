package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type resourceRepository struct{ db *gorm.DB }

// NewResourceRepository creates the persistence adapter for resource metadata.
func NewResourceRepository(db *gorm.DB) interfaces.ResourceRepository {
	return &resourceRepository{db: db}
}

func (r *resourceRepository) Create(ctx context.Context, resource *types.StoredResource) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if lease, ok := types.ProcessingLeaseFromContext(ctx); ok {
			job, err := lockProcessingLease(tx, resource.TenantID, lease)
			if err != nil {
				return err
			}
			if job.RetirementState != "retained" || lease.Step.Phase == types.ProcessingPhaseRetire {
				return ErrProcessingConflict
			}
			resource.CreationJobID = job.ID
		}
		if resource.StorageBackendID != "" {
			q := tx.Where("id = ? AND tenant_id = ? AND status = ?", resource.StorageBackendID, resource.TenantID, types.StorageBackendStatusActive)
			if tx.Dialector.Name() == "postgres" {
				q = q.Clauses(clause.Locking{Strength: "SHARE"})
			}
			var backend types.StorageBackend
			if err := q.Take(&backend).Error; err != nil {
				return err
			}
		}
		if err := tx.Create(resource).Error; err != nil {
			return err
		}
		return commitProcessingStorage(tx, ctx, resource)
	})
}

func (r *resourceRepository) GetByID(ctx context.Context, id string) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).Where("id = ? AND state = ?", id, types.ResourceStateActive).First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func (r *resourceRepository) GetByHandle(ctx context.Context, handle string) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).
		Where("handle = ? AND state = ?", handle, types.ResourceStateActive).
		First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func (r *resourceRepository) GetByTenantLocation(
	ctx context.Context,
	tenantID uint64,
	locationHash string,
) (*types.StoredResource, error) {
	var resource types.StoredResource
	err := r.db.WithContext(ctx).
		Where(
			"tenant_id = ? AND location_hash = ? AND state = ?",
			tenantID,
			locationHash,
			types.ResourceStateActive,
		).
		First(&resource).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &resource, err
}

func (r *resourceRepository) MarkDeleted(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var resource types.StoredResource
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).Take(&resource).Error; err != nil {
			return err
		}
		if err := releaseProcessingResourceStorage(tx, &resource); err != nil {
			return err
		}
		return tx.Unscoped().Model(&resource).Updates(map[string]any{"state": types.ResourceStateDeleted, "deleted_at": time.Now()}).Error
	})
}

func (r *resourceRepository) CreateBinding(ctx context.Context, binding *types.ResourceBinding) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tenant, ok := types.TenantIDFromContext(ctx); ok && tenant != binding.TenantID {
			return ErrProcessingScope
		}
		if binding.OwnerType == "processing_job" {
			lease, ok := types.ProcessingLeaseFromContext(ctx)
			if !ok || lease.Ref.JobID != binding.OwnerID {
				return ErrProcessingConflict
			}
			job, err := lockProcessingLease(tx, binding.TenantID, lease)
			if err != nil {
				return err
			}
			if job.RetirementState != "retained" {
				return ErrProcessingConflict
			}
		}
		resource, err := lockStoredResource(tx, binding.TenantID, binding.ResourceID)
		if err != nil {
			return err
		}
		if resource.State != types.ResourceStateActive || resource.DeletedAt.Valid {
			return ErrProcessingConflict
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(binding).Error
	})
}

func (r *resourceRepository) CreateGrant(ctx context.Context, grant *types.ResourceAccessGrant) error {
	return r.db.WithContext(ctx).Create(grant).Error
}

func (r *resourceRepository) GetValidGrant(
	ctx context.Context,
	tokenHash string,
	now time.Time,
) (*types.ResourceAccessGrant, error) {
	var grant types.ResourceAccessGrant
	err := r.db.WithContext(ctx).
		Where("token_hash = ? AND revoked_at IS NULL AND expires_at > ?", tokenHash, now).
		First(&grant).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &grant, err
}

// DeleteExpiredGrants drops grants that are past their expiry. A revoked grant
// is kept until then on purpose: it is the tombstone that stops a token derived
// for the same resource and window from re-creating the row and reviving access.
func (r *resourceRepository) DeleteExpiredGrants(ctx context.Context, before time.Time) error {
	return r.db.WithContext(ctx).
		Where("expires_at <= ?", before).
		Delete(&types.ResourceAccessGrant{}).Error
}
