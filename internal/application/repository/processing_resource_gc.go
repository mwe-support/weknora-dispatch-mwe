package repository

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// Only lifecycle-owned resources whose producer has already retired are
// eligible. Legacy uploads and retained/pinned source artifacts are excluded.
func (r *ProcessingRepository) ClaimProcessingResourceGarbage(ctx context.Context, after string, limit int) (result []types.StoredResource, next string, failures error) {
	if limit < 1 || limit > 100 {
		return nil, after, ErrProcessingConflict
	}
	now, err := processingDBTime(r.db.WithContext(ctx))
	if err != nil {
		return nil, after, err
	}
	var candidates []types.StoredResource
	err = r.db.WithContext(ctx).Unscoped().Model(&types.StoredResource{}).Select("resources.*").Joins("JOIN processing_jobs j ON j.id = resources.creation_job_id AND j.tenant_id = resources.tenant_id").
		Where("resources.id > ? AND j.retirement_state = ? AND resources.state IN ? AND (resources.state = ? OR resources.updated_at < ?)", after, "deleted", []string{types.ResourceStateActive, "deleting"}, "deleting", now.Add(-7*24*time.Hour)).Order("resources.id").Limit(limit).Find(&candidates).Error
	if err != nil {
		return nil, after, err
	}
	for _, candidate := range candidates {
		var claimed *types.StoredResource
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			resource, err := lockStoredResource(tx, candidate.TenantID, candidate.ID)
			if err != nil {
				return err
			}
			if resource.State != types.ResourceStateActive && resource.State != "deleting" {
				return nil
			}
			if resource.State != "deleting" && !resource.UpdatedAt.Before(now.Add(-7*24*time.Hour)) {
				return nil
			}
			var retired int64
			if err := tx.Model(&types.ProcessingJob{}).Where("id = ? AND tenant_id = ? AND retirement_state = ?", resource.CreationJobID, resource.TenantID, "deleted").Count(&retired).Error; err != nil {
				return err
			}
			if retired != 1 {
				return nil
			}
			var bindings []types.ResourceBinding
			if err := tx.Where("resource_id = ?", resource.ID).Find(&bindings).Error; err != nil {
				return err
			}
			released := false
			for _, binding := range bindings {
				if binding.OwnerType != "faq_chunk" || binding.TenantID != resource.TenantID {
					continue
				}
				var chunk types.Chunk
				err := tx.Where("id = ? AND tenant_id = ? AND chunk_type = ?", binding.OwnerID, binding.TenantID, types.ChunkTypeFAQ).Take(&chunk).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if err == nil {
					handles, err := faqChunkResourceHandles(&chunk)
					if err != nil {
						return err
					}
					if slices.Contains(handles, resource.Handle) {
						continue
					}
				}
				if err := tx.Where("id = ? AND resource_id = ?", binding.ID, resource.ID).Delete(&types.ResourceBinding{}).Error; err != nil {
					return err
				}
				released = true
			}
			if released {
				// The grace period starts when the last effective FAQ reference disappears,
				// not when its original source happened to upload the object.
				return tx.Unscoped().Model(resource).Update("updated_at", now).Error
			}
			var held int64
			if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&held).Error; err != nil {
				return err
			}
			if held > 0 {
				return nil
			}
			if err := tx.Unscoped().Model(resource).Update("state", "deleting").Error; err != nil {
				return err
			}
			resource.State = "deleting"
			claimed = resource
			return nil
		})
		if err == nil && claimed != nil {
			result = append(result, *claimed)
		}
		failures = errors.Join(failures, err)
	}
	if len(candidates) == limit {
		next = candidates[len(candidates)-1].ID
	}
	return
}

func (r *ProcessingRepository) ProcessingResourceGarbageDeleted(ctx context.Context, tenant uint64, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
		var count int64
		if err := tx.Model(&types.ProcessingJob{}).Where("id = ? AND tenant_id = ? AND retirement_state = ?", resource.CreationJobID, tenant, "deleted").Count(&count).Error; err != nil {
			return err
		}
		if count != 1 {
			return ErrProcessingConflict
		}
		if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return ErrProcessingConflict
		}
		if err := releaseProcessingResourceStorage(tx, resource); err != nil {
			return err
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		return tx.Unscoped().Model(resource).Updates(map[string]any{"state": types.ResourceStateDeleted, "deleted_at": now}).Error
	})
}
