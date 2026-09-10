package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func lockProcessingStorageTenant(tx *gorm.DB, tenant uint64) (*types.Tenant, error) {
	var row types.Tenant
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", tenant).Take(&row).Error
	return &row, err
}

// Reserve the backend's estimate for each immutable index attempt before I/O.
// A lost ACK retains the charge until the original destination is cleaned.
func (r *ProcessingRepository) ReserveIndexStorage(ctx context.Context, tenant uint64, lease types.ProcessingLease, bytes int64) error {
	if bytes < 0 || lease.Step.Stage != "index" {
		return ErrProcessingConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if job.KnowledgeID == "" || len(job.IndexDestination) == 0 || job.RetirementState != "retained" {
			return ErrProcessingConflict
		}
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("index-quota/%s/%d", lease.Step.ID, lease.Ref.Attempt))).String()
		var existing types.ProcessingStorageReservation
		err = tx.Where("id = ?", id).Take(&existing).Error
		if err == nil {
			if existing.JobID != job.ID || existing.Bytes != bytes || existing.State != "reserved" || existing.Kind != "index" {
				return ErrProcessingConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		owner, err := lockProcessingStorageTenant(tx, tenant)
		if err != nil {
			return err
		}
		if owner.StorageUsed < 0 || owner.StorageQuota < owner.StorageUsed || bytes > owner.StorageQuota-owner.StorageUsed {
			return types.NewStorageQuotaExceededError()
		}
		row := types.ProcessingStorageReservation{ID: id, TenantID: tenant, JobID: job.ID, StepID: lease.Step.ID, Attempt: lease.Ref.Attempt, Bytes: bytes, Kind: "index", State: "reserved", PhysicalPath: job.KnowledgeID}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		return tx.Model(owner).UpdateColumn("storage_used", owner.StorageUsed+bytes).Error
	})
}

func (r *resourceRepository) ReserveStorage(ctx context.Context, tenant uint64, bytes int64, temporary bool, backendID string) (string, error) {
	lease, ok := types.ProcessingLeaseFromContext(ctx)
	if !ok {
		return "", nil
	} // Legacy knowledge accounting owns its existing charges.
	if bytes < 0 {
		return "", ErrProcessingConflict
	}
	id := uuid.NewString()
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if job.RetirementState != "retained" || lease.Step.Phase == types.ProcessingPhaseRetire {
			return ErrProcessingConflict
		}
		if backendID != "" {
			var backend types.StorageBackend
			if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ? AND tenant_id = ? AND status = ?", backendID, tenant, types.StorageBackendStatusActive).Take(&backend).Error; err != nil {
				return err
			}
		}
		owner, err := lockProcessingStorageTenant(tx, tenant)
		if err != nil {
			return err
		}
		if owner.StorageUsed < 0 || owner.StorageQuota < owner.StorageUsed || bytes > owner.StorageQuota-owner.StorageUsed {
			return types.NewStorageQuotaExceededError()
		}
		reservation := types.ProcessingStorageReservation{ID: id, TenantID: tenant, JobID: job.ID, StepID: lease.Step.ID, Attempt: lease.Ref.Attempt, Bytes: bytes, Temporary: temporary, State: "reserved", StorageBackendID: backendID}
		if err := tx.Create(&reservation).Error; err != nil {
			return err
		}
		return tx.Model(owner).UpdateColumn("storage_used", owner.StorageUsed+bytes).Error
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *resourceRepository) SetStoragePath(ctx context.Context, tenant uint64, id, path string) error {
	if id == "" {
		return nil
	}
	// Recording a completed provider write must work after lease loss so its
	// exact physical object remains discoverable for compensation/retirement.
	if path == "" {
		return ErrProcessingConflict
	}
	backend, _, _ := types.ParseStorageBackendPath(path)
	result := r.db.WithContext(ctx).Model(&types.ProcessingStorageReservation{}).
		Where("id = ? AND tenant_id = ? AND state = ? AND (physical_path = '' OR physical_path = ?)", id, tenant, "reserved", path).
		Where("temporary = ? OR storage_backend_id = ?", true, backend).
		Updates(map[string]any{"physical_path": path, "updated_at": time.Now().UTC()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrProcessingConflict
	}
	return nil
}

func (r *resourceRepository) ReleaseStorage(ctx context.Context, tenant uint64, id string) error {
	if id == "" {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return releaseProcessingStorage(tx, tenant, id, false) })
}

func releaseProcessingStorage(tx *gorm.DB, tenant uint64, id string, committed bool) error {
	owner, err := lockProcessingStorageTenant(tx, tenant)
	if err != nil {
		return err
	}
	var reservation types.ProcessingStorageReservation
	if err := tx.Where("id = ? AND tenant_id = ?", id, tenant).Take(&reservation).Error; err != nil {
		return err
	}
	if reservation.State == "released" {
		return nil
	}
	if reservation.State != "reserved" && !(committed && reservation.State == "committed") {
		return ErrProcessingConflict
	}
	if owner.StorageUsed < reservation.Bytes {
		return errors.New("PROCESSING_STORAGE_ACCOUNTING_MISMATCH")
	}
	if err := tx.Model(owner).UpdateColumn("storage_used", owner.StorageUsed-reservation.Bytes).Error; err != nil {
		return err
	}
	return tx.Model(&reservation).Updates(map[string]any{"state": "released", "updated_at": time.Now().UTC()}).Error
}

func commitProcessingStorage(tx *gorm.DB, ctx context.Context, resource *types.StoredResource) error {
	if resource.CreationJobID == "" {
		return nil
	}
	id := types.ProcessingStorageReservationFromContext(ctx)
	if id == "" {
		return errors.New("PROCESSING_STORAGE_RESERVATION_MISSING")
	}
	if _, err := lockProcessingStorageTenant(tx, resource.TenantID); err != nil {
		return err
	}
	lease, ok := types.ProcessingLeaseFromContext(ctx)
	if !ok {
		return ErrProcessingConflict
	}
	result := tx.Model(&types.ProcessingStorageReservation{}).
		Where("id = ? AND tenant_id = ? AND job_id = ? AND bytes = ? AND state = ? AND temporary = ? AND physical_path = ?", id, resource.TenantID, resource.CreationJobID, resource.Size, "reserved", false, resource.PhysicalPath).
		Where("step_id = ? AND attempt = ? AND storage_backend_id = ?", lease.Step.ID, lease.Ref.Attempt, resource.StorageBackendID).
		Updates(map[string]any{"state": "committed", "resource_id": resource.ID, "updated_at": time.Now().UTC()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrProcessingConflict
	}
	return nil
}

func releaseProcessingResourceStorage(tx *gorm.DB, resource *types.StoredResource) error {
	if resource.CreationJobID == "" {
		return nil
	}
	var reservations []types.ProcessingStorageReservation
	if err := tx.Where("resource_id = ? AND tenant_id = ? AND state <> ?", resource.ID, resource.TenantID, "released").Find(&reservations).Error; err != nil {
		return err
	}
	for _, reservation := range reservations {
		if err := releaseProcessingStorage(tx, resource.TenantID, reservation.ID, true); err != nil {
			return err
		}
	}
	return nil
}

func (r *ProcessingRepository) RetirementStorageReservations(ctx context.Context, tenant uint64, lease types.ProcessingLease) (result []types.ProcessingStorageReservation, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if job.RetirementState != "deleting" || lease.Step.Stage != "retire" {
			return ErrProcessingConflict
		}
		return tx.Where("tenant_id = ? AND job_id = ? AND state = ?", tenant, job.ID, "reserved").Order("id").Find(&result).Error
	})
	return
}

func (r *ProcessingRepository) RetirementStorageRemoved(ctx context.Context, tenant uint64, lease types.ProcessingLease, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if job.RetirementState != "deleting" || lease.Step.Stage != "retire" {
			return ErrProcessingConflict
		}
		var count int64
		if err := tx.Model(&types.ProcessingStorageReservation{}).Where("id = ? AND tenant_id = ? AND job_id = ?", id, tenant, job.ID).Count(&count).Error; err != nil {
			return err
		}
		if count != 1 {
			return ErrProcessingScope
		}
		return releaseProcessingStorage(tx, tenant, id, false)
	})
}
