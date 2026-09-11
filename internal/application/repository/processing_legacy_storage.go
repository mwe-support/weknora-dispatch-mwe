package repository

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Registering an existing verified object does not allocate new file bytes.
// Keep the original legacy accounting, while registering and binding it in
// one lease transaction so a failed bind cannot leave a collectible orphan.
func (r *ProcessingRepository) BindLegacyResource(ctx context.Context, lease types.ProcessingLease, address string, size int64, digest, kind string) error {
	if lease.Step.Stage != "legacy_snapshot" || (kind != "file" && kind != "image") || size <= 0 || !legacyDigest(digest) {
		return ErrProcessingConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, lease.Job.TenantID, lease)
		if err != nil {
			return err
		}
		return bindLegacyResource(tx, job, types.ProcessingLegacyResource{Reference: address, Bytes: size, Digest: digest, Kind: kind})
	})
}

// Both callers already hold KB/source/job ownership: admission, or a lease.
func bindLegacyResource(tx *gorm.DB, job *types.ProcessingJob, input types.ProcessingLegacyResource) error {
	address, size, digest, kind := input.Reference, input.Bytes, input.Digest, input.Kind
	ctx := tx.Statement.Context
	if (kind != "file" && kind != "image") || size <= 0 || !legacyDigest(digest) {
		return ErrProcessingConflict
	}
	if err := (&knowledgeRepository{db: tx}).ValidateLegacyStorageOwnership(ctx, job.TenantID, job.KnowledgeID, address, kind == "file"); err != nil {
		return err
	}
	var resource types.StoredResource
	var err error
	if handle, ok := types.ParseResourcePath(address); ok {
		err = tx.Where("tenant_id = ? AND handle = ? AND state = ?", job.TenantID, handle, types.ResourceStateActive).Take(&resource).Error
	} else {
		location := fmt.Sprintf("%x", sha256.Sum256([]byte(address)))
		err = tx.Where("tenant_id = ? AND location_hash = ? AND state = ?", job.TenantID, location, types.ResourceStateActive).Take(&resource).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			backendID, _, _ := types.ParseStorageBackendPath(address)
			if backendID != "" {
				var backend types.StorageBackend
				if err := tx.Where("id = ? AND tenant_id = ? AND status = ?", backendID, job.TenantID, types.StorageBackendStatusActive).Take(&backend).Error; err != nil {
					return err
				}
			}
			handle := uuid.New()
			resource = types.StoredResource{Handle: base64.RawURLEncoding.EncodeToString(handle[:]), TenantID: job.TenantID, PhysicalPath: address, LocationHash: location, Provider: types.ParseProviderScheme(address), StorageBackendID: backendID, Kind: kind, Size: size, ContentHash: digest}
			err = tx.Create(&resource).Error
		}
	}
	if err != nil {
		return err
	}
	if resource.Size != size || (len(resource.ContentHash) == 64 && resource.ContentHash != digest) {
		return errors.New("LEGACY_RESOURCE_CHECKSUM_MISMATCH")
	}
	binding := types.ResourceBinding{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(job.ID+"/legacy/"+resource.ID)).String(), TenantID: job.TenantID, ResourceID: resource.ID, OwnerType: "processing_job", OwnerID: job.ID, Relation: "legacy_" + kind}
	locked, err := lockStoredResource(tx, job.TenantID, resource.ID)
	if err != nil {
		return err
	}
	if locked.State != types.ResourceStateActive || locked.DeletedAt.Valid {
		return ErrProcessingConflict
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&binding).Error
}

// Called only for the source file or image URLs in a verified, exact-attempt
// snapshot. Older SaveBytes images had no per-knowledge registry binding.
func (r *knowledgeRepository) ValidateLegacyStorageOwnership(ctx context.Context, tenant uint64, knowledgeID, address string, sourceFile bool) error {
	var knowledge types.Knowledge
	if err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ?", knowledgeID, tenant).Take(&knowledge).Error; err != nil {
		return err
	}
	if sourceFile && knowledge.FilePath != address {
		return ErrProcessingScope
	}
	if handle, ok := types.ParseResourcePath(address); ok {
		var resource types.StoredResource
		if err := r.db.WithContext(ctx).Where("handle = ? AND tenant_id = ? AND state = ?", handle, tenant, types.ResourceStateActive).Take(&resource).Error; err != nil {
			return err
		}
		if sourceFile {
			var count int64
			if err := r.db.WithContext(ctx).Model(&types.ResourceBinding{}).Where("resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ? AND relation = ?", resource.ID, tenant, "knowledge", knowledgeID, "source_file").Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return errors.New("LEGACY_RESOURCE_BINDING_UNVERIFIED")
			}
		}
		return nil
	}
	if _, inner, ok := types.ParseStorageBackendPath(address); ok {
		address = inner
	}
	u, err := url.Parse(address)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || strings.Contains(address, "\\") {
		return errors.New("LEGACY_STORAGE_ADDRESS_UNVERIFIED")
	}
	var key string
	switch u.Scheme {
	case "minio":
		if u.Host == "" {
			return errors.New("LEGACY_STORAGE_ADDRESS_UNVERIFIED")
		}
		key = strings.TrimPrefix(u.Path, "/") // The MinIO reader additionally checks the configured bucket.
	case "local":
		key = u.Host + u.Path
	default:
		return errors.New("LEGACY_STORAGE_LAYOUT_UNVERIFIED")
	}
	parts := strings.Split(key, "/")
	owner := "exports"
	if sourceFile {
		owner = knowledgeID
	}
	if len(parts) != 3 || parts[0] != strconv.FormatUint(tenant, 10) || parts[1] != owner || parts[2] == "" || parts[2] == "." || parts[2] == ".." {
		return errors.New("LEGACY_STORAGE_OWNER_MISMATCH")
	}
	return nil
}
