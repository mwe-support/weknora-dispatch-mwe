package repository

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func (r *chunkRepository) RegisterFAQIndexWrites(ctx context.Context, writes []types.FAQIndexWrite) error {
	if len(writes) == 0 {
		return ErrChunkRevisionConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lease, managed := types.ProcessingLeaseFromContext(ctx)
		if managed {
			if _, err := lockProcessingLease(tx, lease.Job.TenantID, lease); err != nil {
				return err
			}
		}
		var scopes []*types.Chunk
		for _, w := range writes {
			scopes = append(scopes, &types.Chunk{TenantID: w.TenantID, KnowledgeBaseID: w.KnowledgeBaseID, ChunkType: types.ChunkTypeFAQ})
		}
		if err := lockFAQChunkScopes(tx, scopes); err != nil {
			return err
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		for _, w := range writes {
			var keys []string
			var destination types.ProcessingIndexDestination
			if w.ID == "" || w.TenantID == 0 || w.KnowledgeID == "" || w.ChunkID == "" || w.BaseRevision < 0 || w.ContentDigest == "" || w.EstimatedBytes < 0 ||
				json.Unmarshal(w.SourceIDs, &keys) != nil || len(keys) == 0 || json.Unmarshal(w.Destination, &destination) != nil ||
				destination.KnowledgeType != types.KnowledgeTypeFAQ || len(destination.Kinds) == 0 || destination.Dimension < 0 ||
				(slices.Contains(destination.Kinds, types.VectorRetrieverType) && destination.Dimension == 0) ||
				((destination.VectorStoreID == nil || *destination.VectorStoreID == "") && destination.EnvironmentDigest == "") {
				return ErrChunkRevisionConflict
			}
			seen := map[string]bool{}
			for _, key := range keys {
				if len(key) != 63 || !strings.HasPrefix(key, "fq-") || seen[key] {
					return ErrChunkRevisionConflict
				}
				if _, err := hex.DecodeString(key[3:]); err != nil {
					return ErrChunkRevisionConflict
				}
				seen[key] = true
			}
			var canonical types.Knowledge
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?", w.KnowledgeID, w.TenantID, w.KnowledgeBaseID, types.KnowledgeTypeFAQ).Take(&canonical).Error; err != nil {
				return err
			}
			var row types.Chunk
			err := tx.Unscoped().Where("id = ?", w.ChunkID).Take(&row).Error
			if w.NewEntry {
				if !managed || !errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrChunkRevisionConflict
				}
			} else if err != nil || row.DeletedAt.Valid || row.TenantID != w.TenantID || row.KnowledgeBaseID != w.KnowledgeBaseID || row.KnowledgeID != w.KnowledgeID || row.ChunkType != types.ChunkTypeFAQ || row.ContentRevision != w.BaseRevision {
				return ErrChunkRevisionConflict
			}
			w.JobID, w.StepID, w.Attempt = "", "", 0
			if managed {
				if lease.Job.TenantID != w.TenantID || lease.Job.KnowledgeBaseID != w.KnowledgeBaseID {
					return ErrProcessingScope
				}
				w.JobID, w.StepID, w.Attempt = lease.Job.ID, lease.Step.ID, lease.Ref.Attempt
			}
			w.State, w.CreatedAt, w.UpdatedAt = "pending", now, now
			w.StorageReleased = false
			if w.EstimatedBytes > 0 {
				owner, err := lockProcessingStorageTenant(tx, w.TenantID)
				if err != nil {
					return err
				}
				if owner.StorageUsed < 0 || owner.StorageQuota < owner.StorageUsed || w.EstimatedBytes > owner.StorageQuota-owner.StorageUsed {
					return types.NewStorageQuotaExceededError()
				}
				if err := tx.Model(owner).UpdateColumn("storage_used", owner.StorageUsed+w.EstimatedBytes).Error; err != nil {
					return err
				}
			}
			if err := tx.Create(&w).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *chunkRepository) ConfirmFAQIndexWrites(ctx context.Context, tenant uint64, ids []string) error {
	if tenant == 0 || len(ids) == 0 {
		return ErrChunkRevisionConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lease, managed := types.ProcessingLeaseFromContext(ctx)
		if managed {
			if _, err := lockProcessingLease(tx, tenant, lease); err != nil {
				return err
			}
		}
		var writes []types.FAQIndexWrite
		if err := tx.Where("tenant_id = ? AND id IN ?", tenant, ids).Find(&writes).Error; err != nil {
			return err
		}
		if len(writes) != len(ids) {
			return ErrChunkRevisionConflict
		}
		var scopes []*types.Chunk
		for _, w := range writes {
			scopes = append(scopes, &types.Chunk{TenantID: tenant, KnowledgeBaseID: w.KnowledgeBaseID, ChunkType: types.ChunkTypeFAQ})
		}
		if err := lockFAQChunkScopes(tx, scopes); err != nil {
			return err
		}
		for _, w := range writes {
			if (w.JobID != "" && (!managed || w.JobID != lease.Job.ID || w.StepID != lease.Step.ID || w.Attempt != lease.Ref.Attempt)) || (w.JobID == "" && managed) {
				return ErrProcessingConflict
			}
			result := tx.Model(&types.FAQIndexWrite{}).Where("id = ? AND tenant_id = ? AND state IN ?", w.ID, tenant, []string{"pending", "indexed"}).Update("state", "indexed")
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrChunkRevisionConflict
			}
		}
		return nil
	})
}

// Called under the same KB lock as GC, before the chunk CAS. A body-only manual
// edit may keep its old manifest; the digest visibility guard hides stale hits.
func validateFAQIndexManifest(tx *gorm.DB, chunk *types.Chunk) error {
	var old types.Chunk
	err := tx.Select("faq_index_manifest").Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", chunk.ID, chunk.TenantID, chunk.KnowledgeBaseID).Take(&old).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if chunk.FAQIndexManifest == "" {
		if old.FAQIndexManifest != "" {
			return ErrChunkRevisionConflict
		}
		return nil
	}
	if err == nil && old.FAQIndexManifest == chunk.FAQIndexManifest {
		return nil
	}
	var manifest types.FAQIndexManifest
	if json.Unmarshal([]byte(chunk.FAQIndexManifest), &manifest) != nil || len(manifest.WriteIDs) == 0 || len(manifest.SourceIDs) == 0 {
		return ErrChunkRevisionConflict
	}
	digest, err := chunk.FAQIndexContentDigest()
	if err != nil || digest != manifest.ContentDigest {
		return ErrChunkRevisionConflict
	}
	var writes []types.FAQIndexWrite
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND chunk_id = ? AND id IN ? AND state = ? AND content_digest = ?", chunk.TenantID, chunk.KnowledgeBaseID, chunk.KnowledgeID, chunk.ID, manifest.WriteIDs, "indexed", digest).Find(&writes).Error; err != nil {
		return err
	}
	if len(writes) != len(manifest.WriteIDs) {
		return ErrChunkRevisionConflict
	}
	keys := map[string]bool{}
	for _, w := range writes {
		var ids []string
		if json.Unmarshal(w.SourceIDs, &ids) != nil {
			return ErrChunkRevisionConflict
		}
		for _, id := range ids {
			if keys[id] {
				return ErrChunkRevisionConflict
			}
			keys[id] = true
		}
	}
	if len(keys) != len(manifest.SourceIDs) {
		return ErrChunkRevisionConflict
	}
	for _, id := range manifest.SourceIDs {
		if !keys[id] {
			return ErrChunkRevisionConflict
		}
		delete(keys, id)
	}
	return nil
}

func faqIndexWriteReferenced(tx *gorm.DB, w types.FAQIndexWrite) (bool, error) {
	var chunk types.Chunk
	err := tx.Select("faq_index_manifest").Where("id = ? AND tenant_id = ?", w.ChunkID, w.TenantID).Take(&chunk).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	if chunk.FAQIndexManifest != "" {
		var manifest types.FAQIndexManifest
		if json.Unmarshal([]byte(chunk.FAQIndexManifest), &manifest) != nil {
			return true, nil
		}
		if slices.Contains(manifest.WriteIDs, w.ID) {
			return true, nil
		}
		var keys []string
		if json.Unmarshal(w.SourceIDs, &keys) != nil {
			return true, nil
		}
		for _, key := range keys {
			if slices.Contains(manifest.SourceIDs, key) {
				return true, nil
			}
		}
	}
	if w.JobID != "" {
		var count int64
		// Confirmed private outputs also belong to retained/pinned versions.
		err := tx.Table("processing_steps AS s").Joins("JOIN processing_jobs AS j ON j.id = s.job_id").Where("j.id = ? AND j.tenant_id = ? AND j.retirement_state = ? AND s.id = ? AND s.step_attempt = ? AND s.status <> ?", w.JobID, w.TenantID, "retained", w.StepID, w.Attempt, types.ProcessingSuperseded).Count(&count).Error
		return count > 0, err
	}
	return false, nil
}

// Claim a bounded global page for the internal maintenance runner. The cursor
// advances across retained rows, so one protected version cannot starve others.
func (r *ProcessingRepository) ClaimFAQIndexGarbage(ctx context.Context, after string, limit int) (result []types.FAQIndexWrite, next string, failures error) {
	if limit < 1 || limit > 100 {
		return nil, after, ErrChunkRevisionConflict
	}
	now, err := processingDBTime(r.db.WithContext(ctx))
	if err != nil {
		return nil, after, err
	}
	var candidates []types.FAQIndexWrite
	if err := r.db.WithContext(ctx).Where("id > ? AND (state = ? OR updated_at < ?)", after, "deleting", now.Add(-7*24*time.Hour)).Order("id").Limit(limit).Find(&candidates).Error; err != nil {
		return nil, after, err
	}
	for _, candidate := range candidates {
		var claimed *types.FAQIndexWrite
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := lockFAQKnowledgeBase(tx.Unscoped(), candidate.TenantID, candidate.KnowledgeBaseID); err != nil {
				return err
			}
			var w types.FAQIndexWrite
			if err := tx.Where("id = ? AND tenant_id = ?", candidate.ID, candidate.TenantID).Take(&w).Error; err != nil {
				return err
			}
			if w.State != "deleting" && !w.UpdatedAt.Before(now.Add(-7*24*time.Hour)) {
				return nil
			}
			referenced, err := faqIndexWriteReferenced(tx, w)
			if err != nil {
				return err
			}
			if referenced {
				return nil
			}
			if err := tx.Model(&w).Update("state", "deleting").Error; err != nil {
				return err
			}
			w.State = "deleting"
			claimed = &w
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

func (r *chunkRepository) FAQIndexGarbageDeleted(ctx context.Context, tenant uint64, id string) error {
	// Keep the tombstone: periodic exact-key deletion reconciles a backend write
	// that completed after its expired worker lost the acknowledgement.
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var write types.FAQIndexWrite
		if err := tx.Where("id = ? AND tenant_id = ?", id, tenant).Take(&write).Error; err != nil {
			return err
		}
		if err := lockFAQKnowledgeBase(tx.Unscoped(), tenant, write.KnowledgeBaseID); err != nil {
			return err
		}
		if err := tx.Where("id = ? AND tenant_id = ?", id, tenant).Take(&write).Error; err != nil {
			return err
		}
		if write.State != "deleting" && write.State != "deleted" {
			return ErrChunkRevisionConflict
		}
		if !write.StorageReleased && write.EstimatedBytes > 0 {
			owner, err := lockProcessingStorageTenant(tx, tenant)
			if err != nil {
				return err
			}
			if owner.StorageUsed < write.EstimatedBytes {
				return errors.New("FAQ_STORAGE_ACCOUNTING_MISMATCH")
			}
			if err := tx.Model(owner).UpdateColumn("storage_used", owner.StorageUsed-write.EstimatedBytes).Error; err != nil {
				return err
			}
		}
		return tx.Model(&write).Updates(map[string]any{"state": "deleted", "storage_released": true}).Error
	})
}
