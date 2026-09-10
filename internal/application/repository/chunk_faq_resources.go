package repository

import (
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

var faqResourceReference = regexp.MustCompile(`resource://[A-Za-z0-9_-]+`)

func faqChunkResourceHandles(chunk *types.Chunk) ([]string, error) {
	meta, err := chunk.FAQMetadata()
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil
	}
	var handles []string
	for _, ref := range faqResourceReference.FindAllString(strings.Join(meta.Answers, "\n"), -1) {
		handle, valid := types.ParseResourcePath(ref)
		if !valid {
			return nil, errors.New("FAQ_RESOURCE_REFERENCE_INVALID")
		}
		handles = append(handles, handle)
	}
	slices.Sort(handles)
	return slices.Compact(handles), nil
}

// Run in the same transaction as every FAQ snapshot write. The resource row
// lock excludes collection before a newly referenced answer becomes visible.
func bindFAQChunkResources(tx *gorm.DB, chunk *types.Chunk) error {
	if chunk.ChunkType != types.ChunkTypeFAQ {
		return nil
	}
	handles, err := faqChunkResourceHandles(chunk)
	if err != nil {
		return err
	}
	for _, handle := range handles {
		var resource types.StoredResource
		if err := tx.Where("tenant_id = ? AND handle = ?", chunk.TenantID, handle).Take(&resource).Error; err != nil {
			return err
		}
		locked, err := lockStoredResource(tx, chunk.TenantID, resource.ID)
		if err != nil {
			return err
		}
		if locked.State != types.ResourceStateActive || locked.DeletedAt.Valid {
			return ErrChunkRevisionConflict
		}
		var existing int64
		if err := tx.Model(&types.ResourceBinding{}).Where("tenant_id = ? AND resource_id = ? AND owner_type = ? AND owner_id = ?", chunk.TenantID, resource.ID, "faq_chunk", chunk.ID).Count(&existing).Error; err != nil {
			return err
		}
		if existing == 0 {
			binding := types.ResourceBinding{TenantID: chunk.TenantID, ResourceID: resource.ID, OwnerType: "faq_chunk", OwnerID: chunk.ID, Relation: "faq_answer"}
			if err := tx.Create(&binding).Error; err != nil {
				return err
			}
		}
	}
	// Old bindings are reconciled by the bounded collector against current FAQ
	// answers. Keeping them until then cannot prematurely remove referenced media.
	return nil
}
