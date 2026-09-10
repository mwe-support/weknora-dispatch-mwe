package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Tencent/WeKnora/internal/common"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrFAQQuestionConflict = errors.New("FAQ question already exists")

func (r *chunkRepository) DeleteChunkSnapshot(ctx context.Context, chunk *types.Chunk) error {
	if chunk == nil || chunk.TenantID == 0 || chunk.ID == "" || chunk.KnowledgeBaseID == "" {
		return ErrChunkRevisionConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockFAQChunkScopes(tx, []*types.Chunk{chunk}); err != nil {
			return err
		}
		result := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND id = ? AND content_revision = ?", chunk.TenantID, chunk.KnowledgeBaseID, chunk.ID, chunk.ContentRevision).Delete(&types.Chunk{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChunkRevisionConflict
		}
		return nil
	})
}

// Match lifecycle's configuration lock order. A separate transaction lock
// serializes FAQ namespace changes without upgrading an existing KB SHARE lock.
func lockFAQKnowledgeBase(tx *gorm.DB, tenant uint64, kb string) error {
	query := tx.Select("id").Where("tenant_id = ? AND id = ?", tenant, kb)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := query.Take(&types.KnowledgeBase{}).Error; err != nil {
		return err
	}
	if tx.Dialector.Name() == "postgres" {
		return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", fmt.Sprintf("faq/%d/%s", tenant, kb)).Error
	}
	return nil // SQLite serializes write transactions.
}

func lockFAQChunkScopes(tx *gorm.DB, chunks []*types.Chunk) error {
	scopes := map[string]*types.Chunk{}
	for _, chunk := range chunks {
		if chunk != nil && chunk.ChunkType == types.ChunkTypeFAQ {
			scopes[fmt.Sprintf("%020d/%s", chunk.TenantID, chunk.KnowledgeBaseID)] = chunk
		}
	}
	keys := make([]string, 0, len(scopes))
	for key := range scopes {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		chunk := scopes[key]
		if err := lockFAQKnowledgeBase(tx, chunk.TenantID, chunk.KnowledgeBaseID); err != nil {
			return err
		}
	}
	return nil
}

func checkStoredFAQQuestions(tx *gorm.DB, chunk *types.Chunk) error {
	meta, err := chunk.FAQMetadata()
	if err != nil {
		return err
	}
	if meta == nil || meta.StandardQuestion == "" {
		return nil
	} // Preserve legacy incomplete entries.
	questions := append([]string{meta.StandardQuestion}, meta.SimilarQuestions...)
	duplicate, err := (&chunkRepository{db: tx}).FindFAQChunkWithDuplicateQuestion(tx.Statement.Context, chunk.TenantID, chunk.KnowledgeBaseID, chunk.ID, questions)
	if err != nil {
		return err
	}
	if duplicate != nil {
		return ErrFAQQuestionConflict
	}
	return nil
}

// All FAQ snapshot writers share ContentRevision, including status/tag changes.
// Update-only SQL prevents delayed imports from resurrecting deleted entries.
// Advance caller snapshots only after the entire batch commits.
func (r *chunkRepository) saveChunkSnapshots(ctx context.Context, chunks []*types.Chunk, columns []string) error {
	if len(chunks) == 0 {
		return nil
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockFAQChunkScopes(tx, chunks); err != nil {
			return err
		}
		for _, chunk := range chunks {
			if chunk == nil || chunk.ID == "" || chunk.TenantID == 0 || chunk.KnowledgeBaseID == "" {
				return errors.New("chunk update requires a complete scoped snapshot")
			}
			next := *chunk
			next.Content = common.CleanInvalidUTF8(next.Content)
			query := tx.Model(&types.Chunk{}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", chunk.ID, chunk.TenantID, chunk.KnowledgeBaseID)
			fields := append([]string(nil), columns...)
			if chunk.ChunkType == types.ChunkTypeFAQ {
				if err := validateFAQIndexManifest(tx, chunk); err != nil {
					return err
				}
				if len(columns) == 0 {
					if err := checkStoredFAQQuestions(tx, chunk); err != nil {
						return err
					}
				}
				query = query.Where("chunk_type = ? AND content_revision = ?", types.ChunkTypeFAQ, chunk.ContentRevision)
				next.ContentRevision++
				if len(fields) > 0 {
					fields = append(fields, "content_revision", "faq_index_manifest")
				}
			} else {
				query = query.Where("chunk_type <> ?", types.ChunkTypeFAQ)
			}
			if len(fields) > 0 {
				query = query.Select(fields)
			} else {
				query = query.Select("*").Omit("id", "seq_id", "tenant_id", "knowledge_base_id", "knowledge_id", "created_at", "deleted_at")
			}
			result := query.Updates(&next)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrChunkRevisionConflict
			}
			if err := bindFAQChunkResources(tx, &next); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		for _, chunk := range chunks {
			if chunk.ChunkType == types.ChunkTypeFAQ {
				chunk.ContentRevision++
			}
		}
	}
	return err
}
