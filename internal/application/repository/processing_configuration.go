package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ConfigurationRevision lets a planner prove it read the same configuration
// that EnsureJob will lock. Only the digest leaves this boundary.
func (r *ProcessingRepository) ConfigurationRevision(ctx context.Context, tenant uint64, kbID string) (revision string, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		revision, err = lockProcessingConfiguration(tx, tenant, kbID)
		return err
	})
	return
}

func lockProcessingConfiguration(tx *gorm.DB, tenant uint64, id string) (string, error) {
	query := tx.Where("id = ? AND tenant_id = ?", id, tenant)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "SHARE"})
	}
	var kb types.KnowledgeBase
	if err := query.Take(&kb).Error; err != nil {
		return "", err
	}
	kb.EnsureDefaults()
	kb.ApplyDeploymentModelDefaults()
	// Whitelist processing inputs. Names, counters, sharing and timestamps do
	// not invalidate a source snapshot. Credential values are hashed in memory.
	inputs := map[string]any{"type": kb.Type, "chunking": kb.ChunkingConfig, "images": kb.ImageProcessingConfig,
		"embedding": kb.EmbeddingModelID, "summary": kb.SummaryModelID, "vlm": kb.VLMConfig, "asr": kb.ASRConfig,
		"storage": kb.StorageProviderConfig, "backend": kb.StorageBackendID, "legacy_storage": kb.StorageConfig,
		"vector_store": kb.VectorStoreID, "extract": kb.ExtractConfig, "faq": kb.FAQConfig,
		"questions": kb.QuestionGenerationConfig, "wiki": kb.WikiConfig, "indexing": kb.IndexingStrategy}
	ids := []string{kb.EmbeddingModelID, kb.SummaryModelID, kb.VLMConfig.ModelID, kb.ASRConfig.ModelID}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	for _, modelID := range ids {
		if modelID == "" {
			continue
		}
		q := tx.Where("id = ? AND (tenant_id = ? OR is_builtin = ?)", modelID, tenant, true)
		if tx.Dialector.Name() == "postgres" {
			q = q.Clauses(clause.Locking{Strength: "SHARE"})
		}
		var model types.Model
		if err := q.Take(&model).Error; err != nil {
			return "", err
		}
		inputs["model/"+modelID] = struct {
			Name       string
			Type       types.ModelType
			Source     types.ModelSource
			Status     types.ModelStatus
			Parameters types.ModelParameters
		}{model.Name, model.Type, model.Source, model.Status, model.Parameters}
	}
	encoded, err := json.Marshal(inputs)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}
