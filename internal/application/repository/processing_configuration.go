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
	inputs, err := lockProcessingConfigurationInputs(tx, tenant, id)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(inputs)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

// Return only input digests; model credentials never leave this boundary.
// The complete revision still fences a running generation at every commit.
func (r *ProcessingRepository) ConfigurationDigests(ctx context.Context, tenant uint64, id string) (revision string, digests map[string]string, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		inputs, readErr := lockProcessingConfigurationInputs(tx, tenant, id)
		if readErr != nil {
			return readErr
		}
		encoded, readErr := json.Marshal(inputs)
		if readErr != nil {
			return readErr
		}
		revision = fmt.Sprintf("%x", sha256.Sum256(encoded))
		digests = make(map[string]string, len(inputs))
		for key, value := range inputs {
			encoded, readErr = json.Marshal(value)
			if readErr != nil {
				return readErr
			}
			digests[key] = fmt.Sprintf("%x", sha256.Sum256(encoded))
		}
		return nil
	})
	return
}

func lockProcessingConfigurationInputs(tx *gorm.DB, tenant uint64, id string) (map[string]any, error) {
	if err := lockTenantParserConfiguration(tx, tenant, true); err != nil {
		return nil, err
	}
	var owner types.Tenant
	if err := tx.Where("id = ?", tenant).Take(&owner).Error; err != nil {
		return nil, err
	}
	query := tx.Where("id = ? AND tenant_id = ?", id, tenant)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "SHARE"})
	}
	var kb types.KnowledgeBase
	if err := query.Take(&kb).Error; err != nil {
		return nil, err
	}
	kb.EnsureDefaults()
	kb.ApplyDeploymentModelDefaults()
	// Whitelist processing inputs. Names, counters, sharing and timestamps do
	// not invalidate a source snapshot. Credential values are hashed in memory.
	inputs := map[string]any{"type": kb.Type, "chunking": kb.ChunkingConfig, "images": kb.ImageProcessingConfig,
		"embedding": kb.EmbeddingModelID, "summary": kb.SummaryModelID, "vlm": kb.VLMConfig, "asr": kb.ASRConfig,
		"storage": kb.StorageProviderConfig, "backend": kb.StorageBackendID, "legacy_storage": kb.StorageConfig,
		"vector_store": kb.VectorStoreID, "extract": kb.ExtractConfig, "faq": kb.FAQConfig,
		"questions": kb.QuestionGenerationConfig, "wiki": kb.WikiConfig, "indexing": kb.IndexingStrategy, "tenant_parser": owner.ParserEngineConfig.ToOverridesMap()}
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
			return nil, err
		}
		inputs["model/"+modelID] = struct {
			Name       string
			Type       types.ModelType
			Source     types.ModelSource
			Status     types.ModelStatus
			Parameters types.ModelParameters
		}{model.Name, model.Type, model.Source, model.Status, model.Parameters}
	}
	return inputs, nil
}

// Configuration readers must not SHARE-lock the quota row: independent
// processing transactions later update that row and would deadlock on upgrade.
// Settings writers take this key before touching the tenant row; quota-only
// writers do not acquire it. SQLite already serializes concurrent writes.
func lockTenantParserConfiguration(tx *gorm.DB, tenant uint64, shared bool) error {
	if tx.Dialector.Name() != "postgres" {
		return nil
	}
	function := "pg_advisory_xact_lock"
	if shared {
		function += "_shared"
	}
	return tx.Exec("SELECT "+function+"(hashtextextended(?,0))", fmt.Sprintf("tenant-parser/%d", tenant)).Error
}
