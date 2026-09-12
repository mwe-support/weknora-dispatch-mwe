package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *knowledgeRepository) PublishSourceCandidate(ctx context.Context, expected *types.DataSource, id string) (bool, error) {
	published := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := tx.Where("id = ? AND tenant_id = ?", expected.ID, expected.TenantID)
		if tx.Dialector.Name() == "postgres" {
			q = q.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var ds types.DataSource
		if err := q.Take(&ds).Error; err != nil {
			return err
		}
		if !ds.TencentFileSync || ds.KnowledgeBaseID != expected.KnowledgeBaseID || !bytes.Equal(ds.Config, expected.Config) || (ds.Status != types.DataSourceStatusActive && ds.Status != types.DataSourceStatusError) {
			return errors.New("source is paused, changed or unavailable")
		}
		var k types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", id, ds.TenantID, ds.KnowledgeBaseID).Take(&k).Error; err != nil {
			return err
		}
		m := k.GetMetadata()
		if m["datasource_id"] != ds.ID || m["datasource_async_publish"] != "true" || m["external_id"] == "" {
			return errors.New("candidate source identity mismatch")
		}
		if !k.IsDataSourceCandidate() {
			published = true
			return nil
		}
		if m["datasource_processing_failed"] != "" || (k.ParseStatus != types.ParseStatusCompleted && (k.ParseStatus != types.ParseStatusProcessing || k.ProcessedAt == nil)) {
			return errors.New("source candidate indexing or images failed/incomplete")
		}
		var newer int64
		if err := tx.Model(&types.Knowledge{}).Where("tenant_id = ? AND knowledge_base_id = ? AND metadata->>'datasource_id' = ? AND metadata->>'external_id' = ? AND created_at > ?", ds.TenantID, ds.KnowledgeBaseID, ds.ID, m["external_id"], k.CreatedAt).Count(&newer).Error; err != nil {
			return err
		}
		if newer > 0 {
			return tx.Model(&k).Scopes(LegacyKnowledge).Updates(map[string]any{"parse_status": types.ParseStatusCancelled, "enable_status": "disabled", "error_message": "Superseded by a newer source snapshot"}).Error
		}
		delete(m, "datasource_candidate")
		m["datasource_index_ready"] = "true"
		body, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if err := tx.Model(&k).Scopes(LegacyKnowledge).Updates(map[string]any{"metadata": types.JSON(body), "enable_status": "enabled"}).Error; err != nil {
			return err
		}
		published = true
		return nil
	})
	return published, err
}
