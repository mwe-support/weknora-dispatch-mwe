package repository

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// ProcessingFileSnapshot exposes one confirmed file artifact of the currently
// published knowledge. It never authorizes candidate or retained old versions.
func (r *knowledgeRepository) ProcessingFileSnapshot(ctx context.Context, tenant uint64, id string) (job types.ProcessingJob, step types.ProcessingStep, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var knowledge types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND enable_status = ? AND parse_status <> ?", id, tenant, "enabled", types.ParseStatusDeleting).Take(&knowledge).Error; err != nil {
			return err
		}
		if knowledge.GetMetadata()["processing_protocol"] != "2" {
			return ErrProcessingConflict
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", knowledge.KnowledgeBaseID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND kind = ? AND is_published = ? AND retirement_state = ?",
			knowledge.GetMetadata()["processing_job_id"], tenant, kb.ID, id, types.ProcessingJobDocument, true, "retained").Take(&job).Error; err != nil {
			return err
		}
		stage := "assets"
		var document struct {
			Kind             string `json:"kind"`
			LegacyEvidenceID string `json:"legacy_evidence_id"`
		}
		if json.Unmarshal(job.Metadata, &document) != nil {
			return ErrProcessingConflict
		}
		if document.LegacyEvidenceID != "" && types.IsSupportedKnowledgeFileExtension(knowledge.FileType) {
			stage = "legacy_snapshot"
		} else if knowledge.FileType == "docx" || (document.Kind == "resource" && types.IsSupportedKnowledgeFileExtension(knowledge.FileType)) {
			stage = "download"
		} else if knowledge.FileType != "md" {
			return ErrProcessingConflict
		}
		if err := tx.Where("job_id = ? AND stage = ? AND unit_key = ? AND status = ?", job.ID, stage, "body", types.ProcessingSucceeded).Take(&step).Error; err != nil {
			return err
		}
		var manifest map[string]types.ProcessingArtifactVersion
		if json.Unmarshal([]byte(job.ActiveIndexManifest), &manifest) != nil {
			return ErrProcessingConflict
		}
		accepted, ok := manifest[step.ID]
		if !ok || accepted.Attempt != step.Attempt || accepted.Digest != step.OutputDigest || len(step.OutputDigest) != 64 || step.OutputManifestRef == "" {
			return ErrProcessingConflict
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return
}
