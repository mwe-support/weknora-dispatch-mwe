package repository

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func commitProcessingQuestions(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, group *types.ProcessingChunkQuestions) error {
	if step.Stage != "question" || step.UnitKey != group.ChunkID || !job.IsPublished || step.ExpectedPublicationEpoch != job.PublicationEpoch || len(group.Questions) == 0 || len(group.Questions) > 10 {
		return ErrProcessingConflict
	}
	query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND content_revision = ?", group.ChunkID, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID, group.ContentRevision)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var chunk types.Chunk
	if err := query.Take(&chunk).Error; err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, question := range group.Questions {
		if question.ID == "" || seen[question.ID] || strings.TrimSpace(question.Question) == "" || question.ContentRevision == nil || *question.ContentRevision != group.ContentRevision {
			return errors.New("invalid processing question output")
		}
		seen[question.ID] = true
	}
	metadata := map[string]json.RawMessage{}
	if len(chunk.Metadata) > 0 {
		if err := json.Unmarshal(chunk.Metadata, &metadata); err != nil {
			return err
		}
	}
	if metadata == nil {
		metadata = map[string]json.RawMessage{}
	}
	metadata["generated_questions"], _ = json.Marshal(group.Questions)
	metadata["generated_questions_revision"], _ = json.Marshal(group.ContentRevision)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return tx.Model(&chunk).Update("metadata", types.JSON(encoded)).Error
}
