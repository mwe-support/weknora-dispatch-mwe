package service

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

func (s *ProcessingService) LegacyTaskAllowed(ctx context.Context, task *asynq.Task) (bool, error) {
	switch task.Type() {
	case types.TypeDocumentProcess, types.TypeManualProcess, types.TypeFAQImport,
		types.TypeSummaryGeneration, types.TypeQuestionGeneration, types.TypeImageMultimodal,
		types.TypeKnowledgePostProcess, types.TypeChunkExtract, types.TypeDataTableSummary, types.TypeWikiIngest:
	default:
		return true, nil
	}
	var payload struct {
		KnowledgeID string `json:"knowledge_id"`
		ChunkID     string `json:"chunk_id"`
	}
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return false, err
	}
	return s.repo.LegacyKnowledgeTaskAllowed(ctx, payload.KnowledgeID, payload.ChunkID)
}

// Both Redis and Lite workers use this boundary before any legacy handler I/O.
// Explicit delete/reparse/clone operations retain their own control handlers.
func (s *ProcessingService) GuardLegacyTask(next asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		allowed, err := s.LegacyTaskAllowed(ctx, task)
		if err != nil || !allowed {
			return err
		}
		return next.ProcessTask(ctx, task)
	})
}
