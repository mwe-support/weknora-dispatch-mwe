package service

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func RebuildProcessingVersion(ctx context.Context, knowledge interfaces.KnowledgeService, repo *repository.ProcessingRepository, tenant uint64, id string, control types.ProcessingControlRequest) (*types.ProcessingJob, error) {
	if result, err := repo.RebuiltJob(ctx, tenant, id, control); err != nil || result != nil {
		return result, err
	}
	s, ok := knowledge.(*knowledgeService)
	if !ok {
		return nil, errors.New("processing rebuild is unavailable")
	}
	job, err := repo.GetJob(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	var document ProcessingDocumentSpec
	if json.Unmarshal(job.Metadata, &document) != nil || document.FileID == "" {
		return nil, errors.New("processing source identity is unavailable")
	}
	before, err := repo.ConfigurationRevision(ctx, tenant, job.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, job.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	after, err := repo.ConfigurationRevision(ctx, tenant, job.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, repository.ErrProcessingScope
	}
	pipeline := ProcessingPipelineFingerprint(s.config)
	plan, err := ProcessingDocumentPlan(kb, document.Kind, pipeline+"/"+after)
	if err != nil {
		return nil, err
	}
	return repo.RebuildJob(ctx, tenant, id, control, pipeline, after, plan)
}
