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
	after, keys, err := repo.ConfigurationDigests(ctx, tenant, job.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, repository.ErrProcessingScope
	}
	pipeline := ProcessingPipelineFingerprint(s.config)
	keys, err = s.processingParserKey(ctx, tenant, keys)
	if err != nil {
		return nil, err
	}
	plan, err := ProcessingDocumentPlan(kb, document.Kind, pipeline+"/"+after, processingStageInputs(kb, s.config, keys))
	if err != nil {
		return nil, err
	}
	if document.Kind == "resource" || document.RevisionMode == "export_snapshot" {
		steps, err := repo.ListSteps(ctx, tenant, id)
		if err != nil {
			return nil, err
		}
		for _, spec := range plan {
			if spec.Stage != "native_read" && spec.Stage != "export_start" && spec.Stage != "export_poll" && spec.Stage != "download" {
				continue
			}
			confirmed := false
			for _, step := range steps {
				if step.Stage == spec.Stage && step.UnitKey == "body" && step.Status == types.ProcessingSucceeded && step.InputFingerprint == spec.InputFingerprint && step.OutputManifestRef != "" && len(step.OutputDigest) == 64 {
					confirmed = true
				}
			}
			if !confirmed {
				return nil, errors.New("RESOURCE_VERIFIED_SNAPSHOT_REQUIRED_FOR_REBUILD")
			}
		}
	}
	return repo.RebuildJob(ctx, tenant, id, control, pipeline, after, plan)
}
