package service

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func BeginLegacyRetry(ctx context.Context, knowledge interfaces.KnowledgeService, repo *repository.ProcessingRepository, request types.ProcessingLegacyRetry) (*types.ProcessingJob, error) {
	s, ok := knowledge.(*knowledgeService)
	if !ok {
		return nil, errors.New("LEGACY_RETRY_UNAVAILABLE")
	}
	source, err := repo.LegacySource(ctx, request.TenantID, request.KnowledgeBaseID, request.DataSourceID)
	if err != nil {
		return nil, err
	}
	if !ProcessingLifecycleEnabled(source) {
		return nil, errors.New("LEGACY_SOURCE_NOT_ENROLLED")
	}
	cfg, err := source.ParseConfig()
	if err != nil {
		return nil, err
	}
	roots, err := tencentdocs.NativeScanRoots(cfg.ResourceIDs)
	if err != nil {
		return nil, err
	}
	scope, auth, err := repository.ProcessingSourceRevisions(source)
	if err != nil {
		return nil, err
	}
	var specs []types.ProcessingStepSpec
	coverage := types.ProcessingStepSpec{Stage: "legacy_retry_coverage", UnitKey: "body", Phase: types.ProcessingPhaseScan, InputFingerprint: processingFingerprint(request.OperationRequestID, "coverage"), RequiredForCompletion: true}
	for _, root := range roots {
		spec := processingScanSpec("scan_page", "root", root, true)
		specs = append(specs, spec)
		coverage.DependsOn = append(coverage.DependsOn, spec.Stage+"/"+spec.UnitKey)
	}
	specs = append(specs, coverage)
	return repo.BeginLegacyRetry(ctx, request, types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, DataSourceID: source.ID, ScopeRevision: scope, AuthRevision: auth, PipelineFingerprint: ProcessingPipelineFingerprint(s.config)}, specs)
}

func (e *processingDocumentExecution) legacyRetryAllows(entry tencentdocs.NativeScanEntry) (bool, error) {
	if len(e.lease.Job.Metadata) == 0 {
		return true, nil
	}
	var metadata types.ProcessingLegacyRetryScan
	if json.Unmarshal(e.lease.Job.Metadata, &metadata) != nil {
		return false, repository.ErrProcessingConflict
	}
	if metadata.LegacyRetry == nil {
		return true, nil
	}
	return slices.ContainsFunc(metadata.LegacyRetry.Errors, func(identity types.ProcessingLegacyIdentity) bool {
		return identity.ExternalID == entry.ExternalID && identity.FileID == entry.FileID
	}), nil
}

func (e *processingDocumentExecution) legacyRetryCoverage(ctx context.Context) (types.ProcessingOutcome, error) {
	complete, err := e.repo.LegacyRetryCoverage(ctx, &e.lease.Job)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if !complete {
		return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "conflict", ErrorCode: "LEGACY_RETRY_FILES_MISSING", Message: "One or more requested files were not verified in the selected source scope"}, nil
	}
	return e.success(ctx, "legacy_retry_coverage", map[string]bool{"requested_files_admitted": true})
}
