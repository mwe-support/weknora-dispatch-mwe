package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func RollbackProcessingVersion(ctx context.Context, knowledge interfaces.KnowledgeService, repo *repository.ProcessingRepository, tenant uint64, jobID string, revision int64, requestID, actor string, reasons ...string) error {
	s, ok := knowledge.(*knowledgeService)
	if !ok {
		return errors.New("processing rollback requires the application knowledge service")
	}
	actualTenant, ok := types.TenantIDFromContext(ctx)
	if !ok || actualTenant != tenant {
		return repository.ErrProcessingScope
	}
	if requestID == "" || len(requestID) > 128 {
		return repository.ErrProcessingConflict
	}
	if done, err := repo.HasRollbackReceipt(ctx, tenant, jobID, requestID, actor); err != nil || done {
		return err
	}
	pinID := fmt.Sprintf("%x", sha256.Sum256([]byte("rollback-pin/"+requestID)))
	if err := repo.SetRollbackPin(ctx, tenant, jobID, revision, true, pinID, actor, reasons...); err != nil {
		return err
	}
	job, kb, err := repo.PinnedRollbackSnapshot(ctx, tenant, jobID)
	if err != nil {
		return err
	}
	if err := s.verifyProcessingRollback(ctx, repo, job, kb); err != nil {
		return errors.Join(err, repo.RollbackVerificationFailed(ctx, tenant, jobID, requestID, actor))
	}
	return repo.RollbackVersion(ctx, tenant, jobID, job.Revision, job.ActiveIndexManifest, requestID, actor, reasons...)
}

func (s *knowledgeService) verifyProcessingRollback(ctx context.Context, repo *repository.ProcessingRepository, job *types.ProcessingJob, kb *types.KnowledgeBase) error {
	tenant, err := s.tenantRepo.GetTenantByID(ctx, job.TenantID)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	if kb.TenantID != job.TenantID {
		return repository.ErrProcessingScope
	}
	fileService := s.fileSvc
	backendID := ""
	if kb.StorageBackendID != nil {
		backendID = strings.TrimSpace(*kb.StorageBackendID)
	}
	if s.storageResolver != nil {
		fileService, _, err = s.storageResolver.ResolveFileService(ctx, tenant, backendID, kb.GetStorageProvider(), strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
		if err != nil {
			return err
		}
	} else if backendID != "" {
		return errors.New("STORAGE_RESOLVER_UNAVAILABLE")
	}
	if fileService == nil {
		return errors.New("ARTIFACT_STORAGE_UNAVAILABLE")
	}
	steps, err := repo.ListSteps(ctx, job.TenantID, job.ID)
	if err != nil {
		return err
	}
	var manifest map[string]types.ProcessingArtifactVersion
	if json.Unmarshal([]byte(job.ActiveIndexManifest), &manifest) != nil || len(manifest) == 0 {
		return repository.ErrProcessingConflict
	}
	e := processingDocumentExecution{s: s, repo: repo, kb: kb, lease: types.ProcessingLease{Job: *job}, artifacts: NewProcessingArtifacts(fileService, s.resourceCatalog), steps: steps}
	confirmed := map[string]types.ProcessingStep{}
	for _, step := range steps {
		confirmed[step.ID] = step
	}
	// Verify every stored output and original media before doing any index I/O.
	for id, version := range manifest {
		step, exists := confirmed[id]
		if !exists || step.Status != types.ProcessingSucceeded || step.Attempt != version.Attempt || step.OutputDigest != version.Digest {
			return repository.ErrProcessingConflict
		}
		data, err := e.artifacts.Read(ctx, *job, step, "*", step.OutputManifestRef, step.OutputDigest)
		if err != nil {
			return err
		}
		if step.Stage == "asset_download" {
			var asset processingAsset
			if json.Unmarshal(data, &asset) != nil || asset.ID == "" {
				return errors.New("ROLLBACK_ASSET_INVALID")
			}
			if _, err := e.readStoredAsset(ctx, asset); err != nil {
				return err
			}
		}
		if step.Stage == "assets" {
			var parsed processingParsed
			if json.Unmarshal(data, &parsed) != nil {
				return errors.New("ROLLBACK_ASSET_INVALID")
			}
			for _, asset := range parsed.Assets {
				if _, err := e.readStoredAsset(ctx, asset); err != nil {
					return err
				}
			}
		}
	}
	// Reassert the confirmed immutable index identities with saved vectors. A
	// deleted/missing provider row is repaired without fetching or re-embedding.
	for _, step := range steps {
		if _, exists := manifest[step.ID]; !exists || step.Stage != "index" {
			continue
		}
		data, err := e.artifacts.Read(ctx, *job, step, "index", step.OutputManifestRef, step.OutputDigest)
		if err != nil {
			return err
		}
		var items []*types.IndexInfo
		if json.Unmarshal(data, &items) != nil || len(items) == 0 {
			return errors.New("ROLLBACK_INDEX_INVALID")
		}
		for _, item := range items {
			if item == nil || item.KnowledgeID != job.KnowledgeID || item.KnowledgeBaseID != job.KnowledgeBaseID {
				return repository.ErrProcessingScope
			}
			id, attempt, err := types.ParseProcessingIndexSourceID(item.SourceID)
			if err != nil || id != step.ID || attempt != step.Attempt || (kb.IsVectorEnabled() && len(item.PreparedEmbedding) == 0) {
				return errors.New("ROLLBACK_INDEX_IDENTITY_INVALID")
			}
		}
		engine, err := e.indexEngine(ctx)
		if err != nil {
			return err
		}
		if err := engine.BatchIndex(ctx, nil, items); err != nil {
			return err
		}
	}
	return nil
}
