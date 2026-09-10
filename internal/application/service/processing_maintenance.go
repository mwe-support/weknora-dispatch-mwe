package service

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// Maintenance has a separate bounded loop so an unavailable storage backend
// cannot stop lease recovery or dispatch. Actual document retirement uses the
// existing maintenance queue, with its ordinary attempt and error ledger.
func (s *ProcessingService) runMaintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	jobCursor, indexCursor, resourceCursor, retiredCursor := "", "", "", ""
	for {
		work, cancel := context.WithTimeout(ctx, 45*time.Second)
		var err error
		if err := s.repo.CollectHistorySnapshots(work); err != nil && ctx.Err() == nil {
			logger.Warnf(ctx, "[Processing] expired history cleanup deferred: %v", err)
		}
		jobCursor, err = s.repo.CollectProcessingGarbage(work, jobCursor, 100)
		if err != nil && ctx.Err() == nil {
			logger.Warnf(ctx, "[Processing] orphan retirement deferred: %v", err)
		}
		indexCursor, err = s.knowledge.collectFAQIndexGarbage(work, s.repo, indexCursor, 20)
		if err != nil && ctx.Err() == nil {
			logger.Warnf(ctx, "[Processing] FAQ index cleanup deferred: %v", err)
		}
		resourceCursor, err = s.knowledge.collectProcessingResourceGarbage(work, s.repo, resourceCursor, 20)
		if err != nil && ctx.Err() == nil {
			logger.Warnf(ctx, "[Processing] orphan media cleanup deferred: %v", err)
		}
		retiredCursor, err = s.knowledge.collectRetiredIndexGarbage(work, s.repo, retiredCursor, 20)
		if err != nil && ctx.Err() == nil {
			logger.Warnf(ctx, "[Processing] retired index reconciliation deferred: %v", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *knowledgeService) collectRetiredIndexGarbage(ctx context.Context, repo *repository.ProcessingRepository, after string, limit int) (string, error) {
	jobs, next, failures := repo.RetiredIndexGarbage(ctx, after, limit)
	for _, job := range jobs {
		if ctx.Err() != nil {
			return next, errors.Join(failures, ctx.Err())
		}
		err := func() error {
			var destination types.ProcessingIndexDestination
			if job.KnowledgeID != types.ProcessingKnowledgeID(job.ID) {
				return errors.New("RETIRED_INDEX_MANIFEST_INVALID")
			}
			scoped := context.WithValue(ctx, types.TenantIDContextKey, job.TenantID)
			writes, err := repo.RetiredGraphWrites(scoped, job.TenantID, job.ID)
			if err != nil {
				return err
			}
			for _, write := range writes {
				graph, ok := s.graphEngine.(*ProcessingGraphRepository)
				if !ok || graph.destination == "" || graph.destination != write.DestinationDigest {
					return errors.New("GRAPH_DESTINATION_CHANGED")
				}
				if err := graph.DelGraph(scoped, []types.NameSpace{{KnowledgeBase: write.KnowledgeBaseID, Knowledge: write.KnowledgeID, ProcessingContribution: write.ID}}); err != nil {
					return err
				}
			}
			if len(job.IndexDestination) == 0 {
				return repo.RetiredIndexGarbageDeleted(scoped, job.TenantID, job.ID)
			}
			if json.Unmarshal(job.IndexDestination, &destination) != nil {
				return errors.New("RETIRED_INDEX_MANIFEST_INVALID")
			}
			if destination.KnowledgeType == types.KnowledgeBaseTypeFAQ {
				// Canonical FAQ indexes use their own exact-write tombstones.
				return repo.RetiredIndexGarbageDeleted(scoped, job.TenantID, job.ID)
			}
			tenant, err := s.tenantRepo.GetTenantByID(scoped, job.TenantID)
			if err != nil {
				return err
			}
			scoped = context.WithValue(scoped, types.TenantInfoContextKey, tenant)
			engine, err := processingIndexEngine(scoped, s, job.TenantID, destination)
			if err != nil {
				return err
			}
			if err := engine.DeleteByKnowledgeIDList(scoped, []string{job.KnowledgeID}, destination.Dimension, destination.KnowledgeType); err != nil {
				return err
			}
			return repo.RetiredIndexGarbageDeleted(scoped, job.TenantID, job.ID)
		}()
		failures = errors.Join(failures, err)
	}
	return next, failures
}

func (s *knowledgeService) collectProcessingResourceGarbage(ctx context.Context, repo *repository.ProcessingRepository, after string, limit int) (string, error) {
	resources, next, failures := repo.ClaimProcessingResourceGarbage(ctx, after, limit)
	for _, resource := range resources {
		if ctx.Err() != nil {
			return next, errors.Join(failures, ctx.Err())
		}
		err := func() error {
			scoped := context.WithValue(ctx, types.TenantIDContextKey, resource.TenantID)
			tenant, err := s.tenantRepo.GetTenantByID(scoped, resource.TenantID)
			if err != nil {
				return err
			}
			scoped = context.WithValue(scoped, types.TenantInfoContextKey, tenant)
			fileService := s.fileSvc
			if s.storageResolver != nil {
				fileService, _, err = s.storageResolver.ResolveFileService(scoped, tenant, resource.StorageBackendID, resource.Provider, strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
				if err != nil {
					return err
				}
			} else if resource.StorageBackendID != "" {
				return errors.New("RETIREMENT_STORAGE_RESOLVER_UNAVAILABLE")
			}
			if fileService == nil {
				return errors.New("RETIREMENT_STORAGE_UNAVAILABLE")
			}
			if err := fileService.DeleteFile(scoped, resource.PhysicalPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			return repo.ProcessingResourceGarbageDeleted(scoped, resource.TenantID, resource.ID)
		}()
		failures = errors.Join(failures, err)
	}
	return next, failures
}

func (s *knowledgeService) collectFAQIndexGarbage(ctx context.Context, repo *repository.ProcessingRepository, after string, limit int) (string, error) {
	writes, next, failures := repo.ClaimFAQIndexGarbage(ctx, after, limit)
	for _, write := range writes {
		if ctx.Err() != nil {
			return next, errors.Join(failures, ctx.Err())
		}
		err := func() error {
			scoped := context.WithValue(ctx, types.TenantIDContextKey, write.TenantID)
			tenant, err := s.tenantRepo.GetTenantByID(scoped, write.TenantID)
			if err != nil {
				return err
			}
			scoped = context.WithValue(scoped, types.TenantInfoContextKey, tenant)
			var destination types.ProcessingIndexDestination
			var keys []string
			if json.Unmarshal(write.Destination, &destination) != nil || json.Unmarshal(write.SourceIDs, &keys) != nil || len(keys) == 0 {
				return errors.New("FAQ_GARBAGE_MANIFEST_INVALID")
			}
			engine, err := processingIndexEngine(scoped, s, write.TenantID, destination)
			if err != nil {
				return err
			}
			if err := engine.DeleteBySourceIDList(scoped, keys, destination.Dimension, destination.KnowledgeType); err != nil {
				return err
			}
			return s.chunkRepo.FAQIndexGarbageDeleted(scoped, write.TenantID, write.ID)
		}()
		failures = errors.Join(failures, err)
	}
	return next, failures
}
