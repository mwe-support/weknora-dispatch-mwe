package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

func (s *knowledgeService) retireProcessingVersion(ctx context.Context, repo *repository.ProcessingRepository, lease types.ProcessingLease, tenant *types.Tenant) (types.ProcessingOutcome, error) {
	if lease.Step.Stage == "wiki_retire_page" {
		return s.retireProcessingWikiPage(ctx, repo, lease, tenant)
	}
	if lease.Step.Stage != "retire" || lease.Step.Phase != types.ProcessingPhaseRetire || lease.Job.RetirementState != "deleting" {
		return types.ProcessingOutcome{}, repository.ErrProcessingConflict
	}
	steps, err := repo.ListSteps(ctx, lease.Job.TenantID, lease.Job.ID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, step := range steps {
		if step.Phase != types.ProcessingPhaseRetire && step.ErrorClass == "uncertain" {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "uncertain", ErrorCode: "RETIREMENT_EXTERNAL_OUTCOME_UNCERTAIN", Message: "Reconcile the outstanding external operation before deleting its retained evidence"}, nil
		}
	}
	writes, err := repo.RetirementGraphWrites(ctx, lease.Job.TenantID, lease)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, write := range writes {
		graph, ok := s.graphEngine.(*ProcessingGraphRepository)
		if !ok || graph.destination == "" || graph.destination != write.DestinationDigest {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "GRAPH_DESTINATION_CHANGED", Message: "Restore the original graph destination before retiring its contributions"}, nil
		}
		if err := graph.DelGraph(ctx, []types.NameSpace{{KnowledgeBase: write.KnowledgeBaseID, Knowledge: write.KnowledgeID, ProcessingContribution: write.ID}}); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if err := repo.RetirementGraphDeleted(ctx, lease.Job.TenantID, lease, write.ID); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	if lease.Step.CheckpointRef != "retirement:indexes_deleted" {
		if len(lease.Job.IndexDestination) > 0 {
			var destination types.ProcessingIndexDestination
			if json.Unmarshal(lease.Job.IndexDestination, &destination) != nil || lease.Job.KnowledgeID == "" {
				return types.ProcessingOutcome{}, errors.New("RETIREMENT_INDEX_DESTINATION_INVALID")
			}
			engine, err := processingIndexEngine(ctx, s, lease.Job.TenantID, destination)
			if err != nil {
				if strings.HasPrefix(err.Error(), "INDEX_DESTINATION_") {
					return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: err.Error(), Message: "Restore or verify the retained index destination before cleanup"}, nil
				}
				return types.ProcessingOutcome{}, err
			}
			if destination.KnowledgeType == types.KnowledgeBaseTypeFAQ {
				writes, err := repo.RetirementFAQIndexWrites(ctx, lease.Job.TenantID, lease)
				if err != nil {
					return types.ProcessingOutcome{}, err
				}
				for _, write := range writes {
					var keys []string
					var target types.ProcessingIndexDestination
					if json.Unmarshal(write.SourceIDs, &keys) != nil || len(keys) == 0 || json.Unmarshal(write.Destination, &target) != nil {
						return types.ProcessingOutcome{}, errors.New("FAQ_RETIREMENT_MANIFEST_INVALID")
					}
					bound, err := processingIndexEngine(ctx, s, lease.Job.TenantID, target)
					if err != nil {
						return types.ProcessingOutcome{}, err
					}
					if err := bound.DeleteBySourceIDList(ctx, keys, target.Dimension, target.KnowledgeType); err != nil {
						return types.ProcessingOutcome{}, err
					}
					if err := s.chunkRepo.FAQIndexGarbageDeleted(ctx, lease.Job.TenantID, write.ID); err != nil {
						return types.ProcessingOutcome{}, err
					}
				}
			} else if err := engine.DeleteByKnowledgeIDList(ctx, []string{lease.Job.KnowledgeID}, destination.Dimension, destination.KnowledgeType); err != nil {
				return types.ProcessingOutcome{}, err
			}
		} else {
			steps, err := repo.ListSteps(ctx, lease.Job.TenantID, lease.Job.ID)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			for _, step := range steps {
				if (step.Stage == "index" || step.Stage == "faq_write") && step.StartedAt != nil {
					return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "integrity", ErrorCode: "RETIREMENT_INDEX_DESTINATION_MISSING", Message: "Recover the original index destination before retiring this retained version"}, nil
				}
			}
		}
		if err := repo.Heartbeat(ctx, lease.Job.TenantID, lease, 2*time.Minute, "retirement:indexes_deleted"); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	reservations, err := repo.RetirementStorageReservations(ctx, lease.Job.TenantID, lease)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, reservation := range reservations {
		if reservation.Kind == "index" {
			// The destination delete above (or its confirmed checkpoint) covers
			// every immutable attempt belonging to this exact knowledge ID.
			if reservation.Temporary || reservation.PhysicalPath != lease.Job.KnowledgeID || len(lease.Job.IndexDestination) == 0 {
				return types.ProcessingOutcome{}, errors.New("RETIREMENT_INDEX_QUOTA_INVALID")
			}
			if err := repo.RetirementStorageRemoved(ctx, lease.Job.TenantID, lease, reservation.ID); err != nil {
				return types.ProcessingOutcome{}, err
			}
			continue
		}
		if reservation.PhysicalPath == "" {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "uncertain", ErrorCode: "RETIREMENT_STORAGE_OUTCOME_UNCERTAIN", Message: "A reserved write has no confirmed physical location; reconcile it before releasing its quota"}, nil
		}
		if reservation.Temporary {
			// multipart.ReadForm creates files directly in the process temp dir.
			// Never follow a stored arbitrary path outside that exact namespace.
			path := filepath.Clean(reservation.PhysicalPath)
			if !filepath.IsAbs(path) || filepath.Dir(path) != filepath.Clean(os.TempDir()) || !strings.HasPrefix(filepath.Base(path), "multipart-") {
				return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "integrity", ErrorCode: "RETIREMENT_TEMPORARY_PATH_UNVERIFIED"}, nil
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return types.ProcessingOutcome{}, err
			}
		} else {
			backend, providerPath, scoped := types.ParseStorageBackendPath(reservation.PhysicalPath)
			if !scoped {
				providerPath = reservation.PhysicalPath
			}
			fileService := s.fileSvc
			if s.storageResolver != nil {
				fileService, _, err = s.storageResolver.ResolveFileService(ctx, tenant, backend, types.ParseProviderScheme(providerPath), strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
				if err != nil {
					return types.ProcessingOutcome{}, err
				}
			} else if backend != "" {
				return types.ProcessingOutcome{}, errors.New("RETIREMENT_STORAGE_RESOLVER_UNAVAILABLE")
			}
			if fileService == nil {
				return types.ProcessingOutcome{}, errors.New("RETIREMENT_STORAGE_UNAVAILABLE")
			}
			if err := fileService.DeleteFile(ctx, reservation.PhysicalPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return types.ProcessingOutcome{}, err
			}
		}
		if err := repo.RetirementStorageRemoved(ctx, lease.Job.TenantID, lease, reservation.ID); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	resources, err := repo.RetirementResources(ctx, lease.Job.TenantID, lease)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, resource := range resources {
		fileService := s.fileSvc
		if s.storageResolver != nil {
			fileService, _, err = s.storageResolver.ResolveFileService(ctx, tenant, resource.StorageBackendID, resource.Provider, strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
		} else if resource.StorageBackendID != "" {
			return types.ProcessingOutcome{}, errors.New("RETIREMENT_STORAGE_RESOLVER_UNAVAILABLE")
		}
		if fileService == nil {
			return types.ProcessingOutcome{}, errors.New("RETIREMENT_STORAGE_UNAVAILABLE")
		}
		// The registry row has already excluded readers and new references. Use
		// its original physical destination, not a current KB storage setting.
		if err := fileService.DeleteFile(ctx, resource.PhysicalPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return types.ProcessingOutcome{}, err
		}
		if err := repo.RetirementResourceDeleted(ctx, lease.Job.TenantID, lease, resource.ID); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	ref := "ledger:retirement/" + lease.Job.ID
	return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: ref, OutputDigest: fmt.Sprintf("%x", sha256.Sum256([]byte(ref)))}, nil
}
