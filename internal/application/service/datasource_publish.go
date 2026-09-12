package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
)

func tencentSourceConfigDigest(ds *types.DataSource) string {
	return fmt.Sprintf("%x", sha256.Sum256(ds.Config))
}

func (s *DataSourceService) PublishSourceCandidate(ctx context.Context, tenant uint64, id string) (bool, error) {
	repo := s.knowledgeService.GetRepository()
	k, err := repo.GetKnowledgeByID(ctx, tenant, id)
	if err != nil {
		return false, err
	}
	if k == nil || k.GetMetadata()["datasource_async_publish"] != "true" {
		return false, errors.New("source candidate is unavailable")
	}
	ds, err := s.dsRepo.FindByID(ctx, k.GetMetadata()["datasource_id"])
	if err != nil {
		return false, err
	}
	if ds == nil || ds.TenantID != tenant || ds.KnowledgeBaseID != k.KnowledgeBaseID {
		return false, errors.New("source configuration changed before publication")
	}
	// Postprocess payloads carry an ID, while storage/index cleanup also needs
	// the tenant object, just as the normal source worker does.
	ctx, err = restoreSummaryRefreshTenantInfo(ctx, s.tenantRepo, tenant)
	if err != nil {
		return false, err
	}
	ctx = context.WithValue(ctx, types.TenantIDContextKey, tenant)
	publisher, ok := repo.(interface {
		PublishSourceCandidate(context.Context, *types.DataSource, string) (bool, error)
	})
	if !ok {
		return false, errors.New("atomic source publication is unavailable")
	}
	if k.IsDataSourceCandidate() {
		if !ds.TencentFileSync || tencentSourceConfigDigest(ds) != k.GetMetadata()["datasource_config_digest"] {
			return false, errors.New("source configuration changed before publication")
		}
		published, err := publisher.PublishSourceCandidate(ctx, ds, id)
		if err != nil || !published {
			return published, err
		}
	}
	previous, err := repo.FindByMetadataKeyPrefix(ctx, tenant, ds.KnowledgeBaseID, "external_id", k.GetMetadata()["external_id"])
	if err != nil {
		return true, err
	}
	for _, old := range previous {
		meta := old.GetMetadata()
		if old.ID == id || meta["external_id"] != k.GetMetadata()["external_id"] || meta["datasource_id"] != ds.ID || old.CreatedAt.After(k.CreatedAt) {
			continue
		}
		if err := s.knowledgeService.DeleteKnowledge(ctx, old.ID); err != nil {
			return true, fmt.Errorf("new source version published; previous version cleanup failed: %w", err)
		}
	}
	return true, nil
}
