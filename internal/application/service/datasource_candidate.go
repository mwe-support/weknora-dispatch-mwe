package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

// ingestTencentCandidate retains the last good document until the replacement
// has finished parsing/indexing. Version identity survives worker restarts;
// retries resume the same candidate instead of deleting/recreating the old row.
func (s *DataSourceService) ingestTencentCandidate(ctx context.Context, ds *types.DataSource, item *types.FetchedItem, tags []string) (bool, error) {
	if item.ExternalID == "" || len(item.Content) == 0 {
		return false, errors.New("Tencent Docs ingestion requires source identity and fetched content")
	}
	identity, _ := json.Marshal([]any{ds.ID, item.ExternalID, item.UpdatedAt, item.FileName, item.Metadata["folder_path"], fmt.Sprintf("%x", sha256.Sum256(item.Content))})
	version := fmt.Sprintf("%x", sha256.Sum256(identity))
	if ds.TencentFileSync {
		content := item.Metadata["source_fingerprint"]
		if content == "" {
			content = fmt.Sprintf("%x", sha256.Sum256(item.Content))
		}
		version = processingFingerprint("tencent-file-v1", ds.ID, item.ExternalID, content, item.FileName, item.Metadata["folder_path"], tencentSourceConfigDigest(ds))
	}
	repo := s.knowledgeService.GetRepository()
	candidate, err := repo.FindByMetadataKey(ctx, ds.TenantID, ds.KnowledgeBaseID, "datasource_version", version)
	if err != nil {
		return false, err
	}
	alreadyExisted := candidate != nil
	if candidate == nil {
		metadata := map[string]string{
			"external_id": item.ExternalID, "source_resource_id": item.SourceResourceID,
			"datasource_id": ds.ID, "datasource_version": version, "datasource_candidate": "true",
			"source_fetch_completed_at": time.Now().UTC().Format(time.RFC3339Nano),
		}
		if ds.TencentFileSync {
			metadata["datasource_async_publish"] = "true"
			metadata["datasource_config_digest"] = tencentSourceConfigDigest(ds)
		}
		for key, value := range item.Metadata {
			// Connector metadata cannot overwrite the candidate's ownership/version.
			if _, protected := metadata[key]; !protected {
				metadata[key] = value
			}
		}
		name := item.FileName
		if metadata["folder_path"] != "" {
			name = metadata["folder_path"] + "/" + name
		}
		file, err := bytesToFileHeader(item.Content, item.FileName)
		if err != nil {
			return false, err
		}
		candidate, err = s.knowledgeService.CreateKnowledgeFromFile(ctx, ds.KnowledgeBaseID, file, metadata, nil, name, tags, types.ChannelTencentDocs, nil)
		if err != nil {
			return false, err
		}
	} else if candidate.GetMetadata()["datasource_id"] != ds.ID || candidate.GetMetadata()["external_id"] != item.ExternalID {
		return false, errors.New("candidate ownership mismatch")
	} else if candidate.ParseStatus == types.ParseStatusFailed || (candidate.ParseStatus == types.ParseStatusCompleted && candidate.GetMetadata()["datasource_processing_failed"] != "") {
		// Only reparse this failed candidate. The published document is untouched.
		metadata := candidate.GetMetadata()
		metadata["datasource_candidate"] = "true"
		delete(metadata, "datasource_index_ready")
		delete(metadata, "datasource_processing_failed")
		candidate.Metadata, err = json.Marshal(metadata)
		if err != nil {
			return false, err
		}
		candidate.ParseStatus = types.ParseStatusFailed
		if err = repo.UpdateKnowledgeColumns(ctx, candidate.ID, map[string]interface{}{"metadata": candidate.Metadata, "parse_status": candidate.ParseStatus}); err != nil {
			return false, err
		}
		candidate, err = s.knowledgeService.ReparseKnowledge(ctx, candidate.ID, nil)
		if err != nil {
			return false, err
		}
	}
	if candidate == nil {
		return false, errors.New("candidate creation returned no knowledge")
	}
	if ds.TencentFileSync {
		if candidate.ParseStatus == types.ParseStatusFailed {
			return false, errors.New(candidate.ErrorMessage)
		}
		// Submission, not downstream completion, ends this file's fetch slot.
		// The existing postprocess fan-in publishes after core/image readiness.
		return alreadyExisted, nil
	}
	// ponytail: reuse the sync worker as a completion barrier. This bounds each
	// wait to ten minutes; very large deployments can replace it with a dedicated
	// reconciliation task without changing the durable candidate/version contract.
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	candidate, err = s.waitForTencentCandidate(waitCtx, ds.TenantID, candidate, true)
	if err != nil {
		return false, err
	}
	// Publish before cleanup: interrupted cleanup may temporarily leave two good
	// versions, but must never leave neither. Next sync repeats cleanup safely.
	if err := s.checkTencentCandidateScope(ctx, ds); err != nil {
		return false, err
	}
	if candidate.GetMetadata()["datasource_candidate"] == "true" {
		metadata := candidate.GetMetadata()
		delete(metadata, "datasource_candidate")
		candidate.Metadata, err = json.Marshal(metadata)
		if err != nil {
			return false, err
		}
		candidate.EnableStatus = "enabled"
		if err = repo.UpdateKnowledgeColumns(ctx, candidate.ID, map[string]interface{}{"metadata": candidate.Metadata, "enable_status": candidate.EnableStatus}); err != nil {
			return false, err
		}
	}
	if candidate.ParseStatus != types.ParseStatusCompleted {
		// The first post-process delivery only announces that core parsing and
		// image indexing are ready. Fan out shared Wiki/graph work after publish.
		attempt := 0
		if tracker, ok := repo.(interface {
			DataSourceProcessingAttempt(context.Context, string) (int, error)
		}); ok {
			attempt, err = tracker.DataSourceProcessingAttempt(ctx, candidate.ID)
			if err != nil {
				return false, err
			}
			if attempt <= 0 {
				return false, errors.New("candidate processing attempt is unavailable")
			}
		}
		body, err := json.Marshal(types.KnowledgePostProcessPayload{TenantID: ds.TenantID, KnowledgeID: candidate.ID, KnowledgeBaseID: ds.KnowledgeBaseID, Attempt: attempt})
		if err != nil {
			return false, err
		}
		_, err = s.taskEnqueuer.Enqueue(asynq.NewTask(types.TypeKnowledgePostProcess, body), asynq.Queue(types.QueuePostProcess), asynq.MaxRetry(3), asynq.Unique(10*time.Minute))
		if err != nil && !errors.Is(err, asynq.ErrDuplicateTask) {
			return false, err
		}
		candidate, err = s.waitForTencentCandidate(waitCtx, ds.TenantID, candidate, false)
		if err != nil {
			return false, err
		}
	}
	if err := s.checkTencentCandidateScope(ctx, ds); err != nil {
		return false, err
	}
	previous, err := repo.FindByMetadataKeyPrefix(ctx, ds.TenantID, ds.KnowledgeBaseID, "external_id", item.ExternalID)
	if err != nil {
		return false, err
	}
	updated := false
	for _, old := range previous {
		meta := old.GetMetadata()
		if old.ID == candidate.ID || meta["external_id"] != item.ExternalID || meta["datasource_id"] != ds.ID {
			continue
		}
		updated = true
		if err = s.knowledgeService.DeleteKnowledge(ctx, old.ID); err != nil {
			return true, fmt.Errorf("replacement ready; old version cleanup failed: %w", err)
		}
	}
	if alreadyExisted && !updated {
		return false, types.NewDuplicateFileError(candidate)
	}
	return updated, nil
}

func (s *DataSourceService) waitForTencentCandidate(ctx context.Context, tenant uint64, candidate *types.Knowledge, readyOnly bool) (*types.Knowledge, error) {
	for {
		if candidate.GetMetadata()["datasource_processing_failed"] != "" {
			return nil, fmt.Errorf("candidate %s has failed processing tasks", candidate.ID)
		}
		if candidate.ParseStatus == types.ParseStatusFailed || candidate.ParseStatus == types.ParseStatusCancelled || candidate.ParseStatus == types.ParseStatusDeleting {
			return nil, fmt.Errorf("candidate %s processing %s: %s", candidate.ID, candidate.ParseStatus, candidate.ErrorMessage)
		}
		if candidate.ParseStatus == types.ParseStatusCompleted || (readyOnly && candidate.GetMetadata()["datasource_index_ready"] == "true") {
			return candidate, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("candidate processing wait: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
		var err error
		candidate, err = s.knowledgeService.GetRepository().GetKnowledgeByID(ctx, tenant, candidate.ID)
		if err != nil {
			return nil, err
		}
		if candidate == nil {
			return nil, errors.New("candidate disappeared while processing")
		}
	}
}

func (s *DataSourceService) checkTencentCandidateScope(ctx context.Context, ds *types.DataSource) error {
	if s.dsRepo != nil {
		latest, err := s.dsRepo.FindByID(ctx, ds.ID)
		if err != nil {
			return err
		}
		if latest == nil || latest.DeletedAt.Valid || (latest.Status == types.DataSourceStatusPaused && ds.Status != types.DataSourceStatusPaused) || latest.Status == types.DataSourceStatusDeleted {
			return datasource.ErrDataSourceNotActive
		}
		if latest.TenantID != ds.TenantID || latest.KnowledgeBaseID != ds.KnowledgeBaseID || latest.TencentFileSync != ds.TencentFileSync || !bytes.Equal(latest.Config, ds.Config) {
			return datasource.ErrInvalidConfig
		}
	}
	return nil
}
