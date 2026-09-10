package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func prepareProcessingCandidate(tx *gorm.DB, job *types.ProcessingJob, input *types.Knowledge) error {
	if !job.IsCurrent || job.Kind != types.ProcessingJobDocument {
		return ErrProcessingConflict
	}
	if job.KnowledgeID != "" {
		return nil
	}
	knowledge := *input
	knowledge.ID, knowledge.TenantID, knowledge.KnowledgeBaseID = types.ProcessingKnowledgeID(job.ID), job.TenantID, job.KnowledgeBaseID
	metadata := knowledge.GetMetadata()
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["processing_protocol"], metadata["processing_job_id"] = "2", job.ID
	metadata["datasource_id"], metadata["external_id"] = job.DataSourceID, job.ExternalID
	metadata["datasource_candidate"], metadata["datasource_version"] = "true", job.ID
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	knowledge.Metadata, knowledge.CustomMetadata = encoded, types.JSON(`{}`)
	knowledge.ParseStatus, knowledge.EnableStatus = types.ParseStatusProcessing, "disabled"
	if knowledge.Type == "" {
		knowledge.Type = "file"
	}
	knowledge.Channel = types.ChannelTencentDocs
	if err := tx.Create(&knowledge).Error; err != nil {
		return err
	}
	job.KnowledgeID = knowledge.ID
	if err := tx.Model(job).Update("knowledge_id", knowledge.ID).Error; err != nil {
		return err
	}
	return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "candidate_created"})
}

// Called inside the step's ownership transaction. The same source lock protects
// first publication, replacements and (later) rollback; no remote I/O is done.
func publishProcessingJob(tx *gorm.DB, job *types.ProcessingJob, now time.Time) error {
	if err := refreshProcessingJob(tx, job); err != nil {
		return err
	}
	if !job.IsCurrent || job.IsPublished || !job.PlanSealed || job.Readiness != "ready" || job.Completeness != "complete" || job.KnowledgeID == "" || job.RetirementState != "retained" {
		return ErrProcessingConflict
	}
	var candidate types.Knowledge
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", job.KnowledgeID, job.TenantID, job.KnowledgeBaseID).Take(&candidate).Error; err != nil {
		return err
	}
	if candidate.GetMetadata()["processing_job_id"] != job.ID {
		return ErrProcessingConflict
	}
	var epoch int64
	if err := processingLogicalQuery(tx, *job).Select("COALESCE(MAX(publication_epoch), 0)").Scan(&epoch).Error; err != nil {
		return err
	}
	var old types.ProcessingJob
	err := processingLogicalQuery(tx, *job).Where("is_published = ?", true).Take(&old).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if old.ID != "" {
		old.IsPublished = false
		if err := tx.Model(&old).Update("is_published", false).Error; err != nil {
			return err
		}
		if err := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", old.KnowledgeID, old.TenantID).Update("enable_status", "disabled").Error; err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, &old, types.ProcessingEvent{Type: "publication_replaced"}); err != nil {
			return err
		}
		if err := stopProcessingJob(tx, &old, types.ProcessingSuperseded, "NEW_PUBLICATION"); err != nil {
			return err
		}
		if err := refreshProcessingRuns(tx, &old); err != nil {
			return err
		}
	}
	var confirmed []types.ProcessingStep
	if err := tx.Where("job_id = ? AND status = ? AND output_digest <> ''", job.ID, types.ProcessingSucceeded).Find(&confirmed).Error; err != nil {
		return err
	}
	manifest := map[string]types.ProcessingArtifactVersion{}
	for _, step := range confirmed {
		manifest[step.ID] = types.ProcessingArtifactVersion{Attempt: step.Attempt, Digest: step.OutputDigest}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	job.IsPublished, job.PublicationEpoch, job.PublishedAt, job.ActiveIndexManifest = true, epoch+1, &now, string(encoded)
	if err := tx.Model(job).Updates(map[string]any{"is_published": true, "publication_epoch": job.PublicationEpoch,
		"published_at": now, "active_index_manifest": job.ActiveIndexManifest}).Error; err != nil {
		return err
	}
	if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND phase = ?", job.ID, types.ProcessingPhaseProjection).
		Update("expected_publication_epoch", job.PublicationEpoch).Error; err != nil {
		return err
	}
	metadata := candidate.GetMetadata()
	delete(metadata, "datasource_candidate")
	encoded, err = json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := tx.Model(&candidate).Updates(map[string]any{"metadata": types.JSON(encoded), "enable_status": "enabled",
		"parse_status": types.ParseStatusFinalizing, "processed_at": now}).Error; err != nil {
		return err
	}
	if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "version_published"}); err != nil {
		return err
	}
	var requested types.ProcessingEvent
	err = tx.Where("job_id = ? AND event_type = ?", job.ID, "faq_rollback_requested").
		Where("id NOT IN (?)", tx.Model(&types.ProcessingEvent{}).Select("resolves_event_id").Where("job_id = ? AND resolves_event_id IS NOT NULL", job.ID)).Order("id DESC").Take(&requested).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "version_rolled_back", ResolvesEventID: &requested.ID, ResolutionType: "rollback_published", Actor: requested.Actor})
}

func activateProcessingArtifact(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep) error {
	if !job.IsPublished || step.ExpectedPublicationEpoch != job.PublicationEpoch {
		return ErrProcessingConflict
	}
	var manifest map[string]types.ProcessingArtifactVersion
	if err := json.Unmarshal([]byte(job.ActiveIndexManifest), &manifest); err != nil {
		return err
	}
	if manifest == nil {
		return errors.New("published artifact manifest is missing")
	}
	manifest[step.ID] = types.ProcessingArtifactVersion{Attempt: step.Attempt, Digest: step.OutputDigest}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	job.ActiveIndexManifest = string(encoded)
	return tx.Model(job).Update("active_index_manifest", job.ActiveIndexManifest).Error
}

// FilterIndexes is a visibility check, not an authorization entry point. The
// caller supplies scopes already authorized by WeKnora's KB access layer. A
// missing job for a protocol-2 knowledge fails closed; legacy data stays visible.
func (r *ProcessingRepository) FilterIndexes(ctx context.Context, scopes []types.KnowledgeSearchScope, hits []*types.IndexWithScore) ([]*types.IndexWithScore, error) {
	if len(hits) == 0 || len(scopes) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		if hit != nil {
			ids = append(ids, hit.KnowledgeID)
		}
	}
	query := r.db.WithContext(ctx).Model(&types.Knowledge{}).Select("id", "tenant_id", "knowledge_base_id", "type", "metadata", "enable_status").Where("id IN ?", ids)
	allowed := r.db.Where("1 = 0")
	for _, scope := range scopes {
		allowed = allowed.Or("tenant_id = ? AND knowledge_base_id = ?", scope.TenantID, scope.KBID)
	}
	var knowledge []types.Knowledge
	if err := query.Where(allowed).Find(&knowledge).Error; err != nil {
		return nil, err
	}
	byID := map[string]types.Knowledge{}
	managedIDs := []string{}
	faqIDs := []string{}
	for _, item := range knowledge {
		byID[item.ID] = item
		if item.Type == types.KnowledgeTypeFAQ {
			faqIDs = append(faqIDs, item.ID)
		}
		if item.GetMetadata()["processing_protocol"] == "2" {
			managedIDs = append(managedIDs, item.ID)
		}
	}
	faqChunks := map[string]types.Chunk{}
	if len(faqIDs) > 0 {
		chunkIDs := make([]string, 0, len(hits))
		for _, hit := range hits {
			if hit != nil {
				chunkIDs = append(chunkIDs, hit.ChunkID)
			}
		}
		var rows []types.Chunk
		if err := r.db.WithContext(ctx).Where("knowledge_id IN ? AND id IN ? AND chunk_type = ?", faqIDs, chunkIDs, types.ChunkTypeFAQ).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			faqChunks[row.ID] = row
		}
	}
	jobs := map[string]types.ProcessingJob{}
	manifests := map[string]map[string]types.ProcessingArtifactVersion{}
	if len(managedIDs) > 0 {
		var rows []types.ProcessingJob
		if err := r.db.WithContext(ctx).Where("knowledge_id IN ?", managedIDs).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			item := byID[row.KnowledgeID]
			if row.ID != item.GetMetadata()["processing_job_id"] || row.TenantID != item.TenantID || row.KnowledgeBaseID != item.KnowledgeBaseID {
				continue
			}
			jobs[row.KnowledgeID] = row
			if row.IsPublished {
				var manifest map[string]types.ProcessingArtifactVersion
				if err := json.Unmarshal([]byte(row.ActiveIndexManifest), &manifest); err != nil {
					return nil, errors.New("published processing manifest is unreadable")
				}
				manifests[row.KnowledgeID] = manifest
			}
		}
	}
	result := make([]*types.IndexWithScore, 0, len(hits))
	for _, hit := range hits {
		if hit == nil {
			continue
		}
		item, exists := byID[hit.KnowledgeID]
		if !exists || hit.KnowledgeBaseID != item.KnowledgeBaseID {
			continue
		}
		if item.Type == types.KnowledgeTypeFAQ {
			chunk, exists := faqChunks[hit.ChunkID]
			if item.EnableStatus == "enabled" && exists && chunk.TenantID == item.TenantID && chunk.KnowledgeBaseID == item.KnowledgeBaseID && chunk.KnowledgeID == item.ID && chunk.AcceptsFAQIndex(hit.SourceID) {
				hit.TagID, hit.IsEnabled = chunk.TagID, chunk.IsEnabled
				result = append(result, hit)
			}
			continue
		}
		if strings.HasPrefix(hit.SourceID, "fq-") {
			continue
		}
		if item.GetMetadata()["processing_protocol"] != "2" {
			result = append(result, hit)
			continue
		}
		job := jobs[item.ID]
		if !job.IsPublished || job.RetirementState != "retained" || item.EnableStatus != "enabled" {
			continue
		}
		step, attempt, err := types.ParseProcessingIndexSourceID(hit.SourceID)
		if err != nil {
			continue
		}
		accepted, exists := manifests[item.ID][step]
		if exists && accepted.Attempt == attempt && accepted.Digest != "" {
			result = append(result, hit)
		}
	}
	return result, nil
}

func (r *knowledgeRepository) FilterProcessingIndexes(ctx context.Context, scopes []types.KnowledgeSearchScope, hits []*types.IndexWithScore) ([]*types.IndexWithScore, error) {
	return NewProcessingRepository(r.db).FilterIndexes(ctx, scopes, hits)
}
