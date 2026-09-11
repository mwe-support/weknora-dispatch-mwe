package service

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
)

type processingLegacyManifest struct {
	SnapshotDigest    string                           `json:"snapshot_digest"`
	FileDigest        string                           `json:"file_digest"`
	FileBytes         int64                            `json:"file_bytes"`
	SourceFile        processingArtifactRef            `json:"source_file"`
	Dimension         int                              `json:"dimension"`
	Chunks            []*types.Chunk                   `json:"chunks"`
	Indexes           []*types.IndexInfo               `json:"indexes"`
	Assets            []processingAsset                `json:"assets"`
	Destination       types.ProcessingIndexDestination `json:"destination"`
	LegacyDestination types.ProcessingIndexDestination `json:"legacy_destination"`
	Reembed           bool                             `json:"reembed"`
}

// No parse/export/embedding calls occur here: an unverifiable legacy artifact
// blocks adoption. Its original file/chunks/indexes remain untouched.
func (s *knowledgeService) verifyLegacySnapshot(ctx context.Context, repo *repository.ProcessingRepository, kb *types.KnowledgeBase, snapshot *repository.ProcessingLegacySnapshot, candidate bool) (processingLegacyManifest, []byte, error) {
	var manifest processingLegacyManifest
	if kb == nil || snapshot == nil || kb.TenantID != snapshot.Knowledge.TenantID || kb.ID != snapshot.Knowledge.KnowledgeBaseID || kb.Type == types.KnowledgeBaseTypeFAQ {
		return manifest, nil, repository.ErrProcessingScope
	}
	if err := repository.VerifyLegacyStages(snapshot, candidate); err != nil {
		return manifest, nil, err
	}
	k := snapshot.Knowledge
	if k.FilePath == "" || k.FileHash == "" || k.FileSize <= 0 || k.FileSize > processingArtifactLimit || !types.IsSupportedKnowledgeFileExtension(k.FileType) {
		return manifest, nil, errors.New("LEGACY_FILE_PROOF_UNAVAILABLE")
	}
	body, err := s.readLegacyFile(ctx, kb, k.ID, k.FilePath, processingArtifactLimit, true)
	if err != nil {
		return manifest, nil, err
	}
	if int64(len(body)) != k.FileSize || fmt.Sprintf("%x", md5.Sum(body)) != strings.ToLower(k.FileHash) {
		return manifest, nil, errors.New("LEGACY_FILE_CHECKSUM_MISMATCH")
	}
	manifest = processingLegacyManifest{SnapshotDigest: snapshot.Digest, FileDigest: fmt.Sprintf("%x", sha256.Sum256(body)), FileBytes: int64(len(body)), Chunks: snapshot.Chunks}
	expected, assets, err := processingLegacyInputs(snapshot, candidate)
	if err != nil {
		return manifest, nil, err
	}
	var embedding types.KnowledgeProcessingSpan
	for _, span := range snapshot.Spans {
		if span.Kind == types.SpanKindStage && span.Name == types.StageEmbedding {
			embedding = span
		}
	}
	oldDimension := legacyCount(embedding.Input, "dim")
	if oldDimension < 0 {
		return manifest, nil, errors.New("LEGACY_EMBEDDING_DIMENSION_UNVERIFIED")
	}
	if k.EmbeddingModelID != "" && k.EmbeddingModelID != fmt.Sprint(embedding.Input["model_id"]) {
		return manifest, nil, errors.New("LEGACY_EMBEDDING_MODEL_UNVERIFIED")
	}
	if kb.IsVectorEnabled() {
		model, err := s.modelService.GetEmbeddingModel(ctx, kb.EmbeddingModelID)
		if err != nil {
			return manifest, nil, err
		}
		manifest.Dimension = model.GetDimensions()
		if manifest.Dimension <= 0 {
			return manifest, nil, errors.New("LEGACY_EMBEDDING_DIMENSION_UNVERIFIED")
		}
		_, digests, err := repo.ConfigurationDigests(ctx, kb.TenantID, kb.ID)
		if err != nil {
			return manifest, nil, err
		}
		originalDigest, _ := embedding.Input["model_configuration_digest"].(string)
		manifest.Reembed = oldDimension != manifest.Dimension || fmt.Sprint(embedding.Input["model_id"]) != kb.EmbeddingModelID || originalDigest == "" || originalDigest != digests["model/"+kb.EmbeddingModelID]
		if manifest.Reembed && !candidate {
			return manifest, nil, errors.New("LEGACY_EMBEDDING_PROVENANCE_UNVERIFIED")
		}
	}
	if int64(max(oldDimension, manifest.Dimension)) > processingArtifactLimit/24/int64(len(expected)) {
		return manifest, nil, errors.New("LEGACY_VECTOR_MANIFEST_TOO_LARGE")
	}
	if !kb.IsVectorEnabled() && !kb.IsKeywordEnabled() {
		return manifest, nil, errors.New("LEGACY_INDEX_CONFIGURATION_UNVERIFIED")
	}
	manifest.Destination, err = knowledgeIndexDestination(ctx, s, kb, manifest.Dimension)
	if err != nil {
		return manifest, nil, err
	}
	// A stored destination is original execution evidence. Without one, locate
	// and verify every old point at the current destination before adopting it;
	// never infer a missing point's destination from today's model settings.
	manifest.LegacyDestination, err = knowledgeIndexDestination(ctx, s, kb, oldDimension)
	if err != nil {
		return manifest, nil, err
	}
	storedDestination := embedding.Input["index_destination"] != nil
	if storedDestination {
		encoded, encodeErr := json.Marshal(embedding.Input["index_destination"])
		// Decode independently: the initial destination contains the KB's
		// store-ID pointer, also used by the new destination. Reusing it would
		// overwrite the new route when the old route has another store ID.
		var original types.ProcessingIndexDestination
		if encodeErr != nil || json.Unmarshal(encoded, &original) != nil || original.Dimension != oldDimension || original.KnowledgeType != kb.Type {
			return manifest, nil, errors.New("LEGACY_INDEX_DESTINATION_INVALID")
		}
		manifest.LegacyDestination = original
	}
	engine, err := processingIndexEngine(ctx, s, kb.TenantID, manifest.LegacyDestination)
	if err != nil {
		return manifest, nil, err
	}
	manifest.Indexes, err = engine.ReadProcessingIndexes(ctx, kb.ID, k.ID, oldDimension, expected)
	if err != nil && candidate && storedDestination && err.Error() == "LEGACY_INDEX_MISSING" {
		manifest.Indexes, manifest.Reembed = expected, kb.IsVectorEnabled()
	} else if err != nil {
		return manifest, nil, err
	}
	// When both stores are enabled, verify the keyword destination separately;
	// having vectors cannot certify a missing keyword copy.
	if slices.Contains(manifest.LegacyDestination.Kinds, types.VectorRetrieverType) && slices.Contains(manifest.LegacyDestination.Kinds, types.KeywordsRetrieverType) {
		if err = engine.VerifyProcessingKeywordIndexes(ctx, kb.ID, k.ID, oldDimension, expected); err != nil && !(candidate && storedDestination && err.Error() == "LEGACY_INDEX_MISSING") {
			return manifest, nil, err
		}
	}
	if manifest.Reembed || !kb.IsVectorEnabled() {
		for _, item := range manifest.Indexes {
			item.PreparedEmbedding = nil
		}
	}
	var mediaBytes int64
	for _, address := range assets {
		data, err := s.readLegacyFile(ctx, kb, k.ID, address, processingImageLimit, false)
		if err != nil {
			return manifest, nil, err
		}
		if _, err = processingImageFormat(data); err != nil {
			return manifest, nil, err
		}
		mediaBytes += int64(len(data))
		if mediaBytes > processingArtifactLimit {
			return manifest, nil, errors.New("LEGACY_MEDIA_SIZE_EXCEEDED")
		}
		manifest.Assets = append(manifest.Assets, processingAsset{ID: processingAssetUnit(address), StoredURL: address, Digest: fmt.Sprintf("%x", sha256.Sum256(data)), Bytes: int64(len(data))})
	}
	return manifest, body, nil
}

func (s *knowledgeService) readLegacyFile(ctx context.Context, kb *types.KnowledgeBase, knowledgeID, address string, limit int64, sourceFile bool) ([]byte, error) {
	// Do not turn historical text/metadata into arbitrary HTTP or local-file
	// access. Only an existing owned storage address can supply proof.
	owner, ok := s.repo.(interface {
		ValidateLegacyStorageOwnership(context.Context, uint64, string, string, bool) error
	})
	if !ok {
		return nil, errors.New("LEGACY_STORAGE_OWNER_VERIFIER_UNAVAILABLE")
	}
	if err := owner.ValidateLegacyStorageOwnership(ctx, kb.TenantID, knowledgeID, address, sourceFile); err != nil {
		return nil, err
	}
	files := s.resolveFileServiceForPath(ctx, kb, address)
	if files == nil {
		return nil, errors.New("LEGACY_STORAGE_UNAVAILABLE")
	}
	reader, err := files.GetFile(ctx, address)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || int64(len(body)) > limit {
		return nil, errors.New("LEGACY_FILE_SIZE_INVALID")
	}
	return body, nil
}

func legacyCount(values types.JSONMap, key string) int {
	encoded, err := json.Marshal(values[key])
	if err != nil {
		return -1
	}
	n, err := strconv.Atoi(string(encoded))
	if err != nil {
		return -1
	}
	return n
}

func processingLegacyInputs(snapshot *repository.ProcessingLegacySnapshot, candidate bool) ([]*types.IndexInfo, []string, error) {
	k := snapshot.Knowledge
	parents := map[string]bool{}
	chunks := map[string]*types.Chunk{}
	for _, chunk := range snapshot.Chunks {
		if chunk == nil || chunk.ID == "" || chunks[chunk.ID] != nil || chunk.TenantID != k.TenantID || chunk.KnowledgeID != k.ID || chunk.KnowledgeBaseID != k.KnowledgeBaseID {
			return nil, nil, errors.New("LEGACY_CHUNK_SCOPE_INVALID")
		}
		if candidate {
			if _, err := uuid.Parse(chunk.ID); err != nil {
				return nil, nil, errors.New("LEGACY_CHUNK_ID_INVALID")
			}
		}
		chunks[chunk.ID] = chunk
		if chunk.ChunkType == types.ChunkTypeParentText {
			parents[chunk.ID] = true
		}
	}
	var items []*types.IndexInfo
	add := func(id string, chunk *types.Chunk, content string, enabled bool) {
		items = append(items, &types.IndexInfo{SourceID: id, ChunkID: chunk.ID, SourceType: types.ChunkSourceType, KnowledgeID: k.ID, KnowledgeBaseID: k.KnowledgeBaseID, Content: content, IsEnabled: enabled})
	}
	textCount, baseCount := 0, 0
	media := map[string]bool{}
	mediaChildren := map[string]int{}
	var markdown strings.Builder
	for _, chunk := range snapshot.Chunks {
		var images []types.ImageInfo
		if chunk.ImageInfo != "" && json.Unmarshal([]byte(chunk.ImageInfo), &images) != nil {
			return nil, nil, errors.New("LEGACY_IMAGE_MANIFEST_INVALID")
		}
		for _, info := range images {
			if info.URL == "" {
				return nil, nil, errors.New("LEGACY_IMAGE_ADDRESS_MISSING")
			}
			media[info.URL] = true
		}
		switch chunk.ChunkType {
		case types.ChunkTypeParentText:
			baseCount++
			markdown.WriteString(chunk.Content + "\n")
		case types.ChunkTypeText:
			baseCount++
			markdown.WriteString(chunk.Content + "\n")
			if len(parents) > 0 && chunk.ParentChunkID == "" {
				continue
			}
			if len(parents) > 0 && !parents[chunk.ParentChunkID] {
				return nil, nil, errors.New("LEGACY_PARENT_CHUNK_MISSING")
			}
			textCount++
			add(chunk.ID, chunk, buildKnowledgeIndexContent(&k, chunk.EmbeddingContent()), true)
		case types.ChunkTypeImageOCR, types.ChunkTypeImageCaption:
			if len(images) != 1 || chunks[chunk.ParentChunkID] == nil {
				return nil, nil, errors.New("LEGACY_IMAGE_CHUNK_UNMAPPED")
			}
			mediaChildren[images[0].URL]++
			add(chunk.ID, chunk, chunk.Content, false)
		case types.ChunkTypeSummary:
			if candidate {
				return nil, nil, errors.New("LEGACY_CANDIDATE_ALREADY_ENRICHED")
			}
			add(chunk.ID, chunk, chunk.Content, true)
		default:
			return nil, nil, errors.New("LEGACY_CHUNK_TYPE_UNVERIFIED")
		}
		meta, err := chunk.DocumentMetadata()
		if err != nil {
			return nil, nil, err
		}
		if meta != nil {
			if candidate && len(meta.GeneratedQuestions) > 0 {
				return nil, nil, errors.New("LEGACY_CANDIDATE_ALREADY_ENRICHED")
			}
			for _, question := range meta.GeneratedQuestions {
				if question.ID == "" || strings.TrimSpace(question.Question) == "" {
					return nil, nil, errors.New("LEGACY_QUESTION_INVALID")
				}
				add(types.GeneratedQuestionSourceID(chunk.ID, question.ID), chunk, buildKnowledgeIndexContent(&k, question.Question), true)
			}
		}
	}
	if textCount == 0 || len(items) == 0 {
		return nil, nil, errors.New("LEGACY_TEXT_COVERAGE_EMPTY")
	}
	imageSpans := map[string]bool{}
	imageExpected := 0
	for _, span := range snapshot.Spans {
		if span.Kind == types.SpanKindStage {
			switch span.Name {
			case types.StageChunking:
				if legacyCount(span.Output, "chunks_written") != baseCount {
					return nil, nil, errors.New("LEGACY_CHUNK_COVERAGE_MISMATCH")
				}
			case types.StageEmbedding:
				if legacyCount(span.Input, "chunks_to_embed") != textCount || legacyCount(span.Output, "vectors_written") != textCount {
					return nil, nil, errors.New("LEGACY_TEXT_INDEX_COVERAGE_MISMATCH")
				}
				if k.StorageSize > 0 && int64(legacyCount(span.Output, "storage_bytes")) != k.StorageSize {
					return nil, nil, errors.New("LEGACY_INDEX_ACCOUNTING_UNVERIFIED")
				}
			case types.StageMultimodal:
				if span.Status == types.SpanStatusDone {
					imageExpected = legacyCount(span.Input, "image_count")
				}
			}
		}
		if span.Kind == types.SpanKindGeneration && strings.HasPrefix(span.Name, "multimodal.image[") {
			address, _ := span.Input["image_url"].(string)
			if address == "" || imageSpans[address] || span.Status != types.SpanStatusDone {
				return nil, nil, errors.New("LEGACY_IMAGE_ATTEMPT_INCOMPLETE")
			}
			if legacyCount(span.Output, "chunks_created") != mediaChildren[address] {
				return nil, nil, errors.New("LEGACY_IMAGE_CHUNKS_MISMATCH")
			}
			imageSpans[address], media[address] = true, true
		}
	}
	if imageExpected != len(imageSpans) {
		return nil, nil, errors.New("LEGACY_IMAGE_COVERAGE_MISMATCH")
	}
	if imageExpected > 0 {
		for address := range media {
			if !imageSpans[address] {
				return nil, nil, errors.New("LEGACY_IMAGE_ATTEMPT_MISSING")
			}
		}
	}
	targets := map[string]string{}
	var assets []string
	for address := range media {
		targets[address] = address
		assets = append(assets, address)
	}
	if _, err := docparser.RewriteProcessingImages(markdown.String(), targets); err != nil {
		return nil, nil, err
	}
	slices.Sort(assets)
	return items, assets, nil
}

func ResolveLegacyCompletion(ctx context.Context, knowledge interfaces.KnowledgeService, repo *repository.ProcessingRepository, evidence types.ProcessingLegacyEvidence) (*types.ProcessingLegacyEvidence, error) {
	s, ok := knowledge.(*knowledgeService)
	if !ok {
		return nil, errors.New("LEGACY_VERIFIER_UNAVAILABLE")
	}
	original, err := repo.InspectLegacyError(ctx, evidence.ProcessingLegacyIdentity)
	if err != nil {
		return nil, err
	}
	if original.Identity != evidence.ProcessingLegacyIdentity {
		return nil, repository.ErrProcessingConflict
	}
	snapshot, err := repo.LegacySnapshot(ctx, evidence.ProcessingLegacyIdentity, evidence.KnowledgeID, evidence.SourceRevision, evidence.Attempt)
	if err != nil {
		return nil, err
	}
	if evidence.SnapshotDigest != snapshot.Digest {
		return nil, repository.ErrProcessingConflict
	}
	evidence.ConfigurationRevision, err = repo.ConfigurationRevision(ctx, evidence.TenantID, evidence.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, types.TenantIDContextKey, evidence.TenantID)
	tenant, err := s.tenantRepo.GetTenantByID(ctx, evidence.TenantID)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, evidence.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	manifest, _, err := s.verifyLegacySnapshot(ctx, repo, kb, snapshot, false)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	evidence.ArtifactDigest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	return repo.RecordLegacyCompletion(ctx, evidence)
}
