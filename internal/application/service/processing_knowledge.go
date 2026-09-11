package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/infrastructure/chunker"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
)

type ProcessingDocumentSpec struct {
	FileID           string                       `json:"file_id"`
	Kind             string                       `json:"kind"`
	Title            string                       `json:"title"`
	FolderPath       string                       `json:"folder_path"`
	URL              string                       `json:"url"`
	Language         string                       `json:"language"`
	Resource         *tencentdocs.NativeScanEntry `json:"resource,omitempty"`
	RevisionMode     string                       `json:"revision_mode,omitempty"`
	IdentityRevision string                       `json:"identity_revision,omitempty"`
}

// The version changes when parser/normalizer behavior changes. App prompts and
// parser configuration are also fixed inputs, separate from live KB/model data.
func ProcessingPipelineFingerprint(cfg *config.Config) string {
	inputs := []any{"tencent-lifecycle-20260911-2"}
	if cfg != nil {
		inputs = append(inputs, cfg.Conversation, cfg.KnowledgeBase, cfg.DocReader, cfg.ExtractManager, cfg.PromptTemplates)
	}
	encoded, _ := json.Marshal(inputs)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

type processingDocumentExecution struct {
	s         *knowledgeService
	repo      *repository.ProcessingRepository
	sources   interfaces.DataSourceRepository
	lease     types.ProcessingLease
	kb        *types.KnowledgeBase
	document  ProcessingDocumentSpec
	artifacts *ProcessingArtifacts
	steps     []types.ProcessingStep
}

func NewKnowledgeProcessingExecutor(knowledge interfaces.KnowledgeService, repo *repository.ProcessingRepository, sources interfaces.DataSourceRepository) (ProcessingExecutor, error) {
	s, ok := knowledge.(*knowledgeService)
	if !ok {
		return nil, errors.New("processing requires the application knowledge service")
	}
	return func(ctx context.Context, lease types.ProcessingLease) (types.ProcessingOutcome, error) {
		ctx = types.WithBackgroundTask(ctx)
		ctx = types.WithProcessingLease(ctx, lease)
		if lease.Step.Stage == "retire_previous" {
			// The repository checks and records cleanup under the publication lock.
			return types.ProcessingOutcome{Status: types.ProcessingSucceeded}, nil
		}
		if lease.Step.Phase != types.ProcessingPhaseRetire && lease.Job.PipelineFingerprint != ProcessingPipelineFingerprint(s.config) {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "PIPELINE_CHANGED", Message: "The processing configuration changed; create a new document generation"}, nil
		}
		ctx = context.WithValue(ctx, types.TenantIDContextKey, lease.Job.TenantID)
		tenant, err := s.tenantRepo.GetTenantByID(ctx, lease.Job.TenantID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
		if lease.Step.Phase == types.ProcessingPhaseRetire {
			outcome, err := s.retireProcessingVersion(ctx, repo, lease, tenant)
			if err != nil && !errors.Is(err, repository.ErrProcessingScope) && !errors.Is(err, repository.ErrProcessingConflict) && !errors.Is(err, context.Canceled) {
				return types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "transient", ErrorCode: "RETIREMENT_IO_FAILED", Message: "Version cleanup did not finish; retry keeps confirmed processing outputs", Retryable: true}, nil
			}
			return outcome, err
		}
		kb, err := s.kbService.GetKnowledgeBaseByID(ctx, lease.Job.KnowledgeBaseID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if kb.TenantID != lease.Job.TenantID {
			return types.ProcessingOutcome{}, repository.ErrProcessingScope
		}
		var document ProcessingDocumentSpec
		if lease.Job.Kind != types.ProcessingJobScan && (json.Unmarshal(lease.Job.Metadata, &document) != nil || document.FileID == "" || document.Kind == "") {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "DOCUMENT_IDENTITY_INVALID"}, nil
		}
		if document.Language != "" {
			ctx = context.WithValue(ctx, types.LanguageContextKey, document.Language)
		}
		document.FolderPath = types.NormalizeKnowledgeFolderPath(document.FolderPath)
		steps, err := repo.ListSteps(ctx, lease.Job.TenantID, lease.Job.ID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		fileService := s.fileSvc
		backendID := ""
		if kb.StorageBackendID != nil {
			backendID = strings.TrimSpace(*kb.StorageBackendID)
		}
		if s.storageResolver != nil {
			fileService, _, err = s.storageResolver.ResolveFileService(ctx, tenant, backendID, kb.GetStorageProvider(), strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
			if err != nil {
				return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "STORAGE_BACKEND_UNAVAILABLE", Message: "The configured artifact storage could not be resolved"}, nil
			}
		} else if backendID != "" {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "STORAGE_RESOLVER_UNAVAILABLE"}, nil
		}
		if fileService == nil {
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "ARTIFACT_STORAGE_UNAVAILABLE"}, nil
		}
		e := processingDocumentExecution{s: s, repo: repo, sources: sources, lease: lease, kb: kb, document: document,
			artifacts: NewProcessingArtifacts(fileService, s.resourceCatalog), steps: steps}
		var outcome types.ProcessingOutcome
		if lease.Job.Kind == types.ProcessingJobScan {
			outcome, err = e.scan(ctx)
		} else {
			outcome, err = e.execute(ctx)
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, repository.ErrProcessingScope) && !errors.Is(err, repository.ErrProcessingConflict) {
			return tencentdocs.ProcessingFailure(lease.Step.Stage, err), nil
		}
		return outcome, err
	}, nil
}

func (e *processingDocumentExecution) dependency(ctx context.Context, stage, unit, kind string, out any) error {
	data, err := e.dependencyBytes(ctx, stage, unit, kind)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return errors.New("processing dependency has an invalid format")
	}
	return nil
}

func (e *processingDocumentExecution) dependencyBytes(ctx context.Context, stage, unit, kind string) ([]byte, error) {
	for _, step := range e.steps {
		if step.Stage == stage && step.UnitKey == unit && step.Status == types.ProcessingSucceeded {
			return e.artifacts.Read(ctx, e.lease.Job, step, kind, step.OutputManifestRef, step.OutputDigest)
		}
	}
	return nil, errors.New("processing dependency is not confirmed")
}

func (e *processingDocumentExecution) success(ctx context.Context, kind string, value any) (types.ProcessingOutcome, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	path, digest, err := e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, kind, data)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: path, OutputDigest: digest}, nil
}

func (e *processingDocumentExecution) execute(ctx context.Context) (types.ProcessingOutcome, error) {
	if outcome, reused, err := e.reuseOutput(ctx); reused || err != nil {
		return outcome, err
	}
	switch e.lease.Step.Stage {
	case "native_read", "export_start", "export_poll", "download":
		source, err := e.sources.FindByID(ctx, e.lease.Job.DataSourceID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if source.TenantID != e.lease.Job.TenantID || source.KnowledgeBaseID != e.lease.Job.KnowledgeBaseID {
			return types.ProcessingOutcome{}, repository.ErrProcessingScope
		}
		configuration, err := source.ParseConfig()
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		client, err := tencentdocs.NewConfiguredMCPClient(configuration)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		defer client.Close()
		if e.document.Kind == "doc" && e.document.RevisionMode == "export_snapshot" {
			verify := func(ctx context.Context) error {
				response, err := client.ReadNative(tencentdocs.WithManagedRetries(ctx), "manage.query_file_info", map[string]interface{}{"file_id": e.document.FileID})
				if err != nil {
					return err
				}
				var metadata tencentdocs.FileInfo
				if response == nil || json.Unmarshal(response.Data, &metadata) != nil || metadata.ID != e.document.FileID || metadata.Type != "doc" || metadata.Status != "normal" || metadata.IsFolder || processingDOCIdentity(metadata) != e.document.IdentityRevision {
					return errors.New("SOURCE_CHANGED_DURING_READ")
				}
				return nil
			}
			if e.lease.Step.Stage != "native_read" {
				return e.exportStage(ctx, client, verify)
			}
			if err := verify(ctx); err != nil {
				return types.ProcessingOutcome{}, err
			}
			// This confirms identity, not body coverage. DOCX inventory, parser
			// coverage and assets still gate publication of this export snapshot.
			return e.success(ctx, "native_snapshot", tencentdocs.NativeSnapshot{FileID: e.document.FileID, Kind: "doc", RevisionKey: e.lease.Job.SourceRevision, CoverageComplete: true, RequiresExport: true})
		}
		if e.document.Kind == "resource" {
			if e.document.Resource == nil || e.document.Resource.FileID != e.document.FileID || e.document.Resource.ExternalID != e.lease.Job.ExternalID || e.document.Resource.Listing == nil || !slices.Contains(configuration.ResourceIDs, e.document.Resource.Listing.ResourceID) {
				return types.ProcessingOutcome{}, errors.New("RESOURCE_LISTING_PROOF_REQUIRED")
			}
			verify := func(ctx context.Context) error {
				return tencentdocs.VerifyNativeResource(tencentdocs.WithManagedRetries(ctx), *e.document.Resource, client.ReadScan)
			}
			if e.lease.Step.Stage != "native_read" {
				return e.exportStage(ctx, client, verify)
			}
			if err := verify(ctx); err != nil {
				return types.ProcessingOutcome{}, err
			}
			return e.success(ctx, "native_snapshot", tencentdocs.NativeSnapshot{FileID: e.document.FileID, Kind: "resource", RevisionKey: e.lease.Job.SourceRevision, CoverageComplete: true, RequiresExport: true})
		}
		read := func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
			return client.ReadNative(tencentdocs.WithManagedRetries(ctx), tool, args)
		}
		if e.lease.Step.Stage != "native_read" {
			return e.exportStage(ctx, client, func(ctx context.Context) error {
				revision, err := tencentdocs.ProbeNativeRevision(ctx, e.document.FileID, e.document.Kind, read)
				if err != nil {
					return err
				}
				if revision != e.lease.Job.SourceRevision {
					return errors.New("SOURCE_CHANGED_DURING_READ")
				}
				return nil
			})
		}
		return ReadProcessingNative(ctx, e.repo, e.artifacts, e.lease, e.document.FileID, e.document.Kind, read)
	case "normalize":
		var snapshot tencentdocs.NativeSnapshot
		if err := e.dependency(ctx, "native_read", "body", "native_snapshot", &snapshot); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if e.document.Kind == "doc" || e.document.Kind == "resource" {
			if !snapshot.CoverageComplete || !snapshot.RequiresExport {
				return types.ProcessingOutcome{}, errors.New("DOC_SOURCE_SNAPSHOT_INVALID")
			}
			var ready processingExportReceipt
			if err := e.dependency(ctx, "export_poll", "body", "export_ready", &ready); err != nil {
				return types.ProcessingOutcome{}, err
			}
			outcome, err := e.success(ctx, "normalize", ready)
			ext := path.Ext(ready.FileName)
			fileName := e.document.Title
			if !strings.EqualFold(path.Ext(fileName), ext) {
				fileName += ext
			}
			outcome.Candidate = &types.Knowledge{Title: e.document.Title, FileName: fileName, FileType: strings.TrimPrefix(ext, "."), Source: e.document.URL, FolderPath: e.document.FolderPath, EmbeddingModelID: e.kb.EmbeddingModelID}
			return outcome, err
		}
		normalized, err := tencentdocs.NormalizeNativeSnapshot(&snapshot)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if len(normalized.Unsupported) > 0 {
			result, _ := json.Marshal(map[string]any{"unsupported": normalized.Unsupported})
			return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "completeness", ErrorCode: "NATIVE_CONTENT_UNSUPPORTED", Message: "The source contains content that cannot yet be read completely", Result: result}, nil
		}
		outcome, err := e.success(ctx, "normalize", normalized)
		if err != nil {
			return outcome, err
		}
		outcome.Candidate = &types.Knowledge{Title: e.document.Title, FileName: e.document.Title + ".md", FileType: "md", Source: e.document.URL, FolderPath: e.document.FolderPath, EmbeddingModelID: e.kb.EmbeddingModelID}
		if len(normalized.Assets) == 0 {
			outcome.Completeness = "complete"
		}
		return outcome, nil
	case "parse":
		if e.document.Kind == "doc" || e.document.Kind == "resource" {
			return e.parseExport(ctx)
		}
		var normalized tencentdocs.NativeNormalized
		if err := e.dependency(ctx, "normalize", "body", "normalize", &normalized); err != nil {
			return types.ProcessingOutcome{}, err
		}
		// Native adapters already supply validated Markdown. Running it through
		// an unrelated file parser would add work and could lose source structure.
		parsed, err := processingNativeParsed(normalized)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		parsed.Metadata["source_type"] = e.document.Kind
		return e.success(ctx, "parse", parsed)
	case "assets":
		return e.assetBarrier(ctx)
	case "asset_download":
		return e.downloadAsset(ctx, e.fetchAsset)
	case "images":
		return e.imageBarrier(ctx)
	case "image_ocr", "image_caption":
		return e.imageText(ctx)
	case "chunk":
		var parsed types.ReadResult
		stage := "parse"
		for _, step := range e.steps {
			if step.Stage == "assets" {
				stage = "assets"
				break
			}
		}
		if err := e.dependency(ctx, stage, "body", stage, &parsed); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if strings.Contains(parsed.MarkdownContent, "](asset:") {
			return types.ProcessingOutcome{}, errors.New("ASSET_COVERAGE_INCOMPLETE")
		}
		chunks, err := buildProcessingChunks(e.kb, e.lease, parsed.MarkdownContent)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		outcome, err := e.success(ctx, "chunk", chunks)
		if err != nil {
			return outcome, err
		}
		outcome.Chunks = chunks
		outcome.Result, _ = json.Marshal(map[string]int{"chunks": len(chunks)})
		return outcome, nil
	case "publish":
		if e.kb.Type == types.KnowledgeBaseTypeFAQ {
			var mutations []types.ProcessingFAQMutation
			if err := e.dependency(ctx, "faq_index", "body", "faq_index", &mutations); err != nil {
				return types.ProcessingOutcome{}, err
			}
			out, err := e.success(ctx, "publication", map[string]any{"job_id": e.lease.Job.ID, "entries": len(mutations)})
			out.FAQMutations = mutations
			return out, err
		}
		return e.success(ctx, "publication", map[string]any{"job_id": e.lease.Job.ID, "generation": e.lease.Job.Generation})
	case "faq_prepare", "faq_index", "faq_entry", "faq_embedding", "faq_write":
		return e.faqStage(ctx)
	case "text_index", "summary_index", "image_index", "question_index":
		return e.indexBarrier(ctx)
	case "embedding":
		return e.embedBatch(ctx)
	case "index":
		return e.indexBatch(ctx)
	case "summary":
		return e.summarize(ctx)
	case "questions":
		return e.questionBarrier(ctx)
	case "question":
		return e.generateChunkQuestions(ctx)
	case "graph":
		return e.graphBarrier(ctx)
	case "graph_extract":
		return e.extractGraph(ctx)
	case "graph_apply":
		return e.applyGraph(ctx)
	case "wiki", "wiki_extract", "wiki_dedup", "wiki_cite", "wiki_summary_part", "wiki_summary", "wiki_prepare", "wiki_pages", "wiki_page",
		"wiki_taxonomy_input", "wiki_taxonomy", "wiki_taxonomy_vectors", "wiki_taxonomy_plan", "wiki_links":
		return e.wikiStage(ctx)
	default:
		return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "capability", ErrorCode: "STAGE_CAPABILITY_UNAVAILABLE", Message: "This processing stage is not supported by this worker"}, nil
	}
}

func buildProcessingChunks(kb *types.KnowledgeBase, lease types.ProcessingLease, markdown string) ([]*types.Chunk, error) {
	markdown = chunker.NormalizeLineEndings(markdown)
	if strings.TrimSpace(markdown) == "" {
		return nil, errors.New("VERIFIED_BODY_EMPTY")
	}
	config := buildSplitterConfig(kb)
	var children []chunker.ChildChunk
	var parents []chunker.Chunk
	if kb.ChunkingConfig.EnableParentChild {
		parentConfig, childConfig := buildParentChildConfigs(kb.ChunkingConfig, config)
		parts := chunker.SplitParentChild(markdown, parentConfig, childConfig)
		children, parents = parts.Children, parts.Parents
	} else {
		for _, part := range chunker.Split(markdown, config) {
			children = append(children, chunker.ChildChunk{Chunk: part})
		}
	}
	if len(children) == 0 {
		return nil, errors.New("CHUNK_COVERAGE_EMPTY")
	}
	stamp, _ := json.Marshal(map[string]string{"processing_job_id": lease.Job.ID, "processing_step_id": lease.Step.ID, "processing_attempt": strconv.Itoa(lease.Ref.Attempt)})
	makeChunk := func(kind types.ChunkType, chunk chunker.Chunk) *types.Chunk {
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%d/%s/%d", lease.Step.ID, lease.Ref.Attempt, kind, chunk.Seq))).String()
		return &types.Chunk{ID: id, TenantID: lease.Job.TenantID, KnowledgeID: lease.Job.KnowledgeID, KnowledgeBaseID: lease.Job.KnowledgeBaseID,
			Content: chunk.Content, SourceContent: chunk.Content, ContextHeader: chunk.ContextHeader, ChunkIndex: chunk.Seq, StartAt: chunk.Start, EndAt: chunk.End,
			ChunkType: kind, IsEnabled: true, Status: int(types.ChunkStatusStored), IndexStatus: "processing", Metadata: stamp}
	}
	result := make([]*types.Chunk, 0, len(parents)+len(children))
	parentChunks := make([]*types.Chunk, len(parents))
	for i, parent := range parents {
		parentChunks[i] = makeChunk(types.ChunkTypeParentText, parent)
		result = append(result, parentChunks[i])
	}
	link := func(chunks []*types.Chunk) {
		for i := 1; i < len(chunks); i++ {
			chunks[i-1].NextChunkID = chunks[i].ID
			chunks[i].PreChunkID = chunks[i-1].ID
		}
	}
	link(parentChunks)
	textChunks := make([]*types.Chunk, len(children))
	for i, child := range children {
		textChunks[i] = makeChunk(types.ChunkTypeText, child.Chunk)
		if len(parents) > 0 {
			if child.ParentIndex < 0 || child.ParentIndex >= len(parentChunks) {
				return nil, errors.New("CHUNK_PARENT_MAPPING_INVALID")
			}
			textChunks[i].ParentChunkID = parentChunks[child.ParentIndex].ID
		}
		result = append(result, textChunks[i])
	}
	if len(parents) == 0 {
		link(textChunks)
	}
	return result, nil
}
