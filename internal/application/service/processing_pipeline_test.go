package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/models/vlm"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

type processingPipelineModels struct {
	interfaces.ModelService
	embed   *processingPipelineEmbedder
	summary *processingPipelineChat
	vision  *processingPipelineVision
}

type processingRollbackFaultFiles struct {
	interfaces.FileService
	fail bool
}

func (s *processingRollbackFaultFiles) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	if s.fail {
		return nil, errors.New("synthetic missing rollback artifact")
	}
	return s.FileService.GetFile(ctx, path)
}

func (m processingPipelineModels) GetVLMModel(context.Context, string) (vlm.VLM, error) {
	return m.vision, nil
}

type processingPipelineVision struct {
	vlm.VLM
	ocr, caption int
}

func (m *processingPipelineVision) Predict(_ context.Context, _ [][]byte, prompt string) (string, error) {
	if strings.Contains(prompt, "OCR assistant") {
		m.ocr++
		if m.ocr == 1 {
			return "", context.DeadlineExceeded
		}
		return "synthetic image text", nil
	}
	m.caption++
	return "synthetic image description", nil
}

func (m processingPipelineModels) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return m.embed, nil
}
func (m processingPipelineModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.summary, nil
}

type processingPipelineEmbedder struct {
	embedding.Embedder
	calls     int
	dimension int
}

func (m *processingPipelineEmbedder) BatchEmbed(_ context.Context, inputs []string) ([][]float32, error) {
	m.calls++
	result := make([][]float32, len(inputs))
	for i := range inputs {
		result[i] = []float32{0.25, 0.75}
		if m.dimension == 3 {
			result[i] = append(result[i], 0.5)
		}
	}
	return result, nil
}
func (m *processingPipelineEmbedder) GetDimensions() int {
	if m.dimension > 0 {
		return m.dimension
	}
	return 2
}
func (m *processingPipelineEmbedder) BatchEmbedWithPool(ctx context.Context, _ embedding.Embedder, inputs []string) ([][]float32, error) {
	return m.BatchEmbed(ctx, inputs)
}

type processingPipelineChat struct {
	summaryContentCaptureChat
	calls         int
	questionCalls int
	graphCalls    int
	wikiCalls     map[string]int
	wikiPageHook  func()
}

func (m *processingPipelineChat) Chat(ctx context.Context, messages []chat.Message, opts *chat.ChatOptions) (*types.ChatResponse, error) {
	if purpose, _ := types.LLMCallMetadataFromContext(ctx); strings.HasPrefix(purpose, "wiki_") {
		return m.wikiResponse(purpose, messages)
	}
	if strings.Contains(messages[0].Content, "GRAPH-TEST") {
		m.graphCalls++
		return &types.ChatResponse{Content: `[{"entity":"Lifecycle"},{"entity":"Receipt"},{"entity1":"Lifecycle","entity2":"Receipt","relation":"records"}]`}, nil
	}
	if strings.Contains(messages[0].Content, "QUESTIONS-TEST") {
		m.questionCalls++
		if m.questionCalls == 1 {
			return nil, context.DeadlineExceeded
		}
		return &types.ChatResponse{Content: "1. QUESTION-END-7391 how does the document work?\n2. What are the document steps?\n3. How is the result verified?"}, nil
	}
	m.calls++
	if m.calls == 1 {
		return nil, context.DeadlineExceeded
	}
	return m.summaryContentCaptureChat.Chat(ctx, messages, opts)
}

type processingPipelineIndex struct {
	interfaces.RetrieveEngineRepository
	calls               int
	writes              []*types.IndexInfo
	mu                  sync.Mutex
	vectors             bool
	questionIndexFailed bool
	deleteCalls         int
	deletedDimensions   []int
	afterWrite          func()
	compensated         []string
}

func (r *processingPipelineIndex) DeleteBySourceIDList(_ context.Context, ids []string, _ int, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.compensated = append(r.compensated, ids...)
	remaining := r.writes[:0]
	for _, item := range r.writes {
		remove := false
		for _, id := range ids {
			if item.SourceID == id {
				remove = true
			}
		}
		if !remove {
			remaining = append(remaining, item)
		}
	}
	r.writes = remaining
	return nil
}

func (r *processingPipelineIndex) DeleteByKnowledgeIDList(_ context.Context, ids []string, dimension int, kind string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteCalls++
	r.deletedDimensions = append(r.deletedDimensions, dimension)
	remaining := r.writes[:0]
	for _, item := range r.writes {
		remove := false
		for _, id := range ids {
			if item.KnowledgeID == id {
				remove = true
			}
		}
		if !remove {
			remaining = append(remaining, item)
		}
	}
	r.writes = remaining
	if r.deleteCalls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

type processingRetirementFaultFiles struct {
	interfaces.FileService
	lostACK bool
	calls   int
}

func (s *processingRetirementFaultFiles) DeleteFile(ctx context.Context, path string) error {
	s.calls++
	err := s.FileService.DeleteFile(ctx, path)
	if err == nil && !s.lostACK {
		s.lostACK = true
		return context.DeadlineExceeded
	}
	return err
}

func (*processingPipelineIndex) EngineType() types.RetrieverEngineType {
	return types.PostgresRetrieverEngineType
}
func (*processingPipelineIndex) EstimateStorageSize(_ context.Context, items []*types.IndexInfo, _ map[string]any) int64 {
	return int64(len(items)) * 64
}
func (*processingPipelineIndex) Support() []types.RetrieverType {
	return []types.RetrieverType{types.VectorRetrieverType, types.KeywordsRetrieverType}
}
func (r *processingPipelineIndex) BatchSave(_ context.Context, items []*types.IndexInfo, params map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	vectors, _ := params["embedding"].(map[string][]float32)
	for _, item := range items {
		if r.vectors && len(vectors[item.SourceID]) != 2 {
			return fmt.Errorf("missing saved vector")
		}
		if !r.vectors && len(vectors) != 0 {
			return fmt.Errorf("unexpected embedding for keyword-only index")
		}
		copy := *item
		r.writes = append(r.writes, &copy)
	}
	if r.afterWrite != nil {
		r.afterWrite()
	}
	if r.calls == 1 {
		return context.DeadlineExceeded
	} // Provider wrote, but its ACK was lost.
	for _, item := range items {
		if strings.Contains(item.Content, "QUESTION-END-7391") && !r.questionIndexFailed {
			r.questionIndexFailed = true
			return context.DeadlineExceeded
		}
	}
	return nil
}

func TestProcessingPipelineRetriesOnlyFailedStageAndPublishesConfirmedVectors(t *testing.T) {
	for _, scenario := range []string{"flat", "parent-child", "multiple-batches", "keyword-only", "no-text-index", "48-images", "images-disabled", "questions", "retirement", "late-index", "graph", "graph-late", "wiki", "wiki-untracked"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
			db := processingServiceTestDatabase(t)
			require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}, &types.Chunk{}, &types.Model{}))
			require.NoError(t, db.Create(&types.Model{ID: "synthetic-vision", TenantID: 1, Name: "synthetic", Type: types.ModelTypeVLLM, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
			require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&types.Tenant{ID: 1, Name: "synthetic", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{
				{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType},
				{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.KeywordsRetrieverType},
			}}}).Error)
			var kb types.KnowledgeBase
			require.NoError(t, db.First(&kb).Error)
			kb.EnsureDefaults()
			kb.ChunkingConfig.EnableParentChild = scenario == "parent-child"
			if scenario == "48-images" {
				kb.VLMConfig = types.VLMConfig{Enabled: true, ModelID: "synthetic-vision"}
			}
			if scenario == "keyword-only" {
				kb.IndexingStrategy = types.IndexingStrategy{KeywordEnabled: true}
			}
			if scenario == "no-text-index" {
				kb.IndexingStrategy = types.IndexingStrategy{WikiEnabled: true}
			}
			if strings.HasPrefix(scenario, "graph") {
				kb.IndexingStrategy.GraphEnabled = true
				kb.ExtractConfig = &types.ExtractConfig{Enabled: true}
				kb.SummaryModelID = "synthetic-summary"
				require.NoError(t, db.Create(&types.Model{ID: kb.SummaryModelID, TenantID: 1, Name: "synthetic", Type: types.ModelTypeKnowledgeQA, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
			}
			if scenario == "questions" {
				kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 3}
				kb.SummaryModelID = "synthetic-summary"
				require.NoError(t, db.Create(&types.Model{ID: kb.SummaryModelID, TenantID: 1, Name: "synthetic", Type: types.ModelTypeKnowledgeQA, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
			}
			if strings.HasPrefix(scenario, "wiki") {
				kb.IndexingStrategy.WikiEnabled = true
				kb.EmbeddingModelID = "synthetic-embedding"
				require.NoError(t, db.Create(&types.Model{ID: kb.EmbeddingModelID, TenantID: 1, Name: "synthetic", Type: types.ModelTypeEmbedding, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
				kb.SummaryModelID = "synthetic-summary"
				require.NoError(t, db.Create(&types.Model{ID: kb.SummaryModelID, TenantID: 1, Name: "synthetic", Type: types.ModelTypeKnowledgeQA, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
			}
			require.NoError(t, db.Save(&kb).Error)
			r := repository.NewProcessingRepository(db)
			index := &processingPipelineIndex{vectors: kb.IsVectorEnabled()}
			registry := retriever.NewRetrieveEngineRegistry(nil, nil)
			require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
			models := processingPipelineModels{embed: &processingPipelineEmbedder{}, summary: &processingPipelineChat{}, vision: &processingPipelineVision{}}
			fs := files.NewLocalFileService(t.TempDir(), "")
			var catalog interfaces.ResourceCatalog
			if scenario == "retirement" {
				catalog = NewResourceCatalog(repository.NewResourceRepository(db))
				fs = files.NewResourceCatalogFileService(fs, catalog)
			}
			s := &knowledgeService{config: &config.Config{Conversation: &config.ConversationConfig{GenerateSummaryPrompt: "Summarize the document.", GenerateQuestionsPrompt: "QUESTIONS-TEST {{content}} {{context}}"}},
				kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}, tenantRepo: repository.NewTenantRepository(db),
				repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db), fileSvc: fs, resourceCatalog: catalog, modelService: models, retrieveEngine: registry}
			var graph *processingTestGraph
			if strings.HasPrefix(scenario, "wiki") {
				setupProcessingWiki(t, db, s, models.summary, scenario == "wiki-untracked")
			}
			if strings.HasPrefix(scenario, "graph") {
				s.config.ExtractManager = &config.ExtractManagerConfig{ExtractGraph: &types.PromptTemplateStructured{Description: "GRAPH-TEST"}}
				graph = &processingTestGraph{writes: map[string]types.NameSpace{}}
				s.graphEngine = NewProcessingGraphRepository(graph, r, true)
				graph.afterWrite = func(ctx context.Context, ns types.NameSpace) {
					visible, _, err := r.VisibleGraph(ctx, ns, s.graphEngine.(*ProcessingGraphRepository).destination)
					require.NoError(t, err)
					require.NotContains(t, visible, ns.ProcessingContribution, "external writes are invisible before the ledger commit")
					if scenario == "graph-late" {
						require.NoError(t, db.AutoMigrate(&types.SyncRunItem{}, &types.SyncLog{}))
						processingControlCandidate(t, db, "file", true, "replacement")
					}
				}
			}
			execute, err := NewKnowledgeProcessingExecutor(s, r, repository.NewDataSourceRepository(db))
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			metadata, _ := json.Marshal(ProcessingDocumentSpec{FileID: "file", Kind: "smartcanvas", Title: "synthetic", FolderPath: "acceptance/nested"})
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "fixed", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: metadata})
			require.NoError(t, err)
			if scenario == "late-index" {
				index.afterWrite = func() {
					_, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "replacement", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: metadata})
					require.NoError(t, err)
				}
			}
			stages := []string{"native_read", "normalize", "parse", "assets", "chunk", "text_index", "images", "image_index", "publish", "summary", "summary_index"}
			if scenario == "questions" {
				stages = append(stages, "questions", "question_index")
			}
			if strings.HasPrefix(scenario, "graph") {
				stages = append(stages, "graph")
			}
			if strings.HasPrefix(scenario, "wiki") {
				stages = append(stages, "wiki")
			}
			var plan []types.ProcessingStepSpec
			for i, stage := range stages {
				spec := types.ProcessingStepSpec{Stage: stage, UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: stage, RequiredForReady: i < 8, RequiredForCompletion: true}
				if i > 0 {
					spec.DependsOn = []string{stages[i-1] + "/body"}
				}
				if stage == "text_index" || stage == "summary_index" || stage == "assets" || stage == "images" || stage == "image_index" || stage == "questions" || stage == "question_index" || stage == "graph" || stage == "wiki" {
					spec.Kind = "barrier"
				}
				if stage == "publish" {
					spec.Phase = types.ProcessingPhasePublish
				}
				if i > 8 {
					spec.Phase = types.ProcessingPhaseProjection
				}
				plan = append(plan, spec)
			}
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
			calls := map[string]int{}
			questionUnits := map[string]int{}
			imageDownloads := map[string]int{}
			wikiLinksChanged := false
			for n := 0; n < 60; n++ {
				job, err = r.GetJob(ctx, 1, job.ID)
				require.NoError(t, err)
				if job.Status == types.ProcessingSucceeded {
					break
				}
				// Advance only due timers, not step state or retry identity.
				require.NoError(t, db.Model(&types.ProcessingStep{}).Where("next_run_at IS NOT NULL").Update("next_run_at", time.Now().Add(-time.Minute)).Error)
				require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
				ops, err := r.PendingDeliveries(ctx, 100)
				require.NoError(t, err)
				require.NotEmpty(t, ops, "pipeline stalled: %+v", job)
				for _, op := range ops {
					var ref types.ProcessingRef
					require.NoError(t, json.Unmarshal(op.Payload, &ref))
					lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
					require.NoError(t, err)
					calls[lease.Step.Stage]++
					if lease.Step.Stage == "question" {
						questionUnits[lease.Step.UnitKey]++
					}
					var outcome types.ProcessingOutcome
					if lease.Step.Stage == "native_read" {
						repeats := 100
						if scenario == "multiple-batches" {
							repeats = 12000
						}
						content := `<Paragraph id="a">` + strings.Repeat("完整正文合成验收文字。", repeats) + `</Paragraph><Paragraph id="tail">NATIVE-PIPELINE-END-7391</Paragraph>`
						if scenario == "48-images" || scenario == "images-disabled" {
							for i := 0; i < 48; i++ {
								content += fmt.Sprintf(`<Image id="img-%02d" src="https://docs.qq.com/synthetic/%d?signature=private" alt="synthetic"/>`, i, i)
							}
						}
						page, _ := json.Marshal(map[string]string{"content": content})
						payload, _ := json.Marshal(tencentdocs.NativeSnapshot{Kind: "smartcanvas", CoverageComplete: true, Pages: []tencentdocs.NativePage{{Tool: "smartcanvas.read", Data: page}}})
						path, digest, err := NewProcessingArtifacts(fs, nil).Save(ctx, lease.Job, lease.Step, "native_snapshot", payload)
						require.NoError(t, err)
						outcome = types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: path, OutputDigest: digest}
					} else if lease.Step.Stage == "asset_download" {
						steps, err := r.ListSteps(ctx, 1, job.ID)
						require.NoError(t, err)
						e := processingDocumentExecution{s: s, repo: r, kb: &kb, lease: *lease, steps: steps, artifacts: NewProcessingArtifacts(fs, nil)}
						outcome, err = e.downloadAsset(ctx, func(_ context.Context, asset processingAsset) ([]byte, error) {
							imageDownloads[asset.ID]++
							if asset.ID == "img-31" && imageDownloads[asset.ID] == 1 {
								return nil, context.DeadlineExceeded
							}
							var i int
							_, err := fmt.Sscanf(asset.ID, "img-%02d", &i)
							require.NoError(t, err)
							return processingTestImage(t, i), nil
						})
						if err != nil {
							outcome = tencentdocs.ProcessingFailure("asset_download", err)
						}
					} else {
						outcome, err = execute(ctx, *lease)
						if scenario == "graph-late" && lease.Step.Stage == "graph_apply" {
							require.ErrorIs(t, err, repository.ErrProcessingConflict)
							require.Empty(t, graph.writes, "the replaced worker must compensate its exact contribution")
							require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, outcome), repository.ErrProcessingConflict)
							return
						}
						if scenario == "late-index" && lease.Step.Stage == "index" {
							require.ErrorIs(t, err, repository.ErrProcessingConflict)
							require.Empty(t, index.writes, "a superseded worker must compensate its uncommitted external write")
							require.NotEmpty(t, index.compensated)
							require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "late", OutputDigest: "late"}), repository.ErrProcessingConflict)
							stopped, err := r.GetJob(ctx, 1, job.ID)
							require.NoError(t, err)
							require.False(t, stopped.IsPublished)
							return
						}
						require.NoError(t, err)
						if lease.Step.Stage == "wiki_links" && outcome.WikiPage != nil && outcome.WikiPage.Page.PageType == types.WikiPageTypeSummary && !wikiLinksChanged {
							wikiLinksChanged = true
							page, err := s.wikiRepo.GetByID(ctx, outcome.WikiPage.Page.ID)
							require.NoError(t, err)
							page.Content += " CONCURRENT-LINKS-7391"
							require.NoError(t, s.wikiService.UpdateAutoLinkedContent(ctx, page))
						}
						if lease.Step.Stage == "wiki_taxonomy_plan" && outcome.ErrorCode == "WIKI_TAXONOMY_INVALID" {
							require.Equal(t, types.ProcessingBlocked, outcome.Status)
							require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
							current, err := r.GetJob(ctx, 1, job.ID)
							require.NoError(t, err)
							require.True(t, current.IsPublished, "invalid postprocessing must not hide readable text")
							require.NoError(t, r.RetryStep(ctx, 1, job.ID, lease.Step.ID, current.Revision, "wiki-taxonomy-retry", "operator"))
							continue
						}
						require.NotEqual(t, types.ProcessingBlocked, outcome.Status, "%s: %+v", lease.Step.Stage, outcome)
					}
					require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
					require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome)) // Duplicate commit/ACK.
				}
			}
			require.Equal(t, types.ProcessingSucceeded, job.Status)
			require.NotNil(t, job.FinishedAt)
			require.True(t, job.IsPublished)
			if scenario == "graph" {
				verifyProcessingGraphLifecycle(t, ctx, db, r, s, execute, job, graph, models.summary)
				return
			}
			if strings.HasPrefix(scenario, "wiki") {
				verifyProcessingWikiLifecycle(t, ctx, db, r, s, execute, job, models.summary, scenario == "wiki-untracked")
				return
			}
			for _, stage := range []string{"native_read", "normalize", "parse", "chunk"} {
				require.Equal(t, 1, calls[stage], stage)
			}
			if kb.IsVectorEnabled() {
				require.Equal(t, calls["embedding"], models.embed.calls, "each batch embedded once")
				if scenario == "multiple-batches" || scenario == "48-images" || scenario == "questions" {
					require.Greater(t, models.embed.calls, 2)
				} else {
					require.Equal(t, 2, models.embed.calls)
				}
			} else {
				require.Zero(t, models.embed.calls)
			}
			require.Equal(t, 2, models.summary.calls, "only summary retries")
			if scenario == "48-images" {
				require.Equal(t, 49, models.vision.ocr)
				require.Equal(t, 48, models.vision.caption)
			} else {
				require.Zero(t, models.vision.ocr)
				require.Zero(t, models.vision.caption)
			}
			if scenario == "48-images" || scenario == "images-disabled" {
				require.Len(t, imageDownloads, 48)
				for id, n := range imageDownloads {
					if id == "img-31" {
						require.Equal(t, 2, n)
					} else {
						require.Equal(t, 1, n)
					}
				}
			}
			if scenario == "no-text-index" {
				require.Zero(t, index.calls)
			} else {
				expectedRetries := 1
				if scenario == "questions" {
					expectedRetries++
				}
				require.Equal(t, calls["embedding"]+expectedRetries, calls["index"], "only the failed index batches retry")
			}
			knowledge, err := s.repo.GetKnowledgeByID(ctx, 1, job.KnowledgeID)
			require.NoError(t, err)
			require.Equal(t, "summary", knowledge.Description)
			require.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
			var chunks []*types.Chunk
			require.NoError(t, db.Where("knowledge_id = ?", job.KnowledgeID).Find(&chunks).Error)
			parentCount := 0
			imageCount := 0
			textCount := 0
			for _, chunk := range chunks {
				if scenario == "questions" && chunk.ChunkType == types.ChunkTypeText {
					textCount++
					metadata, err := chunk.DocumentMetadata()
					require.NoError(t, err)
					require.Len(t, metadata.GeneratedQuestions, 3)
					require.Contains(t, string(chunk.Metadata), "processing_job_id", "question metadata must preserve the ledger stamp")
				}
				require.NotContains(t, chunk.Content, "signature=private")
				if chunk.ChunkType == types.ChunkTypeImageOCR || chunk.ChunkType == types.ChunkTypeImageCaption {
					imageCount++
					require.NotEmpty(t, chunk.ParentChunkID)
					require.Contains(t, chunk.ImageInfo, "local://")
				}
				if chunk.ChunkType == types.ChunkTypeParentText {
					parentCount++
				}
				if chunk.ChunkType != types.ChunkTypeParentText && scenario != "no-text-index" {
					require.Equal(t, "indexed", chunk.IndexStatus)
				}
			}
			if scenario == "parent-child" {
				require.Positive(t, parentCount)
			}
			if scenario == "48-images" {
				require.Equal(t, 96, imageCount)
			} else {
				require.Zero(t, imageCount)
			}
			var hits []*types.IndexWithScore
			for _, item := range index.writes {
				if kb.IsVectorEnabled() {
					require.Equal(t, []float32{0.25, 0.75}, item.PreparedEmbedding)
				} else {
					require.Nil(t, item.PreparedEmbedding)
				}
				hits = append(hits, &types.IndexWithScore{KnowledgeID: item.KnowledgeID, KnowledgeBaseID: item.KnowledgeBaseID, SourceID: item.SourceID, ChunkID: item.ChunkID})
			}
			accepted, err := r.FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: "kb"}}, hits)
			require.NoError(t, err)
			if scenario == "questions" {
				require.Len(t, questionUnits, textCount)
				retried := 0
				for _, calls := range questionUnits {
					if calls == 2 {
						retried++
					} else {
						require.Equal(t, 1, calls)
					}
				}
				require.Equal(t, 1, retried)
				require.Equal(t, textCount+1, models.summary.questionCalls, "index retries must reuse generated questions")
				parents := map[string]bool{}
				for _, chunk := range chunks {
					if chunk.ChunkType == types.ChunkTypeText {
						parents[chunk.ID] = true
					}
				}
				questionHits := 0
				for _, hit := range accepted {
					for _, write := range index.writes {
						if write.SourceID == hit.SourceID && strings.Contains(write.Content, "QUESTION-END-7391") {
							require.True(t, parents[hit.ChunkID], "question search must return the original answer chunk")
							questionHits++
						}
					}
				}
				require.Positive(t, questionHits)
			}
			if scenario != "no-text-index" {
				require.Less(t, len(accepted), len(hits), "lost-ACK orphan index writes must be invisible")
				require.NotEmpty(t, accepted)
			}
			if scenario == "flat" || scenario == "48-images" || scenario == "questions" {
				require.NoError(t, db.AutoMigrate(&types.SyncRunItem{}, &types.SyncLog{}))
				replacement := processingControlCandidate(t, db, "file", true, "replacement")
				job, _ = r.GetJob(ctx, 1, job.ID)
				beforeEmbeddings, beforeQuestions, beforeSummary := models.embed.calls, models.summary.questionCalls, models.summary.calls
				beforeWrites := len(index.writes)
				if scenario == "flat" {
					fault := &processingRollbackFaultFiles{FileService: fs, fail: true}
					s.fileSvc = fault
					require.Error(t, RollbackProcessingVersion(ctx, s, r, 1, job.ID, job.Revision, "failed-verification", "operator"))
					still, err := r.GetJob(ctx, 1, replacement.Job.ID)
					require.NoError(t, err)
					require.True(t, still.IsPublished)
					require.Equal(t, beforeWrites, len(index.writes), "verification failure must precede index I/O")
					fault.fail = false
					job, _ = r.GetJob(ctx, 1, job.ID)
					require.True(t, job.RollbackPin)
				}
				require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, job.ID, job.Revision, "verified-rollback", "operator"))
				job, _ = r.GetJob(ctx, 1, job.ID)
				require.True(t, job.IsPublished)
				require.EqualValues(t, 3, job.PublicationEpoch)
				old, _ := r.GetJob(ctx, 1, replacement.Job.ID)
				require.False(t, old.IsPublished)
				require.Equal(t, beforeEmbeddings, models.embed.calls)
				require.Equal(t, beforeQuestions, models.summary.questionCalls)
				require.Equal(t, beforeSummary, models.summary.calls)
				require.Greater(t, len(index.writes), beforeWrites, "rollback restores confirmed indexes from saved vectors")
				var restored []*types.IndexWithScore
				for _, item := range index.writes[beforeWrites:] {
					restored = append(restored, &types.IndexWithScore{KnowledgeID: item.KnowledgeID, KnowledgeBaseID: item.KnowledgeBaseID, SourceID: item.SourceID, ChunkID: item.ChunkID})
				}
				visible, err := r.FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: "kb"}}, restored)
				require.NoError(t, err)
				require.Len(t, visible, len(restored), "only confirmed attempts are restored")
				if scenario == "flat" {
					var recovered int64
					require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("job_id = ? AND resolution_type = ?", job.ID, "restored_artifact").Count(&recovered).Error)
					require.EqualValues(t, 1, recovered)
				}
			}
			if scenario == "retirement" {
				require.NotEmpty(t, job.IndexDestination)
				var destination types.ProcessingIndexDestination
				require.NoError(t, json.Unmarshal(job.IndexDestination, &destination))
				require.Equal(t, 2, destination.Dimension)
				require.NoError(t, db.AutoMigrate(&types.SyncRunItem{}, &types.SyncLog{}))
				replacement := processingControlCandidate(t, db, "file", true, "replacement")
				job, _ = r.GetJob(ctx, 1, job.ID)
				require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "retire-physical", "operator"))
				var resources []types.StoredResource
				require.NoError(t, db.Where("creation_job_id = ?", job.ID).Find(&resources).Error)
				require.NotEmpty(t, resources)
				fault := &processingRetirementFaultFiles{FileService: fs}
				s.fileSvc = fault
				beforeEmbed, beforeSummary := models.embed.calls, models.summary.calls
				// Cleanup still targets the saved destination after the tenant's
				// defaults and application pipeline change and the KB is deleted.
				require.NoError(t, db.Model(&types.Tenant{}).Where("id = ?", 1).Update("retriever_engines", types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: "removed-default", RetrieverType: types.VectorRetrieverType}}}).Error)
				s.config.Conversation.GenerateSummaryPrompt = "changed pipeline"
				require.NoError(t, repository.NewKnowledgeBaseRepository(db).DeleteKnowledgeBase(ctx, "kb"))
				for attempt := 0; attempt < 3; attempt++ {
					require.NoError(t, db.Model(&types.ProcessingStep{}).Where("job_id = ? AND stage = ? AND next_run_at IS NOT NULL", job.ID, "retire").Update("next_run_at", time.Now().Add(-time.Minute)).Error)
					require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
					steps, err := r.ListSteps(ctx, 1, job.ID)
					require.NoError(t, err)
					var step types.ProcessingStep
					for _, candidate := range steps {
						if candidate.Stage == "retire" {
							step = candidate
						}
					}
					lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
					require.NoError(t, err)
					outcome, err := execute(ctx, *lease)
					require.NoError(t, err)
					if attempt < 2 {
						require.Equal(t, types.ProcessingFailed, outcome.Status)
					} else {
						require.Equal(t, types.ProcessingSucceeded, outcome.Status)
					}
					require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
					require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
				}
				job, _ = r.GetJob(ctx, 1, job.ID)
				require.Equal(t, "deleted", job.RetirementState)
				require.Equal(t, []int{2, 2}, index.deletedDimensions, "file retry must not redo confirmed index cleanup")
				require.Empty(t, index.writes)
				require.Equal(t, beforeEmbed, models.embed.calls)
				require.Equal(t, beforeSummary, models.summary.calls)
				var count int64
				require.NoError(t, db.Unscoped().Model(&types.Chunk{}).Where("knowledge_id = ?", job.KnowledgeID).Count(&count).Error)
				require.Zero(t, count)
				require.NoError(t, db.Unscoped().Model(&types.StoredResource{}).Where("creation_job_id = ? AND state <> ?", job.ID, types.ResourceStateDeleted).Count(&count).Error)
				require.Zero(t, count)
				for _, resource := range resources {
					_, err := fs.GetFile(ctx, resource.PhysicalPath)
					require.Error(t, err)
				}
				still, err := r.GetJob(ctx, 1, replacement.Job.ID)
				require.NoError(t, err)
				require.False(t, still.IsPublished, "explicit KB deletion revokes all its publications")
				require.False(t, still.IsCurrent)
				require.Equal(t, "retained", still.RetirementState, "replacement cleanup has its own durable task")
			}
		})
	}
}
