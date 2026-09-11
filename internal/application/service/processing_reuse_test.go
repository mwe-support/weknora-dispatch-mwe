package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingRebuildReusesOnlyUnchangedStageInputs(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}, &types.Model{}, &types.SyncLog{}, &types.SyncRunItem{}))
	require.NoError(t, db.Create(&types.Tenant{ID: 1, Name: "synthetic", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}, {RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.KeywordsRetrieverType}}}}).Error)
	for id, kind := range map[string]types.ModelType{"embed": types.ModelTypeEmbedding, "summary": types.ModelTypeKnowledgeQA, "vision": types.ModelTypeVLLM} {
		require.NoError(t, db.Create(&types.Model{ID: id, TenantID: 1, Name: id, Type: kind, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
	}
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&kb).Error)
	kb.EnsureDefaults()
	kb.EmbeddingModelID, kb.SummaryModelID = "embed", "summary"
	kb.VLMConfig = types.VLMConfig{Enabled: true, ModelID: "vision"}
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 1}
	kb.IndexingStrategy.GraphEnabled = true
	kb.ExtractConfig = &types.ExtractConfig{Enabled: true}
	require.NoError(t, db.Save(&kb).Error)
	r := repository.NewProcessingRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	models := processingPipelineModels{embed: &processingPipelineEmbedder{}, summary: &processingPipelineChat{calls: 1, questionCalls: 1}, vision: &processingPipelineVision{ocr: 1}}
	index := &processingPipelineIndex{vectors: true, calls: 1}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
	store := files.NewLocalFileService(t.TempDir(), "")
	s := &knowledgeService{config: &config.Config{Conversation: &config.ConversationConfig{GenerateSummaryPrompt: "Summarize the document.", GenerateQuestionsPrompt: "QUESTIONS-TEST {{content}} {{context}}"}, ExtractManager: &config.ExtractManagerConfig{ExtractGraph: &types.PromptTemplateStructured{Description: "GRAPH-TEST"}}},
		kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}, tenantRepo: repository.NewTenantRepository(db), repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db), fileSvc: store, modelService: models, retrieveEngine: registry}
	s.graphEngine = NewProcessingGraphRepository(&processingTestGraph{writes: map[string]types.NameSpace{}}, r, true)
	execute, err := NewKnowledgeProcessingExecutor(s, r, repository.NewDataSourceRepository(db))
	require.NoError(t, err)
	document := ProcessingDocumentSpec{FileID: "file", Kind: "smartcanvas", Title: "synthetic"}
	metadata, _ := json.Marshal(document)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "fixed", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: metadata})
	require.NoError(t, err)
	e := processingDocumentExecution{s: s, repo: r, kb: &kb, lease: types.ProcessingLease{Job: *job}}
	plan, err := e.documentPlan(ctx, "smartcanvas")
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
	nativeCalls, downloadCalls := 0, 0
	drive := func(id string) *types.ProcessingJob {
		t.Helper()
		for n := 0; n < 70; n++ {
			current, err := r.GetJob(ctx, 1, id)
			require.NoError(t, err)
			if current.Status == types.ProcessingSucceeded {
				return current
			}
			require.NoError(t, db.Model(&types.ProcessingStep{}).Where("job_id = ? AND next_run_at IS NOT NULL", id).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, id))
			steps, err := r.ListSteps(ctx, 1, id)
			require.NoError(t, err)
			for _, step := range steps {
				if step.Status != types.ProcessingEnqueuePending {
					continue
				}
				lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: id, Generation: current.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
				require.NoError(t, err)
				var out types.ProcessingOutcome
				if lease.Job.Generation == 1 && step.Stage == "native_read" {
					nativeCalls++
					page, _ := json.Marshal(map[string]string{"content": `<Paragraph id="a">Synthetic complete source. Alice maintains Atlas. REUSE-END-7391.</Paragraph><Image id="img" src="https://docs.qq.com/synthetic/image"/>`})
					body, _ := json.Marshal(tencentdocs.NativeSnapshot{Kind: "smartcanvas", CoverageComplete: true, Pages: []tencentdocs.NativePage{{Tool: "smartcanvas.read", Data: page}}})
					out.OutputManifestRef, out.OutputDigest, err = NewProcessingArtifacts(store, nil).Save(ctx, lease.Job, lease.Step, "native_snapshot", body)
					out.Status = types.ProcessingSucceeded
				} else if lease.Job.Generation == 1 && step.Stage == "asset_download" {
					downloadCalls++
					steps, err := r.ListSteps(ctx, 1, id)
					require.NoError(t, err)
					e := processingDocumentExecution{s: s, repo: r, kb: &kb, lease: *lease, steps: steps, artifacts: NewProcessingArtifacts(store, nil)}
					out, err = e.downloadAsset(ctx, func(context.Context, processingAsset) ([]byte, error) { return processingTestImage(t, 1), nil })
				} else {
					out, err = execute(ctx, *lease)
				}
				require.NoError(t, err, "stage %s generation %d", step.Stage, current.Generation)
				require.NotEqual(t, types.ProcessingBlocked, out.Status, "stage %s: %s", step.Stage, out.ErrorCode)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
			}
		}
		t.Fatal("rebuild did not finish")
		return nil
	}
	job = drive(job.ID)
	for _, change := range []string{"summary", "embed"} {
		t.Run(change, func(t *testing.T) {
			before := *job
			priorEmbed, priorSummary, priorOCR, priorCaption := models.embed.calls, models.summary.calls, models.vision.ocr, models.vision.caption
			priorQuestion, priorGraph := models.summary.questionCalls, models.summary.graphCalls
			priorIndex := len(index.writes)
			require.NoError(t, db.Model(job).Update("rollback_pin", true).Error)
			require.NoError(t, db.Model(&types.Model{}).Where("id = ?", change).Update("name", change+"-updated").Error)
			job, err = RebuildProcessingVersion(ctx, s, r, 1, job.ID, types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: "change-" + change, Actor: "operator", Reason: "synthetic selective invalidation"})
			require.NoError(t, err)
			job = drive(job.ID)
			require.True(t, job.IsPublished)
			require.Greater(t, len(index.writes), priorIndex, "new version writes use new identities")
			require.Equal(t, 1, nativeCalls)
			require.Equal(t, 1, downloadCalls)
			require.Equal(t, priorOCR, models.vision.ocr)
			require.Equal(t, priorCaption, models.vision.caption)
			if change == "summary" {
				require.Equal(t, priorSummary+1, models.summary.calls)
				// A changed summary is embedded; source and image vectors are reused.
				require.Equal(t, priorEmbed+2, models.embed.calls)
				require.Greater(t, models.summary.questionCalls, priorQuestion)
				require.Greater(t, models.summary.graphCalls, priorGraph)
			} else {
				require.Equal(t, priorSummary, models.summary.calls)
				require.Equal(t, priorQuestion, models.summary.questionCalls)
				require.Equal(t, priorGraph, models.summary.graphCalls)
				require.Greater(t, models.embed.calls, priorEmbed)
			}
			var events []types.ProcessingEvent
			require.NoError(t, db.Where("job_id = ? AND event_type = ?", job.ID, "artifact_copied").Find(&events).Error)
			require.GreaterOrEqual(t, len(events), 5)
			var holds int64
			require.NoError(t, db.Model(&types.ProcessingArtifactReference{}).Where("consumer_job_id = ?", job.ID).Count(&holds).Error)
			require.Zero(t, holds, "verified copies must not retain an unbounded version chain")
			old, err := r.GetJob(ctx, 1, before.ID)
			require.NoError(t, err)
			require.False(t, old.IsPublished)
		})
	}
	keys := map[string]string{"chunking": "chunk-v1", "model/embed": "embed-v1", "model/summary": "summary-v1", "model/vision": "vision-v1"}
	first := processingStageInputs(&kb, s.config, keys)
	keys["model/summary"] = "summary-v2"
	second := processingStageInputs(&kb, s.config, keys)
	for _, stage := range []string{"native_read", "parse", "assets", "chunk", "text_index", "images", "image_index"} {
		require.Equal(t, first[stage], second[stage], fmt.Sprint(stage))
	}
	for _, stage := range []string{"summary", "questions", "graph", "wiki"} {
		require.NotEqual(t, first[stage], second[stage], stage)
	}
}
