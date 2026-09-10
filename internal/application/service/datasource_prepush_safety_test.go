package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/models/vlm"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type prepushKnowledgeRepo struct {
	*wikiEnqueueFailureKnowledgeRepo
	events []string
}

func (r *prepushKnowledgeRepo) MarkDataSourceSubtaskFailed(_ context.Context, _, source string) error {
	r.events = append(r.events, "failure")
	m := r.knowledge.GetMetadata()
	m["datasource_processing_failed"] = source
	r.knowledge.Metadata, _ = json.Marshal(m)
	return nil
}
func (r *prepushKnowledgeRepo) FinalizeSubtask(context.Context, string) (int, bool, error) {
	r.events = append(r.events, "finalize")
	r.expectedSubtasks--
	if r.expectedSubtasks == 0 {
		r.knowledge.ParseStatus = types.ParseStatusCompleted
	}
	return r.expectedSubtasks, r.expectedSubtasks == 0, nil
}
func (r *prepushKnowledgeRepo) UpdateKnowledgeColumns(_ context.Context, _ string, values map[string]interface{}) error {
	if status, ok := values["parse_status"].(string); ok {
		r.knowledge.ParseStatus = status
	}
	return nil
}
func newPrepushRepo() *prepushKnowledgeRepo {
	return &prepushKnowledgeRepo{wikiEnqueueFailureKnowledgeRepo: &wikiEnqueueFailureKnowledgeRepo{knowledge: &types.Knowledge{
		ID: "candidate", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing,
		Metadata: types.JSON(`{"datasource_version":"v"}`),
	}}}
}

type prepushQueue struct {
	orphanTaskEnqueuer
	failType string
}

func (q *prepushQueue) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	if task.Type() == q.failType {
		return nil, errors.New("queue unavailable")
	}
	return q.orphanTaskEnqueuer.Enqueue(task, opts...)
}

func TestPrepushSourceEnqueueFailuresAreNotSuccessfulCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, failType, neo4j string
		graph, failed         bool
	}{
		{"summary failure", types.TypeSummaryGeneration, "false", false, true},
		{"graph failure", types.TypeChunkExtract, "true", true, true},
		{"disabled graph", "", "false", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NEO4J_ENABLE", tc.neo4j)
			r := newPrepushRepo()
			kb := &types.KnowledgeBase{ID: "kb"}
			kb.IndexingStrategy.GraphEnabled = tc.graph
			kb.ExtractConfig = &types.ExtractConfig{Enabled: tc.graph}
			s := &KnowledgePostProcessService{knowledgeRepo: r,
				kbService:    &orphanKBService{kb: kb},
				chunkService: &wikiEnqueueFailureChunkService{chunks: []*types.Chunk{{ID: "chunk", ChunkType: types.ChunkTypeText}}},
				taskEnqueuer: &prepushQueue{failType: tc.failType}}
			body, _ := json.Marshal(types.KnowledgePostProcessPayload{KnowledgeID: "candidate", KnowledgeBaseID: "kb", TenantID: 1})
			require.NoError(t, s.Handle(context.Background(), asynq.NewTask(types.TypeKnowledgePostProcess, body)))
			require.Equal(t, tc.failed, r.knowledge.GetMetadata()["datasource_processing_failed"] != "")
			if tc.failed {
				require.Equal(t, []string{"failure", "finalize"}, r.events)
			}
		})
	}
}

func TestPrepushSourceFailureBlocksReadinessInEveryState(t *testing.T) {
	for _, status := range []string{types.ParseStatusProcessing, types.ParseStatusFinalizing, types.ParseStatusCompleted} {
		s, k, ds, item := candidateFixture()
		k.status, k.ready, k.failure = status, true, "multimodal"
		_, err := s.ingestItem(context.Background(), ds, item, nil)
		require.Error(t, err)
		require.NotNil(t, k.r.rows["old"])
		require.Empty(t, k.r.events)
	}
}

func (*candidateRepo) DataSourceProcessingAttempt(context.Context, string) (int, error) {
	return 7, nil
}

type prepushAttemptQueue struct {
	*candidatePostQueue
	attempt int
}

func (q *prepushAttemptQueue) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	var p types.KnowledgePostProcessPayload
	if err := json.Unmarshal(task.Payload(), &p); err != nil {
		return nil, err
	}
	q.attempt = p.Attempt
	return q.candidatePostQueue.Enqueue(task, opts...)
}
func TestPrepushCandidatePostProcessIsBoundToParseAttempt(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.status, k.ready = types.ParseStatusProcessing, true
	q := &prepushAttemptQueue{candidatePostQueue: &candidatePostQueue{r: k.r}}
	s.taskEnqueuer = q
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	require.Equal(t, 7, q.attempt)
}

func TestPrepushSourceImageFanInCountsEachImageOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	r, q := newPrepushRepo(), &prepushQueue{}
	s := &ImageMultimodalService{knowledgeRepo: r, redisClient: rdb, taskEnqueuer: q}
	p := types.ImageMultimodalPayload{KnowledgeID: "candidate", Attempt: 7, ImageIndex: 0}
	ctx := context.Background()
	// A previous attempt's count cannot satisfy this attempt's barrier.
	mr.Set("multimodal:pending:candidate:6", "1")
	require.Error(t, s.checkAndFinalizeAllImages(ctx, p))
	require.Empty(t, q.enqueued)
	mr.Set("multimodal:pending:candidate:7", "2")
	require.NoError(t, s.checkAndFinalizeAllImages(ctx, p))
	require.NoError(t, s.checkAndFinalizeAllImages(ctx, p))
	require.Empty(t, q.enqueued, "a redelivery must not complete a sibling's slot")
	p.ImageIndex = 1
	require.NoError(t, s.checkAndFinalizeAllImages(ctx, p))
	require.Len(t, q.enqueued, 1)
	var post types.KnowledgePostProcessPayload
	require.NoError(t, json.Unmarshal(q.enqueued[0].Payload(), &post))
	require.Equal(t, 7, post.Attempt)
	mr.SetError("redis unavailable")
	require.Error(t, s.checkAndFinalizeAllImages(ctx, p))
	require.Len(t, q.enqueued, 1, "counter error must not fallback-publish")
}

type prepushVLM struct{ vlm.VLM }

func (prepushVLM) Predict(context.Context, [][]byte, string) (string, error) {
	return "", errors.New("VLM unavailable")
}

type prepushModels struct{ interfaces.ModelService }

func (prepushModels) GetVLMModel(context.Context, string) (vlm.VLM, error) { return prepushVLM{}, nil }

func TestPrepushSourceImageFailureIsRecordedBeforeFanIn(t *testing.T) {
	for _, unreadable := range []bool{false, true} {
		t.Run(map[bool]string{false: "OCR and caption failure", true: "unreadable image"}[unreadable], func(t *testing.T) {
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })
			r, q := newPrepushRepo(), &prepushQueue{}
			r.knowledge.Metadata = types.JSON(`{"datasource_version":"v","datasource_candidate":"true"}`)
			mr.Set("multimodal:pending:candidate:7", "1")
			path := filepath.Join(t.TempDir(), "image.png")
			require.NoError(t, os.WriteFile(path, []byte("synthetic image bytes"), 0600))
			if unreadable {
				path += ".missing"
			}
			s := &ImageMultimodalService{knowledgeRepo: r, redisClient: rdb, taskEnqueuer: q,
				kbService: &orphanKBService{kb: &types.KnowledgeBase{ID: "kb", VLMConfig: types.VLMConfig{Enabled: true, ModelID: "model"}}}, modelService: prepushModels{}}
			body, _ := json.Marshal(types.ImageMultimodalPayload{KnowledgeID: "candidate", KnowledgeBaseID: "kb", TenantID: 1,
				Attempt: 7, ImageLocalPath: path, ImageURL: "invalid://synthetic", EnableOCR: true, EnableCaption: true})
			require.NoError(t, s.Handle(context.Background(), asynq.NewTask(types.TypeImageMultimodal, body)))
			require.Equal(t, "multimodal", r.knowledge.GetMetadata()["datasource_processing_failed"])
			require.Len(t, q.enqueued, 1)
			post := &KnowledgePostProcessService{knowledgeRepo: r}
			require.NoError(t, post.Handle(context.Background(), q.enqueued[0]))
			require.Equal(t, types.ParseStatusFailed, r.knowledge.ParseStatus)
			require.True(t, r.knowledge.IsDataSourceCandidate())
		})
	}
}

func TestPrepushSourceImageCounterInitializationFailureStopsFanOut(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	r, q := newPrepushRepo(), &prepushQueue{}
	mr.Set("multimodal:pending:candidate", "1")
	mr.SetError("redis unavailable")
	s := &knowledgeService{repo: r, redisClient: rdb, task: q}
	s.enqueueImageMultimodalTasks(withAttempt(context.Background(), 8), r.knowledge, &types.KnowledgeBase{ID: "kb"},
		[]docparser.StoredImage{{ServingURL: "invalid://one"}, {ServingURL: "invalid://two"}}, nil, nil)
	require.Empty(t, q.enqueued)
	require.Equal(t, types.ParseStatusFailed, r.knowledge.ParseStatus)
}

func TestPrepushMultimodalIndexErrorIsNotSuccess(t *testing.T) {
	s := &ImageMultimodalService{kbService: &orphanKBService{err: errors.New("database unavailable")}}
	require.Error(t, s.indexChunks(context.Background(), types.ImageMultimodalPayload{KnowledgeBaseID: "kb"}, []*types.Chunk{{ID: "image-caption"}}))
}

type prepushSourceRepo struct {
	interfaces.DataSourceRepository
	row *types.DataSource
}

func (r *prepushSourceRepo) FindByID(context.Context, string) (*types.DataSource, error) {
	return r.row, nil
}

func TestPrepushFAQStopsBeforeImportAfterSourceChanges(t *testing.T) {
	ds := &types.DataSource{ID: "ds", TenantID: 1, KnowledgeBaseID: "kb", Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive}
	changed := *ds
	changed.Status = types.DataSourceStatusPaused
	k := &faqSourceKnowledge{}
	s := &DataSourceService{dsRepo: &prepushSourceRepo{row: &changed}, knowledgeService: k}
	h := &streamSyncHandler{svc: s, ds: ds, faq: true, result: &types.SyncResult{}}
	err := h.Emit(context.Background(), types.FetchedItem{ExternalID: "file", FileName: "faq.csv", Content: []byte("question,answers\nQ,A\n")})
	require.ErrorIs(t, err, datasource.ErrDataSourceNotActive)
	require.Zero(t, k.calls)
	require.Empty(t, h.result.FAQCompleted)
}

func TestPrepushFAQScopeChangeDuringIndexingKeepsOldAnswer(t *testing.T) {
	old := &types.FAQChunkMetadata{StandardQuestion: "Q", Answers: []string{"old answer"}}
	row := &types.Chunk{ID: "faq", TenantID: 1, KnowledgeID: "knowledge", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeFAQ, IsEnabled: true}
	require.NoError(t, row.SetFAQMetadata(old))
	r := &faqPublishChunks{row: row}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(&faqPublishEngine{}))
	s := &knowledgeService{chunkRepo: r, repo: &sourcePathRepo{}, retrieveEngine: registry}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 1, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}}}})
	checks := 0
	ctx = context.WithValue(ctx, faqSourceGuardKey{}, func(context.Context) error {
		checks++
		if checks > 1 {
			return datasource.ErrInvalidConfig
		}
		return nil
	})
	ops := []faqMergeOperation{{ExistingChunk: row, MergedMeta: &types.FAQChunkMetadata{StandardQuestion: "Q", Answers: []string{"new answer"}}}}
	_, err := s.executeFAQMergeOperations(ctx, "test", &types.KnowledgeBase{ID: "kb", TenantID: 1, Type: types.KnowledgeBaseTypeFAQ, IndexingStrategy: types.IndexingStrategy{VectorEnabled: true}}, &types.Knowledge{ID: "knowledge"}, &processingPipelineEmbedder{}, types.FAQIndexModeQuestionAnswer, ops, &types.FAQImportProgress{})
	require.ErrorIs(t, err, datasource.ErrInvalidConfig)
	require.Zero(t, r.saves)
	meta, err := r.row.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{"old answer"}, meta.Answers)
}
