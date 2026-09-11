package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	qrepo "github.com/Tencent/WeKnora/internal/application/repository/retriever/qdrant"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	protocol "github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type processingLegacyTransport func(*http.Request) (*http.Response, error)

func (f processingLegacyTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func mockProcessingLegacyMembership(t *testing.T, visible *atomic.Bool) {
	t.Helper()
	server := mcpserver.NewMCPServer("legacy-membership-test", "1.0.0")
	server.AddTool(protocol.Tool{Name: "manage.folder_list"}, func(context.Context, protocol.CallToolRequest) (*protocol.CallToolResult, error) {
		body := `{"list":[{"id":"file","title":"Title"}],"finish":true}`
		if !visible.Load() {
			body = `{"list":[],"finish":true}`
		}
		return protocol.NewToolResultText(body), nil
	})
	streamable := mcpserver.NewStreamableHTTPServer(server, mcpserver.WithStateLess(true))
	httpServer := httptest.NewServer(streamable)
	t.Cleanup(httpServer.Close)
	endpoint, err := url.Parse(httpServer.URL)
	require.NoError(t, err)
	previous := http.DefaultTransport
	http.DefaultTransport = processingLegacyTransport(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "docs.qq.com", r.URL.Host)
		require.Equal(t, "synthetic-membership-token", r.Header.Get("Authorization"))
		forward := r.Clone(r.Context())
		forward.URL.Scheme, forward.URL.Host = endpoint.Scheme, endpoint.Host
		return previous.RoundTrip(forward)
	})
	t.Cleanup(func() { http.DefaultTransport = previous })
}

type processingLegacyAckLoss struct {
	interfaces.RetrieveEngineRepository
	mu   sync.Mutex
	lost bool
}

func (r *processingLegacyAckLoss) BatchSave(ctx context.Context, items []*types.IndexInfo, params map[string]any) error {
	if err := r.RetrieveEngineRepository.BatchSave(ctx, items, params); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.lost && len(items) > 0 && strings.HasPrefix(items[0].SourceID, "p2_") {
		r.lost = true
		return context.DeadlineExceeded
	}
	return nil
}
func (r *processingLegacyAckLoss) ReadProcessingIndexes(ctx context.Context, kb, id string, dimension int, ids []string) ([]*types.IndexInfo, error) {
	return r.RetrieveEngineRepository.(interfaces.ProcessingIndexReader).ReadProcessingIndexes(ctx, kb, id, dimension, ids)
}

func TestProcessingLegacyAdoptionReusesRealQdrantVectorsAndPublishesOriginalFile(t *testing.T) {
	for _, scenario := range []string{"complete", "delete-before-start", "graph-wiki", "moved-before-snapshot", "moved-before-publish", "unproven-model", "changed-same-id", "changed-dimension", "missing-old-vectors", "migrated-store", "migrated-store-without-proof", "keyword-only"} {
		t.Run(scenario, func(t *testing.T) { checkProcessingLegacyAdoption(t, scenario) })
	}
}

func checkProcessingLegacyAdoption(t *testing.T, scenario string) {
	if os.Getenv("PROCESSING_TEST_POSTGRES") == "" || os.Getenv("PROCESSING_TEST_QDRANT") == "" {
		t.Skip("requires isolated PostgreSQL and Qdrant")
	}
	require.Equal(t, "lifecycle-qdrant", os.Getenv("PROCESSING_TEST_QDRANT"))
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	t.Setenv("WEKNORA_PROCESSING_SOURCES", "source")
	var memberVisible atomic.Bool
	memberVisible.Store(true)
	mockProcessingLegacyMembership(t, &memberVisible)
	collection := "legacy_adoption_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Setenv("QDRANT_COLLECTION", collection)
	t.Setenv("QDRANT_HOST", "lifecycle-qdrant")
	dir := t.TempDir()
	t.Setenv("STORAGE_TYPE", "local")
	t.Setenv("LOCAL_STORAGE_BASE_DIR", dir)
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}, &types.Model{}, &types.SyncLog{}, &types.SyncRunItem{}, &types.KnowledgeProcessingSpan{}))
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "versioned", "000088_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(migration)).Error)
	tenant := types.Tenant{ID: 1, Name: "synthetic", StorageUsed: 777, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.QdrantRetrieverEngineType, RetrieverType: types.VectorRetrieverType}}}}
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&tenant).Error)
	for id, kind := range map[string]types.ModelType{"embed": types.ModelTypeEmbedding, "summary": types.ModelTypeKnowledgeQA, "vision": types.ModelTypeVLLM} {
		require.NoError(t, db.Create(&types.Model{ID: id, TenantID: 1, Name: id, Type: kind, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
	}
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&kb).Error)
	kb.EnsureDefaults()
	kb.EmbeddingModelID, kb.SummaryModelID = "embed", "summary"
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true}
	kb.VLMConfig = types.VLMConfig{Enabled: true, ModelID: "vision"}
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 1}
	if scenario == "graph-wiki" {
		kb.IndexingStrategy.GraphEnabled = true
		kb.IndexingStrategy.WikiEnabled = true
		kb.ExtractConfig = &types.ExtractConfig{Enabled: true}
	}
	require.NoError(t, db.Save(&kb).Error)
	var source types.DataSource
	require.NoError(t, db.First(&source).Error)
	source.Config, err = (&types.DataSourceConfig{ResourceIDs: []string{"tdoc:home"}, Credentials: map[string]any{"mcp_token": "synthetic-membership-token"}}).ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Save(&source).Error)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &tenant)
	baseFiles := files.NewLocalFileService(dir, "")
	image, err := baseFiles.SaveBytes(ctx, processingTestImage(t, 1), 1, "legacy.png", false)
	require.NoError(t, err)
	snapshot := processingLegacyFixture()
	_, oldDigests, err := repository.NewProcessingRepository(db).ConfigurationDigests(ctx, 1, kb.ID)
	require.NoError(t, err)
	if scenario != "unproven-model" {
		snapshot.Spans[3].Input["model_configuration_digest"] = oldDigests["model/embed"]
	}
	if scenario == "changed-same-id" || scenario == "changed-dimension" {
		var changed types.Model
		require.NoError(t, db.First(&changed, "id = ?", "embed").Error)
		changed.Name, changed.Parameters.BaseURL = "replacement-embedding", "https://embedding.invalid/v2"
		if scenario == "changed-dimension" {
			changed.Parameters.EmbeddingParameters.Dimension = 3
		}
		require.NoError(t, db.Save(&changed).Error)
	}
	for _, chunk := range snapshot.Chunks {
		chunk.IsEnabled = true
		chunk.Status = int(types.ChunkStatusIndexed)
		chunk.Content = strings.ReplaceAll(chunk.Content, "minio://owned/synthetic.png", image)
		chunk.Content = strings.ReplaceAll(chunk.Content, "Text ![", strings.Repeat("Legacy source contains stable evidence. ", 6)+"LEGACY-ADOPT-END-7391 ![")
		chunk.ImageInfo = strings.ReplaceAll(chunk.ImageInfo, "minio://owned/synthetic.png", image)
	}
	snapshot.Spans[6].Input["image_url"] = image
	snapshot.Spans[3].Output["storage_bytes"] = 777
	body := []byte(snapshot.Chunks[0].Content)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "1", "legacy"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "1", "legacy", "source.md"), body, 0600))
	k := snapshot.Knowledge
	k.Type = "file"
	k.FileType = "md"
	k.FileName = "source.md"
	k.FilePath = "local://1/legacy/source.md"
	k.FileHash = fmt.Sprintf("%x", md5.Sum(body))
	k.FileSize = int64(len(body))
	k.EmbeddingModelID = "embed"
	k.StorageSize = 777
	k.ParseStatus = types.ParseStatusFailed
	k.ErrorMessage = "stale worker" + types.HousekeepingRecoveryErrorSuffix
	k.EnableStatus = "disabled"
	k.Metadata = types.JSON(`{"datasource_id":"source","external_id":"tdoc:home-node:ZmlsZQ","file_id":"file","datasource_version":"version-legacy","datasource_candidate":"true","datasource_index_ready":"true"}`)
	require.NoError(t, db.Create(&k).Error)
	for _, chunk := range snapshot.Chunks {
		require.NoError(t, db.Create(chunk).Error)
	}
	for i := range snapshot.Spans {
		span := &snapshot.Spans[i]
		span.KnowledgeID = k.ID
		span.Attempt = 1
		span.SpanID = fmt.Sprintf("legacy-span-%d", i)
		require.NoError(t, db.Create(span).Error)
	}
	run := types.SyncLog{ID: "legacy-run", TenantID: 1, DataSourceID: source.ID, Status: "running", Result: types.JSON(`{"errors":[{"stage":"ingest","category":"INGEST_FAILED","external_id":"tdoc:home-node:ZmlsZQ","file_id":"file","source_resource_id":"tdoc:home","message":"candidate waiting expired"}]}`), ItemsFailed: 1}
	require.NoError(t, db.Create(&run).Error)
	var runBefore types.SyncLog
	require.NoError(t, db.First(&runBefore, "id = ?", run.ID).Error)
	r := repository.NewProcessingRepository(db)
	original, err := r.InspectLegacyError(ctx, types.ProcessingLegacyIdentity{TenantID: 1, KnowledgeBaseID: kb.ID, DataSourceID: source.ID, RunID: run.ID, ErrorOrdinal: 1})
	require.NoError(t, err)
	snapshot, err = r.LegacySnapshot(ctx, original.Identity, k.ID, "version-legacy", 1)
	require.NoError(t, err)
	inputs, _, err := processingLegacyInputs(snapshot, true)
	require.NoError(t, err)
	client, err := qdrant.NewClient(&qdrant.Config{Host: "lifecycle-qdrant", Port: 6334})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.DeleteCollection(context.Background(), collection+"_2"))
		if scenario == "changed-dimension" {
			require.NoError(t, client.DeleteCollection(context.Background(), collection+"_3"))
		}
		_ = client.Close()
	})
	baseIndex := qrepo.NewQdrantRetrieveEngineRepository(client, nil)
	currentIndex := baseIndex
	for _, item := range inputs {
		if scenario == "missing-old-vectors" && item == inputs[0] {
			continue
		}
		require.NoError(t, baseIndex.Save(ctx, item, map[string]any{"embedding": map[string][]float32{item.SourceID: {0, 1}}}))
	}
	fault := &processingLegacyAckLoss{RetrieveEngineRepository: baseIndex}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	storeRegistry := registry.(interfaces.StoreRegistry)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(fault, types.QdrantRetrieverEngineType)))
	models := processingPipelineModels{embed: &processingPipelineEmbedder{}, summary: &processingPipelineChat{calls: 1, questionCalls: 1}, vision: &processingPipelineVision{}}
	if scenario == "changed-dimension" {
		models.embed.dimension = 3
	}
	catalog := NewResourceCatalog(repository.NewResourceRepository(db))
	store := files.NewResourceCatalogFileService(baseFiles, catalog)
	s := &knowledgeService{config: &config.Config{Conversation: &config.ConversationConfig{GenerateSummaryPrompt: "Summarize the document.", GenerateQuestionsPrompt: "QUESTIONS-TEST {{content}} {{context}}"}}, kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}, tenantRepo: repository.NewTenantRepository(db), repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db), fileSvc: store, resourceCatalog: catalog, modelService: models, retrieveEngine: registry}
	oldDestination, err := knowledgeIndexDestination(ctx, s, &kb, 2)
	require.NoError(t, err)
	if strings.HasPrefix(scenario, "migrated-store") {
		require.NoError(t, db.AutoMigrate(&types.VectorStore{}))
		for _, id := range []string{"legacy-old-store", "legacy-new-store"} {
			require.NoError(t, db.Create(&types.VectorStore{ID: id, TenantID: 1, Name: id, EngineType: types.QdrantRetrieverEngineType}).Error)
		}
		t.Setenv("QDRANT_COLLECTION", collection+"_moved")
		currentIndex = qrepo.NewQdrantRetrieveEngineRepository(client, nil)
		t.Setenv("QDRANT_COLLECTION", collection)
		// Ensure the empty new collection exists so a missing legacy point is
		// distinguished from an unavailable backend.
		probe := *inputs[0]
		probe.SourceID = uuid.NewString()
		require.NoError(t, currentIndex.Save(ctx, &probe, map[string]any{"embedding": map[string][]float32{probe.SourceID: {0, 1}}}))
		require.NoError(t, currentIndex.DeleteBySourceIDList(ctx, []string{probe.SourceID}, 2, kb.Type))
		t.Cleanup(func() { require.NoError(t, client.DeleteCollection(context.Background(), collection+"_moved_2")) })
		storeRegistry.RegisterWithStoreID("legacy-old-store", retriever.NewKVHybridRetrieveEngine(baseIndex, types.QdrantRetrieverEngineType))
		fault = &processingLegacyAckLoss{RetrieveEngineRepository: currentIndex}
		storeRegistry.RegisterWithStoreID("legacy-new-store", retriever.NewKVHybridRetrieveEngine(fault, types.QdrantRetrieverEngineType))
		s.ownership = retriever.NewVectorStoreRepoOwnership(repository.NewVectorStoreRepository(db))
		newID := "legacy-new-store"
		kb.VectorStoreID = &newID
		// Seed a historical migration explicitly; ordinary KB updates keep
		// their create-only store binding immutable.
		require.NoError(t, db.Exec("UPDATE knowledge_bases SET vector_store_id = ? WHERE id = ?", newID, kb.ID).Error)
		oldID := "legacy-old-store"
		oldDestination.VectorStoreID, oldDestination.EnvironmentDigest = &oldID, ""
	}
	if scenario == "keyword-only" {
		currentIndex = processingTestPostgresIndexes(t, db)
		fault = &processingLegacyAckLoss{RetrieveEngineRepository: currentIndex}
		require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(fault, types.PostgresRetrieverEngineType)))
		tenant.RetrieverEngines.Engines = append(tenant.RetrieverEngines.Engines, types.RetrieverEngineParams{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.KeywordsRetrieverType})
		require.NoError(t, db.Model(&tenant).Update("retriever_engines", tenant.RetrieverEngines).Error)
		kb.IndexingStrategy.VectorEnabled, kb.IndexingStrategy.KeywordEnabled = false, true
		require.NoError(t, db.Save(&kb).Error)
	}
	if scenario == "missing-old-vectors" || scenario == "migrated-store" || scenario == "keyword-only" {
		for i := range snapshot.Spans {
			span := &snapshot.Spans[i]
			if span.Name == types.StageEmbedding {
				span.Input["index_destination"] = oldDestination
				require.NoError(t, db.Model(&types.KnowledgeProcessingSpan{}).Where("id = ?", span.ID).Update("input", span.Input).Error)
			}
		}
		snapshot, err = r.LegacySnapshot(ctx, original.Identity, k.ID, "version-legacy", 1)
		require.NoError(t, err)
	}
	if scenario == "graph-wiki" {
		s.config.ExtractManager = &config.ExtractManagerConfig{ExtractGraph: &types.PromptTemplateStructured{Description: "GRAPH-TEST"}}
		s.graphEngine = NewProcessingGraphRepository(&processingTestGraph{writes: map[string]types.NameSpace{}}, r, true)
		setupProcessingWiki(t, db, s, models.summary, false)
	}
	evidence := types.ProcessingLegacyEvidence{ProcessingLegacyIdentity: original.Identity, Action: "candidate_adopted", KnowledgeID: k.ID, SourceRevision: "version-legacy", Attempt: 1, SnapshotDigest: snapshot.Digest, Actor: "synthetic-operator", Reason: "verified legacy fixture", OperationRequestID: "adopt-fixture", EvidenceReference: "synthetic-worker-inventory", EvidenceDigest: strings.Repeat("b", 64)}
	_, err = AdoptLegacyCandidate(ctx, s, r, evidence)
	if scenario == "migrated-store-without-proof" {
		require.ErrorContains(t, err, "LEGACY_INDEX_MISSING")
		var count int64
		require.NoError(t, db.Model(&types.ProcessingJob{}).Count(&count).Error)
		require.Zero(t, count)
		return
	}
	require.ErrorContains(t, err, "LEGACY_DRAIN_REQUIRED")
	revision, err := r.ConfigurationRevision(ctx, 1, kb.ID)
	require.NoError(t, err)
	scope, _, err := repository.ProcessingSourceRevisions(&source)
	require.NoError(t, err)
	var queues []map[string]any
	for _, q := range types.QueueDefinitions() {
		queues = append(queues, map[string]any{"queue": q.Name, "inventory_digest": strings.Repeat("c", 64), "tasks": []any{}})
	}
	inventory, _ := json.Marshal(map[string]any{"complete": true, "workers": []map[string]any{{"old_instance_id": "synthetic-old", "replacement_id": "synthetic-guarded", "image_digest": strings.Repeat("d", 64), "exit_confirmed": true, "guard_protocol": 2}}, "queues": queues})
	drain, err := r.RecordLegacyDrain(ctx, types.ProcessingLegacyDrain{TenantID: 1, KnowledgeBaseID: kb.ID, DataSourceID: source.ID, ScopeRevision: scope, ConfigurationRevision: revision, Inventory: inventory, EvidenceReference: "synthetic-worker-inventory", EvidenceDigest: strings.Repeat("b", 64), Actor: "synthetic-operator", OperationRequestID: "drain-fixture", CheckedAt: time.Now().UTC()})
	require.NoError(t, err)
	evidence.DrainID = drain.ID
	// Simulate producer failure at the final outbox write. Nothing acquires
	// ownership unless candidate/evidence/plan/dispatch all commit.
	require.NoError(t, db.Exec("ALTER TABLE task_pending_ops ADD CONSTRAINT legacy_test_outbox_failure CHECK(task_type <> 'processing:step')").Error)
	_, err = AdoptLegacyCandidate(ctx, s, r, evidence)
	require.Error(t, err)
	var unchanged types.Knowledge
	require.NoError(t, db.First(&unchanged, "id = ?", k.ID).Error)
	require.Empty(t, unchanged.GetMetadata()["processing_protocol"])
	var rows int64
	require.NoError(t, db.Model(&types.ProcessingLegacyEvidence{}).Count(&rows).Error)
	require.Zero(t, rows)
	require.NoError(t, db.Model(&types.ProcessingJob{}).Count(&rows).Error)
	require.Zero(t, rows)
	require.NoError(t, db.Exec("ALTER TABLE task_pending_ops DROP CONSTRAINT legacy_test_outbox_failure").Error)
	var job *types.ProcessingJob
	if scenario == "migrated-store" {
		started, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unlock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unlock()
		require.NoError(t, db.Callback().Create().Before("gorm:create").Register("legacy-adopt-lock-check", func(tx *gorm.DB) {
			if tx.Statement.Schema != nil && tx.Statement.Schema.Name == "ProcessingJob" {
				close(started)
				<-release
			}
		}))
		adopted, removed := make(chan error, 1), make(chan error, 1)
		go func() {
			var adoptErr error
			job, adoptErr = AdoptLegacyCandidate(ctx, s, r, evidence)
			adopted <- adoptErr
		}()
		select {
		case <-started:
		case <-time.After(30 * time.Second):
			t.Fatal("adoption did not reach locked commit")
		}
		stores := &vectorStoreService{db: db, kbRepo: repository.NewKnowledgeBaseRepository(db), storeRegistry: storeRegistry}
		go func() { removed <- stores.DeleteStore(ctx, 1, "legacy-old-store") }()
		select {
		case err := <-removed:
			t.Fatalf("delete escaped the adoption store lock: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		unlock()
		err = <-adopted
		require.NoError(t, err)
		require.ErrorContains(t, <-removed, "retained by processing")
		require.NoError(t, db.Callback().Create().Remove("legacy-adopt-lock-check"))
	} else {
		job, err = AdoptLegacyCandidate(ctx, s, r, evidence)
	}
	require.NoError(t, err)
	same, err := AdoptLegacyCandidate(ctx, s, r, evidence)
	require.NoError(t, err)
	require.Equal(t, job.ID, same.ID)
	execute, err := NewKnowledgeProcessingExecutor(s, r, repository.NewDataSourceRepository(db))
	require.NoError(t, err)
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NotEmpty(t, job.IndexDestination)
	if scenario == "migrated-store" {
		var destination types.ProcessingIndexDestination
		require.NoError(t, json.Unmarshal(job.IndexDestination, &destination))
		require.NotNil(t, destination.VectorStoreID)
		require.Equal(t, "legacy-new-store", *destination.VectorStoreID)
	}
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("owner_type = ? AND owner_id = ?", "processing_job", job.ID).Count(&rows).Error)
	require.EqualValues(t, 2, rows, "legacy files are owned before any worker can start")
	require.NoError(t, db.Model(&types.ProcessingStorageReservation{}).Where("job_id = ? AND kind = ? AND bytes = ? AND state = ?", job.ID, "index", 777, "reserved").Count(&rows).Error)
	require.EqualValues(t, 1, rows)
	if scenario == "delete-before-start" {
		require.NoError(t, s.repo.DeleteKnowledge(ctx, 1, k.ID))
		for n := 0; n < 8; n++ {
			job, err = r.GetJob(ctx, 1, job.ID)
			require.NoError(t, err)
			if job.RetirementState == "deleted" {
				break
			}
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			for _, step := range steps {
				if step.Status != types.ProcessingEnqueuePending {
					continue
				}
				require.Equal(t, types.ProcessingPhaseRetire, step.Phase)
				lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
				require.NoError(t, err)
				out, err := execute(ctx, *lease)
				require.NoError(t, err)
				require.Equal(t, types.ProcessingSucceeded, out.Status, out.ErrorCode)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
			}
		}
		require.Equal(t, "deleted", job.RetirementState)
		require.Zero(t, models.embed.calls)
		require.Equal(t, 1, models.summary.calls)
		var owner types.Tenant
		require.NoError(t, db.First(&owner, "id = ?", 1).Error)
		require.Zero(t, owner.StorageUsed)
		_, err := os.Stat(filepath.Join(dir, "1", "legacy", "source.md"))
		require.True(t, os.IsNotExist(err))
		var ids []string
		for _, item := range inputs {
			ids = append(ids, item.SourceID)
		}
		points, err := baseIndex.(interfaces.ProcessingIndexReader).ReadProcessingIndexes(ctx, kb.ID, k.ID, 2, ids)
		require.NoError(t, err)
		require.Empty(t, points)
		require.NoError(t, db.Model(&types.ProcessingLegacyEvidence{}).Where("action = ?", "recovered").Count(&rows).Error)
		require.Zero(t, rows)
		return
	}
	stages := map[string]int{}
	for n := 0; n < 90; n++ {
		job, err = r.GetJob(ctx, 1, job.ID)
		require.NoError(t, err)
		if job.Status == types.ProcessingSucceeded {
			break
		}
		require.NoError(t, db.Model(&types.ProcessingStep{}).Where("job_id = ? AND next_run_at IS NOT NULL", job.ID).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
		require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		for _, step := range steps {
			if step.Status != types.ProcessingEnqueuePending {
				continue
			}
			if step.Stage == "publish" && scenario == "complete" {
				queue := &processingTestQueue{}
				require.NoError(t, NewProcessingService(r, queue, nil, nil).Dispatch(ctx))
				found := false
				for _, task := range queue.tasks {
					var payload types.ProcessingTaskPayload
					require.NoError(t, json.Unmarshal(task.Payload(), &payload))
					if payload.StepID == step.ID {
						found = true
						require.Equal(t, types.TypeProcessingStep+":"+types.QueueSync, task.Type(), "membership HTTP work must not enter the light postprocess pool")
					}
				}
				require.True(t, found)
			}
			lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
			require.NoError(t, err)
			stages[step.Stage]++
			moved := (scenario == "moved-before-snapshot" && step.Stage == "legacy_snapshot") || (scenario == "moved-before-publish" && step.Stage == "publish")
			if moved {
				memberVisible.Store(false)
			}
			out, err := execute(ctx, *lease)
			require.NoError(t, err)
			if moved {
				require.NotEqual(t, types.ProcessingSucceeded, out.Status)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
				current, err := r.GetJob(ctx, 1, job.ID)
				require.NoError(t, err)
				require.False(t, current.IsPublished)
				var ids []string
				for _, item := range inputs {
					ids = append(ids, item.SourceID)
				}
				old, err := baseIndex.(interfaces.ProcessingIndexReader).ReadProcessingIndexes(ctx, kb.ID, k.ID, 2, ids)
				require.NoError(t, err)
				require.Len(t, old, len(inputs))
				return
			}
			if step.Stage == "wiki_taxonomy_plan" && out.ErrorCode == "WIKI_TAXONOMY_INVALID" {
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
				current, err := r.GetJob(ctx, 1, job.ID)
				require.NoError(t, err)
				require.True(t, current.IsPublished)
				require.NoError(t, r.RetryStep(ctx, 1, job.ID, step.ID, current.Revision, "legacy-wiki-taxonomy-retry", "operator"))
				continue
			}
			require.NotEqual(t, types.ProcessingBlocked, out.Status, "stage %s: %s", step.Stage, out.ErrorCode)
			require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
		}
	}
	require.Equal(t, types.ProcessingSucceeded, job.Status)
	require.True(t, job.IsPublished)
	require.Equal(t, k.ID, job.KnowledgeID)
	for _, stage := range []string{"native_read", "export_start", "export_poll", "download", "parse", "chunk", "images", "image_ocr", "image_caption"} {
		require.Zero(t, stages[stage], stage)
	}
	require.True(t, fault.lost)
	require.NoError(t, db.Model(&types.ProcessingStorageReservation{}).Where("job_id = ? AND kind = ? AND bytes = ? AND state = ?", job.ID, "index", 777, "released").Count(&rows).Error)
	require.EqualValues(t, 1, rows)
	var charged int64
	require.NoError(t, db.Model(&types.ProcessingStorageReservation{}).Where("job_id = ? AND state <> ?", job.ID, "released").Select("COALESCE(SUM(bytes),0)").Scan(&charged).Error)
	var owner types.Tenant
	require.NoError(t, db.First(&owner, "id = ?", 1).Error)
	require.Equal(t, charged, owner.StorageUsed)
	if scenario == "graph-wiki" {
		require.Equal(t, 3, models.summary.graphCalls)
		page, err := s.wikiRepo.GetBySlug(ctx, kb.ID, "entity/lifecycle")
		require.NoError(t, err)
		require.Contains(t, page.Content, "WIKI-PAGE-7391")
		require.Contains(t, page.Content, "MANUAL-CAS-7391")
		require.Contains(t, page.SourceRefs, k.ID)
		require.Equal(t, 1, models.summary.wikiCalls["wiki_candidate_slug"])
		require.NoError(t, db.Model(&types.ProcessingWikiWrite{}).Where("job_id = ? AND state = ?", job.ID, "active").Count(&rows).Error)
		require.EqualValues(t, 3, rows)
		require.Equal(t, 2, stages["embedding"], "source indexing still uses saved vectors; extra embedding calls belong to Wiki")
	} else {
		want := 2
		if scenario == "keyword-only" {
			want = 0
		}
		if scenario == "unproven-model" || scenario == "changed-same-id" || scenario == "changed-dimension" || scenario == "missing-old-vectors" {
			want++
		}
		require.Equal(t, want, models.embed.calls, "only unverified vectors and new summary/question projections embed; an index retry reuses the confirmed batch")
	}
	require.Zero(t, models.vision.ocr)
	require.Zero(t, models.vision.caption)
	var after types.Knowledge
	require.NoError(t, db.First(&after, "id = ?", k.ID).Error)
	file, name, err := s.processingKnowledgeFile(ctx, &after)
	require.NoError(t, err)
	saved, err := io.ReadAll(file)
	require.NoError(t, err)
	_ = file.Close()
	require.Equal(t, body, saved)
	require.Equal(t, "source.md", name)
	var runAfter types.SyncLog
	require.NoError(t, db.First(&runAfter, "id = ?", run.ID).Error)
	require.Equal(t, runBefore, runAfter)
	require.NoError(t, db.Model(&types.ProcessingLegacyEvidence{}).Where("action = ? AND job_id = ?", "recovered", job.ID).Count(&rows).Error)
	require.EqualValues(t, 1, rows)
	oldIDs := []string{}
	for _, item := range inputs {
		oldIDs = append(oldIDs, item.SourceID)
	}
	remaining, err := baseIndex.(interfaces.ProcessingIndexReader).ReadProcessingIndexes(ctx, kb.ID, k.ID, 2, oldIDs)
	require.NoError(t, err)
	require.Empty(t, remaining)
	if scenario == "keyword-only" {
		var count int64
		require.NoError(t, db.Table("embeddings").Where("knowledge_id = ? AND dimension = 0 AND embedding IS NULL AND source_id LIKE 'p2_%'", k.ID).Count(&count).Error)
		require.EqualValues(t, 8, count, "three unconfirmed first-attempt writes remain isolated until retirement")
		var hits []*types.IndexWithScore
		require.NoError(t, db.Table("embeddings").Where("knowledge_id = ? AND is_enabled = true", k.ID).Select("source_id,source_type,chunk_id,knowledge_id,knowledge_base_id,content").Scan(&hits).Error)
		visible, err := r.FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: kb.ID}}, hits)
		require.NoError(t, err)
		require.Len(t, visible, 5, "only the confirmed manifest is visible")
		return
	}
	queryVector := []float32{0, 1}
	if scenario == "changed-dimension" {
		queryVector = []float32{0, 1, 0}
	}
	results, err := currentIndex.Retrieve(ctx, types.RetrieveParams{Embedding: queryVector, KnowledgeBaseIDs: []string{kb.ID}, KnowledgeIDs: []string{k.ID}, TopK: 20, RetrieverType: types.VectorRetrieverType})
	require.NoError(t, err)
	var hits []*types.IndexWithScore
	for _, result := range results {
		hits = append(hits, result.Results...)
	}
	visible, err := r.FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: kb.ID}}, hits)
	require.NoError(t, err)
	require.Len(t, visible, 5)
	seen := map[string]bool{}
	for _, hit := range visible {
		require.True(t, strings.HasPrefix(hit.SourceID, "p2_"))
		require.False(t, seen[hit.SourceID])
		seen[hit.SourceID] = true
	}
}
