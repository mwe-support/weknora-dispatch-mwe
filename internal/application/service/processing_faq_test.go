package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

type processingFAQTags struct{ interfaces.KnowledgeTagService }

type processingFAQBlockedIndex struct {
	*processingPipelineIndex
	block            atomic.Bool
	entered, release chan struct{}
}

func (r *processingFAQBlockedIndex) BatchSave(ctx context.Context, items []*types.IndexInfo, params map[string]any) error {
	if r.block.CompareAndSwap(true, false) {
		close(r.entered)
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.processingPipelineIndex.BatchSave(ctx, items, params)
}

func (processingFAQTags) FindOrCreateTagByName(context.Context, string, string) (*types.KnowledgeTag, error) {
	return &types.KnowledgeTag{ID: "synthetic-tag"}, nil
}

func TestProcessingFAQPrepareValidatesEveryEntryAndNativeSheetHeaders(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	for _, tc := range []struct {
		name, kind, body string
		valid            bool
	}{
		{"doc", "doc", "| 问题 | 机器人回答 |\n|---|---|\n|q1|a1|", true},
		{"smartcanvas", "smartcanvas", "| 问题 | 机器人回答 |\n|---|---|\n|q1|a1|", true},
		{"smartsheet", "smartsheet", "| 问题 | 机器人回答 |\n|---|---|\n|q1|a1|", true},
		{"sheet", "sheet", "## Sheet: Native\n| Source row | A | B |\n|---|---|---|\n|1|问题|机器人回答|\n|101|q1|a1|", true},
		{"invalid-after-valid", "smartcanvas", "| 问题 | 机器人回答 |\n|---|---|\n|q1|a1|\n|SYNTHETIC-NOT-IN-HISTORY||", false},
		{"duplicate-alias", "smartcanvas", "| 问题 | 机器人回答 | 相似问题 |\n|---|---|---|\n|q1|a1|q2|\n|q2|a2||", false},
		{"negative-overlap", "smartcanvas", "| 问题 | 机器人回答 | 反例问题 |\n|---|---|---|\n|q1|a1|q1|", false},
		{"missing-schema", "smartcanvas", "| wrong | columns |\n|---|---|\n|q1|a1|", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			job := types.ProcessingJob{ID: "synthetic-job", TenantID: 1, KnowledgeBaseID: "kb", Generation: 1}
			assets := types.ProcessingStep{ID: "synthetic-assets", Stage: "assets", UnitKey: "body", InputFingerprint: "fixed", Status: types.ProcessingSucceeded, Attempt: 1}
			artifacts := NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil)
			data, _ := json.Marshal(processingParsed{ReadResult: types.ReadResult{MarkdownContent: tc.body}})
			var err error
			assets.OutputManifestRef, assets.OutputDigest, err = artifacts.Save(ctx, job, assets, "assets", data)
			require.NoError(t, err)
			e := processingDocumentExecution{kb: &types.KnowledgeBase{ID: "kb", TenantID: 1, Type: types.KnowledgeBaseTypeFAQ}, lease: types.ProcessingLease{Job: job, Step: types.ProcessingStep{ID: "synthetic-prepare", Stage: "faq_prepare", InputFingerprint: "fixed", Attempt: 1}}, document: ProcessingDocumentSpec{Kind: tc.kind}, steps: []types.ProcessingStep{assets}, artifacts: artifacts}
			out, err := e.faqStage(ctx)
			require.NoError(t, err)
			if tc.valid {
				require.Equal(t, types.ProcessingSucceeded, out.Status)
				require.NotEmpty(t, out.OutputDigest)
			} else {
				require.Equal(t, types.ProcessingBlocked, out.Status)
				require.Equal(t, "FAQ_SOURCE_INVALID", out.ErrorCode)
				require.Empty(t, out.OutputManifestRef)
				encoded, _ := json.Marshal(out)
				require.NotContains(t, string(encoded), "SYNTHETIC-NOT-IN-HISTORY")
			}
		})
	}
}

func TestProcessingManualFAQFailurePreservesPublishedAnswerAndMedia(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}))
	tenant := &types.Tenant{ID: 1, Name: "synthetic", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}}}}
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(tenant).Error)
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&kb).Error)
	kb.Type, kb.EmbeddingModelID = types.KnowledgeBaseTypeFAQ, "synthetic"
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true}
	kb.FAQConfig = &types.FAQConfig{IndexMode: types.FAQIndexModeQuestionAnswer}
	require.NoError(t, db.Save(&kb).Error)
	index := &processingPipelineIndex{vectors: true}
	blocked := &processingFAQBlockedIndex{processingPipelineIndex: index, entered: make(chan struct{}), release: make(chan struct{})}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(blocked, types.PostgresRetrieverEngineType)))
	chunks := repository.NewChunkRepository(db)
	s := &knowledgeService{kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}, tenantRepo: repository.NewTenantRepository(db),
		repo: repository.NewKnowledgeRepository(db), chunkRepo: chunks, chunkService: &chunkService{chunkRepository: chunks},
		modelService: processingPipelineModels{embed: &processingPipelineEmbedder{}}, retrieveEngine: registry}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	canonical, err := s.ensureFAQKnowledge(ctx, 1, &kb)
	require.NoError(t, err)
	row := &types.Chunk{ID: "manual-faq", TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: canonical.ID, ChunkType: types.ChunkTypeFAQ, IsEnabled: true, Status: int(types.ChunkStatusIndexed)}
	media := &types.StoredResource{ID: "manual-image", Handle: "AbCdEfGhIjKlMnOpQrStUv", TenantID: 1, Provider: "local", PhysicalPath: "local://synthetic/image.png", State: types.ResourceStateActive}
	require.NoError(t, db.Create(media).Error)
	publishedAnswer := "Published answer ![image](" + types.BuildResourcePath(media.Handle) + ")"
	require.NoError(t, row.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: "Synthetic question", Answers: []string{publishedAnswer}}))
	require.NoError(t, chunks.CreateChunks(ctx, []*types.Chunk{row}))
	_, err = s.UpdateFAQEntry(ctx, kb.ID, row.SeqID, &types.FAQEntryPayload{StandardQuestion: "Synthetic question", Answers: []string{"Unconfirmed answer"}})
	require.Error(t, err, "the backend writes but loses its acknowledgement")
	current, err := chunks.GetChunkByID(ctx, 1, row.ID)
	require.NoError(t, err)
	meta, err := current.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{publishedAnswer}, meta.Answers)
	require.Equal(t, row.ContentRevision, current.ContentRevision)
	_, err = s.AddSimilarQuestions(ctx, kb.ID, row.SeqID, []string{"Synthetic alias"})
	require.NoError(t, err)
	current, err = chunks.GetChunkByID(ctx, 1, row.ID)
	require.NoError(t, err)
	meta, err = current.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{publishedAnswer}, meta.Answers)
	require.Contains(t, meta.SimilarQuestions, "Synthetic alias")
	var hits []*types.IndexWithScore
	for _, item := range index.writes {
		hits = append(hits, &types.IndexWithScore{SourceID: item.SourceID, ChunkID: item.ChunkID, KnowledgeID: item.KnowledgeID, KnowledgeBaseID: item.KnowledgeBaseID})
	}
	visible, err := repository.NewProcessingRepository(db).FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: "kb"}}, hits)
	require.NoError(t, err)
	require.NotEmpty(t, visible)
	for _, item := range visible {
		require.True(t, current.AcceptsFAQIndex(item.SourceID))
	}
	var bound int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("tenant_id = ? AND resource_id = ? AND owner_type = ? AND owner_id = ?", 1, media.ID, "faq_chunk", row.ID).Count(&bound).Error)
	require.EqualValues(t, 1, bound)
	var charged int64
	require.NoError(t, db.Model(&types.FAQIndexWrite{}).Select("SUM(estimated_bytes)").Scan(&charged).Error)
	require.Positive(t, charged)
	var owner types.Tenant
	require.NoError(t, db.First(&owner, 1).Error)
	require.Equal(t, charged, owner.StorageUsed, "both accepted and uncertain writes retain their estimates")
	require.NoError(t, db.Model(&owner).Update("storage_quota", owner.StorageUsed).Error)
	calls := index.calls
	_, err = s.UpdateFAQEntry(ctx, kb.ID, row.SeqID, &types.FAQEntryPayload{StandardQuestion: "Synthetic question", Answers: []string{"Over quota"}})
	var quota *types.StorageQuotaExceededError
	require.ErrorAs(t, err, &quota)
	require.Equal(t, calls, index.calls, "quota is reserved before provider I/O")
	after, err := chunks.GetChunkByID(ctx, 1, row.ID)
	require.NoError(t, err)
	require.Equal(t, current.ContentRevision, after.ContentRevision)
	old := time.Now().Add(-8 * 24 * time.Hour)
	require.NoError(t, db.Model(&types.FAQIndexWrite{}).Where("tenant_id = ?", 1).Update("updated_at", old).Error)
	_, err = s.collectFAQIndexGarbage(ctx, repository.NewProcessingRepository(db), "", 100)
	require.NoError(t, err)
	require.NoError(t, db.First(&owner, 1).Error)
	require.Less(t, owner.StorageUsed, charged)
	remaining := owner.StorageUsed
	require.Positive(t, remaining, "the published manifest still retains its estimate")
	require.NoError(t, db.Model(&types.FAQIndexWrite{}).Where("tenant_id = ?", 1).Update("updated_at", old).Error)
	_, err = s.collectFAQIndexGarbage(ctx, repository.NewProcessingRepository(db), "", 100)
	require.NoError(t, err)
	require.NoError(t, db.First(&owner, 1).Error)
	require.Equal(t, remaining, owner.StorageUsed, "repeated deletion never releases quota twice")

	// The first writer reaches the backend, then a newer editor publishes.
	// Releasing the delayed write must not overwrite that answer or its media.
	require.NoError(t, db.Model(&owner).Update("storage_quota", 1000000).Error)
	work, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	blocked.block.Store(true)
	result := make(chan error, 1)
	go func() {
		_, err := s.UpdateFAQEntry(work, kb.ID, row.SeqID, &types.FAQEntryPayload{StandardQuestion: "Synthetic question", Answers: []string{"Delayed answer"}})
		result <- err
	}()
	select {
	case <-blocked.entered:
	case <-work.Done():
		t.Fatal("delayed editor never reached its backend")
	}
	_, err = s.UpdateFAQEntry(work, kb.ID, row.SeqID, &types.FAQEntryPayload{StandardQuestion: "Synthetic question", Answers: []string{publishedAnswer + " newer editor"}})
	close(blocked.release)
	require.NoError(t, err)
	require.ErrorIs(t, <-result, repository.ErrChunkRevisionConflict)
	current, err = chunks.GetChunkByID(ctx, 1, row.ID)
	require.NoError(t, err)
	meta, err = current.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{publishedAnswer + " newer editor"}, meta.Answers)
	hits = nil
	for _, item := range index.writes {
		hits = append(hits, &types.IndexWithScore{SourceID: item.SourceID, ChunkID: item.ChunkID, KnowledgeID: item.KnowledgeID, KnowledgeBaseID: item.KnowledgeBaseID})
	}
	visible, err = repository.NewProcessingRepository(db).FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: "kb"}}, hits)
	require.NoError(t, err)
	require.NotEmpty(t, visible)
	for _, item := range visible {
		require.True(t, current.AcceptsFAQIndex(item.SourceID))
	}
}

func TestProcessingFAQSourcePublishesAtomicallyAndReusesUnchangedBatches(t *testing.T) {
	for _, scenario := range []string{"publish", "before-index", "delete-before-publish", "late-index", "assets", "rollback"} {
		t.Run(scenario, func(t *testing.T) { runProcessingFAQSource(t, scenario) })
	}
}

func runProcessingFAQSource(t *testing.T, scenario string) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}, &types.FAQIndexWrite{}))
	tenant := &types.Tenant{ID: 1, Name: "synthetic", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{
		{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType},
	}}}
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(tenant).Error)
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&kb).Error)
	kb.Type = types.KnowledgeBaseTypeFAQ
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true}
	kb.FAQConfig = &types.FAQConfig{IndexMode: types.FAQIndexModeQuestionAnswer, QuestionIndexMode: types.FAQQuestionIndexModeSeparate}
	require.NoError(t, db.Save(&kb).Error)
	r := repository.NewProcessingRepository(db)
	index := &processingPipelineIndex{vectors: true}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
	embed := &processingPipelineEmbedder{}
	s := &knowledgeService{config: &config.Config{}, kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}, tenantRepo: repository.NewTenantRepository(db),
		repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db), fileSvc: files.NewLocalFileService(t.TempDir(), ""),
		modelService: processingPipelineModels{embed: embed}, retrieveEngine: registry, tagService: processingFAQTags{}}
	if scenario == "assets" || scenario == "rollback" {
		s.resourceCatalog = NewResourceCatalog(repository.NewResourceRepository(db))
		s.fileSvc = files.NewResourceCatalogFileService(s.fileSvc, s.resourceCatalog)
	}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	canonical, err := s.ensureFAQKnowledge(ctx, 1, &kb)
	require.NoError(t, err)
	old := &types.Chunk{ID: "old-faq", TenantID: 1, KnowledgeBaseID: kb.ID, KnowledgeID: canonical.ID, ChunkType: types.ChunkTypeFAQ, IsEnabled: true, Status: int(types.ChunkStatusIndexed), TagID: "preserved-tag", SourceContent: "immutable FAQ source", ContextHeader: "preserved context"}
	meta := &types.FAQChunkMetadata{StandardQuestion: "Existing question", Answers: []string{"Old answer"}, Source: "manual", Version: 3}
	for i := 0; i < 34; i++ {
		meta.SimilarQuestions = append(meta.SimilarQuestions, fmt.Sprintf("Existing alias %02d", i))
	}
	require.NoError(t, old.SetFAQMetadata(meta))
	require.NoError(t, s.chunkRepo.CreateChunks(ctx, []*types.Chunk{old}))
	document, _ := json.Marshal(ProcessingDocumentSpec{FileID: "faq-file", Kind: "smartcanvas", Title: "Synthetic FAQ"})
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: kb.ID, DataSourceID: "source", ExternalID: "faq-file", SourceRevision: "v1", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: document})
	require.NoError(t, err)
	lastWriteOffset := 0
	index.afterWrite = func() {
		var hits []*types.IndexWithScore
		for _, item := range index.writes[lastWriteOffset:] {
			hits = append(hits, &types.IndexWithScore{SourceID: item.SourceID, ChunkID: item.ChunkID, KnowledgeID: item.KnowledgeID, KnowledgeBaseID: item.KnowledgeBaseID})
		}
		lastWriteOffset = len(index.writes)
		visible, err := r.FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 1, KBID: kb.ID}}, hits)
		require.NoError(t, err)
		require.Empty(t, visible, "private and partially written indexes cannot expose FAQ answers")
		if scenario == "late-index" {
			_, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: kb.ID, DataSourceID: "source", ExternalID: "faq-file", SourceRevision: "replacement", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: document})
			require.NoError(t, err)
		}
	}
	plan := []types.ProcessingStepSpec{
		{Stage: "assets", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "assets", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "faq_prepare", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "faq_prepare", DependsOn: []string{"assets/body"}, RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "faq_index", UnitKey: "body", Kind: "barrier", Phase: types.ProcessingPhasePrepare, InputFingerprint: "faq_index", DependsOn: []string{"faq_prepare/body"}, RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "publish", UnitKey: "body", Phase: types.ProcessingPhasePublish, InputFingerprint: "publish", DependsOn: []string{"faq_index/body"}, RequiredForCompletion: true},
	}
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
	execute, err := NewKnowledgeProcessingExecutor(s, r, repository.NewDataSourceRepository(db))
	require.NoError(t, err)
	calls := map[string]int{}
	changed := false
	answer := "Source answer"
	assetRef := ""
	change := func() {
		before, err := s.chunkRepo.GetChunkByID(ctx, 1, old.ID)
		require.NoError(t, err)
		m, err := before.FAQMetadata()
		require.NoError(t, err)
		require.Equal(t, []string{"Old answer"}, m.Answers)
		var count int64
		require.NoError(t, db.Model(&types.Chunk{}).Where("chunk_type = ?", types.ChunkTypeFAQ).Count(&count).Error)
		require.EqualValues(t, 1, count)
		if scenario == "delete-before-publish" {
			require.NoError(t, s.chunkRepo.DeleteChunk(ctx, 1, before.ID))
		} else {
			m.SimilarQuestions = append(m.SimilarQuestions, "Concurrent alias")
			require.NoError(t, before.SetFAQMetadata(m))
			require.NoError(t, s.chunkRepo.UpdateChunk(ctx, before))
		}
		changed = true
	}
	run := func() {
		for n := 0; n < 60; n++ {
			job, err = r.GetJob(ctx, 1, job.ID)
			require.NoError(t, err)
			if job.Status == types.ProcessingSucceeded {
				break
			}
			require.NoError(t, db.Model(&types.ProcessingStep{}).Where("next_run_at IS NOT NULL").Update("next_run_at", time.Now().Add(-time.Minute)).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			ops, err := r.PendingDeliveries(ctx, 100)
			require.NoError(t, err)
			require.NotEmpty(t, ops, "stalled: %+v", job)
			for _, op := range ops {
				var ref types.ProcessingRef
				require.NoError(t, json.Unmarshal(op.Payload, &ref))
				lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
				if errors.Is(err, repository.ErrProcessingConflict) {
					continue
				} // A reset invalidates already delivered sibling attempts.
				require.NoError(t, err)
				calls[lease.Step.Stage]++
				var out types.ProcessingOutcome
				if lease.Step.Stage == "assets" {
					e := processingDocumentExecution{s: s, repo: r, kb: &kb, lease: *lease, artifacts: NewProcessingArtifacts(s.fileSvc, s.resourceCatalog)}
					parsed := processingParsed{}
					if (scenario == "assets" || scenario == "rollback") && job.SourceRevision == "v1" {
						active := types.WithProcessingLease(ctx, *lease)
						data := processingTestImage(t, 1)
						assetRef, err = s.fileSvc.SaveBytes(active, data, 1, "FAQ answer.png", false)
						require.NoError(t, err)
						require.NoError(t, s.resourceCatalog.Bind(active, assetRef, "processing_job", job.ID, "source_image"))
						parsed.Assets = []processingAsset{{ID: "answer-image", StoredURL: assetRef, Bytes: int64(len(data)), Digest: fmt.Sprintf("%x", sha256.Sum256(data))}}
						answer += " ![synthetic](" + assetRef + ")"
					}
					parsed.MarkdownContent = "| 问题 | 机器人回答 |\n|---|---|\n| Existing question | " + answer + " |\n| New question | New answer |"
					if job.SourceRevision == "v2" {
						parsed.MarkdownContent = "| 问题 | 机器人回答 |\n|---|---|\n| New question | Replacement answer |"
					}
					out, err = e.success(ctx, "assets", parsed)
					out.Candidate = &types.Knowledge{Title: "Source file"}
					out.Completeness = "complete"
				} else {
					if scenario == "before-index" && lease.Step.Stage == "faq_write" && strings.HasPrefix(lease.Step.UnitKey, "000000/") && !changed {
						change()
					}
					out, err = execute(ctx, *lease)
				}
				if scenario == "late-index" && lease.Step.Stage == "faq_write" {
					require.ErrorIs(t, err, repository.ErrProcessingConflict)
					require.NotEmpty(t, index.compensated)
					require.Empty(t, index.writes)
					require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, out), repository.ErrProcessingConflict)
					return
				}
				require.NoError(t, err)
				require.NotEqual(t, types.ProcessingBlocked, out.Status, "%s: %+v", lease.Step.Stage, out)
				if lease.Step.Stage == "publish" && !changed {
					change()
				}
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
			}
		}
	}
	run()
	if scenario == "late-index" {
		return
	}
	require.Equal(t, types.ProcessingSucceeded, job.Status)
	require.True(t, job.IsPublished)
	require.Equal(t, 1, calls["faq_prepare"])
	expectedPublications := 2
	if scenario == "before-index" {
		expectedPublications = 1
	}
	require.Equal(t, expectedPublications, calls["publish"])
	require.Equal(t, 4, embed.calls, "initial 3 batches; only the changed alias batch needs embedding again")
	var rows []types.Chunk
	require.NoError(t, db.Where("chunk_type = ?", types.ChunkTypeFAQ).Find(&rows).Error)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.NotEmpty(t, row.FAQIndexManifest)
		var manifest types.FAQIndexManifest
		require.NoError(t, json.Unmarshal([]byte(row.FAQIndexManifest), &manifest))
		require.True(t, row.AcceptsFAQIndex(manifest.SourceIDs[0]))
		if row.ID == old.ID {
			m, err := row.FAQMetadata()
			require.NoError(t, err)
			require.Equal(t, []string{answer}, m.Answers)
			require.Contains(t, m.SimilarQuestions, "Concurrent alias")
			require.Equal(t, "manual", m.Source)
			require.Equal(t, "preserved-tag", row.TagID)
			require.Equal(t, "immutable FAQ source", row.SourceContent)
			require.Equal(t, "preserved context", row.ContextHeader)
		}
	}
	if scenario == "delete-before-publish" {
		var tombstone types.Chunk
		require.NoError(t, db.Unscoped().Where("id = ?", old.ID).Take(&tombstone).Error)
		require.True(t, tombstone.DeletedAt.Valid, "source merge must never resurrect a deleted ID")
	}
	if scenario == "assets" || scenario == "rollback" {
		resource, err := s.resourceCatalog.Resolve(ctx, assetRef)
		require.NoError(t, err)
		var count int64
		require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ? AND owner_id = ?", resource.ID, "faq_chunk", old.ID).Count(&count).Error)
		require.Positive(t, count, "the canonical FAQ owns answer media independently of the source")
		first := *job
		job, err = r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: kb.ID, DataSourceID: "source", ExternalID: "faq-file", SourceRevision: "v2", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: document})
		require.NoError(t, err)
		require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
		run()
		require.Equal(t, types.ProcessingSucceeded, job.Status)
		retireID := first.ID
		if scenario == "rollback" {
			second := *job
			later := &types.Chunk{ID: "later-faq", TenantID: 1, KnowledgeBaseID: kb.ID, KnowledgeID: canonical.ID, ChunkType: types.ChunkTypeFAQ, IsEnabled: true, Status: int(types.ChunkStatusIndexed)}
			require.NoError(t, later.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: "Unrelated later question", Answers: []string{"Keep this later answer"}}))
			require.NoError(t, s.chunkRepo.CreateChunks(ctx, []*types.Chunk{later}))
			target, err := r.GetJob(ctx, 1, first.ID)
			require.NoError(t, err)
			require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, target.ID, target.Revision, "faq-rollback", "operator"))
			pending, err := r.GetJob(ctx, 1, target.ID)
			require.NoError(t, err)
			require.False(t, pending.IsPublished, "FAQ rollback must remerge before switching publication")
			current, err := r.GetJob(ctx, 1, second.ID)
			require.NoError(t, err)
			require.True(t, current.IsPublished)
			require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, target.ID, target.Revision, "faq-rollback", "operator"), "lost acknowledgement during rollback preparation")
			job = pending
			run()
			require.Equal(t, types.ProcessingSucceeded, job.Status)
			require.True(t, job.IsPublished)
			require.EqualValues(t, 3, job.PublicationEpoch)
			require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, target.ID, target.Revision, "faq-rollback", "operator"), "completed rollback receipt is idempotent")
			currentFAQ, err := s.chunkRepo.FindFAQChunkWithDuplicateQuestion(ctx, 1, kb.ID, "", []string{"New question"})
			require.NoError(t, err)
			require.NotNil(t, currentFAQ)
			meta, err := currentFAQ.FAQMetadata()
			require.NoError(t, err)
			require.Equal(t, []string{"New answer"}, meta.Answers)
			_, err = s.chunkRepo.GetChunkByID(ctx, 1, later.ID)
			require.NoError(t, err, "rollback preserves entries absent from the old source")
			require.ErrorIs(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "retire-pinned-faq", "operator"), repository.ErrProcessingConflict)
			retireID = second.ID
		}
		retiring, err := r.GetJob(ctx, 1, retireID)
		require.NoError(t, err)
		require.NoError(t, r.PlanRetirement(ctx, 1, retireID, retiring.Revision, "retire-faq-source", "operator"))
		for n := 0; n < 10; n++ {
			retiring, err = r.GetJob(ctx, 1, retireID)
			require.NoError(t, err)
			if retiring.RetirementState == "deleted" {
				break
			}
			require.NoError(t, db.Model(&types.ProcessingStep{}).Where("next_run_at IS NOT NULL").Update("next_run_at", time.Now().Add(-time.Minute)).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, retireID))
			ops, err := r.PendingDeliveries(ctx, 100)
			require.NoError(t, err)
			require.NotEmpty(t, ops)
			for _, op := range ops {
				var ref types.ProcessingRef
				require.NoError(t, json.Unmarshal(op.Payload, &ref))
				lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
				require.NoError(t, err)
				out, err := execute(ctx, *lease)
				require.NoError(t, err)
				require.NotEqual(t, types.ProcessingBlocked, out.Status, "%+v", out)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
			}
		}
		require.Equal(t, "deleted", retiring.RetirementState)
		require.Zero(t, index.deleteCalls, "FAQ retirement must never delete an entire canonical knowledge index")
		still, err := s.chunkRepo.GetChunkByID(ctx, 1, old.ID)
		require.NoError(t, err)
		m, err := still.FAQMetadata()
		require.NoError(t, err)
		require.Equal(t, []string{answer}, m.Answers, "a row absent from v2 remains in append-mode FAQ")
		var manifest types.FAQIndexManifest
		require.NoError(t, json.Unmarshal([]byte(still.FAQIndexManifest), &manifest))
		for _, key := range manifest.SourceIDs {
			require.True(t, still.AcceptsFAQIndex(key))
			require.NotContains(t, index.compensated, key)
		}
		reader, err := s.fileSvc.GetFile(ctx, assetRef)
		require.NoError(t, err, "canonical answer media must survive source retirement")
		require.NoError(t, reader.Close())
		var jobBindings int64
		require.NoError(t, db.Model(&types.ResourceBinding{}).Where("owner_type = ? AND owner_id = ?", "processing_job", retireID).Count(&jobBindings).Error)
		require.Zero(t, jobBindings)
	}
}
