package repository

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func confirmFAQTestManifest(t *testing.T, r interfaces.ChunkRepository, chunk *types.Chunk) {
	t.Helper()
	var manifest types.FAQIndexManifest
	require.NoError(t, json.Unmarshal([]byte(chunk.FAQIndexManifest), &manifest))
	keys, _ := json.Marshal(manifest.SourceIDs)
	destination, _ := json.Marshal(types.ProcessingIndexDestination{KnowledgeType: types.KnowledgeTypeFAQ, Kinds: []types.RetrieverType{types.KeywordsRetrieverType}, EnvironmentDigest: "synthetic"})
	w := types.FAQIndexWrite{ID: manifest.WriteIDs[0], TenantID: chunk.TenantID, KnowledgeBaseID: chunk.KnowledgeBaseID, KnowledgeID: chunk.KnowledgeID, ChunkID: chunk.ID, BaseRevision: chunk.ContentRevision, ContentDigest: manifest.ContentDigest, SourceIDs: keys, Destination: destination}
	require.NoError(t, r.RegisterFAQIndexWrites(context.Background(), []types.FAQIndexWrite{w}))
	require.NoError(t, r.ConfirmFAQIndexWrites(context.Background(), chunk.TenantID, []string{w.ID}))
}

func faqTestStore(t *testing.T) *gorm.DB {
	if os.Getenv("PROCESSING_TEST_POSTGRES") == "" {
		return processingTestStore(t).db
	}
	dsn := os.Getenv("PROCESSING_TEST_POSTGRES")
	require.Contains(t, dsn, "host=lifecycle-pg ")
	require.Contains(t, dsn, "dbname=lifecycle_test ")
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	schema := "faq_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		_ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		raw, _ := admin.DB()
		_ = raw.Close()
	})
	db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 1, Type: types.KnowledgeBaseTypeFAQ}).Error)
	return db
}

func TestProcessingFAQConcurrentCreatesShareCanonicalContainerAndQuestions(t *testing.T) {
	db := faqTestStore(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}))
	ctx := context.Background()
	repo, chunks := NewKnowledgeRepository(db), NewChunkRepository(db)
	var wg sync.WaitGroup
	ids, failures := make(chan string, 8), make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			knowledge := &types.Knowledge{ID: uuid.NewString(), TenantID: 1, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeFAQ}
			failures <- repo.CreateKnowledge(ctx, knowledge)
			ids <- knowledge.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	canonical := ""
	for id := range ids {
		if canonical == "" {
			canonical = id
		}
		require.Equal(t, canonical, id)
	}
	failures = make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &types.Chunk{ID: uuid.NewString(), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: canonical, ChunkType: types.ChunkTypeFAQ}
			if err := c.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: "same question", Answers: []string{"answer"}}); err != nil {
				failures <- err
				return
			}
			failures <- chunks.CreateChunks(ctx, []*types.Chunk{c})
		}()
	}
	wg.Wait()
	close(failures)
	succeeded := 0
	for err := range failures {
		if err == nil {
			succeeded++
		}
	}
	require.Equal(t, 1, succeeded)
	var count int64
	require.NoError(t, db.Model(&types.Chunk{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestProcessingFAQFailedCreateRollbackPreservesConcurrentEdit(t *testing.T) {
	db := faqTestStore(t)
	ctx := context.Background()
	r := NewChunkRepository(db)
	chunk := &types.Chunk{ID: "rollback", TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "canonical", ChunkType: types.ChunkTypeFAQ, Status: int(types.ChunkStatusStored)}
	require.NoError(t, r.CreateChunks(ctx, []*types.Chunk{chunk}))
	stale := *chunk
	chunk.Content = "concurrent edit"
	require.NoError(t, r.SaveChunks(ctx, []*types.Chunk{chunk}))
	require.ErrorIs(t, r.DeleteChunkSnapshot(ctx, &stale), ErrChunkRevisionConflict)
	found, err := r.GetChunkByID(ctx, 1, chunk.ID)
	require.NoError(t, err)
	require.Equal(t, "concurrent edit", found.Content)
	require.NoError(t, r.DeleteChunkSnapshot(ctx, found))
	require.ErrorIs(t, r.DeleteChunkSnapshot(ctx, found), ErrChunkRevisionConflict)
}

func TestProcessingFAQIndexRegistryFencesPublicationAndGarbage(t *testing.T) {
	db := faqTestStore(t)
	require.NoError(t, db.AutoMigrate(&types.FAQIndexWrite{}, &types.Knowledge{}))
	ctx := context.Background()
	r := NewChunkRepository(db)
	require.NoError(t, db.Create(&types.Knowledge{ID: "canonical", TenantID: 1, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeFAQ}).Error)
	chunk := &types.Chunk{ID: "registry", TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "canonical", ChunkType: types.ChunkTypeFAQ, IsEnabled: true, Status: int(types.ChunkStatusIndexed), Content: "synthetic answer"}
	require.NoError(t, r.CreateChunks(ctx, []*types.Chunk{chunk}))
	register := func(attempt string) types.FAQIndexWrite {
		manifest, err := types.VersionFAQIndexes(attempt, chunk, []*types.IndexInfo{{SourceID: "question"}})
		require.NoError(t, err)
		var m types.FAQIndexManifest
		require.NoError(t, json.Unmarshal([]byte(manifest), &m))
		keys, _ := json.Marshal(m.SourceIDs)
		destination, _ := json.Marshal(types.ProcessingIndexDestination{KnowledgeType: types.KnowledgeTypeFAQ, Kinds: []types.RetrieverType{types.KeywordsRetrieverType}, EnvironmentDigest: "synthetic"})
		write := types.FAQIndexWrite{ID: m.WriteIDs[0], TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "canonical", ChunkID: chunk.ID, BaseRevision: chunk.ContentRevision, ContentDigest: m.ContentDigest, SourceIDs: keys, Destination: destination}
		require.NoError(t, r.RegisterFAQIndexWrites(ctx, []types.FAQIndexWrite{write}))
		chunk.FAQIndexManifest = manifest
		return write
	}
	first := register("first")
	require.Error(t, r.SaveChunks(ctx, []*types.Chunk{chunk}), "unacknowledged external writes cannot become visible")
	require.NoError(t, r.ConfirmFAQIndexWrites(ctx, 1, []string{first.ID}))
	require.NoError(t, r.SaveChunks(ctx, []*types.Chunk{chunk}))
	confirmed := chunk.FAQIndexManifest
	chunk.FAQIndexManifest = ""
	require.ErrorIs(t, r.SaveChunks(ctx, []*types.Chunk{chunk}), ErrChunkRevisionConflict, "a managed entry cannot downgrade to accepting legacy indexes")
	chunk.FAQIndexManifest = confirmed
	second := register("second")
	require.NoError(t, r.ConfirmFAQIndexWrites(ctx, 1, []string{second.ID}))
	require.NoError(t, db.Model(&types.FAQIndexWrite{}).Where("tenant_id = ?", 1).Updates(map[string]any{"created_at": time.Now().Add(-8 * 24 * time.Hour), "updated_at": time.Now().Add(-8 * 24 * time.Hour)}).Error)
	garbage, cursor, err := NewProcessingRepository(db).ClaimFAQIndexGarbage(ctx, "", 1)
	require.NoError(t, err)
	require.Empty(t, garbage)
	require.Equal(t, first.ID, cursor, "the cursor must advance across a protected row")
	garbage, _, err = NewProcessingRepository(db).ClaimFAQIndexGarbage(ctx, cursor, 1)
	require.NoError(t, err)
	require.Len(t, garbage, 1, "current entry references survive source-independent cleanup")
	require.Equal(t, second.ID, garbage[0].ID)
	require.Error(t, r.SaveChunks(ctx, []*types.Chunk{chunk}), "a GC claim must exclude a delayed publication")
	require.Error(t, r.ConfirmFAQIndexWrites(ctx, 1, []string{second.ID}))
	require.Error(t, r.FAQIndexGarbageDeleted(ctx, 2, second.ID))
	require.NoError(t, r.FAQIndexGarbageDeleted(ctx, 1, second.ID))
	require.Error(t, r.RegisterFAQIndexWrites(ctx, []types.FAQIndexWrite{second}), "attempt identities cannot be reused after deletion")
	// Keep tombstones reconcilable: a worker can die after a delayed backend write.
	require.NoError(t, db.Model(&types.FAQIndexWrite{}).Where("id = ?", second.ID).Update("updated_at", time.Now().Add(-8*24*time.Hour)).Error)
	garbage, _, err = NewProcessingRepository(db).ClaimFAQIndexGarbage(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, garbage, 1)
	require.Equal(t, second.ID, garbage[0].ID)
}
