package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSaveChunkRevisionIsAtomicAndOptimistic(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}, &types.ChunkRevision{}))
	repo := NewChunkRepository(db)
	ctx := context.Background()
	now := time.Now()
	chunk := &types.Chunk{
		ID: uuid.NewString(), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "knowledge",
		Content: "before", SourceContent: "before", ChunkType: types.ChunkTypeText,
		IsEnabled: true, IndexStatus: "ready", CreatedAt: now, UpdatedAt: now,
	}
	require.NoError(t, repo.CreateChunks(ctx, []*types.Chunk{chunk}))

	snapshot := &types.ChunkRevision{
		ID: uuid.NewString(), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "knowledge",
		ChunkID: chunk.ID, Revision: 0, Content: "before", IsEnabled: true,
		EditSource: "user", EditedAt: now, CreatedAt: now,
	}
	chunk.Content = "after"
	chunk.ContentRevision = 1
	require.NoError(t, repo.SaveChunkRevision(ctx, chunk, snapshot, 0))

	stored, err := repo.GetChunkByID(ctx, 1, chunk.ID)
	require.NoError(t, err)
	require.Equal(t, "after", stored.Content)
	require.Equal(t, 1, stored.ContentRevision)
	revisions, err := repo.ListChunkRevisions(ctx, 1, chunk.ID)
	require.NoError(t, err)
	require.Len(t, revisions, 1)
	require.Equal(t, "before", revisions[0].Content)

	stale := *chunk
	stale.Content = "stale write"
	stale.ContentRevision = 1
	staleSnapshot := *snapshot
	staleSnapshot.ID = uuid.NewString()
	require.ErrorIs(t, repo.SaveChunkRevision(ctx, &stale, &staleSnapshot, 0), ErrChunkRevisionConflict)

	stored, err = repo.GetChunkByID(ctx, 1, chunk.ID)
	require.NoError(t, err)
	require.Equal(t, "after", stored.Content)
	count := int64(0)
	require.NoError(t, db.Model(&types.ChunkRevision{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.False(t, errors.Is(gorm.ErrRecordNotFound, ErrChunkRevisionConflict))
}

func TestProcessingFAQSharedWritesRejectStaleAndDeletedSnapshots(t *testing.T) {
	db := faqTestStore(t)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}))
	repo := NewChunkRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	makeFAQ := func(question string) *types.Chunk {
		c := &types.Chunk{ID: uuid.NewString(), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "faq", ChunkType: types.ChunkTypeFAQ, Content: question, IsEnabled: true}
		require.NoError(t, c.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: question, Answers: []string{"answer"}}))
		require.NoError(t, repo.CreateChunks(ctx, []*types.Chunk{c}))
		return c
	}
	first, second := makeFAQ("one"), makeFAQ("two")
	stale := *first
	first.Content = "manual edit"
	require.NoError(t, repo.UpdateChunk(ctx, first))
	require.Equal(t, stale.ContentRevision+1, first.ContentRevision)
	stale.Content = "late source merge"
	require.ErrorIs(t, repo.UpdateChunk(ctx, &stale), ErrChunkRevisionConflict)
	second.Content = "must roll back"
	require.ErrorIs(t, repo.SaveChunks(ctx, []*types.Chunk{second, &stale}), ErrChunkRevisionConflict)
	stored, err := repo.GetChunkByID(ctx, 1, second.ID)
	require.NoError(t, err)
	require.Equal(t, "two", stored.Content)
	require.Equal(t, stored.ContentRevision, second.ContentRevision, "failed batch must not advance in-memory revisions")

	beforeFlags := *first
	require.NoError(t, repo.UpdateChunkFlagsBatch(ctx, 1, "kb", map[string]types.ChunkFlags{first.ID: types.ChunkFlagRecommended}, nil))
	require.ErrorIs(t, repo.UpdateChunk(ctx, &beforeFlags), ErrChunkRevisionConflict)
	first, err = repo.GetChunkByID(ctx, 1, first.ID)
	require.NoError(t, err)
	beforeTag := *first
	newTag := "manual-tag"
	_, err = repo.UpdateChunkFieldsByTagID(ctx, 1, "kb", "", nil, 0, 0, &newTag, []string{second.ID})
	require.NoError(t, err)
	require.ErrorIs(t, repo.UpdateChunks(ctx, []*types.Chunk{&beforeTag}), ErrChunkRevisionConflict)
	first, err = repo.GetChunkByID(ctx, 1, first.ID)
	require.NoError(t, err)
	wrongScope := *first
	wrongScope.TenantID = 2
	require.Error(t, repo.UpdateChunk(ctx, &wrongScope))
	require.NoError(t, repo.DeleteChunk(ctx, 1, first.ID))
	require.Error(t, repo.UpdateChunk(ctx, first))
	var count int64
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", first.ID).Count(&count).Error)
	require.Zero(t, count, "late writer cannot resurrect a deleted FAQ")
}
