package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestDataSourceCandidateMetadataTransitionsAreConditional(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}))
	now := time.Now()
	k := &types.Knowledge{ID: "candidate", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing, ProcessedAt: &now, Metadata: types.JSON(`{"datasource_version":"version","datasource_candidate":"true","keep":"value"}`)}
	require.NoError(t, db.Create(k).Error)
	r := &knowledgeRepository{db: db}
	ctx := context.Background()
	require.NoError(t, r.MarkDataSourceIndexReady(ctx, k.ID))
	got, err := r.GetKnowledgeByIDOnly(ctx, k.ID)
	require.NoError(t, err)
	require.Equal(t, "true", got.GetMetadata()["datasource_index_ready"])
	stale := *got
	// Simulate publication racing with a second readiness delivery.
	require.NoError(t, db.Model(k).Update("metadata", types.JSON(`{"datasource_version":"version","keep":"value"}`)).Error)
	require.NoError(t, r.MarkDataSourceIndexReady(ctx, k.ID))
	require.NoError(t, r.MarkDataSourceSubtaskFailed(ctx, k.ID, "wiki"))
	stale.Description = "slow summary result"
	require.NoError(t, r.UpdateKnowledge(ctx, &stale))
	got, err = r.GetKnowledgeByIDOnly(ctx, k.ID)
	require.NoError(t, err)
	require.False(t, got.IsDataSourceCandidate())
	require.Empty(t, got.GetMetadata()["datasource_index_ready"])
	require.Equal(t, "wiki", got.GetMetadata()["datasource_processing_failed"])
	require.Equal(t, "value", got.GetMetadata()["keep"])
}
