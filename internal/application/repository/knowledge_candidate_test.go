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

func TestSourcePublicationIsReadyScopedAndMonotonic(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	raw, _ := db.DB()
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.DataSource{}))
	ds := &types.DataSource{ID: "source", TenantID: 1, KnowledgeBaseID: "kb", TencentFileSync: true, Status: types.DataSourceStatusActive}
	require.NoError(t, db.Create(ds).Error)
	r := &knowledgeRepository{db: db}
	now := time.Now().UTC()
	k := &types.Knowledge{ID: "file", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusPending, EnableStatus: "disabled", CreatedAt: now, Metadata: types.JSON(`{"datasource_id":"source","external_id":"file","datasource_version":"v1","datasource_candidate":"true","datasource_async_publish":"true"}`)}
	require.NoError(t, db.Create(k).Error)
	_, err = r.PublishSourceCandidate(context.Background(), ds, k.ID)
	require.Error(t, err, "submission alone cannot publish")
	require.NoError(t, db.Model(k).Updates(map[string]any{"parse_status": types.ParseStatusProcessing, "processed_at": now}).Error)
	require.NoError(t, db.Model(ds).Update("status", types.DataSourceStatusPaused).Error)
	_, err = r.PublishSourceCandidate(context.Background(), ds, k.ID)
	require.Error(t, err, "pause fences publication")
	require.NoError(t, db.Model(ds).Update("status", types.DataSourceStatusActive).Error)
	ok, err := r.PublishSourceCandidate(context.Background(), ds, k.ID)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := r.GetKnowledgeByIDOnly(context.Background(), k.ID)
	require.NoError(t, err)
	require.False(t, got.IsDataSourceCandidate())
	require.Equal(t, "enabled", got.EnableStatus)
	// A slow older candidate must not overtake a newly submitted file version.
	older := *k
	older.ID, older.CreatedAt, older.ProcessedAt, older.ParseStatus = "older", now.Add(-time.Minute), &now, types.ParseStatusProcessing
	require.NoError(t, db.Create(&older).Error)
	ok, err = r.PublishSourceCandidate(context.Background(), ds, older.ID)
	require.NoError(t, err)
	require.False(t, ok)
	got, err = r.GetKnowledgeByIDOnly(context.Background(), older.ID)
	require.NoError(t, err)
	require.Equal(t, types.ParseStatusCancelled, got.ParseStatus)
}
