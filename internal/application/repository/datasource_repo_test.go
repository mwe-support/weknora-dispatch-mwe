package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupDataSourceRepoTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{}, &types.ProcessingJob{}, &types.ProcessingStep{}, &types.ProcessingEvent{}, &types.SyncRunItem{}, &types.ProcessingLegacyEvidence{}, &types.ProcessingLegacyDrain{}))
	return db
}

func TestDataSourceRepositoryUpdateSyncStateClearsErrorMessage(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	now := time.Now().UTC()
	result := types.JSON(`{"total":0}`)

	ds := &types.DataSource{
		ID:              "ds-1",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Feishu",
		Type:            types.ConnectorTypeFeishu,
		Status:          types.DataSourceStatusError,
		ErrorMessage:    "previous failure",
	}
	require.NoError(t, repo.Create(context.Background(), ds))

	ds.Status = types.DataSourceStatusActive
	ds.ErrorMessage = ""
	ds.LastSyncAt = &now
	ds.LastSyncResult = result
	require.NoError(t, repo.UpdateSyncState(context.Background(), ds))

	var stored types.DataSource
	require.NoError(t, db.First(&stored, "id = ?", ds.ID).Error)
	assert.Equal(t, types.DataSourceStatusActive, stored.Status)
	assert.Empty(t, stored.ErrorMessage)
	assert.Equal(t, result.ToString(), stored.LastSyncResult.ToString())
	require.NotNil(t, stored.LastSyncAt)
}

func TestDataSourceSettingsCannotOverwriteRetryCursor(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()
	ds := &types.DataSource{ID: "ds-cursor", TenantID: 1, KnowledgeBaseID: "kb", Name: "Before", Type: types.ConnectorTypeTencentDocs, LastSyncCursor: types.JSON(`{"server":"original"}`)}
	require.NoError(t, repo.Create(ctx, ds))
	// Simulate a worker advancing state after a settings form was loaded.
	worker := *ds
	worker.LastSyncCursor = types.JSON(`{"server":"newer"}`)
	require.NoError(t, repo.UpdateSyncState(ctx, &worker))
	ds.Name = "After"
	ds.LastSyncCursor = types.JSON(`{"file_retries":{"injected":{}}}`)
	require.NoError(t, repo.Update(ctx, ds))
	stored, err := repo.FindByID(ctx, ds.ID)
	require.NoError(t, err)
	assert.Equal(t, "After", stored.Name)
	assert.Equal(t, `{"server":"newer"}`, stored.LastSyncCursor.ToString())
}

func TestDataSourceSettingsPersistExplicitFalseAndEmptySchedule(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()
	ds := &types.DataSource{ID: "settings-zero", TenantID: 1, KnowledgeBaseID: "kb", Type: types.ConnectorTypeTencentDocs, Name: "synthetic", SyncDeletions: false}
	require.NoError(t, repo.Create(ctx, ds))
	saved, err := repo.FindByID(ctx, ds.ID)
	require.NoError(t, err)
	require.False(t, saved.SyncDeletions, "explicit false must override the database default")
	ds.SyncSchedule, ds.SyncDeletions = "*/10 * * * * *", true
	require.NoError(t, repo.Update(ctx, ds))
	require.NoError(t, db.Model(ds).Update("last_sync_cursor", types.JSON(`{"server":"new"}`)).Error)
	ds.SyncSchedule, ds.SyncDeletions = "", false
	require.NoError(t, repo.Update(ctx, ds))
	saved, err = repo.FindByID(ctx, ds.ID)
	require.NoError(t, err)
	require.Empty(t, saved.SyncSchedule)
	require.False(t, saved.SyncDeletions)
	require.JSONEq(t, `{"server":"new"}`, string(saved.LastSyncCursor))
}

func TestDataSourceSettingsRejectStaleRenameAndCredentials(t *testing.T) {
	checkDataSourceSettingsConflict(t, setupDataSourceRepoTestDB(t))
}

func checkDataSourceSettingsConflict(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	repo := NewDataSourceRepository(db)
	source := &types.DataSource{ID: "settings-conflict", TenantID: 1, KnowledgeBaseID: "kb", Type: types.ConnectorTypeTencentDocs, Name: "before", SyncSchedule: "* * * * * *", SyncDeletions: true}
	require.NoError(t, repo.Create(ctx, source))
	rename, err := repo.FindByID(ctx, source.ID)
	require.NoError(t, err)
	credentials := *rename
	closing := *rename
	closing.SyncSchedule, closing.SyncDeletions, closing.Status = "", false, types.DataSourceStatusPaused
	require.NoError(t, repo.Update(ctx, &closing))
	rename.Name = "stale rename"
	require.ErrorContains(t, repo.Update(ctx, rename), "settings changed")
	credentials.Config = types.JSON(`{"settings":{"changed":true}}`)
	require.ErrorContains(t, repo.Update(ctx, &credentials), "settings changed")
	current, err := repo.FindByID(ctx, source.ID)
	require.NoError(t, err)
	require.Empty(t, current.SyncSchedule)
	require.False(t, current.SyncDeletions)
	require.Equal(t, types.DataSourceStatusPaused, current.Status)
	require.Equal(t, "before", current.Name)
	// Advancing a worker cursor alone does not invalidate a settings form.
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", source.ID).Update("last_sync_cursor", types.JSON(`{"page":2}`)).Error)
	current.Name = "fresh rename"
	require.NoError(t, repo.Update(ctx, current))
	current, err = repo.FindByID(ctx, source.ID)
	require.NoError(t, err)
	require.Equal(t, "fresh rename", current.Name)
	require.Empty(t, current.SyncSchedule)
	require.False(t, current.SyncDeletions)
	require.JSONEq(t, `{"page":2}`, string(current.LastSyncCursor))
}

func TestDataSourceCheckpointPreservesPauseAndRejectsChangedScope(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()
	ds := &types.DataSource{ID: "source-cas", TenantID: 1, Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive, Config: types.JSON(`{"scope":"old"}`)}
	require.NoError(t, repo.Create(ctx, ds))
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).Update("status", types.DataSourceStatusPaused).Error)
	ds.LastSyncCursor = types.JSON(`{"confirmed":"old"}`)
	require.NoError(t, repo.UpdateSyncState(ctx, ds))
	stored, err := repo.FindByID(ctx, ds.ID)
	require.NoError(t, err)
	require.Equal(t, types.DataSourceStatusPaused, stored.Status)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).Update("config", types.JSON(`{"scope":"new"}`)).Error)
	ds.LastSyncCursor = types.JSON(`{"confirmed":"wrong-scope"}`)
	require.Error(t, repo.UpdateSyncState(ctx, ds))
	stored, err = repo.FindByID(ctx, ds.ID)
	require.NoError(t, err)
	require.Equal(t, `{"confirmed":"old"}`, stored.LastSyncCursor.ToString())
}

func TestDataSourceRepositoryDeleteSoftDeletesOnSQLite(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()

	target := &types.DataSource{
		ID:              "ds-delete-target",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Delete target",
		Type:            types.ConnectorTypeFeishu,
	}
	other := &types.DataSource{
		ID:              "ds-delete-other",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Other data source",
		Type:            types.ConnectorTypeFeishu,
	}
	require.NoError(t, repo.Create(ctx, target))
	require.NoError(t, repo.Create(ctx, other))

	require.NoError(t, repo.Delete(ctx, target.ID))

	var deleted types.DataSource
	require.NoError(t, db.Unscoped().First(&deleted, "id = ?", target.ID).Error)
	assert.True(t, deleted.DeletedAt.Valid)

	found, err := repo.FindByID(ctx, target.ID)
	assert.Error(t, err)
	assert.Nil(t, found)

	untouched, err := repo.FindByID(ctx, other.ID)
	require.NoError(t, err)
	assert.Equal(t, other.ID, untouched.ID)
}

func TestSyncLogRepositoryUpdateResultClearsErrorMessage(t *testing.T) {
	db := setupDataSourceRepoTestDB(t)
	repo := NewSyncLogRepository(db)
	finishedAt := time.Now().UTC()
	result := types.JSON(`{"total":0}`)

	log := &types.SyncLog{
		ID:           "log-1",
		DataSourceID: "ds-1",
		TenantID:     1,
		Status:       types.SyncLogStatusFailed,
		ErrorMessage: "previous failure",
		ItemsTotal:   1,
		ItemsFailed:  1,
	}
	require.NoError(t, repo.Create(context.Background(), log))

	log.Status = types.SyncLogStatusSuccess
	log.ErrorMessage = ""
	log.FinishedAt = &finishedAt
	log.ItemsTotal = 0
	log.ItemsFailed = 0
	log.Result = result
	require.NoError(t, repo.UpdateResult(context.Background(), log))

	var stored types.SyncLog
	require.NoError(t, db.First(&stored, "id = ?", log.ID).Error)
	assert.Equal(t, types.SyncLogStatusSuccess, stored.Status)
	assert.Empty(t, stored.ErrorMessage)
	assert.Zero(t, stored.ItemsTotal)
	assert.Zero(t, stored.ItemsFailed)
	assert.Equal(t, result.ToString(), stored.Result.ToString())
	require.NotNil(t, stored.FinishedAt)
}
