package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupTestDB creates an in-memory SQLite database with tenant table.
func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.TenantMember{}))
	return db
}

func TestDeleteTenant_SoftDeletesMemberships(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	repo := NewTenantRepository(db)

	tenant := &types.Tenant{Name: "gone", Status: "active"}
	require.NoError(t, db.Create(tenant).Error)

	member := &types.TenantMember{
		UserID:   "user-1",
		TenantID: tenant.ID,
		Role:     types.TenantRoleOwner,
		Status:   types.TenantMemberStatusActive,
	}
	require.NoError(t, db.Create(member).Error)

	require.NoError(t, repo.DeleteTenant(ctx, tenant.ID))

	var tenantCount int64
	require.NoError(t, db.Model(&types.Tenant{}).Count(&tenantCount).Error)
	assert.Equal(t, int64(0), tenantCount)

	var memberCount int64
	require.NoError(t, db.Model(&types.TenantMember{}).Count(&memberCount).Error)
	assert.Equal(t, int64(0), memberCount)

	// Unscoped: rows still exist but are soft-deleted.
	var rawTenantCount int64
	require.NoError(t, db.Unscoped().Model(&types.Tenant{}).Count(&rawTenantCount).Error)
	assert.Equal(t, int64(1), rawTenantCount)

	var rawMemberCount int64
	require.NoError(t, db.Unscoped().Model(&types.TenantMember{}).Count(&rawMemberCount).Error)
	assert.Equal(t, int64(1), rawMemberCount)
}

func TestInheritedParserDefaultsAreNotPersistedByTenantUpdates(t *testing.T) {
	t.Setenv(types.DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	db := setupTestDB(t)
	ctx := context.Background()
	repo := NewTenantRepository(db)

	tenant := &types.Tenant{Name: "inherited", Status: "active"}
	require.NoError(t, repo.CreateTenant(ctx, tenant))
	require.Equal(t, "http://mineru-api:8000", tenant.ParserEngineConfig.MinerUEndpoint)

	tenant.Name = "renamed"
	require.NoError(t, repo.UpdateTenant(ctx, tenant))
	require.NoError(t, repo.AdjustStorageUsed(ctx, tenant.ID, 128))

	var persisted types.Tenant
	require.NoError(t, db.Session(&gorm.Session{SkipHooks: true}).First(&persisted, tenant.ID).Error)
	require.Nil(t, persisted.ParserEngineConfig)
	require.Equal(t, int64(128), persisted.StorageUsed)

	loaded, err := repo.GetTenantByID(ctx, tenant.ID)
	require.NoError(t, err)
	require.Equal(t, "http://mineru-api:8000", loaded.ParserEngineConfig.MinerUEndpoint)
}

func TestExplicitParserOverrideIsPersisted(t *testing.T) {
	t.Setenv(types.DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	db := setupTestDB(t)
	ctx := context.Background()
	repo := NewTenantRepository(db)

	tenant := &types.Tenant{Name: "explicit", Status: "active"}
	require.NoError(t, repo.CreateTenant(ctx, tenant))
	tenant.ParserEngineConfig.MinerUEndpoint = "http://workspace-mineru:8000"
	tenant.MarkParserEngineConfigExplicit()
	require.NoError(t, repo.UpdateTenant(ctx, tenant))

	var persisted types.Tenant
	require.NoError(t, db.Session(&gorm.Session{SkipHooks: true}).First(&persisted, tenant.ID).Error)
	require.Equal(t, "http://workspace-mineru:8000", persisted.ParserEngineConfig.MinerUEndpoint)
}
