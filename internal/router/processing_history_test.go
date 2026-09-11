package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/database"
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type processingHistoryKBLookup struct {
	interfaces.KnowledgeBaseService
	gone bool
}

func (s *processingHistoryKBLookup) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	if s.gone {
		return nil, repository.ErrKnowledgeBaseNotFound
	}
	return &types.KnowledgeBase{ID: "kb", TenantID: 1}, nil
}

func TestProcessingHistoryRoutesRecheckAccessAndRejectScopedGlobalKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, err := gorm.Open(database.SQLite(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.ProcessingJob{}, &types.ProcessingStep{}, &types.ProcessingEvent{}, &repository.ProcessingHistorySnapshot{}, &repository.ProcessingHistoryRow{}))
	// Route tests exercise the real cache/repository and middleware. SQL-view
	// semantics have separate SQLite and PostgreSQL migration tests.
	require.NoError(t, db.Exec(`CREATE VIEW mwe_processing_current_document_lifecycle AS
 SELECT id AS row_id,id AS job_id,tenant_id,knowledge_base_id,datasource_id,external_id,origin_run_id AS run_id,
 status,'' AS stage,external_id AS title,updated_at AS observed_at FROM processing_jobs`).Error)
	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, db.Create(&types.ProcessingJob{ID: id, TenantID: 1, KnowledgeBaseID: "kb", Kind: "document", DataSourceID: "source", ExternalID: id, SourceRevision: "v1", PipelineFingerprint: "p1", Generation: 1}).Error)
	}
	kb := &processingHistoryKBLookup{}
	enabled := true
	g := &rbacGuards{cfg: &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &enabled}}, kbService: kb,
		kbShareService: &downloadKBShareStub{permission: types.OrgRoleEditor, source: 1}}
	h := handler.NewProcessingHandler(repository.NewProcessingRepository(db), nil)
	role, tenant := types.TenantRoleViewer, uint64(1)
	var key *types.TenantAPIKeyScope
	engine := gin.New()
	engine.Use(middleware.ErrorHandler())
	engine.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, tenant)
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		ctx = context.WithValue(ctx, types.UserIDContextKey, "alice")
		if key != nil {
			ctx = types.WithTenantAPIKeyScope(ctx, *key)
		}
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), tenant)
		c.Set(types.UserIDContextKey.String(), "alice")
		c.Next()
	})
	engine.Use(g.ensureAPIKeyAuthorizer().Middleware())
	RegisterProcessingRoutes(engine.Group("/api/v1"), h, g)
	g.assertAPIKeyPoliciesMatchRoutes(engine)
	call := func(method, path string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(`{"reason":"synthetic"}`))
		req.Header.Set("Content-Type", "application/json")
		engine.ServeHTTP(out, req)
		return out
	}
	local := "/api/v1/knowledge-bases/kb/processing/jobs"
	first := call(http.MethodGet, local+"?limit=1")
	require.Equal(t, 200, first.Code, first.Body.String())
	var page repository.ProcessingHistoryPage
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &page))
	require.Equal(t, 3, page.Total)
	next := local + "/snapshots/" + page.ID + "?filter_digest=" + page.FilterDigest + "&after=1&limit=1"
	require.Equal(t, 200, call(http.MethodGet, next).Code)
	for _, action := range []string{"retry", "cancel", "rebuild", "resolve-export", "pin", "rollback", "retire"} {
		require.Equal(t, 403, call(http.MethodPost, local+"/a/"+action).Code, action)
	}
	legacy := "/api/v1/knowledge-bases/kb/processing/legacy/sources/source"
	require.Equal(t, 403, call(http.MethodGet, legacy).Code)
	for _, path := range []string{"/drains", "/runs/original/retry", "/runs/original/errors/1/complete", "/runs/original/errors/1/adopt"} {
		require.Equal(t, 403, call(http.MethodPost, legacy+path).Code)
	}
	require.Equal(t, 403, call(http.MethodGet, "/api/v1/processing/jobs").Code)
	kb.gone = true
	require.Equal(t, 404, call(http.MethodGet, next).Code, "each page must recheck KB access")
	role = types.TenantRoleAdmin
	require.Equal(t, 200, call(http.MethodGet, "/api/v1/processing/jobs/a").Code, "deleted-KB cleanup remains visible to its tenant admin")
	tenant = 2
	require.Equal(t, 404, call(http.MethodGet, "/api/v1/processing/jobs/a").Code)
	kb.gone = false
	require.Equal(t, 200, call(http.MethodGet, local).Code, "shared history remains readable")
	for _, action := range []string{"retry", "cancel", "rebuild", "resolve-export", "pin", "rollback", "retire"} {
		require.Equal(t, 403, call(http.MethodPost, local+"/a/"+action).Code, "receiving-tenant admin cannot manage source lifecycle: "+action)
	}
	tenant = 1
	kb.gone = false
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.DataSource{}, &types.SyncLog{}, &types.Tenant{}))
	require.NoError(t, db.Create(&types.Tenant{ID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.Create(&types.DataSource{ID: "source", TenantID: 1, KnowledgeBaseID: "kb", Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive, Config: types.JSON(`{"credentials":{"token":"SECRET_LEGACY_TOKEN"}}`)}).Error)
	require.NoError(t, db.Create(&types.SyncLog{ID: "original", TenantID: 1, DataSourceID: "source", Result: types.JSON(`{"errors":[{"file_id":"synthetic","stage":"ingest","message":"SECRET_LEGACY_BODY"}]}`)}).Error)
	scope := call(http.MethodGet, legacy)
	require.Equal(t, 200, scope.Code, scope.Body.String())
	require.NotContains(t, scope.Body.String(), "SECRET_")
	row := call(http.MethodGet, legacy+"/runs/original/errors/1")
	require.Equal(t, 200, row.Code, row.Body.String())
	require.NotContains(t, row.Body.String(), "SECRET_")
	tenant = 2
	for _, path := range []string{"/drains", "/runs/original/retry", "/runs/original/errors/1/complete", "/runs/original/errors/1/adopt"} {
		require.Equal(t, 403, call(http.MethodPost, legacy+path).Code)
	}
	require.Equal(t, 403, call(http.MethodGet, legacy).Code)
	tenant = 1
	key = &types.TenantAPIKeyScope{KeyID: 10, Capabilities: types.StringArray{string(types.APIKeyCapabilityManageDataSources)}, KnowledgeBaseIDs: types.StringArray{"kb"}}
	require.Equal(t, 200, call(http.MethodGet, local).Code)
	require.Equal(t, 403, call(http.MethodGet, "/api/v1/processing/jobs").Code)
	key.FullAccess = true
	require.Equal(t, 403, call(http.MethodGet, "/api/v1/processing/jobs").Code, "full capability still has a restricted KB scope")
	key.KnowledgeBaseIDs = nil
	result := call(http.MethodGet, "/api/v1/processing/jobs?limit=1")
	require.Equal(t, 200, result.Code)
	require.NoError(t, json.Unmarshal(result.Body.Bytes(), &page))
	globalNext := "/api/v1/processing/jobs/snapshots/" + page.ID + "?filter_digest=" + page.FilterDigest + "&after=1"
	require.Equal(t, 200, call(http.MethodGet, globalNext).Code)
	key.KeyID = 11
	require.Equal(t, 404, call(http.MethodGet, globalNext).Code, "keys owned by the same user cannot share a snapshot")
	key.FullAccess = false
	key.Capabilities = types.StringArray{string(types.APIKeyCapabilityRetrieve)}
	require.Equal(t, 403, call(http.MethodGet, legacy).Code)
	require.Equal(t, 403, call(http.MethodPost, legacy+"/drains").Code)
}
