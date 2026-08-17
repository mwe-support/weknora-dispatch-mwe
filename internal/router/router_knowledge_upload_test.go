package router

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type uploadKnowledgeBaseServiceStub struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *uploadKnowledgeBaseServiceStub) GetKnowledgeBaseByID(
	_ context.Context,
	id string,
) (*types.KnowledgeBase, error) {
	if s.kb != nil && s.kb.ID == id {
		return s.kb, nil
	}
	return nil, apprepo.ErrKnowledgeBaseNotFound
}

type uploadKnowledgeServiceStub struct {
	interfaces.KnowledgeService
	called   bool
	tenantID uint64
}

func (s *uploadKnowledgeServiceStub) CreateKnowledgeFromFile(
	ctx context.Context,
	kbID string,
	_ *multipart.FileHeader,
	_ map[string]string,
	_ *bool,
	_ string,
	_ []string,
	_ string,
	_ *types.KnowledgeProcessOverrides,
) (*types.Knowledge, error) {
	s.called = true
	s.tenantID, _ = types.TenantIDFromContext(ctx)
	return &types.Knowledge{
		ID:              "uploaded-by-editor",
		KnowledgeBaseID: kbID,
		TenantID:        s.tenantID,
		Title:           "editor-upload.txt",
	}, nil
}

func newKnowledgeUploadTestEngine(
	t *testing.T,
	role types.TenantRole,
) (*gin.Engine, *uploadKnowledgeServiceStub) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	const (
		tenantID     = uint64(10001)
		kbID         = "existing-kb"
		ownerUserID  = "support-user"
		callerUserID = "invited-member"
	)
	enabled := true
	cfg := &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &enabled}}
	kb := &types.KnowledgeBase{
		ID:        kbID,
		TenantID:  tenantID,
		CreatorID: ownerUserID,
	}
	kbService := &uploadKnowledgeBaseServiceStub{kb: kb}
	knowledgeService := &uploadKnowledgeServiceStub{}
	knowledgeHandler := handler.NewKnowledgeHandler(
		cfg,
		knowledgeService,
		kbService,
		nil,
		nil,
		nil,
		nil,
	)
	guards := &rbacGuards{
		cfg:       cfg,
		kbService: kbService,
		kbCreator: func(_ *gin.Context) (string, error) {
			return ownerUserID, nil
		},
	}

	engine := gin.New()
	engine.Use(middleware.ErrorHandler())
	engine.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, tenantID)
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		ctx = context.WithValue(ctx, types.UserIDContextKey, callerUserID)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), tenantID)
		c.Set(types.UserIDContextKey.String(), callerUserID)
		c.Next()
	})
	RegisterKnowledgeRoutes(engine.Group("/api/v1"), knowledgeHandler, guards)
	return engine, knowledgeService
}

func performKnowledgeUpload(t *testing.T, engine *gin.Engine) *httptest.ResponseRecorder {
	t.Helper()

	const kbID = "existing-kb"

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "editor-upload.txt")
	require.NoError(t, err)
	_, err = part.Write([]byte("content uploaded by an invited editor"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/knowledge-bases/"+kbID+"/knowledge/file",
		&body,
	)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	engine.ServeHTTP(recorder, request)
	return recorder
}

func TestInvitedContributorCanUploadFileToExistingKnowledgeBase(t *testing.T) {
	const tenantID = uint64(10001)
	engine, knowledgeService := newKnowledgeUploadTestEngine(t, types.TenantRoleContributor)
	recorder := performKnowledgeUpload(t, engine)

	require.Equal(t, http.StatusOK, recorder.Code, "body=%s", recorder.Body.String())
	require.True(t, knowledgeService.called, "upload must reach the knowledge service")
	require.Equal(t, tenantID, knowledgeService.tenantID, "upload must execute in the KB tenant")
}

func TestWorkspaceViewerCannotUploadFileToExistingKnowledgeBase(t *testing.T) {
	engine, knowledgeService := newKnowledgeUploadTestEngine(t, types.TenantRoleViewer)
	recorder := performKnowledgeUpload(t, engine)

	require.Equal(t, http.StatusForbidden, recorder.Code, "body=%s", recorder.Body.String())
	require.False(t, knowledgeService.called, "viewer upload must stop before the knowledge service")
}
