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
	"github.com/hibiken/asynq"
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
	called            bool
	tenantID          uint64
	existingKnowledge *types.Knowledge
	reparseCalled     bool
	moveFolderCalled  bool
}

func (s *uploadKnowledgeServiceStub) CheckKnowledgeDeletion(context.Context, uint64, []string) error {
	return nil
}

func (s *uploadKnowledgeServiceStub) GetKnowledgeBatch(
	_ context.Context,
	_ uint64,
	ids []string,
) ([]*types.Knowledge, error) {
	if s.existingKnowledge != nil && len(ids) == 1 && ids[0] == s.existingKnowledge.ID {
		return []*types.Knowledge{s.existingKnowledge}, nil
	}
	return nil, nil
}

func (s *uploadKnowledgeServiceStub) MoveKnowledgeToFolder(
	_ context.Context,
	_ string,
	_ []string,
	_ string,
) (int64, error) {
	s.moveFolderCalled = true
	return 1, nil
}

func (s *uploadKnowledgeServiceStub) ReparseKnowledge(
	_ context.Context,
	_ string,
	_ *types.KnowledgeProcessOverrides,
) (*types.Knowledge, error) {
	s.reparseCalled = true
	return s.existingKnowledge, nil
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

func (s *uploadKnowledgeServiceStub) GetKnowledgeByIDOnly(
	_ context.Context,
	id string,
) (*types.Knowledge, error) {
	if s.existingKnowledge != nil && s.existingKnowledge.ID == id {
		return s.existingKnowledge, nil
	}
	return nil, apprepo.ErrKnowledgeNotFound
}

func (s *uploadKnowledgeServiceStub) GetOwningKBCreatorID(
	_ context.Context,
	knowledgeID string,
) (string, error) {
	if s.existingKnowledge != nil && s.existingKnowledge.ID == knowledgeID {
		return "support-user", nil
	}
	return "", apprepo.ErrKnowledgeNotFound
}

type uploadTaskEnqueuerStub struct {
	called bool
}

func (s *uploadTaskEnqueuerStub) Enqueue(
	_ *asynq.Task,
	_ ...asynq.Option,
) (*asynq.TaskInfo, error) {
	s.called = true
	return &asynq.TaskInfo{ID: "content-task"}, nil
}

func newKnowledgeUploadTestEngine(
	t *testing.T,
	role types.TenantRole,
) (*gin.Engine, *uploadKnowledgeServiceStub, *uploadTaskEnqueuerStub) {
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
	knowledgeService := &uploadKnowledgeServiceStub{
		existingKnowledge: &types.Knowledge{
			ID:              "existing-document",
			KnowledgeBaseID: kbID,
			TenantID:        tenantID,
		},
	}
	taskEnqueuer := &uploadTaskEnqueuerStub{}
	knowledgeHandler := handler.NewKnowledgeHandler(
		cfg,
		knowledgeService,
		kbService,
		nil,
		nil,
		taskEnqueuer,
		nil,
	)
	guards := &rbacGuards{
		cfg:              cfg,
		kbService:        kbService,
		knowledgeService: knowledgeService,
		kbCreator: func(_ *gin.Context) (string, error) {
			return ownerUserID, nil
		},
		knowledgeKBCreator: func(_ *gin.Context) (string, error) {
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
	return engine, knowledgeService, taskEnqueuer
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
	engine, knowledgeService, _ := newKnowledgeUploadTestEngine(t, types.TenantRoleContributor)
	recorder := performKnowledgeUpload(t, engine)

	require.Equal(t, http.StatusOK, recorder.Code, "body=%s", recorder.Body.String())
	require.True(t, knowledgeService.called, "upload must reach the knowledge service")
	require.Equal(t, tenantID, knowledgeService.tenantID, "upload must execute in the KB tenant")
}

func TestWorkspaceViewerCannotUploadFileToExistingKnowledgeBase(t *testing.T) {
	engine, knowledgeService, _ := newKnowledgeUploadTestEngine(t, types.TenantRoleViewer)
	recorder := performKnowledgeUpload(t, engine)

	require.Equal(t, http.StatusForbidden, recorder.Code, "body=%s", recorder.Body.String())
	require.False(t, knowledgeService.called, "viewer upload must stop before the knowledge service")
}

func TestInvitedContributorCanDeleteExistingKnowledgeDocument(t *testing.T) {
	engine, _, taskEnqueuer := newKnowledgeUploadTestEngine(t, types.TenantRoleContributor)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/knowledge/existing-document", nil)
	engine.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, "body=%s", recorder.Body.String())
	require.True(t, taskEnqueuer.called, "document delete must reach the content task queue")
}

func TestInvitedContributorCanReparseExistingKnowledgeDocument(t *testing.T) {
	engine, knowledgeService, _ := newKnowledgeUploadTestEngine(t, types.TenantRoleContributor)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/knowledge/existing-document/reparse", nil)
	engine.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, "body=%s", recorder.Body.String())
	require.True(t, knowledgeService.reparseCalled, "reparse must reach the knowledge service")
}

func TestInvitedContributorCanBatchDeleteKnowledgeDocuments(t *testing.T) {
	engine, _, taskEnqueuer := newKnowledgeUploadTestEngine(t, types.TenantRoleContributor)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/knowledge/batch-delete",
		bytes.NewBufferString(`{"kb_id":"existing-kb","ids":["existing-document"]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, "body=%s", recorder.Body.String())
	require.True(t, taskEnqueuer.called, "batch delete must reach the content task queue")
}

func TestInvitedContributorCanMoveKnowledgeDocumentToFolder(t *testing.T) {
	engine, knowledgeService, _ := newKnowledgeUploadTestEngine(t, types.TenantRoleContributor)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/knowledge/folder",
		bytes.NewBufferString(`{"kb_id":"existing-kb","knowledge_ids":["existing-document"],"folder_path":"归档"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, "body=%s", recorder.Body.String())
	require.True(t, knowledgeService.moveFolderCalled, "folder move must reach the knowledge service")
}
