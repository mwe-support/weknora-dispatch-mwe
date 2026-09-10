package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type processingDeleteService struct{ interfaces.KnowledgeService }

func (processingDeleteService) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return &types.Knowledge{ID: "k1", TenantID: 42, KnowledgeBaseID: "kb1"}, nil
}
func (s processingDeleteService) GetKnowledgeBatch(ctx context.Context, _ uint64, _ []string) ([]*types.Knowledge, error) {
	k, err := s.GetKnowledgeByIDOnly(ctx, "k1")
	return []*types.Knowledge{k}, err
}
func (s processingDeleteService) ListKnowledgeByKnowledgeBaseID(ctx context.Context, _ string) ([]*types.Knowledge, error) {
	return s.GetKnowledgeBatch(ctx, 42, nil)
}
func (processingDeleteService) CheckKnowledgeDeletion(context.Context, uint64, []string) error {
	return fmt.Errorf("held: %w", repository.ErrProcessingConflict)
}

type processingDeleteKBService struct {
	interfaces.KnowledgeBaseService
}

func (processingDeleteKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{ID: "kb1", TenantID: 42, Name: "synthetic"}, nil
}
func (processingDeleteKBService) DeleteKnowledgeBase(context.Context, string) error {
	return fmt.Errorf("held: %w", repository.ErrProcessingConflict)
}

func TestProcessingDeletePinReturns409BeforeQueueing(t *testing.T) {
	r := gin.New()
	r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(42))
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleOwner)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(42))
		c.Next()
	})
	h := &KnowledgeHandler{kgService: processingDeleteService{}, kbService: processingDeleteKBService{}}
	kb := &KnowledgeBaseHandler{service: processingDeleteKBService{}}
	r.DELETE("/knowledge/:id", h.DeleteKnowledge)
	r.POST("/knowledge/batch-delete", h.BatchDeleteKnowledge)
	r.DELETE("/knowledge-bases/:id/knowledge", h.ClearKnowledgeBaseContents)
	r.DELETE("/knowledge-bases/:id", kb.DeleteKnowledgeBase)
	for _, test := range []struct{ method, path, body string }{
		{http.MethodDelete, "/knowledge/k1", ""},
		{http.MethodPost, "/knowledge/batch-delete", `{"kb_id":"kb1","ids":["k1"]}`},
		{http.MethodDelete, "/knowledge-bases/kb1/knowledge", ""},
		{http.MethodDelete, "/knowledge-bases/kb1", ""},
	} {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		req.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		r.ServeHTTP(out, req)
		require.Equal(t, http.StatusConflict, out.Code, out.Body.String())
		require.Contains(t, out.Body.String(), "processing history")
	}
}
