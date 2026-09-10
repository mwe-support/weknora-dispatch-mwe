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

type processingFAQConflictService struct {
	interfaces.KnowledgeService
	failure error
}

func (s processingFAQConflictService) UpdateFAQEntry(context.Context, string, int64, *types.FAQEntryPayload) (*types.FAQEntry, error) {
	return nil, fmt.Errorf("wrapped: %w", s.failure)
}

func TestProcessingFAQConflictReturnsHTTP409(t *testing.T) {
	for _, failure := range []error{repository.ErrChunkRevisionConflict, repository.ErrFAQQuestionConflict} {
		r := gin.New()
		r.Use(middleware.ErrorHandler())
		h := NewFAQHandler(processingFAQConflictService{failure: failure}, nil)
		r.PUT("/kb/:id/faq/:entry_id", h.UpdateEntry)
		request := httptest.NewRequest(http.MethodPut, "/kb/test/faq/1", strings.NewReader(`{"standard_question":"Synthetic question","answers":["Synthetic answer"]}`))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		r.ServeHTTP(response, request)
		require.Equal(t, http.StatusConflict, response.Code)
		require.Contains(t, response.Body.String(), `"success":false`)
	}
}
