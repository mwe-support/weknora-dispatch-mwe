package handler

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func processingTenant(c *gin.Context) uint64 {
	if tenant, ok := types.TenantIDFromContext(c.Request.Context()); ok {
		return tenant
	}
	return c.GetUint64(types.TenantIDContextKey.String())
}

func (h *ProcessingHandler) GlobalScope(c *gin.Context) {
	if scope, ok := types.TenantAPIKeyScopeFromContext(c.Request.Context()); ok && (!scope.FullAccess || scope.IsKnowledgeBaseRestricted()) {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "global processing history requires unrestricted workspace access"})
		return
	}
	c.Next()
}

func processingHistoryPrincipal(c *gin.Context) string {
	// A keyed request remains bound to its key even when middleware also fills
	// an owner user ID. A second key must not replay another key's snapshot.
	if scope, ok := types.TenantAPIKeyScopeFromContext(c.Request.Context()); ok && scope.KeyID > 0 {
		return "api_key:" + strconv.FormatUint(scope.KeyID, 10)
	}
	if user := c.GetString(types.UserIDContextKey.String()); user != "" {
		return "user:" + user
	}
	c.JSON(http.StatusForbidden, gin.H{"error": "an authenticated reader is required"})
	return ""
}

func (h *ProcessingHandler) History(c *gin.Context) {
	principal := processingHistoryPrincipal(c)
	if principal == "" {
		return
	}
	tenant := processingTenant(c)
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil || limit < 1 || limit > 100 {
		processingHistoryError(c, repository.ErrProcessingHistoryFilter)
		return
	}
	filter := repository.ProcessingHistoryFilter{View: c.DefaultQuery("view", "current_document_lifecycle"), SourceID: c.Query("source_id"), RunID: c.Query("run_id"), JobID: c.Query("job_id"), Status: c.Query("status"), Stage: c.Query("stage"), Search: c.Query("search")}
	for _, item := range []struct {
		name   string
		target **time.Time
	}{{"from", &filter.From}, {"to", &filter.To}} {
		if raw := c.Query(item.name); raw != "" {
			parsed, e := time.Parse(time.RFC3339, raw)
			if e != nil {
				processingHistoryError(c, repository.ErrProcessingHistoryFilter)
				return
			}
			*item.target = &parsed
		}
	}
	result, err := h.repo.CreateHistorySnapshot(c.Request.Context(), tenant, principal, c.Param("id"), filter, limit)
	if err != nil {
		processingHistoryError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *ProcessingHandler) HistoryPage(c *gin.Context) {
	principal := processingHistoryPrincipal(c)
	if principal == "" {
		return
	}
	after, err := strconv.Atoi(c.DefaultQuery("after", "0"))
	if err != nil {
		processingHistoryError(c, repository.ErrProcessingHistoryFilter)
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil {
		processingHistoryError(c, repository.ErrProcessingHistoryFilter)
		return
	}
	result, err := h.repo.HistoryPage(c.Request.Context(), processingTenant(c), principal, c.Param("id"), c.Param("snapshot_id"), c.Query("filter_digest"), after, limit)
	if err != nil {
		processingHistoryError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

func processingHistoryError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repository.ErrProcessingHistoryExpired):
		c.JSON(http.StatusGone, gin.H{"code": "SNAPSHOT_EXPIRED", "error": "history snapshot expired; refresh the list"})
	case errors.Is(err, repository.ErrProcessingHistoryLimit):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"code": "FILTER_REQUIRED", "error": "too many records for a complete snapshot; narrow the source, date or status filters"})
	case errors.Is(err, repository.ErrProcessingHistoryFilter):
		c.JSON(http.StatusBadRequest, gin.H{"code": "INVALID_FILTER", "error": "invalid history filter or page cursor"})
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "history snapshot not found"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": "HISTORY_UNAVAILABLE", "error": "history is temporarily unavailable; no empty result was inferred"})
	}
}
