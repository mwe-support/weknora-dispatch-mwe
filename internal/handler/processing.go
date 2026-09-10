package handler

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type ProcessingHandler struct {
	repo      *repository.ProcessingRepository
	knowledge interfaces.KnowledgeService
}

func NewProcessingHandler(repo *repository.ProcessingRepository, knowledge interfaces.KnowledgeService) *ProcessingHandler {
	return &ProcessingHandler{repo: repo, knowledge: knowledge}
}

func (h *ProcessingHandler) ownedJob(c *gin.Context) *types.ProcessingJob {
	tenant := processingTenant(c)
	if tenant == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "workspace context is required"})
		return nil
	}
	job, err := h.repo.GetJob(c.Request.Context(), tenant, c.Param("job_id"))
	if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && c.Param("id") != "" && job.KnowledgeBaseID != c.Param("id")) {
		c.JSON(http.StatusNotFound, gin.H{"error": "processing job not found"})
		return nil
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "processing state is unavailable"})
		return nil
	}
	return job
}

func (h *ProcessingHandler) Detail(c *gin.Context) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	job, steps, err := h.repo.JobDetail(c.Request.Context(), job.TenantID, job.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "processing state is unavailable"})
		return
	}
	job.AuthRevision = ""
	var document service.ProcessingDocumentSpec
	_ = json.Unmarshal(job.Metadata, &document)
	document.URL = ""
	job.Metadata, job.IndexDestination, job.ActiveIndexManifest = nil, nil, ""
	var external []map[string]string
	for i := range steps {
		if steps[i].Stage == "export_start" && steps[i].ErrorCode == "EXPORT_START_UNCERTAIN" && document.FileID != "" {
			external = append(external, map[string]string{"step_id": steps[i].ID, "file_id": document.FileID, "source_revision": job.SourceRevision,
				"request_digest": fmt.Sprintf("%x", sha256.Sum256([]byte(job.ID+"/"+document.FileID+"/"+job.SourceRevision)))})
		}
		steps[i].Input = nil
		steps[i].CheckpointRef = ""
		steps[i].OutputManifestRef = ""
		steps[i].Result = processingPublicResult(steps[i].Result)
	}
	c.JSON(http.StatusOK, gin.H{"job": job, "document": document, "steps": steps, "external_intents": external})
}

func processingControl(c *gin.Context) (types.ProcessingControlRequest, bool) {
	var request types.ProcessingControlRequest
	if c.ShouldBindJSON(&request) != nil || request.ExpectedRevision < 1 || strings.TrimSpace(request.OperationRequestID) == "" || len(request.OperationRequestID) > 128 || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "revision, operation request ID and a brief reason are required"})
		return request, false
	}
	request.Actor = processingActor(c)
	return request, request.Actor != ""
}

func (h *ProcessingHandler) Rebuild(c *gin.Context) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	request, ok := processingControl(c)
	if !ok {
		return
	}
	if h.knowledge == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "processing rebuild is unavailable"})
		return
	}
	result, err := service.RebuildProcessingVersion(c.Request.Context(), h.knowledge, h.repo, job.TenantID, job.ID, request)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "this version cannot be rebuilt; refresh history and reconcile outstanding external operations first"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "job_id": result.ID, "generation": result.Generation, "operation_request_id": request.OperationRequestID})
}

func (h *ProcessingHandler) Cancel(c *gin.Context) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	request, ok := processingControl(c)
	if !ok {
		return
	}
	if err := h.repo.CancelJob(c.Request.Context(), job.TenantID, job.ID, request); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "this version changed or cannot be canceled; refresh history"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "operation_request_id": request.OperationRequestID})
}

func (h *ProcessingHandler) ResolveExport(c *gin.Context) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	var request types.ProcessingExportResolution
	if c.ShouldBindJSON(&request) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "an exact external intent and operator verification are required"})
		return
	}
	request.Actor = processingActor(c)
	if request.Actor == "" {
		return
	}
	if err := h.repo.ResolveExport(c.Request.Context(), job.TenantID, job.ID, request); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "external verification does not match the recorded intent or its current revision"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "verification": "operator_attestation", "operation_request_id": request.OperationRequestID})
}

func (h *ProcessingHandler) Events(c *gin.Context) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	after, err := strconv.ParseInt(c.DefaultQuery("after", "0"), 10, 64)
	if err != nil || after < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event cursor"})
		return
	}
	events, err := h.repo.ListEvents(c.Request.Context(), job.TenantID, job.ID, after, 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "processing history is unavailable"})
		return
	}
	if len(events) > 0 {
		after = events[len(events)-1].ID
	}
	for i := range events {
		events[i].Detail = processingPublicResult(events[i].Detail)
	}
	c.JSON(http.StatusOK, gin.H{"events": events, "next_after": after})
}

// Projection output is an explicit scalar allowlist; adding a provider field
// to an internal checkpoint must never accidentally publish it through history.
func processingPublicResult(raw types.JSON) types.JSON {
	var input map[string]any
	if json.Unmarshal(raw, &input) != nil {
		return nil
	}
	output := map[string]any{}
	for _, key := range []string{"kind", "pages", "units", "raw_bytes", "requires_export", "chunks", "images", "media_bytes", "characters", "no_text", "requested", "retained", "retired", "bytes", "file_type", "ocr_requested", "documents", "succeeded", "failed", "blocked", "active", "unadmitted", "containers", "links"} {
		value, ok := input[key]
		if !ok {
			continue
		}
		switch item := value.(type) {
		case float64, bool:
			output[key] = item
		case string:
			if len(item) <= 64 && !strings.ContainsAny(item, "/\\:@?=&\n\r") {
				output[key] = item
			}
		}
	}
	if len(output) == 0 {
		return nil
	}
	encoded, _ := json.Marshal(output)
	return types.JSON(encoded)
}

func (h *ProcessingHandler) Retry(c *gin.Context) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	var request struct {
		StepID             string `json:"step_id"`
		ExpectedRevision   int64  `json:"expected_revision"`
		OperationRequestID string `json:"operation_request_id"`
		Reason             string `json:"reason"`
	}
	if c.ShouldBindJSON(&request) != nil || request.StepID == "" || request.ExpectedRevision < 1 || strings.TrimSpace(request.OperationRequestID) == "" || len(request.OperationRequestID) > 128 || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "step, revision, operation request ID and reason are required"})
		return
	}
	actor := processingActor(c)
	if actor == "" {
		return
	}
	err := h.repo.RetryStep(c.Request.Context(), job.TenantID, job.ID, request.StepID, request.ExpectedRevision, request.OperationRequestID, actor, request.Reason)
	if errors.Is(err, repository.ErrProcessingConflict) || errors.Is(err, repository.ErrProcessingScope) {
		c.JSON(http.StatusConflict, gin.H{"error": "processing state or source configuration changed; refresh before retrying"})
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "processing step not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "this stage cannot be retried without resolving its blocking condition"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "operation_request_id": request.OperationRequestID})
}

func processingActor(c *gin.Context) string {
	if scope, ok := types.TenantAPIKeyScopeFromContext(c.Request.Context()); ok && scope.KeyID > 0 {
		return "api_key:" + strconv.FormatUint(scope.KeyID, 10)
	}
	actor := c.GetString(types.UserIDContextKey.String())
	if actor == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "an authenticated operator is required"})
	}
	return actor
}

func (h *ProcessingHandler) Rollback(c *gin.Context) { h.versionControl(c, "rollback") }
func (h *ProcessingHandler) Pin(c *gin.Context)      { h.versionControl(c, "pin") }
func (h *ProcessingHandler) Retire(c *gin.Context)   { h.versionControl(c, "retire") }

func (h *ProcessingHandler) versionControl(c *gin.Context, action string) {
	job := h.ownedJob(c)
	if job == nil {
		return
	}
	var request struct {
		ExpectedRevision   int64  `json:"expected_revision"`
		OperationRequestID string `json:"operation_request_id"`
		Pinned             *bool  `json:"pinned"`
		Reason             string `json:"reason"`
	}
	if c.ShouldBindJSON(&request) != nil || request.ExpectedRevision < 1 || strings.TrimSpace(request.OperationRequestID) == "" || len(request.OperationRequestID) > 128 || (action == "pin" && request.Pinned == nil) || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "revision, operation ID and reason are required; pin changes also require pinned"})
		return
	}
	actor := processingActor(c)
	if actor == "" {
		return
	}
	var err error
	if action == "rollback" {
		if h.knowledge == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "artifact verification is unavailable"})
			return
		}
		err = service.RollbackProcessingVersion(c.Request.Context(), h.knowledge, h.repo, job.TenantID, job.ID, request.ExpectedRevision, request.OperationRequestID, actor, request.Reason)
	} else if action == "retire" {
		err = h.repo.PlanRetirement(c.Request.Context(), job.TenantID, job.ID, request.ExpectedRevision, request.OperationRequestID, actor, request.Reason)
	} else {
		err = h.repo.SetRollbackPin(c.Request.Context(), job.TenantID, job.ID, request.ExpectedRevision, *request.Pinned, request.OperationRequestID, actor, request.Reason)
	}
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "version control could not complete; refresh the processing history before retrying"})
		return
	}
	if action == "retire" {
		c.JSON(http.StatusAccepted, gin.H{"queued": true, "operation_request_id": request.OperationRequestID})
		return
	}
	c.JSON(http.StatusOK, gin.H{"completed": true, "operation_request_id": request.OperationRequestID})
}
