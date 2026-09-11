package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

func (h *ProcessingHandler) legacyIdentity(c *gin.Context) (types.ProcessingLegacyIdentity, bool) {
	ordinal, err := strconv.Atoi(c.Param("error_ordinal"))
	identity := types.ProcessingLegacyIdentity{TenantID: processingTenant(c), KnowledgeBaseID: c.Param("id"), DataSourceID: c.Param("source_id"), RunID: c.Param("run_id"), ErrorOrdinal: ordinal}
	if err != nil || ordinal < 1 || identity.TenantID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "an exact original run and error ordinal are required"})
		return identity, false
	}
	return identity, true
}

func (h *ProcessingHandler) LegacyScope(c *gin.Context) {
	tenant := processingTenant(c)
	source, err := h.repo.LegacySource(c.Request.Context(), tenant, c.Param("id"), c.Param("source_id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "source not found"})
		return
	}
	scope, _, err := repository.ProcessingSourceRevisions(source)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "source configuration unavailable"})
		return
	}
	configuration, err := h.repo.ConfigurationRevision(c.Request.Context(), tenant, source.KnowledgeBaseID)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "processing configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"source_id": source.ID, "knowledge_base_id": source.KnowledgeBaseID, "scope_revision": scope, "configuration_revision": configuration, "enrolled": service.ProcessingLifecycleEnabled(source)})
}

func (h *ProcessingHandler) LegacyError(c *gin.Context) {
	identity, ok := h.legacyIdentity(c)
	if !ok {
		return
	}
	original, err := h.repo.InspectLegacyError(c.Request.Context(), identity)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "original legacy error not found"})
		return
	}
	response := gin.H{"identity": original.Identity, "stage": original.Error.Stage, "category": original.Error.Category, "retry_state": original.Error.RetryState, "evidence_basis": "legacy_unverified", "original_knowledge_id": original.KnowledgeID, "original_source_revision": original.SourceRevision, "original_attempt": original.Attempt}
	if knowledgeID := c.Query("knowledge_id"); knowledgeID != "" {
		attempt, _ := strconv.Atoi(c.Query("attempt"))
		snapshot, err := h.repo.LegacySnapshot(c.Request.Context(), original.Identity, knowledgeID, c.Query("source_revision"), attempt)
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "knowledge does not match the original source identity and attempt"})
			return
		}
		response["snapshot_digest"], response["knowledge_id"], response["chunks"], response["spans"] = snapshot.Digest, knowledgeID, len(snapshot.Chunks), len(snapshot.Spans)
		response["parse_status"], response["enable_status"], response["candidate"] = snapshot.Knowledge.ParseStatus, snapshot.Knowledge.EnableStatus, snapshot.Knowledge.IsDataSourceCandidate()
		// File bytes and index vectors have not been checked by this read.
		response["evidence_basis"] = "legacy_database_snapshot"
	}
	c.JSON(http.StatusOK, response)
}

func (h *ProcessingHandler) LegacyDrain(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	var request types.ProcessingLegacyDrain
	if c.ShouldBindJSON(&request) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a bounded worker and queue attestation is required"})
		return
	}
	request.Actor = processingActor(c)
	if request.Actor == "" {
		return
	}
	request.TenantID, request.KnowledgeBaseID, request.DataSourceID = processingTenant(c), c.Param("id"), c.Param("source_id")
	drain, err := h.repo.RecordLegacyDrain(c.Request.Context(), request)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "worker/queue proof is incomplete, stale or does not match this source"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"drain_id": drain.ID, "expires_at": drain.ExpiresAt, "evidence_basis": "operator_attestation"})
}

func (h *ProcessingHandler) LegacyComplete(c *gin.Context) { h.legacyAction(c, false) }
func (h *ProcessingHandler) LegacyAdopt(c *gin.Context)    { h.legacyAction(c, true) }

func (h *ProcessingHandler) LegacyRetry(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 128<<10)
	var request types.ProcessingLegacyRetry
	if c.ShouldBindJSON(&request) != nil || len(request.Errors) == 0 || len(request.Errors) > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a bounded list of exact original errors and a drained delivery receipt are required"})
		return
	}
	request.Actor = processingActor(c)
	if request.Actor == "" {
		return
	}
	request.TenantID, request.KnowledgeBaseID, request.DataSourceID, request.RunID = processingTenant(c), c.Param("id"), c.Param("source_id"), c.Param("run_id")
	for i := range request.Errors {
		identity := &request.Errors[i]
		identity.TenantID, identity.KnowledgeBaseID, identity.DataSourceID, identity.RunID = request.TenantID, request.KnowledgeBaseID, request.DataSourceID, request.RunID
	}
	job, err := service.BeginLegacyRetry(c.Request.Context(), h.knowledge, h.repo, request)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "original errors, dead-letter delivery or worker drain could not be verified"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "job_id": job.ID, "run_id": job.OriginRunID, "requested_count": len(request.Errors)})
}

func (h *ProcessingHandler) legacyAction(c *gin.Context, adopt bool) {
	identity, ok := h.legacyIdentity(c)
	if !ok {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var request types.ProcessingLegacyEvidence
	if c.ShouldBindJSON(&request) != nil || len(request.ErrorDigest) != 64 || len(request.SnapshotDigest) != 64 || request.KnowledgeID == "" || request.SourceRevision == "" || request.Attempt < 1 || strings.TrimSpace(request.OperationRequestID) == "" || len(request.OperationRequestID) > 128 || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "exact error/snapshot digests, knowledge version/attempt, operation ID and reason are required"})
		return
	}
	request.Actor = processingActor(c)
	if request.Actor == "" {
		return
	}
	identity.ErrorDigest = request.ErrorDigest
	original, err := h.repo.InspectLegacyError(c.Request.Context(), identity)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "the original legacy error changed or is unavailable"})
		return
	}
	request.ProcessingLegacyIdentity = original.Identity
	request.ID, request.JobID, request.ArtifactDigest, request.ConfigurationRevision = "", "", "", ""
	if h.knowledge == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "legacy artifact verification unavailable"})
		return
	}
	if adopt {
		request.Action = "candidate_adopted"
		job, err := service.AdoptLegacyCandidate(c.Request.Context(), h.knowledge, h.repo, request)
		if err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "candidate artifacts, worker drain or source configuration could not be verified"})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"accepted": true, "job_id": job.ID, "knowledge_id": job.KnowledgeID, "generation": job.Generation})
		return
	}
	if request.Action != "late_completion" && request.Action != "manual_confirmed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "choose an exact-attempt completion or an explicit manual confirmation"})
		return
	}
	evidence, err := service.ResolveLegacyCompletion(c.Request.Context(), h.knowledge, h.repo, request)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "the original attempt and its complete artifacts could not be verified"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"evidence_id": evidence.ID, "action": evidence.Action, "knowledge_id": evidence.KnowledgeID})
}
