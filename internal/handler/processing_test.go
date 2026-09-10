package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestProcessingHandlerRetryScopesRevisionAndReceipt(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.DataSource{}, &types.ProcessingJob{}, &types.ProcessingStep{}, &types.ProcessingEvent{}, &types.TaskPendingOp{}, &types.ProcessingArtifactReference{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.Create(&types.DataSource{ID: "source", TenantID: 1, KnowledgeBaseID: "kb", Status: types.DataSourceStatusActive}).Error)
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "pipeline"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "v1", Input: types.JSON(`{"private":"DO-NOT-EXPOSE"}`), RequiredForCompletion: true}}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	step := steps[0]
	lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "transient", ErrorCode: "SYNTHETIC", CheckpointRef: "DO-NOT-EXPOSE-CHECKPOINT", Result: types.JSON(`{"body":"DO-NOT-EXPOSE-BODY","pages":7,"kind":"https://DO-NOT-EXPOSE"}`)}))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	h := NewProcessingHandler(r, nil)
	tenant, actor := uint64(1), "operator"
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), tenant)
		c.Set(types.UserIDContextKey.String(), actor)
		c.Next()
	})
	engine.GET("/kb/:id/jobs/:job_id", h.Detail)
	engine.POST("/kb/:id/jobs/:job_id/retry", h.Retry)
	engine.POST("/kb/:id/jobs/:job_id/pin", h.Pin)
	engine.POST("/kb/:id/jobs/:job_id/rollback", h.Rollback)
	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		engine.ServeHTTP(out, req)
		return out
	}
	path := "/kb/kb/jobs/" + job.ID
	view := request(http.MethodGet, path, "")
	require.Equal(t, 200, view.Code)
	require.NotContains(t, view.Body.String(), "DO-NOT-EXPOSE")
	require.Contains(t, view.Body.String(), `"pages":7`)
	body, _ := json.Marshal(map[string]any{"step_id": step.ID, "expected_revision": job.Revision - 1, "operation_request_id": "retry-operation", "reason": "retry verified input"})
	require.Equal(t, 409, request(http.MethodPost, path+"/retry", string(body)).Code)
	body, _ = json.Marshal(map[string]any{"step_id": step.ID, "expected_revision": job.Revision, "operation_request_id": "retry-operation", "reason": "retry verified input"})
	tenant = 2
	require.Equal(t, 404, request(http.MethodPost, path+"/retry", string(body)).Code)
	tenant = 1
	require.Equal(t, 404, request(http.MethodPost, "/kb/other/jobs/"+job.ID+"/retry", string(body)).Code)
	actor = ""
	require.Equal(t, 403, request(http.MethodPost, path+"/retry", string(body)).Code)
	actor = "operator"
	require.Equal(t, 202, request(http.MethodPost, path+"/retry", string(body)).Code)
	require.Equal(t, 202, request(http.MethodPost, path+"/retry", string(body)).Code)
	var count int64
	require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("event_type = ?", "manual_retry").Count(&count).Error)
	require.EqualValues(t, 1, count)
	job, _ = r.GetJob(ctx, 1, job.ID)
	body, _ = json.Marshal(map[string]any{"expected_revision": job.Revision, "operation_request_id": "pin-op", "pinned": true, "reason": "preserve rollback candidate"})
	tenant = 2
	require.Equal(t, 404, request(http.MethodPost, path+"/pin", string(body)).Code)
	tenant = 1
	actor = ""
	require.Equal(t, 403, request(http.MethodPost, path+"/pin", string(body)).Code)
	actor = "operator"
	require.Equal(t, 200, request(http.MethodPost, path+"/pin", string(body)).Code)
	require.Equal(t, 200, request(http.MethodPost, path+"/pin", string(body)).Code)
	job, _ = r.GetJob(ctx, 1, job.ID)
	require.True(t, job.RollbackPin)
	require.Equal(t, 503, request(http.MethodPost, path+"/rollback", string(body)).Code)
}
