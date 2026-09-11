package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func installProcessingHistoryTestSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.TaskDeadLetter{}, &types.SyncLog{}))
	dialect, version := "sqlite", "000010"
	if db.Dialector.Name() == "postgres" {
		dialect, version = "versioned", "000087"
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", dialect, version+"_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(data)).Error)
	version = "000011"
	if dialect == "versioned" {
		version = "000088"
	}
	data, err = os.ReadFile(filepath.Join("..", "..", "..", "migrations", dialect, version+"_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(data)).Error)
	version = "000012"
	if dialect == "versioned" {
		version = "000089"
	}
	data, err = os.ReadFile(filepath.Join("..", "..", "..", "migrations", dialect, version+"_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(data)).Error)
}

func TestProcessingHistorySnapshotAndUnresolvedCleanup(t *testing.T) {
	r := processingTestStore(t)
	checkProcessingHistorySnapshotAndUnresolvedCleanup(t, r)
}

func checkProcessingHistorySnapshotAndUnresolvedCleanup(t *testing.T, r *ProcessingRepository) {
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "doc", SourceRevision: "v1", PipelineFingerprint: "p1",
		Metadata: types.JSON(`{"title":"synthetic private title","url":"https://example.test/?token=secret","body":"SECRET_BODY"}`)})
	require.NoError(t, err)
	require.NoError(t, r.db.Model(job).Updates(map[string]any{"status": "succeeded", "auth_revision": "SECRET_AUTH", "index_destination": types.JSON(`{"secret":"SECRET_STORE"}`)}).Error)
	step := types.ProcessingStep{ID: "cleanup", JobID: job.ID, Stage: "retire", Phase: "retire", Status: "blocked", Attempt: 2, DispatchSeq: 3, ErrorClass: "storage", ErrorCode: "RETIREMENT_STORAGE_UNAVAILABLE", Input: types.JSON(`{"secret":"SECRET_INPUT"}`), CheckpointRef: "SECRET_CHECKPOINT"}
	require.NoError(t, r.db.Create(&step).Error)
	old := time.Now().UTC().Add(-60 * 24 * time.Hour)
	for i := 0; i < 3; i++ {
		event := types.ProcessingEvent{TenantID: 1, JobID: job.ID, JobRevision: int64(100 + i), Generation: 1, StepID: step.ID, Type: "step_failed", ErrorCode: step.ErrorCode, Message: "SECRET_MESSAGE", Detail: types.JSON(`{"body":"SECRET_DETAIL"}`), CreatedAt: old.Add(time.Duration(i) * time.Second)}
		require.NoError(t, r.db.Create(&event).Error)
	}
	filter := ProcessingHistoryFilter{View: "unresolved_incidents"}
	first, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", filter, 2)
	require.NoError(t, err)
	require.Equal(t, 3, first.Total)
	require.Len(t, first.Rows, 2)
	require.True(t, first.HasMore)
	encoded, err := json.Marshal(first)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "SECRET_")
	require.NotContains(t, string(encoded), "token=")
	var saved map[string]any
	require.NoError(t, json.Unmarshal(first.Rows[0], &saved))
	require.Equal(t, "succeeded", saved["job_status"])
	// Resolve the oldest error after page one and rename the current document.
	var oldest types.ProcessingEvent
	require.NoError(t, r.db.Where("job_id = ? AND error_code <> ''", job.ID).Order("id").First(&oldest).Error)
	require.NoError(t, r.db.Create(&types.ProcessingEvent{TenantID: 1, JobID: job.ID, JobRevision: 200, Generation: 1, Type: "incident_resolved", ResolvesEventID: &oldest.ID, ResolutionType: "recovered_retry"}).Error)
	require.NoError(t, r.db.Model(job).Update("metadata", types.JSON(`{"title":"changed"}`)).Error)
	second, err := r.HistoryPage(ctx, 1, "user:alice", "kb", first.ID, first.FilterDigest, first.NextAfter, 2)
	require.NoError(t, err)
	require.Equal(t, 3, second.Total)
	require.Len(t, second.Rows, 1)
	require.False(t, second.HasMore)
	require.Contains(t, string(second.Rows[0]), "synthetic private title")
	fresh, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", filter, 100)
	require.NoError(t, err)
	require.Equal(t, 2, fresh.Total)
	for _, scope := range []struct {
		tenant                uint64
		principal, kb, digest string
	}{{2, "user:alice", "kb", first.FilterDigest}, {1, "user:bob", "kb", first.FilterDigest}, {1, "user:alice", "other", first.FilterDigest}, {1, "user:alice", "kb", "wrong"}} {
		_, err := r.HistoryPage(ctx, scope.tenant, scope.principal, scope.kb, first.ID, scope.digest, 0, 2)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	}
	queue, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", ProcessingHistoryFilter{View: "stage_retry_queue"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, queue.Total)
	timeline, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", ProcessingHistoryFilter{View: "attempt_timeline"}, 100)
	require.NoError(t, err)
	for _, row := range timeline.Rows {
		require.NotContains(t, string(row), "step_failed")
	}
	// New attempts never make a previous dead letter look current.
	payload := types.ProcessingTaskPayload{ProcessingRef: types.ProcessingRef{Protocol: 2, JobID: job.ID, StepID: step.ID, Generation: 1, Attempt: 1, DispatchSeq: 3}, TenantID: 1}
	raw, _ := json.Marshal(payload)
	require.NoError(t, r.db.Create(&types.TaskDeadLetter{TenantID: 1, Payload: raw}).Error)
	require.NoError(t, r.db.Model(&step).Update("status", "running").Error)
	anomalies, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", ProcessingHistoryFilter{View: "lifecycle_inconsistencies", Status: "DEAD_LETTER_RUNNING"}, 100)
	require.NoError(t, err)
	require.Zero(t, anomalies.Total)
	payload.Attempt = 2
	raw, _ = json.Marshal(payload)
	require.NoError(t, r.db.Create(&types.TaskDeadLetter{TenantID: 1, Payload: raw}).Error)
	anomalies, err = r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", ProcessingHistoryFilter{View: "lifecycle_inconsistencies", Status: "DEAD_LETTER_RUNNING"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, anomalies.Total)
	// Eight live snapshots per principal, with row cleanup rather than an orphan cache.
	for i := 0; i < 9; i++ {
		_, err := r.CreateHistorySnapshot(ctx, 1, "user:bob", "kb", filter, 100)
		require.NoError(t, err)
	}
	var count int64
	require.NoError(t, r.db.Model(&ProcessingHistorySnapshot{}).Where("principal = ?", "user:bob").Count(&count).Error)
	require.EqualValues(t, 8, count)
	require.NoError(t, r.db.Model(&ProcessingHistorySnapshot{}).Where("id = ?", fresh.ID).Update("expires_at", old).Error)
	_, err = r.HistoryPage(ctx, 1, "user:alice", "kb", fresh.ID, fresh.FilterDigest, 0, 2)
	require.ErrorIs(t, err, ErrProcessingHistoryExpired)
	require.NoError(t, r.CollectHistorySnapshots(ctx))
	require.NoError(t, r.db.Model(&ProcessingHistoryRow{}).Where("snapshot_id = ?", fresh.ID).Count(&count).Error)
	require.Zero(t, count)
	legacy := types.Knowledge{ID: "legacy-history", TenantID: 1, KnowledgeBaseID: "kb", Title: "Legacy", ParseStatus: "completed", EnableStatus: "enabled", Metadata: types.JSON(`{"datasource_id":"source","external_id":"legacy","body":"SECRET_LEGACY"}`)}
	require.NoError(t, r.db.Create(&legacy).Error)
	legacyView, err := r.CreateHistorySnapshot(ctx, 1, "user:legacy", "kb", ProcessingHistoryFilter{View: "current_document_lifecycle", Search: "Legacy"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, legacyView.Total)
	require.Contains(t, string(legacyView.Rows[0]), `"evidence_basis":"legacy_unverified"`)
	require.Contains(t, string(legacyView.Rows[0]), `"job_id":""`)
	require.NotContains(t, string(legacyView.Rows[0]), "SECRET_LEGACY")
	require.Contains(t, string(legacyView.Rows[0]), `"native_raw_bytes":null`)

	require.NoError(t, r.db.Create(&types.ProcessingStep{ID: "native-metrics", JobID: job.ID, Stage: "native_read", UnitKey: "body", Phase: "prepare", Status: "succeeded",
		Result: types.JSON(`{"raw_bytes":456,"pages":3,"units":201,"body":"SECRET_RESULT"}`)}).Error)
	require.NoError(t, r.db.Create(&types.ProcessingStep{ID: "asset-metrics", JobID: job.ID, Stage: "assets", UnitKey: "body", Phase: "prepare", Status: "succeeded",
		Result: types.JSON(`{"images":0,"media_bytes":"SECRET_NOT_NUMBER"}`)}).Error)
	for _, write := range []types.FAQIndexWrite{
		{ID: "charged-index", TenantID: 1, JobID: job.ID, EstimatedBytes: 64},
		{ID: "released-index", TenantID: 1, JobID: job.ID, EstimatedBytes: 128, StorageReleased: true},
		{ID: "other-tenant-index", TenantID: 2, JobID: job.ID, EstimatedBytes: 256},
	} {
		write.SourceIDs, write.Destination = types.JSON(`[]`), types.JSON(`{}`)
		require.NoError(t, r.db.Create(&write).Error)
	}
	metrics, err := r.CreateHistorySnapshot(ctx, 1, "user:metrics", "kb", ProcessingHistoryFilter{View: "current_document_lifecycle", JobID: job.ID}, 100)
	require.NoError(t, err)
	require.Len(t, metrics.Rows, 1)
	require.Contains(t, string(metrics.Rows[0]), `"native_raw_bytes":456`)
	require.Contains(t, string(metrics.Rows[0]), `"images":0`)
	require.Contains(t, string(metrics.Rows[0]), `"media_bytes":null`)
	require.Contains(t, string(metrics.Rows[0]), `"charged_bytes":64`)
	require.Contains(t, string(metrics.Rows[0]), `"estimated_index_bytes":64`)
	require.NotContains(t, string(metrics.Rows[0]), "SECRET_")
	stale, future := time.Now().Add(-10*time.Minute), time.Now().Add(time.Minute)
	require.NoError(t, r.db.Model(&step).Updates(map[string]any{"status": "running", "progress_at": stale, "lease_expires_at": future}).Error)
	stalled, err := r.CreateHistorySnapshot(ctx, 1, "user:metrics", "kb", ProcessingHistoryFilter{View: "lifecycle_inconsistencies", Status: "NO_PROGRESS"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, stalled.Total)
	still, err := r.GetStep(ctx, 1, job.ID, step.ID)
	require.NoError(t, err)
	require.Equal(t, "running", still.Status, "observability must not fail a worker with a valid lease")
	require.NoError(t, r.db.Model(&step).Updates(map[string]any{"status": "retry_wait", "next_run_at": stale}).Error)
	stalled, err = r.CreateHistorySnapshot(ctx, 1, "user:metrics", "kb", ProcessingHistoryFilter{View: "lifecycle_inconsistencies", Status: "RETRY_OVERDUE"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, stalled.Total)
}

func TestProcessingHistoryViewsAndSnapshotLimit(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	for view := range processingHistoryViews {
		_, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", ProcessingHistoryFilter{View: view}, 100)
		require.NoError(t, err, view)
	}
	for _, filter := range []ProcessingHistoryFilter{{View: "data_sources"}, {View: "attempt_timeline; DROP TABLE data_sources"}, {View: "stage_retry_queue", Search: strings.Repeat("x", 257)}} {
		_, err := r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", filter, 100)
		require.ErrorIs(t, err, ErrProcessingHistoryFilter)
	}
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "limit", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	events := make([]types.ProcessingEvent, 10001)
	for i := range events {
		events[i] = types.ProcessingEvent{TenantID: 1, JobID: job.ID, JobRevision: int64(i + 100), Generation: 1, Type: "step_failed", ErrorCode: fmt.Sprint("FAIL_", i)}
	}
	require.NoError(t, r.db.CreateInBatches(events, 100).Error)
	_, err = r.CreateHistorySnapshot(ctx, 1, "user:alice", "kb", ProcessingHistoryFilter{View: "unresolved_incidents"}, 100)
	require.ErrorIs(t, err, ErrProcessingHistoryLimit)
}
