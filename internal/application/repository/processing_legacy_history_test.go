package repository

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestProcessingLegacyHistoryPreservesOriginalAndResolution(t *testing.T) {
	checkProcessingLegacyHistory(t, processingTestStore(t))
}

func checkProcessingLegacyHistory(t *testing.T, r *ProcessingRepository) {
	t.Helper()
	ctx := context.Background()
	run := types.SyncLog{ID: "legacy-history-run", TenantID: 1, DataSourceID: "source", Status: "running", StartedAt: time.Now().UTC(), Result: types.JSON(`{"errors":[{"file_id":"one","external_id":"one","title":"same title","stage":"ingest","retry_state":"scheduled","message":"SECRET_BODY"},{"file_id":"two","external_id":"two","title":"same title","stage":"ingest","retry_state":"scheduled","message":"SECRET_OTHER"},"SECRET_STRING_ERROR"]}`)}
	require.NoError(t, r.db.Create(&run).Error)
	dead := types.TaskDeadLetter{TenantID: 1, TaskType: types.TypeDataSourceSync, Scope: types.TaskScopeTenant, ScopeID: "1", Payload: json.RawMessage(`{"data_source_id":"source","sync_log_id":"legacy-history-run","secret":"SECRET_TASK"}`), FailCount: 6}
	require.NoError(t, r.db.Create(&dead).Error)
	_, replayErr := r.BeginScan(ctx, types.ProcessingJob{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", Kind: types.ProcessingJobScan, OriginRunID: run.ID, ExternalID: "run:" + run.ID, SourceRevision: run.ID, PipelineFingerprint: "late-old-delivery"}, []types.ProcessingStepSpec{{Stage: "scan_page", UnitKey: "root", Phase: types.ProcessingPhaseScan, InputFingerprint: "fixture", RequiredForCompletion: true}})
	require.ErrorIs(t, replayErr, ErrProcessingConflict, "an old raw result cannot become a newly owned v2 run")
	filter := ProcessingHistoryFilter{View: "unresolved_incidents", RunID: run.ID}
	first, err := r.CreateHistorySnapshot(ctx, 1, "history-reader", "kb", filter, 1)
	require.NoError(t, err)
	require.Equal(t, 3, first.Total)
	original, err := r.InspectLegacyError(ctx, types.ProcessingLegacyIdentity{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", RunID: run.ID, ErrorOrdinal: 1})
	require.NoError(t, err)
	// Projection fixture only: full artifact verification is tested separately.
	_, err = appendLegacyEvidence(r.db, types.ProcessingLegacyEvidence{ProcessingLegacyIdentity: original.Identity, Action: "manual_confirmed", KnowledgeID: "legacy-knowledge", JobID: "linked-new-job", SourceRevision: "v1", Attempt: 1,
		SnapshotDigest: strings.Repeat("a", 64), ConfigurationRevision: strings.Repeat("b", 64), ArtifactDigest: strings.Repeat("c", 64), EvidenceReference: "synthetic-history-proof", EvidenceDigest: strings.Repeat("d", 64), Actor: "verified-operator", Reason: "SECRET_OPERATOR_NOTE", OperationRequestID: "history-proof"})
	require.NoError(t, err)
	second, err := r.HistoryPage(ctx, 1, "history-reader", "kb", first.ID, first.FilterDigest, 1, 2)
	require.NoError(t, err)
	require.Equal(t, 3, second.Total)
	fresh, err := r.CreateHistorySnapshot(ctx, 1, "history-reader", "kb", filter, 100)
	require.NoError(t, err)
	require.Equal(t, 2, fresh.Total)
	for _, check := range []struct {
		view  string
		total int
	}{{"current_run_progress", 1}, {"stage_retry_queue", 1}, {"attempt_timeline", 4}} {
		page, err := r.CreateHistorySnapshot(ctx, 1, "history-reader", "kb", ProcessingHistoryFilter{View: check.view, RunID: run.ID}, 100)
		require.NoError(t, err)
		require.Equal(t, check.total, page.Total, check.view)
		proofFound := false
		for _, encoded := range page.Rows {
			require.NotContains(t, string(encoded), "SECRET_")
			var row map[string]any
			require.NoError(t, json.Unmarshal(encoded, &row))
			require.Empty(t, row["job_id"], "never invent a v2 job for a legacy row")
			require.Equal(t, "running", row["legacy_run_status"])
			require.EqualValues(t, dead.ID, row["legacy_dead_letter_id"])
			if row["event_type"] == "legacy_evidence" {
				proofFound = true
				require.Equal(t, original.Identity.ErrorDigest, row["original_error_digest"])
				require.Equal(t, "linked-new-job", row["linked_job_id"])
				require.Equal(t, "verified-operator", row["legacy_actor"])
			}
		}
		if check.view == "attempt_timeline" {
			require.True(t, proofFound)
		}
	}
	anomaly, err := r.CreateHistorySnapshot(ctx, 1, "history-reader", "kb", ProcessingHistoryFilter{View: "lifecycle_inconsistencies", RunID: run.ID, Status: "LEGACY_DEAD_LETTER_RUNNING"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1, anomaly.Total, "resolution does not rewrite or hide the old dead-letter/run contradiction")
	_, err = r.HistoryPage(ctx, 2, "history-reader", "kb", first.ID, first.FilterDigest, 0, 1)
	require.Error(t, err)
	require.NoError(t, r.db.Model(&run).Update("result", types.JSON(strings.Replace(string(run.Result), "SECRET_BODY", "SECRET_CHANGED", 1))).Error)
	changed, err := r.CreateHistorySnapshot(ctx, 1, "history-reader", "kb", filter, 100)
	require.NoError(t, err)
	require.Equal(t, 3, changed.Total, "an exact raw error change invalidates its former resolution")
	logs := NewSyncLogRepository(r.db)
	originalRow := types.SyncLog{}
	require.NoError(t, r.db.First(&originalRow, "id = ?", run.ID).Error)
	late := originalRow
	late.Status, late.ErrorMessage, late.Result = "canceled", "late callback", types.JSON(`{"errors":[]}`)
	if r.db.Dialector.Name() == "postgres" {
		tx := r.db.Begin()
		require.NoError(t, tx.Error)
		defer tx.Rollback()
		var source types.DataSource
		require.NoError(t, tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", "source").Take(&source).Error)
		done := make(chan error, 1)
		go func() { done <- logs.UpdateResult(ctx, &late) }()
		select {
		case err := <-done:
			t.Fatalf("legacy update escaped the ownership lock: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		require.NoError(t, tx.Commit().Error)
		require.NoError(t, <-done)
	}
	require.NoError(t, logs.Update(ctx, &late))
	require.NoError(t, logs.UpdateResult(ctx, &late))
	unchanged := types.SyncLog{}
	require.NoError(t, r.db.First(&unchanged, "id = ?", run.ID).Error)
	require.Equal(t, originalRow, unchanged, "both shared update paths preserve a referenced original")
	require.NoError(t, logs.CancelPendingByDataSource(ctx, "source"))
	var retained types.SyncLog
	require.NoError(t, r.db.First(&retained, "id = ?", run.ID).Error)
	require.Equal(t, "running", retained.Status, "legacy cancellation must preserve a referenced original record")
	old := time.Now().AddDate(0, 0, -200)
	require.NoError(t, r.db.Model(&run).Updates(map[string]any{"status": "canceled", "finished_at": old}).Error)
	require.NoError(t, r.db.Create(&types.SyncLog{ID: "unreferenced-terminal", Status: "success", FinishedAt: &old}).Error)
	for _, item := range []types.SyncLog{
		{ID: "unresolved-export", Status: "canceled", FinishedAt: &old, Result: types.JSON(`{"errors":[{"category":"EXPORT_START_UNCERTAIN"}]}`)},
		{ID: "unresolved-message", Status: "success", FinishedAt: &old, ErrorMessage: "EXPORT_START_UNCERTAIN"},
		{ID: "unresolved-count", Status: "canceled", FinishedAt: &old, ItemsFailed: 1},
	} {
		require.NoError(t, r.db.Create(&item).Error)
	}
	require.NoError(t, logs.CleanupOldLogs(ctx, 90))
	require.NoError(t, r.db.First(&retained, "id = ?", run.ID).Error)
	var ordinary int64
	require.NoError(t, r.db.Model(&types.SyncLog{}).Where("id = ?", "unreferenced-terminal").Count(&ordinary).Error)
	require.Zero(t, ordinary, "normal unreferenced terminal logs still expire")
	require.NoError(t, r.db.Model(&types.SyncLog{}).Where("id IN ?", []string{"unresolved-export", "unresolved-message", "unresolved-count"}).Count(&ordinary).Error)
	require.EqualValues(t, 3, ordinary, "cancellation or a terminal label does not resolve an unknown export")
}
