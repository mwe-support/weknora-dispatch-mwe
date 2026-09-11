package repository

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
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
}
