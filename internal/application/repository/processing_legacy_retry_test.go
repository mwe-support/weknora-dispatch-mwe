package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingLegacyRestrictedRetry(t *testing.T) {
	checkProcessingLegacyRetry(t, processingTestStore(t))
}

func checkProcessingLegacyRetry(t *testing.T, r *ProcessingRepository) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, r.db.AutoMigrate(&types.SyncLog{}, &types.TaskDeadLetter{}))
	var source types.DataSource
	require.NoError(t, r.db.First(&source, "id = ?", "source").Error)
	source.Config, _ = (&types.DataSourceConfig{ResourceIDs: []string{"tdoc:space:c3BhY2U"}}).ToJSON()
	require.NoError(t, r.db.Save(&source).Error)
	var errorRows []types.SyncItemError
	for i := 1; i <= 8; i++ {
		errorRows = append(errorRows, types.SyncItemError{ExternalID: fmt.Sprintf("legacy-retry-external-%d", i), FileID: fmt.Sprintf("legacy-retry-file-%d", i), Stage: "ingest", Category: "INGEST_FAILED", RetryState: "scheduled", SourceResourceID: "tdoc:space:c3BhY2U", Message: "synthetic timeout"})
	}
	raw, _ := json.Marshal(map[string]any{"errors": errorRows})
	oldRun := types.SyncLog{ID: "legacy-retry-run", TenantID: 1, DataSourceID: source.ID, Status: types.SyncLogStatusRunning, Result: raw, ItemsFailed: 8}
	require.NoError(t, r.db.Create(&oldRun).Error)
	require.NoError(t, r.db.First(&oldRun, "id = ?", oldRun.ID).Error)
	payload, _ := json.Marshal(types.DataSourceSyncPayload{TenantID: 1, DataSourceID: source.ID, SyncLogID: oldRun.ID})
	dead := types.TaskDeadLetter{TenantID: 1, TaskType: types.TypeDataSourceSync, Scope: types.TaskScopeTenant, ScopeID: "1", Payload: payload, FailCount: 6, FailedAt: time.Now().UTC()}
	require.NoError(t, r.db.Create(&dead).Error)
	require.NoError(t, r.db.First(&dead, "id = ?", dead.ID).Error)
	digest, err := ProcessingLegacyPayloadDigest(dead.Payload)
	require.NoError(t, err)
	var queues []map[string]any
	for _, queue := range types.QueueDefinitions() {
		tasks := []any{}
		if queue.Name == types.QueueSync {
			tasks = append(tasks, map[string]any{"task_id": "old-retry-delivery", "payload_digest": digest, "state": "archived"})
		}
		queues = append(queues, map[string]any{"queue": queue.Name, "inventory_digest": strings.Repeat("a", 64), "tasks": tasks})
	}
	inventory, _ := json.Marshal(map[string]any{"complete": true, "workers": []map[string]any{{"old_instance_id": "old-worker", "replacement_id": "new-worker", "image_digest": strings.Repeat("b", 64), "exit_confirmed": true, "guard_protocol": 2}}, "queues": queues})
	scope, _, err := ProcessingSourceRevisions(&source)
	require.NoError(t, err)
	configuration, err := r.ConfigurationRevision(ctx, 1, "kb")
	require.NoError(t, err)
	drain, err := r.RecordLegacyDrain(ctx, types.ProcessingLegacyDrain{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, ScopeRevision: scope, ConfigurationRevision: configuration, Inventory: inventory, EvidenceReference: "synthetic-inventory", EvidenceDigest: strings.Repeat("c", 64), Actor: "test-operator", OperationRequestID: "legacy-retry-drain", CheckedAt: time.Now().UTC()})
	require.NoError(t, err)
	request := types.ProcessingLegacyRetry{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, RunID: oldRun.ID, DeadLetterID: dead.ID, QueueTaskID: "old-retry-delivery", PayloadDigest: digest, DrainID: drain.ID, Actor: "test-operator", Reason: "recover only the eight scheduled errors", OperationRequestID: "restricted-eight"}
	for i := 1; i <= 8; i++ {
		original, err := r.InspectLegacyError(ctx, types.ProcessingLegacyIdentity{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, RunID: oldRun.ID, ErrorOrdinal: i})
		require.NoError(t, err)
		request.Errors = append(request.Errors, original.Identity)
	}
	input := types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, PipelineFingerprint: "legacy-retry-pipeline"}
	plan := []types.ProcessingStepSpec{{Stage: "scan_document", UnitKey: "fixture", Phase: types.ProcessingPhaseScan, InputFingerprint: "fixture", RequiredForCompletion: true},
		{Stage: "legacy_retry_coverage", UnitKey: "body", Phase: types.ProcessingPhaseScan, InputFingerprint: "coverage", RequiredForCompletion: true, DependsOn: []string{"scan_document/fixture"}}}
	for _, mutate := range []func(*types.ProcessingLegacyRetry){
		func(v *types.ProcessingLegacyRetry) { v.DrainID = "" }, func(v *types.ProcessingLegacyRetry) { v.QueueTaskID = "different-delivery" },
		func(v *types.ProcessingLegacyRetry) { v.PayloadDigest = strings.Repeat("e", 64) }, func(v *types.ProcessingLegacyRetry) { v.DeadLetterID = dead.ID + 999 },
		func(v *types.ProcessingLegacyRetry) { v.RunID = "other-run" }, func(v *types.ProcessingLegacyRetry) { v.Errors = append(v.Errors, v.Errors[0]) },
	} {
		bad := request
		bad.Errors = append([]types.ProcessingLegacyIdentity{}, request.Errors...)
		mutate(&bad)
		_, err := r.BeginLegacyRetry(ctx, bad, input, plan)
		require.Error(t, err)
	}
	// A failed plan must roll back the new run, intent receipts and every outbox.
	broken := append(append([]types.ProcessingStepSpec{}, plan...), types.ProcessingStepSpec{Stage: "invalid"})
	_, err = r.BeginLegacyRetry(ctx, request, input, broken)
	require.Error(t, err)
	var count int64
	require.NoError(t, r.db.Model(&types.SyncLog{}).Where("id = ?", legacyRetryRunID(request)).Count(&count).Error)
	require.Zero(t, count)
	job, err := r.BeginLegacyRetry(ctx, request, input, plan)
	require.NoError(t, err)
	same, err := r.BeginLegacyRetry(ctx, request, input, plan)
	require.NoError(t, err)
	require.Equal(t, job.ID, same.ID)
	bad := request
	bad.Reason = "changed reason"
	_, err = r.BeginLegacyRetry(ctx, bad, input, plan)
	require.ErrorIs(t, err, ErrProcessingConflict)
	bad = request
	bad.OperationRequestID = "second-concurrent-request"
	_, err = r.BeginLegacyRetry(ctx, bad, input, plan)
	require.ErrorContains(t, err, "LEGACY_RETRY_ALREADY_LINKED")
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	var discovery types.ProcessingStep
	for _, step := range steps {
		if step.Stage == "scan_document" {
			discovery = step
		}
	}
	lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: discovery.ID, Attempt: discovery.Attempt, DispatchSeq: discovery.DispatchSeq, InputFingerprint: discovery.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	out := types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic-verified-scan", OutputDigest: "verified"}
	for _, identity := range request.Errors {
		metadata, _ := json.Marshal(map[string]string{"file_id": identity.FileID, "kind": "smartcanvas", "title": "synthetic"})
		out.AdmitDocuments = append(out.AdmitDocuments, types.ProcessingAdmission{Job: types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: source.ID, ExternalID: identity.ExternalID, SourceRevision: "fresh-version", SourceDigest: strings.Repeat("d", 64), PipelineFingerprint: input.PipelineFingerprint, Metadata: metadata}, Steps: []types.ProcessingStepSpec{{Stage: "native_read", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "fresh", RequiredForReady: true, RequiredForCompletion: true}}})
	}
	out.AdmitDocuments[0].Steps = []types.ProcessingStepSpec{{Stage: "assets", UnitKey: "body", Kind: "barrier", Phase: types.ProcessingPhasePrepare, InputFingerprint: "fresh-empty", RequiredForReady: true, RequiredForCompletion: true}}
	outside := out
	outside.AdmitDocuments = append([]types.ProcessingAdmission{}, out.AdmitDocuments...)
	outside.AdmitDocuments[7].Job.ExternalID = "not-requested"
	require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, outside), ErrProcessingScope)
	require.NoError(t, r.db.Model(&types.SyncRunItem{}).Where("run_id = ?", job.OriginRunID).Count(&count).Error)
	require.Zero(t, count, "late invalid admission must roll back all earlier files and evidence")
	require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
	require.NoError(t, r.FinishStep(ctx, 1, *lease, out))
	var admissions []types.ProcessingLegacyEvidence
	require.NoError(t, r.db.Where("run_id = ? AND action = ?", oldRun.ID, "retry_admitted").Find(&admissions).Error)
	require.Len(t, admissions, 8)
	for _, admission := range admissions {
		require.Equal(t, "fresh-version", admission.SourceRevision)
		// Each new document has its own durable first delivery.
		ops, err := r.PendingDeliveries(ctx, 1000)
		require.NoError(t, err)
		n := 0
		for _, op := range ops {
			var ref types.ProcessingRef
			require.NoError(t, json.Unmarshal(op.Payload, &ref))
			if ref.JobID == admission.JobID {
				n++
			}
		}
		require.Equal(t, 1, n)
	}
	complete, err := r.LegacyRetryCoverage(ctx, job)
	require.NoError(t, err)
	require.True(t, complete)
	require.NoError(t, r.db.Model(&types.ProcessingLegacyEvidence{}).Where("run_id = ? AND action = ?", oldRun.ID, "recovered").Count(&count).Error)
	require.Zero(t, count, "admission is not completion")
	var emptyJobID string
	for _, evidence := range admissions {
		if evidence.ErrorOrdinal == 1 {
			emptyJobID = evidence.JobID
		}
	}
	emptyJob, err := r.GetJob(ctx, 1, emptyJobID)
	require.NoError(t, err)
	emptySteps, err := r.ListSteps(ctx, 1, emptyJobID)
	require.NoError(t, err)
	emptyStep := emptySteps[0]
	emptyLease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: emptyJob.ID, Generation: emptyJob.Generation, StepID: emptyStep.ID, Attempt: emptyStep.Attempt, DispatchSeq: emptyStep.DispatchSeq, InputFingerprint: emptyStep.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	require.NoError(t, r.FinishStep(ctx, 1, *emptyLease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, Completeness: "verified_empty", OutputManifestRef: "synthetic-empty", OutputDigest: strings.Repeat("e", 64), SealPlan: true}))
	require.NoError(t, r.db.Model(&types.ProcessingLegacyEvidence{}).Where("run_id = ? AND action = ?", oldRun.ID, "policy_skipped").Count(&count).Error)
	require.EqualValues(t, 1, count)
	remaining, err := r.CreateHistorySnapshot(ctx, 1, "retry-test", "kb", ProcessingHistoryFilter{View: "unresolved_incidents", RunID: oldRun.ID}, 100)
	require.NoError(t, err)
	require.Equal(t, 7, remaining.Total, "verified empty has an explicit policy resolution, not a successful publication")
	// Even a fabricated successful coverage outcome cannot hide a missing file.
	require.NoError(t, r.db.Where("run_id = ? AND external_id = ?", job.OriginRunID, request.Errors[7].ExternalID).Delete(&types.SyncRunItem{}).Error)
	steps, err = r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		if step.Stage != "legacy_retry_coverage" {
			continue
		}
		coverage, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		require.NoError(t, r.FinishStep(ctx, 1, *coverage, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "fabricated", OutputDigest: "fabricated"}))
		actual, err := r.GetStep(ctx, 1, job.ID, step.ID)
		require.NoError(t, err)
		require.Equal(t, "LEGACY_RETRY_FILES_MISSING", actual.ErrorCode)
		require.Equal(t, types.ProcessingBlocked, actual.Status)
	}
	var after types.SyncLog
	require.NoError(t, r.db.First(&after, "id = ?", oldRun.ID).Error)
	require.Equal(t, oldRun, after)
	var deadAfter types.TaskDeadLetter
	require.NoError(t, r.db.First(&deadAfter, "id = ?", dead.ID).Error)
	require.Equal(t, dead, deadAfter)
}
