package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingSourcePauseIsAtomicAndKeepsUncertainExport(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.WithValue(context.Background(), types.UserIDContextKey, "test-operator")
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"}
	job, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "export_start", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "export", RequiredForReady: true, RequiredForCompletion: true}}))
	ops, err := r.PendingDeliveries(ctx, 10)
	require.NoError(t, err)
	var ref types.ProcessingRef
	require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
	lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
	require.NoError(t, err)
	sources := repository.NewDataSourceRepository(db)
	source, err := sources.FindByID(ctx, "source")
	require.NoError(t, err)
	source.Status = types.DataSourceStatusPaused
	// Force event insertion to fail: source settings and invalidation must
	// roll back together, including the previously live lease.
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("ALTER TABLE processing_events ADD CONSTRAINT injected_cancel_failure CHECK (event_type <> 'step_invalidated')").Error)
	} else {
		require.NoError(t, db.Exec("CREATE TRIGGER injected_cancel_failure BEFORE INSERT ON processing_events WHEN NEW.event_type = 'step_invalidated' BEGIN SELECT RAISE(ABORT, 'test cancellation failure'); END").Error)
	}
	require.Error(t, sources.Update(ctx, source))
	stored, err := sources.FindByID(ctx, "source")
	require.NoError(t, err)
	require.Equal(t, types.DataSourceStatusActive, stored.Status)
	require.NoError(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""))
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("ALTER TABLE processing_events DROP CONSTRAINT injected_cancel_failure").Error)
	} else {
		require.NoError(t, db.Exec("DROP TRIGGER injected_cancel_failure").Error)
	}
	require.NoError(t, sources.Update(ctx, source))
	step, err := r.GetStep(ctx, 1, job.ID, ref.StepID)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingBlocked, step.Status)
	require.Equal(t, "EXPORT_START_UNCERTAIN", step.ErrorCode)
	require.Empty(t, step.LeaseToken)
	require.NotNil(t, step.LastErrorEventID)
	var resolutions int64
	require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("resolves_event_id = ?", *step.LastErrorEventID).Count(&resolutions).Error)
	require.Zero(t, resolutions)
	source.Status = types.DataSourceStatusActive
	require.NoError(t, sources.Update(ctx, source))
	require.Error(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "late", OutputDigest: "late"}))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingCanceled, job.Status)
	require.Error(t, r.RetryStep(ctx, 1, job.ID, step.ID, job.Revision, "wrong-retry", "test-operator"))
}

func TestProcessingRunProjectionOrderingCancellationAndRetention(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.SyncRunItem{}, &types.ProcessingLegacyEvidence{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	started := time.Now().UTC().Add(-time.Hour)
	runs := []types.SyncLog{
		{ID: "first", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusRunning, StartedAt: started},
		{ID: "second", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusRunning, StartedAt: started.Add(time.Minute)},
	}
	leases := make([]*types.ProcessingLease, 0, 2)
	for i := range runs {
		run := &runs[i]
		require.NoError(t, db.Create(run).Error)
		job, err := r.BeginScan(ctx, types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "run:" + run.ID, OriginRunID: run.ID, SourceRevision: run.ID, PipelineFingerprint: "p1"}, []types.ProcessingStepSpec{{Stage: "scan_document", UnitKey: "body", Phase: types.ProcessingPhaseScan, InputFingerprint: "input", RequiredForCompletion: true}})
		require.NoError(t, err)
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		step := steps[0]
		lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		leases = append(leases, lease)
	}
	admission := types.ProcessingAdmission{Job: types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"}, Steps: []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "body", RequiredForCompletion: true}}}
	require.NoError(t, r.FinishStep(ctx, 1, *leases[1], types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "scan", OutputDigest: "scan", AdmitDocuments: []types.ProcessingAdmission{admission}}))
	sources := repository.NewDataSourceRepository(db)
	source, err := sources.FindByID(ctx, "source")
	require.NoError(t, err)
	require.Contains(t, string(source.LastSyncResult), leases[1].Job.ID)
	// The earlier empty scan now finishes, but cannot overwrite the new run.
	require.NoError(t, r.FinishStep(ctx, 1, *leases[0], types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "empty-scan", OutputDigest: "empty-scan"}))
	source, err = sources.FindByID(ctx, "source")
	require.NoError(t, err)
	require.Contains(t, string(source.LastSyncResult), leases[1].Job.ID)
	source.Status = types.DataSourceStatusPaused
	require.NoError(t, sources.Update(ctx, source))
	var frozen types.SyncLog
	require.NoError(t, db.Where("id = ?", "second").Take(&frozen).Error)
	require.Equal(t, types.SyncLogStatusCanceled, frozen.Status)
	require.Zero(t, frozen.ItemsFailed, "cancellation is not a processing failure")
	require.NotNil(t, frozen.FinishedAt)
	var snapshot repository.ProcessingRunSnapshot
	require.NoError(t, json.Unmarshal(frozen.Result, &snapshot))
	require.Equal(t, 1, snapshot.Canceled)
	require.Zero(t, snapshot.Active)
	source, err = sources.FindByID(ctx, "source")
	require.NoError(t, err)
	require.Equal(t, types.DataSourceStatusPaused, source.Status)
	require.NotNil(t, source.LastSyncAt)
	require.JSONEq(t, string(frozen.Result), string(source.LastSyncResult))
	// Late legacy callbacks cannot rewrite the ledger's immutable run receipt
	// or replace source state after this source has entered protocol 2.
	legacy := frozen
	legacy.Status, legacy.ErrorMessage, legacy.Result = types.SyncLogStatusFailed, "stale worker", types.JSON(`{"legacy":true}`)
	logsRepo := repository.NewSyncLogRepository(db)
	require.NoError(t, logsRepo.UpdateResult(ctx, &legacy))
	require.NoError(t, logsRepo.Update(ctx, &legacy))
	require.Error(t, sources.UpdateSyncState(ctx, source))
	var unchanged types.SyncLog
	require.NoError(t, db.Where("id = ?", "second").Take(&unchanged).Error)
	require.Equal(t, frozen.Status, unchanged.Status)
	require.JSONEq(t, string(frozen.Result), string(unchanged.Result))
	old := time.Now().UTC().AddDate(0, 0, -120)
	recent := time.Now().UTC().AddDate(0, 0, -3)
	logs := []types.SyncLog{
		{ID: "old-safe", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusSuccess, StartedAt: old, FinishedAt: &old},
		{ID: "old-running", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusRunning, StartedAt: old},
		{ID: "old-failed", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusFailed, StartedAt: old, FinishedAt: &old},
		{ID: "recent-safe", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusSuccess, StartedAt: recent, FinishedAt: &recent},
	}
	require.NoError(t, db.Create(&logs).Error)
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id IN ?", []string{"first", "second"}).Updates(map[string]any{"started_at": old, "finished_at": old}).Error)
	require.NoError(t, repository.NewSyncLogRepository(db).CleanupOldLogs(ctx, 1))
	var retained []string
	require.NoError(t, db.Model(&types.SyncLog{}).Order("id").Pluck("id", &retained).Error)
	require.Equal(t, []string{"first", "old-failed", "old-running", "recent-safe", "second"}, retained)
}
