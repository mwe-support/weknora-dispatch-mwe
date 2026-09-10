package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingInvalidScanScopeLeavesBlockedLedger(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	var source types.DataSource
	require.NoError(t, db.First(&source).Error)
	source.Config, _ = (&types.DataSourceConfig{ResourceIDs: []string{"tdoc:file:invalid"}}).ToJSON()
	require.NoError(t, db.Save(&source).Error)
	run := &types.SyncLog{ID: "invalid-scope-run", TenantID: source.TenantID, DataSourceID: source.ID, Status: types.SyncLogStatusRunning}
	require.NoError(t, db.Create(run).Error)
	job, err := BeginProcessingScan(ctx, r, &source, run, nil)
	require.NoError(t, err)
	steps, err := r.ListSteps(ctx, source.TenantID, job.ID)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	step := steps[0]
	lease, err := r.ClaimStep(ctx, source.TenantID, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	// No connector or storage is present: invalid scope must do no external I/O.
	e := processingDocumentExecution{lease: *lease}
	out, err := e.scan(ctx)
	require.NoError(t, err)
	require.Equal(t, "SCAN_SCOPE_INVALID", out.ErrorCode)
	require.NoError(t, r.FinishStep(ctx, source.TenantID, *lease, out))
	require.NoError(t, db.First(run, "id = ?", run.ID).Error)
	require.Equal(t, types.SyncLogStatusFailed, run.Status)
	require.NotNil(t, run.FinishedAt)
	again, err := BeginProcessingScan(ctx, r, &source, run, nil)
	require.NoError(t, err)
	require.Equal(t, job.ID, again.ID)
}

func TestProcessingScanAdmissionIsAtomicFencedAndSharedAcrossRuns(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	admission := types.ProcessingAdmission{Job: types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"},
		Steps: []types.ProcessingStepSpec{{Stage: "native_read", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "v1", RequiredForCompletion: true, RequiredForReady: true}}}
	var childID string
	for _, run := range []string{"first-run", "second-run"} {
		require.NoError(t, db.Create(&types.SyncLog{ID: run, TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusRunning}).Error)
		job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "run:" + run, OriginRunID: run, SourceRevision: run, PipelineFingerprint: "p1"})
		require.NoError(t, err)
		require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "scan_document", UnitKey: "file", Phase: types.ProcessingPhaseScan, InputFingerprint: "input", RequiredForCompletion: true}}))
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		step := steps[0]
		lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		outcome := types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/scan", OutputDigest: "digest", AdmitDocuments: []types.ProcessingAdmission{admission}, DiscoveredItems: []types.SyncRunItem{{Kind: "container", ExternalID: "folder", Disposition: "container"}}}
		stale := *lease
		stale.Token = "stale"
		require.ErrorIs(t, r.FinishStep(ctx, 1, stale, outcome), repository.ErrProcessingConflict)
		bad := outcome
		bad.AdmitDocuments = append([]types.ProcessingAdmission{}, admission, admission)
		bad.AdmitDocuments[1].Job.ExternalID = "other"
		bad.AdmitDocuments[1].Steps = []types.ProcessingStepSpec{{Stage: "invalid"}}
		require.Error(t, r.FinishStep(ctx, 1, *lease, bad))
		var count int64
		require.NoError(t, db.Model(&types.SyncRunItem{}).Where("run_id = ?", run).Count(&count).Error)
		require.Zero(t, count)
		if childID == "" {
			require.NoError(t, db.Model(&types.ProcessingJob{}).Where("kind = ?", types.ProcessingJobDocument).Count(&count).Error)
			require.Zero(t, count, "a later invalid plan must roll back the earlier admission")
		}
		require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
		require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
		var items []types.SyncRunItem
		require.NoError(t, db.Where("run_id = ?", run).Order("kind").Find(&items).Error)
		require.Len(t, items, 2)
		require.Equal(t, "container", items[0].Kind)
		require.Empty(t, items[0].JobID)
		require.Equal(t, "document", items[1].Kind)
		require.NotEmpty(t, items[1].JobID)
		if childID == "" {
			childID = items[1].JobID
		} else {
			require.Equal(t, childID, items[1].JobID)
		}
		child, err := r.GetJob(ctx, 1, childID)
		require.NoError(t, err)
		require.True(t, child.PlanSealed)
		ops, err := r.PendingDeliveries(ctx, 100)
		require.NoError(t, err)
		documentDeliveries := 0
		for _, op := range ops {
			var ref types.ProcessingRef
			require.NoError(t, json.Unmarshal(op.Payload, &ref))
			if ref.JobID == childID {
				documentDeliveries++
			}
		}
		require.Equal(t, 1, documentDeliveries)
	}
}

func TestProcessingScanResumesPagesAndFinishesBeforeDocumentPipeline(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.SyncRunItem{}))
	r := repository.NewProcessingRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	var source types.DataSource
	require.NoError(t, db.First(&source).Error)
	source.Config, _ = (&types.DataSourceConfig{ResourceIDs: []string{"tdoc:space:" + base64.RawURLEncoding.EncodeToString([]byte("space"))}}).ToJSON()
	require.NoError(t, db.Save(&source).Error)
	run := &types.SyncLog{ID: "scan-run", TenantID: 1, DataSourceID: source.ID, Status: types.SyncLogStatusRunning}
	require.NoError(t, db.Create(run).Error)
	job, err := BeginProcessingScan(ctx, r, &source, run, nil)
	require.NoError(t, err)
	again, err := BeginProcessingScan(ctx, r, &source, run, nil)
	require.NoError(t, err)
	require.Equal(t, job.ID, again.ID)
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&kb).Error)
	store := NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil)
	rootCalls, tailCalls, docFailures := 0, 0, 0
	read := func(_ context.Context, tool string, args map[string]interface{}, _ bool) (*tencentdocs.NativeResponse, error) {
		var data string
		switch tool {
		case "query_space_node":
			if args["num"] == 0 {
				rootCalls++
				data = `{"children":[{"node_id":"one","title":"One","node_type":"wiki_file","doc_type":"smartcanvas"},{"node_id":"link","node_type":"link","has_child":true}],"has_next":true}`
			} else {
				tailCalls++
				if tailCalls == 1 {
					return nil, context.DeadlineExceeded
				}
				data = `{"children":[{"node_id":"two","title":"Tail","node_type":"wiki_file","doc_type":"smartcanvas"}],"has_next":false}`
			}
		case "manage.query_file_info":
			if args["file_id"] == "two" && docFailures == 0 {
				docFailures++
				return nil, context.DeadlineExceeded
			}
			encoded, _ := json.Marshal(map[string]any{"file_id": args["file_id"], "title": "synthetic", "type": "smartcanvas", "status": "normal", "last_modify_time": 7391, "url": "https://docs.qq.com/space/synthetic?resourceId=one&mode=wiki_mode&signature=private#fragment"})
			data = string(encoded)
		case "smartcanvas.get_top_level_pages":
			data = `{"top_level_pages":[{"id":"page","children":["block"],"version":1}]}`
		default:
			t.Fatalf("unexpected scan read %s", tool)
		}
		return &tencentdocs.NativeResponse{Data: json.RawMessage(data)}, nil
	}
	interrupted := false
	for pass := 0; pass < 20; pass++ {
		job, err = r.GetJob(ctx, 1, job.ID)
		require.NoError(t, err)
		if job.Status == types.ProcessingSucceeded {
			break
		}
		require.NoError(t, db.Model(&types.ProcessingStep{}).Where("job_id = ? AND next_run_at IS NOT NULL", job.ID).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
		require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		for _, step := range steps {
			if step.Status != types.ProcessingEnqueuePending {
				continue
			}
			lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
			require.NoError(t, err)
			e := processingDocumentExecution{repo: r, lease: *lease, kb: &kb, artifacts: store, steps: steps}
			var outcome types.ProcessingOutcome
			if step.Stage == "scan_page" {
				var request tencentdocs.NativeScanRequest
				require.NoError(t, json.Unmarshal(step.Input, &request))
				outcome, err = e.scanPage(ctx, request, read)
				if !interrupted && err == nil {
					interrupted = true
					err = context.DeadlineExceeded
				}
			} else {
				outcome, err = e.scanDocument(ctx, read)
			}
			if err != nil {
				outcome = tencentdocs.ProcessingFailure(step.Stage, err)
			}
			require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
		}
	}
	require.Equal(t, types.ProcessingSucceeded, job.Status)
	require.Equal(t, 1, rootCalls, "checkpointed root must survive interruption before its plan commit")
	require.Equal(t, 2, tailCalls)
	require.Equal(t, 1, docFailures)
	var items []types.SyncRunItem
	require.NoError(t, db.Where("run_id = ?", run.ID).Find(&items).Error)
	require.Len(t, items, 3)
	documents := 0
	for _, item := range items {
		if item.Kind != "document" {
			require.Empty(t, item.JobID)
			continue
		}
		documents++
		child, err := r.GetJob(ctx, 1, item.JobID)
		require.NoError(t, err)
		require.NotEqual(t, types.ProcessingSucceeded, child.Status)
		require.True(t, child.PlanSealed)
		var document ProcessingDocumentSpec
		require.NoError(t, json.Unmarshal(child.Metadata, &document))
		require.Equal(t, "https://docs.qq.com/space/synthetic?mode=wiki_mode&resourceId=one", document.URL)
		require.NotEmpty(t, child.SourceDigest)
	}
	require.Equal(t, 2, documents)
	require.NoError(t, db.First(run, "id = ?", run.ID).Error)
	require.Equal(t, types.SyncLogStatusRunning, run.Status)
	snapshot, err := r.RunSnapshot(ctx, 1, run.ID)
	require.NoError(t, err)
	require.True(t, snapshot.DiscoveryComplete)
	require.Equal(t, 2, snapshot.Active)
	require.Equal(t, 1, snapshot.Links)
	_, err = r.RunSnapshot(ctx, 2, run.ID)
	require.Error(t, err)
	for _, item := range items {
		if item.Kind != "document" {
			continue
		}
		child, err := r.GetJob(ctx, 1, item.JobID)
		require.NoError(t, err)
		steps, err := r.ListSteps(ctx, 1, item.JobID)
		require.NoError(t, err)
		for _, step := range steps {
			if step.Stage != "native_read" {
				continue
			}
			lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: child.ID, Generation: child.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
			require.NoError(t, err)
			require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "permission", ErrorCode: "SYNTHETIC_DENIED"}))
		}
	}
	require.NoError(t, db.First(run, "id = ?", run.ID).Error)
	require.Equal(t, types.SyncLogStatusFailed, run.Status)
	require.NotNil(t, run.FinishedAt)
	frozen := string(run.Result)
	// These fixture state changes exercise projection independence only;
	// full document completion is covered by the separate pipeline tests.
	require.NoError(t, db.Model(&types.ProcessingJob{}).Where("kind = ?", types.ProcessingJobDocument).Update("status", types.ProcessingSucceeded).Error)
	snapshot, err = r.RunSnapshot(ctx, 1, run.ID)
	require.NoError(t, err)
	require.Equal(t, 2, snapshot.Succeeded)
	require.NoError(t, db.First(run, "id = ?", run.ID).Error)
	require.Equal(t, frozen, string(run.Result))
	var events []types.ProcessingEvent
	require.NoError(t, db.Where("event_type = ? AND run_id = ?", "run_finished", run.ID).Find(&events).Error)
	require.Len(t, events, 1)
	require.JSONEq(t, frozen, string(events[0].Detail))
}
