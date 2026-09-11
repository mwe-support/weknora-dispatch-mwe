package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProcessingRestartExportPreservesEvidenceAndRejectsUnsafeReplay(t *testing.T) {
	for _, scenario := range []string{"poll_missing", "download_expired", "uncertain", "downloaded", "parsed", "wrong_tenant", "stale_revision"} {
		t.Run(scenario, func(t *testing.T) {
			r := processingTestStore(t)
			ctx := context.Background()
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "export", SourceRevision: "v1", PipelineFingerprint: "p1"})
			require.NoError(t, err)
			var plan []types.ProcessingStepSpec
			prev := ""
			for _, stage := range []string{"native_read", "export_start", "export_poll", "download", "normalize", "parse"} {
				s := types.ProcessingStepSpec{Stage: stage, UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: stage, RequiredForReady: true}
				if prev != "" {
					s.DependsOn = []string{prev + "/body"}
				}
				plan = append(plan, s)
				prev = stage
			}
			plan = append(plan, types.ProcessingStepSpec{Stage: "questions", UnitKey: "body", Phase: types.ProcessingPhaseProjection, InputFingerprint: "disabled"})
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
			require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage='questions'", job.ID).Update("status", types.ProcessingSkipped).Error)
			require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage IN ?", job.ID, []string{"native_read", "export_start"}).Updates(map[string]any{"status": types.ProcessingSucceeded, "checkpoint_ref": `{"task_id":"acknowledged","request":"saved"}`, "output_manifest_ref": "resource://saved", "output_digest": "digest"}).Error)
			require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage='export_poll'", job.ID).Updates(map[string]any{"status": types.ProcessingBlocked, "error_code": "TENCENT_404"}).Error)
			if scenario == "download_expired" {
				require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage IN ?", job.ID, []string{"export_poll", "normalize"}).Update("status", types.ProcessingSucceeded).Error)
				require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage='download'", job.ID).Updates(map[string]any{"status": types.ProcessingBlocked, "error_code": "EXPORT_DOWNLOAD_URL_EXPIRED"}).Error)
			}
			if scenario == "uncertain" {
				require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage='export_start'", job.ID).Update("error_class", "uncertain").Error)
			}
			if scenario == "downloaded" {
				require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage='download'", job.ID).Update("output_manifest_ref", "resource://bytes").Error)
			}
			if scenario == "parsed" {
				require.NoError(t, r.db.Model(&types.ProcessingStep{}).Where("job_id=? AND stage='parse'", job.ID).Update("status", types.ProcessingSucceeded).Error)
			}
			job, err = r.GetJob(ctx, 1, job.ID)
			require.NoError(t, err)
			control := types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: "one-export-repair", Actor: "operator", Reason: "replace expired acknowledged export"}
			tenant := uint64(1)
			if scenario == "wrong_tenant" {
				tenant = 2
			}
			if scenario == "stale_revision" {
				control.ExpectedRevision++
			}
			err = r.RestartExpiredExport(ctx, tenant, job.ID, control)
			if scenario != "poll_missing" && scenario != "download_expired" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, r.RestartExpiredExport(ctx, 1, job.ID, control))
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			for _, s := range steps {
				if s.Stage == "native_read" {
					require.Equal(t, "resource://saved", s.OutputManifestRef)
				}
				if s.Stage == "export_start" {
					require.Empty(t, s.CheckpointRef)
					require.Empty(t, s.OutputManifestRef)
					require.Equal(t, types.ProcessingEnqueuePending, s.Status)
				}
				if s.Stage == "parse" {
					require.Equal(t, types.ProcessingPlanned, s.Status)
				}
			}
			var events []types.ProcessingEvent
			require.NoError(t, r.db.Where("job_id=? AND event_type='export_attempt_restarted'", job.ID).Order("id").Find(&events).Error)
			require.Len(t, events, 4)
			require.Contains(t, string(events[0].Detail), "acknowledged")
		})
	}
}
