package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingLegacyRetryScansOnlyRequestedFiles(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	db := processingServiceTestDatabase(t)
	r := repository.NewProcessingRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "run:restricted", SourceRevision: "restricted", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "scan_page", UnitKey: "root", Kind: "barrier", Phase: types.ProcessingPhaseScan, InputFingerprint: "root", RequiredForCompletion: true}}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	step := steps[0]
	lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	lease.Job.Metadata, _ = json.Marshal(types.ProcessingLegacyRetryScan{LegacyRetry: &types.ProcessingLegacyRetry{Errors: []types.ProcessingLegacyIdentity{{ExternalID: "tdoc:node:c3BhY2U:b25l", FileID: "one"}}}})
	e := processingDocumentExecution{repo: r, lease: *lease, artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil)}
	request := tencentdocs.NativeScanRequest{ResourceID: "tdoc:space:c3BhY2U"}
	read := func(context.Context, string, map[string]interface{}, bool) (*tencentdocs.NativeResponse, error) {
		return &tencentdocs.NativeResponse{Data: json.RawMessage(`{"children":[{"node_id":"one","node_type":"wiki_file","doc_type":"smartcanvas"},{"node_id":"outside","node_type":"wiki_file","doc_type":"smartcanvas"},{"node_id":"folder","node_type":"folder","has_child":true}],"has_next":true}`)}, nil
	}
	out, err := e.scanPage(ctx, request, read)
	require.NoError(t, err)
	documents, pages := 0, 0
	for _, child := range out.ChildSteps {
		if child.Stage == "scan_page" {
			pages++
			continue
		}
		var entry tencentdocs.NativeScanEntry
		require.NoError(t, json.Unmarshal(child.Input, &entry))
		require.Equal(t, "one", entry.FileID)
		documents++
	}
	require.Equal(t, 1, documents)
	require.Equal(t, 2, pages, "continue folder and next-page coverage without admitting unrelated files")
	for _, item := range out.DiscoveredItems {
		require.NotContains(t, item.ExternalID, "outside")
	}
	// A forged delivery cannot bypass the same allowlist before provider I/O.
	e.lease.Step.Input, _ = json.Marshal(tencentdocs.NativeScanEntry{ExternalID: "outside", FileID: "outside", Disposition: "document"})
	calls := 0
	_, err = e.scanDocument(ctx, func(context.Context, string, map[string]interface{}, bool) (*tencentdocs.NativeResponse, error) {
		calls++
		return nil, nil
	})
	require.ErrorIs(t, err, repository.ErrProcessingScope)
	require.Zero(t, calls)
}
