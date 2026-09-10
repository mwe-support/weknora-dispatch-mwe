package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingNativeResumesConfirmedPageAfterFailure(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	r := processingServiceTestStore(t)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	store := NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil)
	firstPageCalls, tailCalls, revision := 0, 0, "1"
	read := func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
		var data any
		if tool == "manage.query_file_info" {
			data = map[string]any{"file_id": "file", "type": "doc", "status": "normal", "last_modify_time": "7391", "title": "synthetic"}
		} else {
			offset, limit := args["offset"].(int), args["limit"].(int)
			if !verify && offset == 0 && limit == 150 {
				firstPageCalls++
			}
			if !verify && offset == 150 {
				tailCalls++
				if tailCalls == 1 {
					return nil, errors.New("synthetic interrupted page")
				}
			}
			nodes := []map[string]any{}
			for i := offset; i < min(offset+limit, 151); i++ {
				nodes = append(nodes, map[string]any{"paragraph_index": i + 1, "text_preview": "synthetic"})
			}
			data = map[string]any{"version": revision, "nodes": nodes, "pagination": map[string]any{"total_nodes": 151, "returned_nodes": len(nodes), "has_more": offset+len(nodes) < 151}}
		}
		encoded, err := json.Marshal(data)
		return &tencentdocs.NativeResponse{Data: encoded, Bytes: int64(len(encoded))}, err
	}
	version, err := tencentdocs.ProbeNativeRevision(ctx, "file", "doc", read)
	require.NoError(t, err)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb",
		DataSourceID: "source", ExternalID: "file", SourceRevision: version, PipelineFingerprint: "pipeline"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "native_read", UnitKey: "source",
		Phase: types.ProcessingPhasePrepare, InputFingerprint: "source", RequiredForReady: true, RequiredForCompletion: true}}))
	claim := func() *types.ProcessingLease {
		t.Helper()
		ops, err := r.PendingDeliveries(ctx, 10)
		require.NoError(t, err)
		require.Len(t, ops, 1)
		var ref types.ProcessingRef
		require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
		lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
		require.NoError(t, err)
		return lease
	}
	first := claim()
	_, err = ReadProcessingNative(ctx, r, store, *first, "file", "doc", read)
	require.ErrorContains(t, err, "synthetic interrupted")
	require.NoError(t, r.FinishStep(ctx, 1, *first, types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "transient", ErrorCode: "SYNTHETIC"}))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.RetryStep(ctx, 1, job.ID, first.Step.ID, job.Revision, "resume", "synthetic-user"))
	second := claim()
	require.NotEmpty(t, second.Step.CheckpointRef)
	outcome, err := ReadProcessingNative(ctx, r, store, *second, "file", "doc", read)
	require.NoError(t, err)
	require.NoError(t, r.FinishStep(ctx, 1, *second, outcome))
	require.Equal(t, 1, firstPageCalls)
	require.Equal(t, 2, tailCalls)
	data, err := store.Read(ctx, second.Job, second.Step, "native_snapshot", outcome.OutputManifestRef, outcome.OutputDigest)
	require.NoError(t, err)
	var snapshot tencentdocs.NativeSnapshot
	require.NoError(t, json.Unmarshal(data, &snapshot))
	require.Equal(t, 151, snapshot.Units)
	require.True(t, snapshot.RequiresExport)
	require.Empty(t, outcome.Completeness) // A complete preview is not a complete DOC body.
	revision = "2"
	_, err = ReadProcessingNative(ctx, r, store, *second, "file", "doc", read)
	require.ErrorContains(t, err, "SOURCE_CHANGED")
	require.Equal(t, 1, firstPageCalls)
}

func TestProcessingNativeSheetResumesConfirmedRangesWithFreshRevisionChecks(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	r := processingServiceTestStore(t)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	store := NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil)
	captured := map[int]int{}
	value := 0
	read := func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
		var data any
		switch tool {
		case "manage.query_file_info":
			data = map[string]any{"file_id": "file", "type": "sheet", "status": "normal", "last_modify_time": "7391", "title": "synthetic"}
		case "sheet.get_sheet_info":
			data = map[string]any{"sheets": []map[string]any{{"sheet_id": "sheet", "sheet_type": "worksheet", "row_count": 201, "col_count": 2}}}
		case "sheet.get_cell_data":
			start := args["start_row"].(int)
			if !verify {
				captured[start]++
				if start == 100 && captured[start] == 1 {
					return nil, context.DeadlineExceeded
				}
			}
			data = map[string]any{"cells": []map[string]any{{"row": 0, "col": 0, "value_type": "NUMBER", "number_value": value}}}
		}
		encoded, err := json.Marshal(data)
		return &tencentdocs.NativeResponse{Data: encoded, Bytes: int64(len(encoded))}, err
	}
	revision, err := tencentdocs.ProbeNativeRevision(ctx, "file", "sheet", read)
	require.NoError(t, err)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: revision, PipelineFingerprint: "fixed"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "native_read", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "fixed", RequiredForReady: true, RequiredForCompletion: true}}))
	claim := func() *types.ProcessingLease {
		ops, err := r.PendingDeliveries(ctx, 10)
		require.NoError(t, err)
		require.Len(t, ops, 1)
		var ref types.ProcessingRef
		require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
		lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
		require.NoError(t, err)
		return lease
	}
	first := claim()
	_, err = ReadProcessingNative(ctx, r, store, *first, "file", "sheet", read)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, r.FinishStep(ctx, 1, *first, types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "transient", ErrorCode: "SYNTHETIC"}))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.RetryStep(ctx, 1, job.ID, first.Step.ID, job.Revision, "resume", "synthetic-user"))
	second := claim()
	out, err := ReadProcessingNative(ctx, r, store, *second, "file", "sheet", read)
	require.NoError(t, err)
	require.NoError(t, r.FinishStep(ctx, 1, *second, out))
	require.Equal(t, map[int]int{0: 1, 100: 2, 200: 1}, captured, "confirmed ranges are replayed; only fresh revision checks reread every range")
	value = 7
	_, err = ReadProcessingNative(ctx, r, store, *second, "file", "sheet", read)
	require.ErrorContains(t, err, "SOURCE_CHANGED_DURING_READ")
}
