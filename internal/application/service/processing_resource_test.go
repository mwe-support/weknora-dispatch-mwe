package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingResourceAdmitsFullExportWithoutNativeMetadata(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	root := tencentdocs.NativeScanRequest{ResourceID: "tdoc:home-node:" + base64.RawURLEncoding.EncodeToString([]byte("folder")), ParentID: "folder", Ancestors: []string{"folder"}}
	listing := func(context.Context, tencentdocs.NativeScanRequest) (*tencentdocs.NativeResponse, error) {
		return &tencentdocs.NativeResponse{Data: json.RawMessage(`{"list":[{"id":"pdf","title":"sample.pdf"}],"finish":true}`)}, nil
	}
	page, err := tencentdocs.ReadNativeScanPage(ctx, root, func(ctx context.Context, _ string, _ map[string]interface{}, _ bool) (*tencentdocs.NativeResponse, error) {
		return listing(ctx, root)
	})
	require.NoError(t, err)
	input, _ := json.Marshal(page.Entries[0])
	e := processingDocumentExecution{kb: &types.KnowledgeBase{}, artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil), lease: types.ProcessingLease{Job: types.ProcessingJob{ID: "scan", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", Generation: 1, OriginRunID: "run1", PipelineFingerprint: "pipeline"}, Step: types.ProcessingStep{ID: "scan-document", Input: input, Attempt: 1, InputFingerprint: "input"}}}
	read := func(context.Context, string, map[string]interface{}, bool) (*tencentdocs.NativeResponse, error) {
		return nil, &tencentdocs.MCPToolError{Tool: "manage.query_file_info", Code: 400001, Message: "file type not support query"}
	}
	out, err := e.scanDocument(ctx, read, listing)
	require.NoError(t, err)
	require.Len(t, out.AdmitDocuments, 1)
	first := out.AdmitDocuments[0]
	var document ProcessingDocumentSpec
	require.NoError(t, json.Unmarshal(first.Job.Metadata, &document))
	require.Equal(t, "resource", document.Kind)
	require.Equal(t, "export_snapshot", document.RevisionMode)
	require.NotNil(t, document.Resource.Listing)
	stages := map[string]bool{}
	for _, step := range first.Steps {
		stages[step.Stage] = true
	}
	for _, stage := range []string{"export_start", "export_poll", "download", "parse", "assets", "text_index", "publish"} {
		require.True(t, stages[stage], stage)
	}
	again, err := e.scanDocument(ctx, read, listing)
	require.NoError(t, err)
	require.Equal(t, first.Job.SourceRevision, again.AdmitDocuments[0].Job.SourceRevision)
	e.lease.Job.OriginRunID = "run2"
	again, err = e.scanDocument(ctx, read, listing)
	require.NoError(t, err)
	require.NotEqual(t, first.Job.SourceRevision, again.AdmitDocuments[0].Job.SourceRevision)
	_, err = e.scanDocument(ctx, read)
	require.Error(t, err, "metadata errors alone cannot authorize export")
}
