package service

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Uses an already created synthetic export, never creates a second provider
// task. The token arrives on stdin, and the signed URL stays in process memory.
func TestProcessingExportLiveDOCX(t *testing.T) {
	if os.Getenv("PROCESSING_EXPORT_LIVE") != "1" {
		t.Skip("live synthetic export is opt-in")
	}
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	var credentials struct {
		Token       string `json:"token"`
		DownloadURL string `json:"download_url"`
	}
	require.NoError(t, json.NewDecoder(os.Stdin).Decode(&credentials))
	client, err := tencentdocs.NewTencentDocsMCPClient(tencentdocs.MCPClientConfig{Token: credentials.Token, Timeout: 45 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, types.TenantIDContextKey, uint64(1))
	// Tencent no longer exposes this completed task on a second progress
	// request; use the original successful response supplied in memory.
	status := &tencentdocs.ExportStatus{FileURL: credentials.DownloadURL}
	require.True(t, strings.HasSuffix(strings.ToLower(status.ResolvedFileName()), ".docx"))
	data, err := client.DownloadExport(ctx, status.FileURL)
	require.NoError(t, err)
	runs, images, err := processingDOCXInventory(data)
	require.NoError(t, err)
	reader, err := docparser.NewGRPCDocumentReader("lifecycle-docreader:50051")
	require.NoError(t, err)
	result, err := reader.Read(ctx, &types.ReadRequest{FileContent: data, FileName: "source.docx", FileType: "docx", ParserEngine: "builtin", RequestID: "synthetic-lifecycle-export"})
	require.NoError(t, err)
	require.Empty(t, result.Error)
	t.Logf("DOCX source_images=%d parsed_images=%d footnote_present=%v", images, len(result.ImageRefs), strings.Contains(result.MarkdownContent, "DOC-FOOTNOTE-7391"))
	require.NoError(t, validateProcessingDOCXText(runs, images, result))
	require.Contains(t, result.MarkdownContent, "DOC-END-7391")
	require.Contains(t, result.MarkdownContent, "DOC-P-165")
	require.Contains(t, result.MarkdownContent, "DOC-FOOTNOTE-7391")
	require.Equal(t, 1, images)
	e := processingDocumentExecution{artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil), lease: types.ProcessingLease{Job: types.ProcessingJob{ID: "synthetic-export-live", Generation: 1, TenantID: 1}, Step: types.ProcessingStep{ID: "parse", InputFingerprint: "synthetic-export"}}}
	parsed, err := e.prepareParsedImages(ctx, *result)
	require.NoError(t, err)
	require.Len(t, parsed.Assets, 1)
	t.Logf("verified DOCX bytes=%d text_runs=%d declared_images=%d parsed_text_bytes=%d parsed_images=%d", len(data), len(runs), images, len(result.MarkdownContent), len(result.ImageRefs))
}
