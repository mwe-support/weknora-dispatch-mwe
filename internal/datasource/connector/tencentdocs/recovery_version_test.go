package tencentdocs

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTencentRetryDoesNotAcknowledgeNewVersionUsingOldExport(t *testing.T) {
	root := encodeSpaceResourceID("s")
	id := encodeNodeResourceID("s", "pdf")
	cfg := testDataSourceConfig(root)
	client := &fakeConnectorClient{
		nodes:       map[string][]Node{"s/": {{ID: "pdf", Type: "resource"}}},
		infos:       map[string]*FileInfo{"pdf": {ID: "pdf", Title: "Doc", Type: "pdf", ModifiedAt: 3}},
		exportTasks: map[string]*ExportTask{"pdf": {ID: "task-v3"}},
		exportStats: map[string]*ExportStatus{
			"task-v2": {Progress: 100, FileName: "doc.pdf", FileURL: "https://docs.qq.com/old.pdf"},
			"task-v3": {Progress: 100, FileName: "doc.pdf", FileURL: "https://docs.qq.com/new.pdf"},
		},
		exportData: map[string][]byte{"https://docs.qq.com/old.pdf": []byte("VERSION-2"), "https://docs.qq.com/new.pdf": []byte("VERSION-3")},
	}
	p := copyTencentDocsCursor(nil)
	p.RetryScope = retryScope(cfg)
	p.DocumentTimes[id] = 1
	p.FileRetries[id] = fileRetry{Node: Node{ID: "pdf", Title: "Doc", Type: "resource"}, SpaceID: "s", SourceResourceID: root, ExportTaskID: "task-v2", ExportModifiedAt: 2, State: "scheduled", NextAt: time.Now().Add(-time.Minute)}
	c := testConnector(client)
	h := &recordingStreamHandler{}
	cursor, err := c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(p), h)
	require.NoError(t, err)
	require.Len(t, h.items, 1)
	require.Empty(t, h.items[0].Content)
	require.Contains(t, h.items[0].Metadata["error"], "SOURCE_CHANGED")
	require.Equal(t, 0, client.exportStarts)
	state, err := decodeTencentDocsCursor(cursor)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.DocumentTimes[id])
	require.Equal(t, "exhausted", state.FileRetries[id].State)
}

type rejectedIngestHandler struct{ recordingStreamHandler }

func (*rejectedIngestHandler) ItemRejected(string) bool { return true }
func (*rejectedIngestHandler) ItemIngestError(string) error {
	return errors.New("temporary database unavailable")
}

func TestTencentIngestFailureRetainsBoundedRetryBudget(t *testing.T) {
	root := encodeSpaceResourceID("s")
	id := encodeNodeResourceID("s", "doc")
	cfg := testDataSourceConfig(root)
	client := &fakeConnectorClient{nodes: map[string][]Node{"s/": {{ID: "doc", Type: "wiki_file"}}}, infos: map[string]*FileInfo{"doc": {ID: "doc", Type: "doc", ModifiedAt: 2}}, contents: map[string]*DocumentContent{"doc": {Text: "new body"}}}
	c := testConnector(client)
	h := &rejectedIngestHandler{}
	cursor, err := c.FetchStream(context.Background(), cfg, nil, h)
	require.NoError(t, err)
	for attempt := 0; attempt <= maxFileRetries; attempt++ {
		state, err := decodeTencentDocsCursor(cursor)
		require.NoError(t, err)
		require.NotContains(t, state.DocumentTimes, id)
		r := state.FileRetries[id]
		require.Equal(t, attempt, r.Attempt)
		if attempt == maxFileRetries {
			require.Equal(t, "exhausted", r.State)
			break
		}
		require.Equal(t, "scheduled", r.State)
		r.NextAt = time.Now().Add(-time.Minute)
		state.FileRetries[id] = r
		cursor, err = c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(state), h)
		require.NoError(t, err)
	}
}

func TestTencentMCPReadRetryClassification(t *testing.T) {
	for _, err := range []error{io.ErrUnexpectedEOF, errors.New("request failed with status 503"), errors.New("connection refused")} {
		require.True(t, isRetryableTencentDocsMCPError(toolGetContent, err))
		require.False(t, isRetryableTencentDocsMCPError(toolExportFile, err))
	}
	category, retry := fileRetryCategory(&MCPToolError{Code: 11607}, "export_start")
	require.Equal(t, "INVALID_REQUEST", category)
	require.False(t, retry)
	_, retry = fileRetryCategory(&ExportSizeExceededError{LimitBytes: 100 << 20}, "download")
	require.False(t, retry)
}
