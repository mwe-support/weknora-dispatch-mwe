package tencentdocs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func pathClient() *fakeConnectorClient {
	return &fakeConnectorClient{
		nodes: map[string][]Node{
			"s/":       {{ID: "folder", Title: "部门", Type: "folder", HasChildren: true}, {ID: "rootdoc", Title: "root", Type: "wiki_file", DocumentType: "doc"}},
			"s/folder": {{ID: "nested", Title: "流程/制度", Type: "folder", HasChildren: true}},
			"s/nested": {{ID: "doc", Title: "FAQ", Type: "wiki_file", DocumentType: "doc"}},
		},
		infos:    map[string]*FileInfo{"folder": {ID: "folder", Title: "部门", IsFolder: true}, "doc": {ID: "doc", Title: "FAQ", Type: "doc", ModifiedAt: 7}, "rootdoc": {ID: "rootdoc", Title: "root", Type: "doc", ModifiedAt: 8}},
		contents: map[string]*DocumentContent{"doc": {Text: "body"}, "rootdoc": {Text: "root body"}},
	}
}

func TestTencentSourceFolderPaths(t *testing.T) {
	for _, root := range []string{encodeSpaceResourceID("s"), encodeNodeResourceID("s", "folder")} {
		c := pathClient()
		items, err := testConnector(c).FetchAll(context.Background(), testDataSourceConfig(root), []string{root})
		require.NoError(t, err)
		require.NotEmpty(t, items)
		require.Equal(t, "部门/流程／制度", items[0].Metadata["folder_path"])
		require.Equal(t, "部门/流程／制度/FAQ", items[0].Metadata["source_path"])
		if len(items) > 1 {
			require.Empty(t, items[1].Metadata["folder_path"], "sibling must not inherit recursive path")
		}
	}
	p := sourceFolder("", strings.Repeat("中", 80))
	require.True(t, utf8.ValidString(p))
	require.LessOrEqual(t, len(p), 128)
}

type rejectingPathHandler struct{ recordingStreamHandler }

func (h *rejectingPathHandler) ItemRejected(id string) bool {
	return id == encodeNodeResourceID("s", "doc")
}

func TestTencentRejectedItemDoesNotAdvanceCursor(t *testing.T) {
	c := pathClient()
	h := &rejectingPathHandler{}
	cursor, err := testConnector(c).FetchStream(context.Background(), testDataSourceConfig(encodeSpaceResourceID("s")), nil, h)
	require.NoError(t, err)
	state, err := decodeTencentDocsCursor(cursor)
	require.NoError(t, err)
	require.NotContains(t, state.DocumentTimes, encodeNodeResourceID("s", "doc"))
	require.Contains(t, state.DocumentTimes, encodeNodeResourceID("s", "rootdoc"))
	require.NotContains(t, state.FolderPaths, encodeNodeResourceID("s", "doc"))
}

func TestTencentMovedFolderRefetchesUnchangedDocument(t *testing.T) {
	c := pathClient()
	h := &recordingStreamHandler{}
	connector := testConnector(c)
	cfg := testDataSourceConfig(encodeSpaceResourceID("s"))
	cursor, err := connector.FetchStream(context.Background(), cfg, nil, h)
	require.NoError(t, err)
	c.nodes["s/folder"][0].Title = "新目录"
	h = &recordingStreamHandler{}
	_, err = connector.FetchStream(context.Background(), cfg, cursor, h)
	require.NoError(t, err)
	require.Len(t, h.items, 1)
	require.Equal(t, "部门/新目录", h.items[0].Metadata["folder_path"])
}

func TestTencentRetryKeepsFolder(t *testing.T) {
	s := &fetchState{folderPath: "部门/流程", next: copyTencentDocsCursor(nil)}
	item := failedFetchedItem("id", "doc", "s", Node{ID: "doc", Type: "wiki_file"}, "scope", context.DeadlineExceeded)
	item.Metadata["folder_path"] = s.folderPath
	s.trackFileRetry(&item)
	require.Equal(t, s.folderPath, s.next.FileRetries["id"].FolderPath)
}

func TestTencentRetryStreamPreservesRealFolderPath(t *testing.T) {
	client := pathClient()
	client.contentErrs = map[string]error{"doc": errors.New("i/o timeout")}
	c := testConnector(client)
	cfg := testDataSourceConfig(encodeSpaceResourceID("s"))
	cursor, err := c.FetchStream(context.Background(), cfg, nil, &recordingStreamHandler{})
	require.NoError(t, err)
	state, err := decodeTencentDocsCursor(cursor)
	require.NoError(t, err)
	id := encodeNodeResourceID("s", "doc")
	retry := state.FileRetries[id]
	require.Equal(t, "部门/流程／制度", retry.FolderPath)
	retry.NextAt = time.Now().Add(-time.Minute)
	state.FileRetries[id] = retry
	delete(client.contentErrs, "doc")
	h := &recordingStreamHandler{}
	cursor, err = c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(state), h)
	require.NoError(t, err)
	require.Len(t, h.items, 1)
	require.Equal(t, "部门/流程／制度", h.items[0].Metadata["folder_path"])
	require.Equal(t, "部门/流程／制度/FAQ", h.items[0].Metadata["source_path"])
	state, err = decodeTencentDocsCursor(cursor)
	require.NoError(t, err)
	require.Equal(t, "部门/流程／制度", state.FolderPaths[id])
}
