package tencentdocs

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if os.Getenv("SYSTEM_AES_KEY") == "" {
		_ = os.Setenv("SYSTEM_AES_KEY", "01234567890123456789012345678901")
	}
	os.Exit(m.Run())
}

type sourceExportClient struct {
	fakeConnectorClient
	infoCalls, polls int
	getFailure       bool
	downloadFailure  bool
}

func (c *sourceExportClient) GetFileInfo(ctx context.Context, id string) (*FileInfo, error) {
	c.infoCalls++
	if c.getFailure && id == "bad" {
		return nil, context.DeadlineExceeded
	}
	return c.fakeConnectorClient.GetFileInfo(ctx, id)
}
func (c *sourceExportClient) StartExport(_ context.Context, id string) (*ExportTask, error) {
	c.exportStarts++
	return &ExportTask{ID: id}, nil
}
func (c *sourceExportClient) GetExportProgress(_ context.Context, id string) (*ExportStatus, error) {
	c.polls++
	return &ExportStatus{Progress: 100, FileName: "sample." + sourceExportFormat(c.infos[id].Type), FileURL: "https://docs.qq.com/export/" + id + "?signature=never-plain"}, nil
}
func (c *sourceExportClient) DownloadExport(_ context.Context, url string) ([]byte, error) {
	c.downloads++
	if c.downloadFailure {
		return nil, io.ErrUnexpectedEOF
	}
	return []byte("downloaded-file"), nil
}

func TestSourceExportsFourTypesWithoutReadingBody(t *testing.T) {
	for _, kind := range []string{"doc", "sheet", "smartcanvas", "smartsheet"} {
		t.Run(kind, func(t *testing.T) {
			c := &sourceExportClient{fakeConnectorClient: fakeConnectorClient{infos: map[string]*FileInfo{"file": {ID: "file", Title: "sample", Type: kind, Status: "normal"}}, nodes: map[string][]Node{"space/": {{ID: "folder", Title: "Training", Type: "wiki_folder", HasChildren: true}}, "space/folder": {{ID: "file", Type: "wiki_file", DocumentType: kind}}}}}
			connector := newConnectorWithClientFactory(func(MCPClientConfig) (Client, error) { return c, nil })
			h := &recordingStreamHandler{}
			_, err := connector.FetchStream(context.Background(), testDataSourceConfig(encodeSpaceResourceID("space")), nil, h)
			require.NoError(t, err)
			require.Len(t, h.items, 1)
			require.Equal(t, "Training", h.items[0].Metadata["folder_path"])
			require.True(t, strings.HasSuffix(h.items[0].FileName, "."+sourceExportFormat(kind)))
			require.Equal(t, []byte("downloaded-file"), h.items[0].Content)
			require.Equal(t, 1, c.infoCalls)
			require.Equal(t, 1, c.exportStarts)
			require.Equal(t, 1, c.polls)
			require.Zero(t, c.contentGets)
		})
	}
}

func TestSourceOneRetryReusesExportAndThenStaysPermanent(t *testing.T) {
	c := &sourceExportClient{downloadFailure: true, fakeConnectorClient: fakeConnectorClient{infos: map[string]*FileInfo{"bad": {ID: "bad", Title: "bad", Type: "doc", Status: "normal"}}, nodes: map[string][]Node{"space/": {{ID: "bad", Type: "wiki_file", DocumentType: "doc"}}}}}
	connector := newConnectorWithClientFactory(func(MCPClientConfig) (Client, error) { return c, nil })
	config := testDataSourceConfig(encodeSpaceResourceID("space"))
	h := &recordingStreamHandler{}
	cursor, err := connector.FetchStream(context.Background(), config, nil, h)
	require.NoError(t, err)
	require.Equal(t, "scheduled", h.items[0].Metadata["retry_state"])
	encoded, _ := json.Marshal(cursor)
	require.NotContains(t, string(encoded), "signature=never-plain")
	h = &recordingStreamHandler{}
	cursor, err = connector.FetchRetryStream(context.Background(), config, cursor, h)
	require.NoError(t, err)
	require.Equal(t, "exhausted", h.items[0].Metadata["retry_state"])
	require.Equal(t, "1", h.items[0].Metadata["retry_attempt"])
	require.Equal(t, 1, c.exportStarts)
	require.Equal(t, 1, c.polls, "retry downloads the saved URL instead of polling a possibly removed export task")
	require.Equal(t, 2, c.downloads)
	infoCalls := c.infoCalls
	h = &recordingStreamHandler{}
	_, err = connector.FetchStream(context.Background(), config, cursor, h)
	require.NoError(t, err)
	require.Equal(t, infoCalls, c.infoCalls, "a new scan does not reset a permanent failure")
	require.Equal(t, 2, c.downloads)
	require.Contains(t, h.items[0].Metadata["error"], "EOF")
}

func TestSourceNormalFilesContinueBeforeCompensation(t *testing.T) {
	c := &sourceExportClient{getFailure: true, fakeConnectorClient: fakeConnectorClient{infos: map[string]*FileInfo{"bad": {ID: "bad", Type: "doc"}, "good": {ID: "good", Type: "sheet"}}, nodes: map[string][]Node{"space/": {{ID: "bad", Type: "wiki_file", DocumentType: "doc"}, {ID: "good", Type: "wiki_file", DocumentType: "sheet"}}}}}
	connector := newConnectorWithClientFactory(func(MCPClientConfig) (Client, error) { return c, nil })
	h := &recordingStreamHandler{}
	_, err := connector.FetchStream(context.Background(), testDataSourceConfig(encodeSpaceResourceID("space")), nil, h)
	require.NoError(t, err)
	require.Len(t, h.items, 2)
	require.Equal(t, "scheduled", h.items[0].Metadata["retry_state"])
	require.Equal(t, []byte("downloaded-file"), h.items[1].Content)
	require.Equal(t, 2, c.infoCalls, "the failed document was not retried inside the normal pass")
}

func TestSourceRejectsUnsupportedWithoutMetadataOrExport(t *testing.T) {
	c := &sourceExportClient{fakeConnectorClient: fakeConnectorClient{nodes: map[string][]Node{"space/": {{ID: "slide", Type: "wiki_file", DocumentType: "slide"}}}}}
	connector := newConnectorWithClientFactory(func(MCPClientConfig) (Client, error) { return c, nil })
	h := &recordingStreamHandler{}
	_, err := connector.FetchStream(context.Background(), testDataSourceConfig(encodeSpaceResourceID("space")), nil, h)
	require.NoError(t, err)
	require.Len(t, h.items, 1)
	require.Equal(t, "needs_manual", h.items[0].Metadata["retry_state"])
	require.Equal(t, "UNSUPPORTED_FILE_TYPE", h.items[0].Metadata["retry_category"])
	require.Zero(t, c.infoCalls)
	require.Zero(t, c.exportStarts)
}

func TestSourceExportFingerprintIgnoresOfficeExportProperties(t *testing.T) {
	pack := func(body, properties string) []byte {
		var b bytes.Buffer
		z := zip.NewWriter(&b)
		for _, part := range []struct{ name, text string }{{"word/document.xml", body}, {"docProps/core.xml", properties}} {
			w, err := z.Create(part.name)
			require.NoError(t, err)
			_, err = w.Write([]byte(part.text))
			require.NoError(t, err)
		}
		require.NoError(t, z.Close())
		return b.Bytes()
	}
	require.Equal(t, exportContentDigest(pack("same body", "time1")), exportContentDigest(pack("same body", "time2")))
	require.NotEqual(t, exportContentDigest(pack("same body", "time1")), exportContentDigest(pack("changed body", "time1")))
}

func TestSourceResumesSameRunAndMovesDirectoryWithoutReindex(t *testing.T) {
	c := &sourceExportClient{fakeConnectorClient: fakeConnectorClient{infos: map[string]*FileInfo{"file": {ID: "file", Title: "sample", Type: "doc"}}, nodes: map[string][]Node{"space/": {{ID: "folder", Title: "Old", Type: "wiki_folder", HasChildren: true}}, "space/folder": {{ID: "file", Type: "wiki_file", DocumentType: "doc"}}}}}
	connector := newConnectorWithClientFactory(func(MCPClientConfig) (Client, error) { return c, nil })
	config := testDataSourceConfig(encodeSpaceResourceID("space"))
	config.SyncRunID = "run1"
	h := &recordingStreamHandler{}
	cursor, err := connector.FetchStream(context.Background(), config, nil, h)
	require.NoError(t, err)
	require.Len(t, h.items, 1)
	h = &recordingStreamHandler{}
	_, err = connector.FetchStream(context.Background(), config, cursor, h)
	require.NoError(t, err)
	require.Empty(t, h.items)
	require.Equal(t, 1, c.infoCalls, "same-run recovery does not reread a submitted file")
	c.nodes["space/"][0].Title = "New"
	config.SyncRunID = "run2"
	h = &recordingStreamHandler{}
	_, err = connector.FetchStream(context.Background(), config, cursor, h)
	require.NoError(t, err)
	require.Len(t, h.items, 1)
	require.Equal(t, "New", h.items[0].Metadata["folder_path"])
	require.Equal(t, "true", h.items[0].Metadata["directory_only"])
}
