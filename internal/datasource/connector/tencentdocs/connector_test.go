package tencentdocs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

type fakeConnectorClient struct {
	validateErr error
	spaces      []Space
	nodes       map[string][]Node
	nodeErrs    map[string]error
	listCalls   int
	homeNodes   map[string][]HomeNode
	infos       map[string]*FileInfo
	contents    map[string]*DocumentContent
	infoErrs    map[string]error
	contentErrs map[string]error
	exportTasks map[string]*ExportTask
	exportStats map[string]*ExportStatus
	exportData  map[string][]byte
	closed      int
}

func (f *fakeConnectorClient) Validate(context.Context) error { return f.validateErr }
func (f *fakeConnectorClient) ListSpaces(context.Context) ([]Space, error) {
	return append([]Space(nil), f.spaces...), nil
}
func (f *fakeConnectorClient) ListNodes(_ context.Context, spaceID, parentID string) ([]Node, error) {
	f.listCalls++
	if err := f.nodeErrs[spaceID+"/"+parentID]; err != nil {
		return nil, err
	}
	return append([]Node(nil), f.nodes[spaceID+"/"+parentID]...), nil
}
func (f *fakeConnectorClient) ListHomeNodes(_ context.Context, folderID string) ([]HomeNode, error) {
	return append([]HomeNode(nil), f.homeNodes[folderID]...), nil
}

type recordingStreamHandler struct {
	items       []types.FetchedItem
	checkpoints []*types.SyncCursor
}

func (h *recordingStreamHandler) Emit(_ context.Context, item types.FetchedItem) error {
	h.items = append(h.items, item)
	return nil
}

func (h *recordingStreamHandler) Checkpoint(_ context.Context, cursor *types.SyncCursor) error {
	h.checkpoints = append(h.checkpoints, cursor)
	return nil
}
func (f *fakeConnectorClient) GetFileInfo(_ context.Context, fileID string) (*FileInfo, error) {
	if err := f.infoErrs[fileID]; err != nil {
		return nil, err
	}
	info, ok := f.infos[fileID]
	if !ok {
		return nil, errors.New("missing file info")
	}
	copy := *info
	return &copy, nil
}
func (f *fakeConnectorClient) GetContent(_ context.Context, fileID string) (*DocumentContent, error) {
	if err := f.contentErrs[fileID]; err != nil {
		return nil, err
	}
	content, ok := f.contents[fileID]
	if !ok {
		return nil, errors.New("missing content")
	}
	copy := *content
	return &copy, nil
}
func (f *fakeConnectorClient) StartExport(context.Context, string) (*ExportTask, error) {
	for _, task := range f.exportTasks {
		copy := *task
		return &copy, nil
	}
	return nil, errors.New("missing export task")
}
func (f *fakeConnectorClient) GetExportProgress(_ context.Context, taskID string) (*ExportStatus, error) {
	status, ok := f.exportStats[taskID]
	if !ok {
		return nil, errors.New("missing export status")
	}
	copy := *status
	return &copy, nil
}
func (f *fakeConnectorClient) DownloadExport(_ context.Context, fileURL string) ([]byte, error) {
	data, ok := f.exportData[fileURL]
	if !ok {
		return nil, errors.New("missing export data")
	}
	return append([]byte(nil), data...), nil
}
func (f *fakeConnectorClient) Close() error { f.closed++; return nil }

func testConnector(client *fakeConnectorClient) *Connector {
	return newConnectorWithClientFactory(func(MCPClientConfig) (Client, error) { return client, nil })
}

func testDataSourceConfig(resourceIDs ...string) *types.DataSourceConfig {
	return &types.DataSourceConfig{
		Type:        types.ConnectorTypeTencentDocs,
		Credentials: map[string]interface{}{"mcp_token": "secret"},
		ResourceIDs: resourceIDs,
	}
}

func TestConnectorTypeAndValidate(t *testing.T) {
	client := &fakeConnectorClient{}
	connector := testConnector(client)
	if got := connector.Type(); got != types.ConnectorTypeTencentDocs {
		t.Fatalf("Type() = %q, want %q", got, types.ConnectorTypeTencentDocs)
	}
	if err := connector.Validate(context.Background(), testDataSourceConfig()); err != nil {
		t.Fatalf("Validate() error: %v", err)
	}
	if client.closed != 1 {
		t.Fatalf("Close() calls = %d, want 1", client.closed)
	}
}

func TestConnectorValidateRequiresMCPToken(t *testing.T) {
	err := testConnector(&fakeConnectorClient{}).Validate(context.Background(), &types.DataSourceConfig{})
	if err == nil {
		t.Fatal("Validate() expected missing-token error")
	}
}

func TestConnectorListResourcesLazyHierarchy(t *testing.T) {
	client := &fakeConnectorClient{
		spaces: []Space{{ID: "space-1", Title: "财务部", Description: "财务制度"}},
		nodes: map[string][]Node{
			"space-1/": {{ID: "folder-1", Title: "制度", Type: "folder", HasChildren: true}},
			"space-1/folder-1": {{
				ID: "doc-1", Title: "报销制度", Type: "wiki_file", DocumentType: "smartcanvas",
				URL: "https://docs.qq.com/doc/doc-1",
			}},
		},
	}
	connector := testConnector(client)
	ctx := context.Background()

	resources, err := connector.ListResources(ctx, testDataSourceConfig(), "")
	if err != nil || len(resources) != 2 {
		t.Fatalf("ListResources(root) = %+v, %v", resources, err)
	}
	spaces := resources[1:]
	spaceID := encodeSpaceResourceID("space-1")
	if spaces[0].ExternalID != spaceID || spaces[0].Type != resourceTypeSpace || !spaces[0].HasChildren {
		t.Fatalf("space resource = %+v", spaces[0])
	}

	folders, err := connector.ListResources(ctx, testDataSourceConfig(), spaceID)
	if err != nil || len(folders) != 1 {
		t.Fatalf("ListResources(space) = %+v, %v", folders, err)
	}
	folderID := encodeNodeResourceID("space-1", "folder-1")
	if folders[0].ExternalID != folderID || folders[0].ParentID != spaceID || !folders[0].HasChildren {
		t.Fatalf("folder resource = %+v", folders[0])
	}

	docs, err := connector.ListResources(ctx, testDataSourceConfig(), folderID)
	if err != nil || len(docs) != 1 {
		t.Fatalf("ListResources(folder) = %+v, %v", docs, err)
	}
	if docs[0].ParentID != folderID || docs[0].Type != "smartcanvas" {
		t.Fatalf("document resource = %+v", docs[0])
	}
}

func TestConnectorListResourcesIncludesPersonalHomeHierarchy(t *testing.T) {
	client := &fakeConnectorClient{
		spaces: []Space{{ID: "space-1", Title: "财务部"}},
		homeNodes: map[string][]HomeNode{
			"": {
				{ID: "home-folder-1", Title: "制度", IsFolder: true},
				{ID: "home-doc-1", Title: "报销说明", URL: "https://docs.qq.com/doc/home-doc-1"},
			},
			"home-folder-1": {{ID: "home-doc-2", Title: "采购说明"}},
		},
	}
	connector := testConnector(client)
	ctx := context.Background()

	root, err := connector.ListResources(ctx, testDataSourceConfig(), "")
	if err != nil || len(root) != 2 {
		t.Fatalf("ListResources(root) = %+v, %v", root, err)
	}
	if root[0].ExternalID != homeRootResourceID || root[0].Type != resourceTypeHome || !root[0].HasChildren {
		t.Fatalf("personal-home resource = %+v", root[0])
	}
	if root[1].ExternalID != encodeSpaceResourceID("space-1") {
		t.Fatalf("space resource = %+v", root[1])
	}

	home, err := connector.ListResources(ctx, testDataSourceConfig(), homeRootResourceID)
	if err != nil || len(home) != 2 {
		t.Fatalf("ListResources(home) = %+v, %v", home, err)
	}
	folderID := encodeHomeNodeResourceID("home-folder-1")
	if home[0].ExternalID != folderID || home[0].ParentID != homeRootResourceID || !home[0].HasChildren {
		t.Fatalf("home folder resource = %+v", home[0])
	}
	if home[1].ExternalID != encodeHomeNodeResourceID("home-doc-1") || home[1].HasChildren {
		t.Fatalf("home document resource = %+v", home[1])
	}

	children, err := connector.ListResources(ctx, testDataSourceConfig(), folderID)
	if err != nil || len(children) != 1 || children[0].ParentID != folderID {
		t.Fatalf("ListResources(home folder) = %+v, %v", children, err)
	}
}

func TestConnectorResolvePersonalHomeAncestors(t *testing.T) {
	client := &fakeConnectorClient{
		homeNodes: map[string][]HomeNode{
			"":            {{ID: "home-folder", Title: "制度", IsFolder: true}},
			"home-folder": {{ID: "home-doc", Title: "报销说明"}},
		},
	}
	connector := testConnector(client)
	ancestors, err := connector.ResolveResourceAncestors(
		context.Background(), testDataSourceConfig(), []string{encodeHomeNodeResourceID("home-doc")},
	)
	if err != nil {
		t.Fatalf("ResolveResourceAncestors() error: %v", err)
	}
	want := []string{homeRootResourceID, encodeHomeNodeResourceID("home-folder")}
	if !reflect.DeepEqual(ancestors, want) {
		t.Fatalf("ancestors = %v, want %v", ancestors, want)
	}
}

func TestConnectorFetchAllSelectedSpaceTraversesAndDeduplicates(t *testing.T) {
	client := &fakeConnectorClient{
		nodes: map[string][]Node{
			"space-1/": {
				{ID: "doc-1", Title: "报销制度", Type: "wiki_file", DocumentType: "smartcanvas"},
				{ID: "folder-1", Title: "采购", Type: "folder", HasChildren: true},
			},
			"space-1/folder-1": {
				{ID: "doc-2", Title: "采购制度", Type: "wiki_file", DocumentType: "smartcanvas"},
			},
		},
		infos: map[string]*FileInfo{
			"doc-1":    {ID: "doc-1", Title: "报销制度", Type: "smartcanvas", ModifiedAt: 100},
			"doc-2":    {ID: "doc-2", Title: "采购制度", Type: "smartcanvas", ModifiedAt: 200},
			"folder-1": {ID: "folder-1", Title: "采购", IsFolder: true},
		},
		contents: map[string]*DocumentContent{
			"doc-1": {Text: "# 报销制度"},
			"doc-2": {Text: "# 采购制度"},
		},
	}
	connector := testConnector(client)
	spaceID := encodeSpaceResourceID("space-1")
	folderID := encodeNodeResourceID("space-1", "folder-1")
	items, err := connector.FetchAll(context.Background(), testDataSourceConfig(spaceID, folderID), []string{spaceID, folderID})
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("FetchAll() len = %d, want 2: %+v", len(items), items)
	}
	if items[0].ContentType != "text/markdown" || items[0].Metadata["channel"] != types.ChannelTencentDocs {
		t.Fatalf("item metadata = %+v", items[0])
	}
}

func TestConnectorFetchAllPersonalHomeAndSpaceTraversesAndDeduplicatesByFileID(t *testing.T) {
	client := &fakeConnectorClient{
		homeNodes: map[string][]HomeNode{
			"": {
				{ID: "doc-shared", Title: "共同制度"},
				{ID: "home-folder", Title: "个人资料", IsFolder: true},
			},
			"home-folder": {{ID: "doc-home", Title: "个人说明"}},
		},
		nodes: map[string][]Node{
			"space-1/": {
				{ID: "doc-shared", Title: "共同制度", Type: "wiki_file", DocumentType: "smartcanvas"},
				{ID: "doc-space", Title: "空间制度", Type: "wiki_file", DocumentType: "smartcanvas"},
			},
		},
		infos: map[string]*FileInfo{
			"doc-shared": {ID: "doc-shared", Title: "共同制度", Type: "smartcanvas", ModifiedAt: 100},
			"doc-home":   {ID: "doc-home", Title: "个人说明", Type: "smartcanvas", ModifiedAt: 200},
			"doc-space":  {ID: "doc-space", Title: "空间制度", Type: "smartcanvas", ModifiedAt: 300, SpaceID: "space-1"},
		},
		contents: map[string]*DocumentContent{
			"doc-shared": {Text: "shared"},
			"doc-home":   {Text: "home"},
			"doc-space":  {Text: "space"},
		},
	}
	connector := testConnector(client)
	spaceID := encodeSpaceResourceID("space-1")
	items, err := connector.FetchAll(
		context.Background(), testDataSourceConfig(homeRootResourceID, spaceID),
		[]string{homeRootResourceID, spaceID},
	)
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("FetchAll() len = %d, want 3 unique files: %+v", len(items), items)
	}
	seen := make(map[string]int)
	for _, item := range items {
		seen[item.Metadata["file_id"]]++
	}
	if !reflect.DeepEqual(seen, map[string]int{"doc-shared": 1, "doc-home": 1, "doc-space": 1}) {
		t.Fatalf("file-id counts = %#v", seen)
	}
	if items[0].Metadata["location"] != "personal_home" {
		t.Fatalf("personal-home metadata = %+v", items[0].Metadata)
	}
}

func TestConnectorFetchAllSelectedPersonalHomeResourceExportsFile(t *testing.T) {
	client := &fakeConnectorClient{
		homeNodes: map[string][]HomeNode{"": {{ID: "home-pdf", Title: "附件.pdf"}}},
		infos: map[string]*FileInfo{
			"home-pdf": {ID: "home-pdf", Title: "附件.pdf", Type: "pdf", ModifiedAt: 100},
		},
		exportTasks: map[string]*ExportTask{"home-pdf": {ID: "task-home-pdf"}},
		exportStats: map[string]*ExportStatus{
			"task-home-pdf": {Progress: 100, Status: "done", FileName: "附件.pdf", FileURL: "https://example.invalid/home-pdf"},
		},
		exportData: map[string][]byte{"https://example.invalid/home-pdf": []byte("pdf-bytes")},
	}
	connector := testConnector(client)
	items, err := connector.FetchAll(
		context.Background(), testDataSourceConfig(homeRootResourceID), []string{homeRootResourceID},
	)
	if err != nil || len(items) != 1 {
		t.Fatalf("FetchAll() = %+v, %v", items, err)
	}
	item := items[0]
	if string(item.Content) != "pdf-bytes" || item.ExternalID != encodeHomeNodeResourceID("home-pdf") || item.Metadata["location"] != "personal_home" {
		t.Fatalf("personal-home resource item = %+v", item)
	}
}

func TestConnectorFetchAllContinuesAfterOneDocumentFails(t *testing.T) {
	client := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {
			{ID: "doc-broken", Title: "损坏文档", Type: "wiki_file", DocumentType: "smartcanvas"},
			{ID: "doc-ok", Title: "正常文档", Type: "wiki_file", DocumentType: "smartcanvas"},
		}},
		infos: map[string]*FileInfo{
			"doc-broken": {ID: "doc-broken", Title: "损坏文档", ModifiedAt: 100},
			"doc-ok":     {ID: "doc-ok", Title: "正常文档", ModifiedAt: 100},
		},
		contents:    map[string]*DocumentContent{"doc-ok": {Text: "ok"}},
		contentErrs: map[string]error{"doc-broken": errors.New("unsupported document")},
	}
	items, err := testConnector(client).FetchAll(
		context.Background(), testDataSourceConfig(encodeSpaceResourceID("space-1")),
		[]string{encodeSpaceResourceID("space-1")},
	)
	if err != nil {
		t.Fatalf("FetchAll() must continue after a per-document error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %+v, want error placeholder + successful document", items)
	}
	if items[0].Metadata["error"] == "" || string(items[1].Content) != "ok" {
		t.Fatalf("items = %+v, want classified failure then successful content", items)
	}
}

func TestConnectorFetchAllExportsResourceFiles(t *testing.T) {
	client := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {
			{ID: "binary-1", Title: "附件.pdf", Type: "resource"},
			{ID: "doc-ok", Title: "正常文档", Type: "wiki_file", DocumentType: "smartcanvas"},
		}},
		infos: map[string]*FileInfo{
			"binary-1": {ID: "binary-1", Title: "附件.pdf", Type: "pdf", ModifiedAt: 90},
			"doc-ok":   {ID: "doc-ok", Title: "正常文档", ModifiedAt: 100},
		},
		contents:    map[string]*DocumentContent{"doc-ok": {Text: "ok"}},
		exportTasks: map[string]*ExportTask{"binary-1": {ID: "task-1"}},
		exportStats: map[string]*ExportStatus{"task-1": {
			Progress: 100, FileName: "附件.pdf", FileURL: "https://example.myqcloud.com/attachment.pdf",
		}},
		exportData: map[string][]byte{"https://example.myqcloud.com/attachment.pdf": []byte("pdf-bytes")},
	}
	items, err := testConnector(client).FetchAll(
		context.Background(), testDataSourceConfig(encodeSpaceResourceID("space-1")),
		[]string{encodeSpaceResourceID("space-1")},
	)
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %+v, want exported resource + document", items)
	}
	if string(items[0].Content) != "pdf-bytes" || items[0].FileName != "附件.pdf" {
		t.Fatalf("resource item = %+v, want exported bytes and file name", items[0])
	}
	if string(items[1].Content) != "ok" {
		t.Fatalf("document item = %+v", items[1])
	}
}

func TestConnectorFetchIncrementalReallySkipsUnchangedDocument(t *testing.T) {
	spaceID := encodeSpaceResourceID("space-1")
	externalID := encodeNodeResourceID("space-1", "doc-1")
	client := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {{
			ID: "doc-1", Title: "A", Type: "wiki_file", DocumentType: "smartcanvas",
		}}},
		infos:    map[string]*FileInfo{"doc-1": {ID: "doc-1", Title: "A", ModifiedAt: 100}},
		contents: map[string]*DocumentContent{},
	}
	cursor := &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"document_times": map[string]interface{}{externalID: float64(100)},
	}}
	items, next, err := testConnector(client).FetchIncremental(
		context.Background(), testDataSourceConfig(spaceID), cursor,
	)
	if err != nil {
		t.Fatalf("FetchIncremental() error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("items = %+v, want unchanged document to be skipped", items)
	}
	if next == nil {
		t.Fatal("next cursor is nil")
	}
}

func TestConnectorFetchIncrementalFileInfoFailureDoesNotEmitDeletion(t *testing.T) {
	spaceID := encodeSpaceResourceID("space-1")
	externalID := encodeNodeResourceID("space-1", "doc-1")
	client := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {{
			ID: "doc-1", Title: "A", Type: "wiki_file", DocumentType: "smartcanvas",
		}}},
		infoErrs: map[string]error{"doc-1": errors.New("temporary metadata failure")},
	}
	cursor := &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"document_times": map[string]interface{}{externalID: float64(100)},
	}}
	items, _, err := testConnector(client).FetchIncremental(
		context.Background(), testDataSourceConfig(spaceID), cursor,
	)
	if err != nil {
		t.Fatalf("FetchIncremental() error: %v", err)
	}
	if len(items) != 1 || items[0].IsDeleted || items[0].Metadata["error"] == "" {
		t.Fatalf("items = %+v, want one non-deletion failure", items)
	}
}

func TestConnectorFetchIncrementalSkipsUnchangedAndDetectsDeletion(t *testing.T) {
	spaceID := encodeSpaceResourceID("space-1")
	first := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {
			{ID: "doc-1", Title: "A", Type: "wiki_file", DocumentType: "smartcanvas"},
			{ID: "doc-2", Title: "B", Type: "wiki_file", DocumentType: "smartcanvas"},
		}},
		infos: map[string]*FileInfo{
			"doc-1": {ID: "doc-1", Title: "A", ModifiedAt: 100},
			"doc-2": {ID: "doc-2", Title: "B", ModifiedAt: 100},
		},
		contents: map[string]*DocumentContent{
			"doc-1": {Text: "a"}, "doc-2": {Text: "b"},
		},
	}
	_, cursor, err := testConnector(first).FetchIncremental(
		context.Background(), testDataSourceConfig(spaceID), nil,
	)
	if err != nil {
		t.Fatalf("first FetchIncremental() error: %v", err)
	}

	second := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {
			{ID: "doc-1", Title: "A2", Type: "wiki_file", DocumentType: "smartcanvas"},
		}},
		infos:    map[string]*FileInfo{"doc-1": {ID: "doc-1", Title: "A2", ModifiedAt: 200}},
		contents: map[string]*DocumentContent{"doc-1": {Text: "a2"}},
	}
	items, next, err := testConnector(second).FetchIncremental(
		context.Background(), testDataSourceConfig(spaceID), cursor,
	)
	if err != nil {
		t.Fatalf("second FetchIncremental() error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %+v, want changed + deleted", items)
	}
	got := map[string]bool{}
	for _, item := range items {
		got[item.ExternalID+"/deleted="+map[bool]string{true: "yes", false: "no"}[item.IsDeleted]] = true
	}
	want := map[string]bool{
		encodeNodeResourceID("space-1", "doc-1") + "/deleted=no":  true,
		encodeNodeResourceID("space-1", "doc-2") + "/deleted=yes": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("items identities = %#v, want %#v", got, want)
	}
	if next == nil {
		t.Fatal("next cursor is nil")
	}
}

func TestConnectorResolveResourceAncestors(t *testing.T) {
	client := &fakeConnectorClient{nodes: map[string][]Node{
		"space-1/":         {{ID: "folder-1", Title: "制度", Type: "folder", HasChildren: true}},
		"space-1/folder-1": {{ID: "doc-1", Title: "报销制度", Type: "wiki_file"}},
	}}
	connector := testConnector(client)
	got, err := connector.ResolveResourceAncestors(
		context.Background(), testDataSourceConfig(), []string{encodeNodeResourceID("space-1", "doc-1")},
	)
	if err != nil {
		t.Fatalf("ResolveResourceAncestors() error: %v", err)
	}
	want := []string{encodeSpaceResourceID("space-1"), encodeNodeResourceID("space-1", "folder-1")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ancestors = %#v, want %#v", got, want)
	}
}

func TestConnectorResolveResourceAncestorsTraversesEachSpaceOnce(t *testing.T) {
	client := &fakeConnectorClient{nodes: map[string][]Node{
		"space-1/": {
			{ID: "folder-1", Title: "制度", Type: "folder", HasChildren: true},
		},
		"space-1/folder-1": {
			{ID: "doc-1", Title: "报销制度", Type: "wiki_file"},
			{ID: "doc-2", Title: "采购制度", Type: "wiki_file"},
		},
	}}
	connector := testConnector(client)
	got, err := connector.ResolveResourceAncestors(
		context.Background(), testDataSourceConfig(), []string{
			encodeNodeResourceID("space-1", "doc-1"),
			encodeNodeResourceID("space-1", "doc-2"),
		},
	)
	if err != nil {
		t.Fatalf("ResolveResourceAncestors() error: %v", err)
	}
	want := []string{encodeSpaceResourceID("space-1"), encodeNodeResourceID("space-1", "folder-1")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ancestors = %#v, want %#v", got, want)
	}
	if client.listCalls != 2 {
		t.Fatalf("ListNodes calls = %d, want one traversal of the space", client.listCalls)
	}
}

func TestConnectorSelectedDocumentDoesNotTraverseWholeSpace(t *testing.T) {
	client := &fakeConnectorClient{
		infos: map[string]*FileInfo{"doc-1": {
			ID: "doc-1", Title: "直选文档", Type: "smartcanvas", ModifiedAt: 100,
		}},
		contents: map[string]*DocumentContent{"doc-1": {Text: "直选内容"}},
	}
	selected := encodeNodeResourceID("space-1", "doc-1")
	items, err := testConnector(client).FetchAll(
		context.Background(), testDataSourceConfig(selected), []string{selected},
	)
	if err != nil || len(items) != 1 {
		t.Fatalf("FetchAll(selected) = %+v, %v", items, err)
	}
	if client.listCalls != 0 {
		t.Fatalf("ListNodes calls = %d, want 0 for a directly selected document", client.listCalls)
	}
}

func TestConnectorFetchStreamEmitsAndCheckpointsWithoutBuffering(t *testing.T) {
	client := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {
			{ID: "doc-1", Title: "A", Type: "wiki_file", DocumentType: "smartcanvas"},
			{ID: "doc-2", Title: "B", Type: "wiki_file", DocumentType: "smartcanvas"},
		}},
		infos: map[string]*FileInfo{
			"doc-1": {ID: "doc-1", Title: "A", ModifiedAt: 100},
			"doc-2": {ID: "doc-2", Title: "B", ModifiedAt: 200},
		},
		contents: map[string]*DocumentContent{
			"doc-1": {Text: "a"}, "doc-2": {Text: "b"},
		},
	}
	handler := &recordingStreamHandler{}
	spaceID := encodeSpaceResourceID("space-1")
	next, err := testConnector(client).FetchStream(
		context.Background(), testDataSourceConfig(spaceID), nil, handler,
	)
	if err != nil {
		t.Fatalf("FetchStream() error: %v", err)
	}
	if len(handler.items) != 2 || len(handler.checkpoints) != 2 {
		t.Fatalf("items=%d checkpoints=%d, want 2/2", len(handler.items), len(handler.checkpoints))
	}
	if next == nil || next.ConnectorCursor == nil {
		t.Fatal("final cursor is nil")
	}
}

func TestConnectorChildListingFailureIsPartialNotFatal(t *testing.T) {
	client := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {
			{ID: "folder-broken", Title: "受限目录", Type: "folder", HasChildren: true},
			{ID: "doc-ok", Title: "正常文档", Type: "wiki_file", DocumentType: "smartcanvas"},
		}},
		nodeErrs: map[string]error{"space-1/folder-broken": errors.New("permission denied")},
		infos:    map[string]*FileInfo{"doc-ok": {ID: "doc-ok", Title: "正常文档", ModifiedAt: 1}},
		contents: map[string]*DocumentContent{"doc-ok": {Text: "ok"}},
	}
	spaceID := encodeSpaceResourceID("space-1")
	items, err := testConnector(client).FetchAll(
		context.Background(), testDataSourceConfig(spaceID), []string{spaceID},
	)
	if err != nil {
		t.Fatalf("FetchAll() must continue after one subtree fails: %v", err)
	}
	if len(items) != 2 || items[0].Metadata["error"] == "" || string(items[1].Content) != "ok" {
		t.Fatalf("items = %+v, want subtree failure + successful sibling", items)
	}
}
