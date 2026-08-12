package tencentdocs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

const (
	resourceTypeSpace  = "tencent_docs_space"
	spaceIDPrefix      = "tdoc:space:"
	nodeIDPrefix       = "tdoc:node:"
	exportPollInterval = 3 * time.Second
	exportTimeout      = 2 * time.Minute
)

type connectorClientFactory func(MCPClientConfig) (Client, error)

// Connector adapts Tencent Docs' MCP hierarchy to WeKnora's generic data-source
// lifecycle (credential validation, lazy range selection and scheduled sync).
type Connector struct {
	clientFactory connectorClientFactory
}

var _ datasource.Connector = (*Connector)(nil)
var _ datasource.StreamingConnector = (*Connector)(nil)

// NewConnector constructs the production Tencent Docs connector.
func NewConnector() *Connector {
	return newConnectorWithClientFactory(func(config MCPClientConfig) (Client, error) {
		return NewTencentDocsMCPClient(config)
	})
}

func newConnectorWithClientFactory(factory connectorClientFactory) *Connector {
	return &Connector{clientFactory: factory}
}

func (c *Connector) Type() string { return types.ConnectorTypeTencentDocs }

func (c *Connector) newClient(config *types.DataSourceConfig) (Client, error) {
	if c == nil || c.clientFactory == nil {
		return nil, errors.New("Tencent Docs client factory is not configured")
	}
	clientConfig, err := parseMCPClientConfig(config)
	if err != nil {
		return nil, err
	}
	return c.clientFactory(clientConfig)
}

func parseMCPClientConfig(config *types.DataSourceConfig) (MCPClientConfig, error) {
	if config == nil {
		return MCPClientConfig{}, fmt.Errorf("%w: Tencent Docs configuration is required", datasource.ErrInvalidConfig)
	}
	raw, ok := config.Credentials["mcp_token"]
	if !ok {
		return MCPClientConfig{}, fmt.Errorf("%w: Tencent Docs MCP token is required", datasource.ErrInvalidCredentials)
	}
	token, ok := raw.(string)
	if !ok || strings.TrimSpace(token) == "" {
		return MCPClientConfig{}, fmt.Errorf("%w: Tencent Docs MCP token is required", datasource.ErrInvalidCredentials)
	}
	return MCPClientConfig{Token: strings.TrimSpace(token)}, nil
}

func (c *Connector) Validate(ctx context.Context, config *types.DataSourceConfig) error {
	client, err := c.newClient(config)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Validate(ctx); err != nil {
		return fmt.Errorf("Tencent Docs connection failed: %w", err)
	}
	return nil
}

// ListResources provides a lazy hierarchy: root -> spaces -> direct nodes.
func (c *Connector) ListResources(
	ctx context.Context,
	config *types.DataSourceConfig,
	parentID string,
) ([]types.Resource, error) {
	client, err := c.newClient(config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if parentID == "" {
		spaces, err := client.ListSpaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("list Tencent Docs spaces: %w", err)
		}
		resources := make([]types.Resource, 0, len(spaces))
		for _, space := range spaces {
			resources = append(resources, types.Resource{
				ExternalID:  encodeSpaceResourceID(space.ID),
				Name:        space.Title,
				Type:        resourceTypeSpace,
				Description: space.Description,
				HasChildren: true,
				ModifiedAt:  timestamp(space.UpdatedAt),
				Metadata: map[string]interface{}{
					"space_id":   space.ID,
					"file_count": space.FileCount,
					"is_owner":   space.IsOwner,
				},
			})
		}
		sort.SliceStable(resources, func(i, j int) bool { return resources[i].Name < resources[j].Name })
		return resources, nil
	}

	ref, err := decodeResourceID(parentID)
	if err != nil {
		return nil, err
	}
	parentNodeID := ""
	if ref.kind == resourceKindNode {
		parentNodeID = ref.nodeID
	}
	nodes, err := client.ListNodes(ctx, ref.spaceID, parentNodeID)
	if err != nil {
		return nil, fmt.Errorf("list Tencent Docs nodes: %w", err)
	}
	return nodesToResources(ref.spaceID, parentID, nodes), nil
}

func nodesToResources(spaceID, parentID string, nodes []Node) []types.Resource {
	resources := make([]types.Resource, 0, len(nodes))
	for _, node := range nodes {
		if isLinkNode(node) {
			continue
		}
		resourceType := node.DocumentType
		if resourceType == "" {
			resourceType = node.Type
		}
		resources = append(resources, types.Resource{
			ExternalID:  encodeNodeResourceID(spaceID, node.ID),
			Name:        node.Title,
			Type:        resourceType,
			Description: node.Type,
			URL:         node.URL,
			ParentID:    parentID,
			HasChildren: node.HasChildren,
			Metadata: map[string]interface{}{
				"space_id":  spaceID,
				"node_id":   node.ID,
				"node_type": node.Type,
				"doc_type":  node.DocumentType,
			},
		})
	}
	return resources
}

func (c *Connector) ResolveResourceAncestors(
	ctx context.Context,
	config *types.DataSourceConfig,
	resourceIDs []string,
) ([]string, error) {
	client, err := c.newClient(config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	type spaceTargets struct {
		spaceID string
		targets map[string]bool
		order   []string
	}
	grouped := make(map[string]*spaceTargets)
	spaceOrder := make([]string, 0)
	for _, resourceID := range resourceIDs {
		ref, err := decodeResourceID(resourceID)
		if err != nil {
			return nil, err
		}
		if ref.kind == resourceKindSpace {
			continue
		}
		group := grouped[ref.spaceID]
		if group == nil {
			group = &spaceTargets{spaceID: ref.spaceID, targets: make(map[string]bool)}
			grouped[ref.spaceID] = group
			spaceOrder = append(spaceOrder, ref.spaceID)
		}
		if !group.targets[ref.nodeID] {
			group.targets[ref.nodeID] = true
			group.order = append(group.order, ref.nodeID)
		}
	}

	result := make([]string, 0)
	seen := make(map[string]bool)
	for _, spaceID := range spaceOrder {
		group := grouped[spaceID]
		paths := make(map[string][]string, len(group.targets))
		rootID := encodeSpaceResourceID(spaceID)
		if err := findAncestorPaths(
			ctx, client, spaceID, "", group.targets, []string{rootID},
			make(map[string]bool), paths,
		); err != nil {
			return nil, fmt.Errorf("resolve Tencent Docs ancestors in space %s: %w", spaceID, err)
		}
		for _, nodeID := range group.order {
			path, found := paths[nodeID]
			if !found {
				return nil, fmt.Errorf("%w: Tencent Docs node %s", datasource.ErrResourceNotFound, nodeID)
			}
			for _, id := range path {
				if !seen[id] {
					seen[id] = true
					result = append(result, id)
				}
			}
		}
	}
	return result, nil
}

// findAncestorPaths traverses a Tencent Docs space once for all selected
// nodes. The resource picker may ask for several selected nodes at once; doing
// one full DFS per node makes editing a data source quadratic on large spaces.
func findAncestorPaths(
	ctx context.Context,
	client Client,
	spaceID, parentNodeID string,
	targetNodeIDs map[string]bool,
	path []string,
	visited map[string]bool,
	paths map[string][]string,
) error {
	if len(paths) == len(targetNodeIDs) {
		return nil
	}
	visitKey := spaceID + "/" + parentNodeID
	if visited[visitKey] {
		return nil
	}
	visited[visitKey] = true
	nodes, err := client.ListNodes(ctx, spaceID, parentNodeID)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if targetNodeIDs[node.ID] {
			paths[node.ID] = append([]string(nil), path...)
		}
		if len(paths) == len(targetNodeIDs) {
			return nil
		}
		if !node.HasChildren {
			continue
		}
		childPath := append(append([]string(nil), path...), encodeNodeResourceID(spaceID, node.ID))
		if err := findAncestorPaths(
			ctx, client, spaceID, node.ID, targetNodeIDs, childPath, visited, paths,
		); err != nil {
			return err
		}
	}
	return nil
}

func (c *Connector) FetchAll(
	ctx context.Context,
	config *types.DataSourceConfig,
	resourceIDs []string,
) ([]types.FetchedItem, error) {
	items, _, err := c.fetch(ctx, config, resourceIDs, nil, false, nil)
	return items, err
}

type tencentDocsCursor struct {
	DocumentTimes map[string]uint64 `json:"document_times"`
}

func (c *Connector) FetchIncremental(
	ctx context.Context,
	config *types.DataSourceConfig,
	cursor *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	if config == nil || len(config.ResourceIDs) == 0 {
		return nil, nil, errors.New("no Tencent Docs resources configured")
	}
	var previous *tencentDocsCursor
	if cursor != nil && cursor.ConnectorCursor != nil {
		previous = &tencentDocsCursor{}
		encoded, _ := json.Marshal(cursor.ConnectorCursor)
		if err := json.Unmarshal(encoded, previous); err != nil {
			return nil, nil, fmt.Errorf("decode Tencent Docs sync cursor: %w", err)
		}
	}
	items, next, err := c.fetch(ctx, config, config.ResourceIDs, previous, true, nil)
	if err != nil {
		return nil, nil, err
	}
	cursorMap := make(map[string]interface{})
	encoded, _ := json.Marshal(next)
	_ = json.Unmarshal(encoded, &cursorMap)
	return items, &types.SyncCursor{LastSyncTime: time.Now().UTC(), ConnectorCursor: cursorMap}, nil
}

// FetchStream interleaves Tencent Docs retrieval with WeKnora ingestion and
// persists a complete cursor after each item. This bounds memory for large
// spaces and lets a retried sync converge instead of restarting from zero.
func (c *Connector) FetchStream(
	ctx context.Context,
	config *types.DataSourceConfig,
	cursor *types.SyncCursor,
	handler datasource.StreamHandler,
) (*types.SyncCursor, error) {
	if config == nil || len(config.ResourceIDs) == 0 {
		return nil, errors.New("no Tencent Docs resources configured")
	}
	if handler == nil {
		return nil, errors.New("Tencent Docs stream handler is required")
	}
	previous, err := decodeTencentDocsCursor(cursor)
	if err != nil {
		return nil, err
	}
	_, next, err := c.fetch(ctx, config, config.ResourceIDs, previous, cursor != nil, handler)
	if err != nil {
		return nil, err
	}
	return syncCursorFromTencentDocs(next), nil
}

func decodeTencentDocsCursor(cursor *types.SyncCursor) (*tencentDocsCursor, error) {
	if cursor == nil || cursor.ConnectorCursor == nil {
		return nil, nil
	}
	previous := &tencentDocsCursor{}
	encoded, _ := json.Marshal(cursor.ConnectorCursor)
	if err := json.Unmarshal(encoded, previous); err != nil {
		return nil, fmt.Errorf("decode Tencent Docs sync cursor: %w", err)
	}
	return previous, nil
}

func syncCursorFromTencentDocs(cursor *tencentDocsCursor) *types.SyncCursor {
	cursorMap := make(map[string]interface{})
	encoded, _ := json.Marshal(cursor)
	_ = json.Unmarshal(encoded, &cursorMap)
	return &types.SyncCursor{LastSyncTime: time.Now().UTC(), ConnectorCursor: cursorMap}
}

func (c *Connector) fetch(
	ctx context.Context,
	config *types.DataSourceConfig,
	resourceIDs []string,
	previous *tencentDocsCursor,
	incremental bool,
	handler datasource.StreamHandler,
) ([]types.FetchedItem, *tencentDocsCursor, error) {
	if len(resourceIDs) == 0 {
		return nil, nil, errors.New("no Tencent Docs resources configured")
	}
	client, err := c.newClient(config)
	if err != nil {
		return nil, nil, err
	}
	defer client.Close()

	state := &fetchState{
		client:       client,
		handler:      handler,
		previous:     previous,
		incremental:  incremental,
		seenDocs:     make(map[string]bool),
		visitedNodes: make(map[string]bool),
		next:         copyTencentDocsCursor(previous),
	}
	for _, selectedID := range resourceIDs {
		ref, err := decodeResourceID(selectedID)
		if err != nil {
			return nil, nil, err
		}
		if ref.kind == resourceKindSpace {
			nodes, err := client.ListNodes(ctx, ref.spaceID, "")
			if err != nil {
				return nil, nil, fmt.Errorf("list Tencent Docs space %s: %w", ref.spaceID, err)
			}
			if err := state.walkNodes(ctx, ref.spaceID, nodes, selectedID); err != nil {
				return nil, nil, err
			}
			continue
		}

		info, err := client.GetFileInfo(ctx, ref.nodeID)
		if err != nil {
			return nil, nil, fmt.Errorf("get selected Tencent Docs node %s: %w", ref.nodeID, err)
		}
		node := nodeFromFileInfo(info)
		if info.IsFolder {
			children, listErr := client.ListNodes(ctx, ref.spaceID, ref.nodeID)
			if listErr != nil {
				return nil, nil, fmt.Errorf("list selected Tencent Docs folder %s: %w", ref.nodeID, listErr)
			}
			if err := state.walkNodes(ctx, ref.spaceID, children, selectedID); err != nil {
				return nil, nil, err
			}
			continue
		}
		if !isSyncableNode(node) {
			return nil, nil, fmt.Errorf("%w: Tencent Docs node %s is not a syncable document", datasource.ErrInvalidConfig, ref.nodeID)
		}
		if err := state.fetchNode(ctx, ref.spaceID, node, info, selectedID); err != nil {
			return nil, nil, err
		}
	}

	if incremental && previous != nil {
		for externalID := range previous.DocumentTimes {
			if !state.seenDocs[externalID] {
				delete(state.next.DocumentTimes, externalID)
				if err := state.emit(ctx, types.FetchedItem{ExternalID: externalID, IsDeleted: true}); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return state.items, state.next, nil
}

func copyTencentDocsCursor(previous *tencentDocsCursor) *tencentDocsCursor {
	next := &tencentDocsCursor{DocumentTimes: make(map[string]uint64)}
	if previous != nil {
		for externalID, modifiedAt := range previous.DocumentTimes {
			next.DocumentTimes[externalID] = modifiedAt
		}
	}
	return next
}

func nodeFromFileInfo(info *FileInfo) Node {
	node := Node{ID: info.ID, Title: info.Title, URL: info.URL, HasChildren: info.IsFolder}
	if info.IsFolder {
		node.Type = "folder"
		return node
	}
	if isTencentDocsOnlineType(info.Type) {
		node.Type = "wiki_file"
		node.DocumentType = info.Type
	} else {
		node.Type = "resource"
	}
	return node
}

type fetchState struct {
	client       Client
	handler      datasource.StreamHandler
	previous     *tencentDocsCursor
	incremental  bool
	seenDocs     map[string]bool
	visitedNodes map[string]bool
	next         *tencentDocsCursor
	items        []types.FetchedItem
}

func (s *fetchState) emit(ctx context.Context, item types.FetchedItem) error {
	if s.handler == nil {
		s.items = append(s.items, item)
		return nil
	}
	if err := s.handler.Emit(ctx, item); err != nil {
		return err
	}
	return s.checkpoint(ctx)
}

func (s *fetchState) checkpoint(ctx context.Context) error {
	if s.handler == nil {
		return nil
	}
	return s.handler.Checkpoint(ctx, syncCursorFromTencentDocs(s.next))
}

func (s *fetchState) walkNodes(ctx context.Context, spaceID string, nodes []Node, sourceResourceID string) error {
	for _, node := range nodes {
		visitKey := spaceID + "/" + node.ID
		if s.visitedNodes[visitKey] {
			continue
		}
		s.visitedNodes[visitKey] = true

		if isSyncableNode(node) {
			if err := s.fetchNode(ctx, spaceID, node, nil, sourceResourceID); err != nil {
				return err
			}
		}
		if node.HasChildren {
			children, err := s.client.ListNodes(ctx, spaceID, node.ID)
			if err != nil {
				failure := failedFetchedItem(
					encodeNodeResourceID(spaceID, node.ID), node.Title, spaceID, node, sourceResourceID,
					fmt.Errorf("list Tencent Docs children: %w", err),
				)
				if emitErr := s.emit(ctx, failure); emitErr != nil {
					return emitErr
				}
				continue
			}
			if err := s.walkNodes(ctx, spaceID, children, sourceResourceID); err != nil {
				return err
			}
		}
	}
	return nil
}

func isSyncableNode(node Node) bool {
	if node.DocumentType != "" {
		return true
	}
	switch strings.ToLower(node.Type) {
	case "folder", "wiki_folder", "space", "link", "shortcut":
		return false
	case "wiki_file", "file", "document", "doc", "resource":
		return true
	default:
		return false
	}
}

func isLinkNode(node Node) bool {
	switch strings.ToLower(node.Type) {
	case "link", "shortcut":
		return true
	default:
		return false
	}
}

func isTencentDocsOnlineType(value string) bool {
	switch strings.ToLower(value) {
	case "doc", "word", "excel", "form", "slide", "smartcanvas", "smartsheet", "mind", "flowchart", "sheet":
		return true
	default:
		return false
	}
}

func (s *fetchState) fetchNode(
	ctx context.Context,
	spaceID string,
	node Node,
	knownInfo *FileInfo,
	sourceResourceID string,
) error {
	if strings.EqualFold(node.Type, "resource") {
		return s.fetchResource(ctx, spaceID, node, knownInfo, sourceResourceID)
	}
	return s.fetchDocument(ctx, spaceID, node, knownInfo, sourceResourceID)
}

func (s *fetchState) fetchDocument(
	ctx context.Context,
	spaceID string,
	node Node,
	knownInfo *FileInfo,
	sourceResourceID string,
) error {
	externalID := encodeNodeResourceID(spaceID, node.ID)
	if s.seenDocs[externalID] {
		return nil
	}
	s.seenDocs[externalID] = true

	info := knownInfo
	var err error
	if info == nil {
		info, err = s.client.GetFileInfo(ctx, node.ID)
	}
	if err != nil {
		return s.emit(ctx, failedFetchedItem(
			externalID, node.Title, spaceID, node, sourceResourceID,
			fmt.Errorf("get Tencent Docs file info: %w", err),
		))
	}
	if s.incremental && s.previous != nil && s.previous.DocumentTimes[externalID] == info.ModifiedAt && info.ModifiedAt != 0 {
		s.next.DocumentTimes[externalID] = info.ModifiedAt
		return s.checkpoint(ctx)
	}

	content, err := s.client.GetContent(ctx, node.ID)
	if err != nil {
		title := firstNonEmpty(info.Title, node.Title)
		if s.previous != nil {
			if previousTime, ok := s.previous.DocumentTimes[externalID]; ok {
				s.next.DocumentTimes[externalID] = previousTime
			}
		}
		return s.emit(ctx, failedFetchedItem(
			externalID, title, spaceID, node, sourceResourceID,
			fmt.Errorf("get Tencent Docs content: %w", err),
		))
	}
	s.next.DocumentTimes[externalID] = info.ModifiedAt
	title := info.Title
	if title == "" {
		title = node.Title
	}
	url := info.URL
	if url == "" {
		url = node.URL
	}
	return s.emit(ctx, types.FetchedItem{
		ExternalID:       externalID,
		Title:            title,
		Content:          []byte(content.Text),
		ContentType:      "text/markdown",
		FileName:         sanitizeTencentDocsFileName(title) + ".md",
		URL:              url,
		UpdatedAt:        timestamp(info.ModifiedAt),
		SourceResourceID: sourceResourceID,
		Metadata: map[string]string{
			"channel":       types.ChannelTencentDocs,
			"space_id":      spaceID,
			"file_id":       node.ID,
			"node_type":     node.Type,
			"document_type": firstNonEmpty(node.DocumentType, info.Type),
		},
	})
}

func (s *fetchState) fetchResource(
	ctx context.Context,
	spaceID string,
	node Node,
	knownInfo *FileInfo,
	sourceResourceID string,
) error {
	externalID := encodeNodeResourceID(spaceID, node.ID)
	if s.seenDocs[externalID] {
		return nil
	}
	s.seenDocs[externalID] = true

	info := knownInfo
	var err error
	if info == nil {
		info, err = s.client.GetFileInfo(ctx, node.ID)
	}
	if err != nil {
		return s.emit(ctx, failedFetchedItem(
			externalID, node.Title, spaceID, node, sourceResourceID,
			fmt.Errorf("get Tencent Docs resource info: %w", err),
		))
	}
	if s.incremental && s.previous != nil && s.previous.DocumentTimes[externalID] == info.ModifiedAt && info.ModifiedAt != 0 {
		s.next.DocumentTimes[externalID] = info.ModifiedAt
		return s.checkpoint(ctx)
	}

	task, err := s.client.StartExport(ctx, node.ID)
	if err != nil {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
			fmt.Errorf("start Tencent Docs resource export: %w", err))
	}
	exportCtx, cancel := context.WithTimeout(ctx, exportTimeout)
	defer cancel()
	var status *ExportStatus
	for {
		status, err = s.client.GetExportProgress(exportCtx, task.ID)
		if err != nil {
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
				fmt.Errorf("poll Tencent Docs resource export: %w", err))
		}
		if status.Error != "" {
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
				fmt.Errorf("Tencent Docs resource export failed: %s", status.Error))
		}
		if status.Progress >= 100 {
			break
		}
		select {
		case <-exportCtx.Done():
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
				fmt.Errorf("Tencent Docs resource export timed out: %w", exportCtx.Err()))
		case <-time.After(exportPollInterval):
		}
	}
	if strings.TrimSpace(status.FileURL) == "" {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
			errors.New("Tencent Docs resource export returned no download URL"))
	}
	data, err := s.client.DownloadExport(ctx, status.FileURL)
	if err != nil {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID, err)
	}
	fileName := firstNonEmpty(status.FileName, info.Title, node.Title)
	if fileName == "" {
		fileName = "tencent-docs-resource"
	}
	s.next.DocumentTimes[externalID] = info.ModifiedAt
	return s.emit(ctx, types.FetchedItem{
		ExternalID:       externalID,
		Title:            firstNonEmpty(info.Title, node.Title, fileName),
		Content:          data,
		ContentType:      "application/octet-stream",
		FileName:         fileName,
		URL:              firstNonEmpty(info.URL, node.URL),
		UpdatedAt:        timestamp(info.ModifiedAt),
		SourceResourceID: sourceResourceID,
		Metadata: map[string]string{
			"channel":   types.ChannelTencentDocs,
			"space_id":  spaceID,
			"file_id":   node.ID,
			"node_type": node.Type,
		},
	})
}

func (s *fetchState) emitResourceFailure(
	ctx context.Context,
	externalID, spaceID string,
	node Node,
	info *FileInfo,
	sourceResourceID string,
	err error,
) error {
	if s.previous != nil {
		if previousTime, ok := s.previous.DocumentTimes[externalID]; ok {
			s.next.DocumentTimes[externalID] = previousTime
		}
	}
	return s.emit(ctx, failedFetchedItem(
		externalID, firstNonEmpty(info.Title, node.Title), spaceID, node, sourceResourceID, err,
	))
}

func failedFetchedItem(
	externalID, title, spaceID string,
	node Node,
	sourceResourceID string,
	err error,
) types.FetchedItem {
	return types.FetchedItem{
		ExternalID:       externalID,
		Title:            title,
		SourceResourceID: sourceResourceID,
		Metadata: map[string]string{
			"channel":   types.ChannelTencentDocs,
			"space_id":  spaceID,
			"file_id":   node.ID,
			"node_type": node.Type,
			"error":     err.Error(),
		},
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type resourceKind int

const (
	resourceKindSpace resourceKind = iota + 1
	resourceKindNode
)

type resourceRef struct {
	kind    resourceKind
	spaceID string
	nodeID  string
}

func encodeSpaceResourceID(spaceID string) string {
	return spaceIDPrefix + encodeIDPart(spaceID)
}

func encodeNodeResourceID(spaceID, nodeID string) string {
	return nodeIDPrefix + encodeIDPart(spaceID) + ":" + encodeIDPart(nodeID)
}

func encodeIDPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeIDPart(value string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) == 0 {
		return "", fmt.Errorf("%w: malformed Tencent Docs resource ID", datasource.ErrInvalidConfig)
	}
	return string(decoded), nil
}

func decodeResourceID(value string) (resourceRef, error) {
	if strings.HasPrefix(value, spaceIDPrefix) {
		spaceID, err := decodeIDPart(strings.TrimPrefix(value, spaceIDPrefix))
		return resourceRef{kind: resourceKindSpace, spaceID: spaceID}, err
	}
	if strings.HasPrefix(value, nodeIDPrefix) {
		parts := strings.Split(strings.TrimPrefix(value, nodeIDPrefix), ":")
		if len(parts) != 2 {
			return resourceRef{}, fmt.Errorf("%w: malformed Tencent Docs node resource ID", datasource.ErrInvalidConfig)
		}
		spaceID, err := decodeIDPart(parts[0])
		if err != nil {
			return resourceRef{}, err
		}
		nodeID, err := decodeIDPart(parts[1])
		if err != nil {
			return resourceRef{}, err
		}
		return resourceRef{kind: resourceKindNode, spaceID: spaceID, nodeID: nodeID}, nil
	}
	return resourceRef{}, fmt.Errorf("%w: unknown Tencent Docs resource ID", datasource.ErrInvalidConfig)
}

func timestamp(value uint64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	if value > 10_000_000_000 {
		return time.UnixMilli(int64(value)).UTC()
	}
	return time.Unix(int64(value), 0).UTC()
}

func sanitizeTencentDocsFileName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "tencent-doc"
	}
	value = strings.Map(func(r rune) rune {
		if r < 32 || strings.ContainsRune(`<>:"/\\|?*`, r) {
			return '_'
		}
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Trim(value, " .")
	if value == "" {
		return "tencent-doc"
	}
	return value
}
