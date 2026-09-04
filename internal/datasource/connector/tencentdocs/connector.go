package tencentdocs

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

const (
	resourceTypeSpace              = "tencent_docs_space"
	resourceTypeHome               = "tencent_docs_home"
	homeRootResourceID             = "tdoc:home"
	spaceIDPrefix                  = "tdoc:space:"
	nodeIDPrefix                   = "tdoc:node:"
	homeNodeIDPrefix               = "tdoc:home-node:"
	exportPollInterval             = 3 * time.Second
	exportTimeout                  = 2 * time.Minute
	fileInfoUnsupportedTypeCode    = 400001
	fileInfoUnsupportedTypeMessage = "file type not support query"
	exportUnsupportedTypeCode      = 323908
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
		resources := make([]types.Resource, 0, len(spaces)+1)
		resources = append(resources, types.Resource{
			ExternalID:  homeRootResourceID,
			Name:        "个人首页",
			Type:        resourceTypeHome,
			Description: "腾讯文档个人首页中的文件和文件夹",
			HasChildren: true,
		})
		spaceResources := make([]types.Resource, 0, len(spaces))
		for _, space := range spaces {
			spaceResources = append(spaceResources, types.Resource{
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
		sort.SliceStable(spaceResources, func(i, j int) bool { return spaceResources[i].Name < spaceResources[j].Name })
		resources = append(resources, spaceResources...)
		return resources, nil
	}
	if parentID == homeRootResourceID {
		nodes, err := client.ListHomeNodes(ctx, "")
		if err != nil {
			return nil, fmt.Errorf("list Tencent Docs personal home: %w", err)
		}
		return homeNodesToResources(parentID, nodes), nil
	}

	ref, err := decodeResourceID(parentID)
	if err != nil {
		return nil, err
	}
	if ref.kind == resourceKindHomeNode {
		nodes, err := client.ListHomeNodes(ctx, ref.nodeID)
		if err != nil {
			return nil, fmt.Errorf("list Tencent Docs personal-home folder: %w", err)
		}
		return homeNodesToResources(parentID, nodes), nil
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

func homeNodesToResources(parentID string, nodes []HomeNode) []types.Resource {
	resources := make([]types.Resource, 0, len(nodes))
	for _, node := range nodes {
		resourceType := "file"
		if node.IsFolder {
			resourceType = "folder"
		}
		resources = append(resources, types.Resource{
			ExternalID:  encodeHomeNodeResourceID(node.ID),
			Name:        node.Title,
			Type:        resourceType,
			URL:         node.URL,
			ParentID:    parentID,
			HasChildren: node.IsFolder,
			Metadata: map[string]interface{}{
				"file_id":   node.ID,
				"is_folder": node.IsFolder,
				"location":  "personal_home",
			},
		})
	}
	sort.SliceStable(resources, func(i, j int) bool {
		if resources[i].HasChildren != resources[j].HasChildren {
			return resources[i].HasChildren
		}
		return resources[i].Name < resources[j].Name
	})
	return resources
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
	homeTargets := make(map[string]bool)
	homeOrder := make([]string, 0)
	for _, resourceID := range resourceIDs {
		ref, err := decodeResourceID(resourceID)
		if err != nil {
			return nil, err
		}
		if ref.kind == resourceKindSpace || ref.kind == resourceKindHomeRoot {
			continue
		}
		if ref.kind == resourceKindHomeNode {
			if !homeTargets[ref.nodeID] {
				homeTargets[ref.nodeID] = true
				homeOrder = append(homeOrder, ref.nodeID)
			}
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
	if len(homeTargets) > 0 {
		paths := make(map[string][]string, len(homeTargets))
		if err := findHomeAncestorPaths(
			ctx, client, "", homeTargets, []string{homeRootResourceID},
			make(map[string]bool), paths,
		); err != nil {
			return nil, fmt.Errorf("resolve Tencent Docs personal-home ancestors: %w", err)
		}
		for _, nodeID := range homeOrder {
			path, found := paths[nodeID]
			if !found {
				return nil, fmt.Errorf("%w: Tencent Docs personal-home node %s", datasource.ErrResourceNotFound, nodeID)
			}
			for _, id := range path {
				if !seen[id] {
					seen[id] = true
					result = append(result, id)
				}
			}
		}
	}
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

func findHomeAncestorPaths(
	ctx context.Context,
	client Client,
	parentFolderID string,
	targetNodeIDs map[string]bool,
	path []string,
	visited map[string]bool,
	paths map[string][]string,
) error {
	if len(paths) == len(targetNodeIDs) {
		return nil
	}
	if visited[parentFolderID] {
		return nil
	}
	visited[parentFolderID] = true
	nodes, err := client.ListHomeNodes(ctx, parentFolderID)
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
		if !node.IsFolder {
			continue
		}
		childPath := append(append([]string(nil), path...), encodeHomeNodeResourceID(node.ID))
		if err := findHomeAncestorPaths(
			ctx, client, node.ID, targetNodeIDs, childPath, visited, paths,
		); err != nil {
			return err
		}
	}
	return nil
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
	DocumentTimes        map[string]uint64    `json:"document_times"`
	ResourceFingerprints map[string]string    `json:"resource_fingerprints,omitempty"`
	FileRetries          map[string]fileRetry `json:"file_retries,omitempty"`
	RetryScope           string               `json:"retry_scope,omitempty"`
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
		client:            client,
		handler:           handler,
		previous:          previous,
		incremental:       incremental,
		traversalComplete: true,
		seenDocs:          make(map[string]bool),
		seenFileIDs:       make(map[string]bool),
		visitedNodes:      make(map[string]bool),
		next:              copyTencentDocsCursor(previous),
	}
	if state.next.RetryScope != "" && state.next.RetryScope != retryScope(config) {
		state.next.FileRetries = map[string]fileRetry{}
	}
	state.next.RetryScope = retryScope(config)
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
		if ref.kind == resourceKindHomeRoot {
			nodes, err := client.ListHomeNodes(ctx, "")
			if err != nil {
				return nil, nil, fmt.Errorf("list Tencent Docs personal home: %w", err)
			}
			if err := state.walkHomeNodes(ctx, nodes, selectedID); err != nil {
				return nil, nil, err
			}
			continue
		}

		info, err := client.GetFileInfo(ctx, ref.nodeID)
		if err != nil {
			nodeType := "wiki_file"
			spaceID := ref.spaceID
			if ref.kind == resourceKindHomeNode {
				nodeType = "file"
				spaceID = ""
			}
			node := Node{ID: ref.nodeID, Type: nodeType}
			if shouldExportUnclassifiedNode(node, err) {
				if err := state.fetchResource(ctx, spaceID, Node{
					ID: ref.nodeID, Type: "resource",
				}, &FileInfo{ID: ref.nodeID, SpaceID: spaceID}, selectedID); err != nil {
					return nil, nil, err
				}
				continue
			}
		}
		if err != nil {
			return nil, nil, fmt.Errorf("get selected Tencent Docs node %s: %w", ref.nodeID, err)
		}
		node := nodeFromFileInfo(info)
		if info.IsFolder {
			var childrenErr error
			if ref.kind == resourceKindHomeNode {
				children, listErr := client.ListHomeNodes(ctx, ref.nodeID)
				if listErr == nil {
					childrenErr = state.walkHomeNodes(ctx, children, selectedID)
				} else {
					childrenErr = listErr
				}
				if childrenErr != nil {
					return nil, nil, fmt.Errorf("list selected Tencent Docs personal-home folder %s: %w", ref.nodeID, childrenErr)
				}
				continue
			}
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
		spaceID := ref.spaceID
		if ref.kind == resourceKindHomeNode {
			spaceID = ""
		}
		if err := state.fetchNode(ctx, spaceID, node, info, selectedID); err != nil {
			return nil, nil, err
		}
	}

	if incremental && previous != nil && state.traversalComplete {
		for externalID := range previous.DocumentTimes {
			if !state.seenDocs[externalID] {
				delete(state.next.DocumentTimes, externalID)
				delete(state.next.ResourceFingerprints, externalID)
				if err := state.emit(ctx, types.FetchedItem{ExternalID: externalID, IsDeleted: true}); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return state.items, state.next, nil
}

func copyTencentDocsCursor(previous *tencentDocsCursor) *tencentDocsCursor {
	next := &tencentDocsCursor{
		DocumentTimes:        make(map[string]uint64),
		ResourceFingerprints: make(map[string]string),
		FileRetries:          make(map[string]fileRetry),
	}
	if previous != nil {
		next.RetryScope = previous.RetryScope
		for id, retry := range previous.FileRetries {
			next.FileRetries[id] = retry
		}
		for externalID, modifiedAt := range previous.DocumentTimes {
			next.DocumentTimes[externalID] = modifiedAt
		}
		for externalID, fingerprint := range previous.ResourceFingerprints {
			next.ResourceFingerprints[externalID] = fingerprint
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
	client            Client
	handler           datasource.StreamHandler
	previous          *tencentDocsCursor
	incremental       bool
	traversalComplete bool
	seenDocs          map[string]bool
	seenFileIDs       map[string]bool
	visitedNodes      map[string]bool
	next              *tencentDocsCursor
	items             []types.FetchedItem
}

func (s *fetchState) walkHomeNodes(ctx context.Context, nodes []HomeNode, sourceResourceID string) error {
	for _, homeNode := range nodes {
		visitKey := "personal_home/" + homeNode.ID
		if s.visitedNodes[visitKey] {
			continue
		}
		s.visitedNodes[visitKey] = true

		if homeNode.IsFolder {
			children, err := s.client.ListHomeNodes(ctx, homeNode.ID)
			if err != nil {
				s.traversalComplete = false
				failureNode := Node{ID: homeNode.ID, Title: homeNode.Title, URL: homeNode.URL, Type: "folder", HasChildren: true}
				failure := failedFetchedItem(
					encodeHomeNodeResourceID(homeNode.ID), homeNode.Title, "", failureNode, sourceResourceID,
					fmt.Errorf("list Tencent Docs personal-home children: %w", err),
				)
				if emitErr := s.emit(ctx, failure); emitErr != nil {
					return emitErr
				}
				continue
			}
			if err := s.walkHomeNodes(ctx, children, sourceResourceID); err != nil {
				return err
			}
			continue
		}

		node := Node{ID: homeNode.ID, Title: homeNode.Title, URL: homeNode.URL, Type: "file"}
		if err := s.fetchNode(ctx, "", node, nil, sourceResourceID); err != nil {
			return err
		}
	}
	return nil
}

func (s *fetchState) emit(ctx context.Context, item types.FetchedItem) error {
	s.trackFileRetry(&item)
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
				s.traversalComplete = false
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
	case "doc", "word", "excel", "form", "slide", "smartcanvas", "smartsheet", "mind", "flowchart", "sheet", "tencentsheet":
		return true
	default:
		return false
	}
}

func shouldExportUnclassifiedNode(node Node, err error) bool {
	// Tencent may label uploaded attachments as resource + pdf/png while
	// manage.query_file_info still rejects them as non-online documents. The
	// exact resource type is sufficient to use the export path; for all other
	// node types, only unclassified nodes are eligible for this fallback.
	if !strings.EqualFold(strings.TrimSpace(node.Type), "resource") &&
		strings.TrimSpace(node.DocumentType) != "" {
		return false
	}
	if !isSyncableNode(node) {
		return false
	}

	var toolErr *MCPToolError
	return errors.As(err, &toolErr) &&
		toolErr.Tool == toolQueryFileInfo &&
		toolErr.Code == fileInfoUnsupportedTypeCode &&
		strings.Contains(strings.ToLower(toolErr.Message), fileInfoUnsupportedTypeMessage)
}

func isUnsupportedResourceExportError(err error) bool {
	var toolErr *MCPToolError
	return errors.As(err, &toolErr) &&
		toolErr.Tool == toolExportProgress &&
		toolErr.Code == exportUnsupportedTypeCode
}

func exportFileNameFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	disposition := parsed.Query().Get("response-content-disposition")
	if disposition == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(disposition)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(params["filename"])
}

func exportFileName(statusFileName, rawURL string, fallbacks ...string) string {
	urlFileName := exportFileNameFromURL(rawURL)
	candidates := append([]string{statusFileName, urlFileName}, fallbacks...)
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" && filepath.Ext(candidate) != "" {
			return candidate
		}
	}
	return firstNonEmpty(candidates...)
}

func (s *fetchState) fetchNode(
	ctx context.Context,
	spaceID string,
	node Node,
	knownInfo *FileInfo,
	sourceResourceID string,
) error {
	info := knownInfo
	var err error
	if info == nil {
		info, err = s.client.GetFileInfo(ctx, node.ID)
	}
	if err != nil && shouldExportUnclassifiedNode(node, err) {
		resourceNode := node
		resourceNode.Type = "resource"
		resourceNode.DocumentType = ""
		return s.fetchResource(ctx, spaceID, resourceNode, &FileInfo{
			ID: node.ID, Title: node.Title, URL: node.URL, SpaceID: spaceID,
		}, sourceResourceID)
	}
	if err != nil {
		externalID := fetchedNodeResourceID(spaceID, node.ID)
		if s.seenFileIDs[node.ID] {
			return nil
		}
		s.seenFileIDs[node.ID] = true
		s.seenDocs[externalID] = true
		if s.previous != nil {
			if previousTime, ok := s.previous.DocumentTimes[externalID]; ok {
				s.next.DocumentTimes[externalID] = previousTime
			}
		}
		return s.emit(ctx, failedFetchedItem(
			externalID, node.Title, spaceID, node, sourceResourceID,
			withFileStage("fetch_metadata", fmt.Errorf("get Tencent Docs file info: %w", err)),
		))
	}
	resolved := nodeFromFileInfo(info)
	resolved.Title = firstNonEmpty(resolved.Title, node.Title)
	resolved.URL = firstNonEmpty(resolved.URL, node.URL)
	if strings.TrimSpace(info.Type) == "" {
		resolved.Type = node.Type
		resolved.DocumentType = node.DocumentType
		resolved.HasChildren = node.HasChildren
	}
	if strings.EqualFold(resolved.Type, "resource") {
		return s.fetchResource(ctx, spaceID, resolved, info, sourceResourceID)
	}
	return s.fetchDocument(ctx, spaceID, resolved, info, sourceResourceID)
}

func (s *fetchState) fetchDocument(
	ctx context.Context,
	spaceID string,
	node Node,
	knownInfo *FileInfo,
	sourceResourceID string,
) error {
	externalID := fetchedNodeResourceID(spaceID, node.ID)
	if s.seenFileIDs[node.ID] {
		return nil
	}
	s.seenFileIDs[node.ID] = true
	s.seenDocs[externalID] = true

	info := knownInfo
	var err error
	if info == nil {
		info, err = s.client.GetFileInfo(ctx, node.ID)
	}
	if err != nil {
		return s.emit(ctx, failedFetchedItem(
			externalID, node.Title, spaceID, node, sourceResourceID,
			withFileStage("fetch_metadata", fmt.Errorf("get Tencent Docs file info: %w", err)),
		))
	}
	if s.incremental && s.previous != nil && s.previous.DocumentTimes[externalID] == info.ModifiedAt && info.ModifiedAt != 0 {
		s.next.DocumentTimes[externalID] = info.ModifiedAt
		return s.checkpoint(ctx)
	}

	documentType := firstNonEmpty(node.DocumentType, info.Type)
	contentText, contentMetadata, err := fetchOnlineDocumentContent(
		ctx, s.client, node.ID, documentType,
	)
	if err != nil {
		title := firstNonEmpty(info.Title, node.Title)
		if s.previous != nil {
			if previousTime, ok := s.previous.DocumentTimes[externalID]; ok {
				s.next.DocumentTimes[externalID] = previousTime
			}
		}
		return s.emit(ctx, failedFetchedItem(
			externalID, title, spaceID, node, sourceResourceID,
			withFileStage("fetch_content", fmt.Errorf("get Tencent Docs content: %w", err)),
		))
	}
	s.next.DocumentTimes[externalID] = info.ModifiedAt
	delete(s.next.ResourceFingerprints, externalID)
	title := info.Title
	if title == "" {
		title = node.Title
	}
	url := info.URL
	if url == "" {
		url = node.URL
	}
	metadata := map[string]string{
		"channel":       types.ChannelTencentDocs,
		"space_id":      spaceID,
		"file_id":       node.ID,
		"node_type":     node.Type,
		"document_type": documentType,
	}
	for key, value := range contentMetadata {
		metadata[key] = value
	}
	if spaceID == "" {
		metadata["location"] = "personal_home"
	}
	return s.emit(ctx, types.FetchedItem{
		ExternalID:       externalID,
		Title:            title,
		Content:          []byte(contentText),
		ContentType:      "text/markdown",
		FileName:         sanitizeTencentDocsFileName(title) + ".md",
		URL:              url,
		UpdatedAt:        timestamp(info.ModifiedAt),
		SourceResourceID: sourceResourceID,
		Metadata:         metadata,
	})
}

func (s *fetchState) fetchResource(
	ctx context.Context,
	spaceID string,
	node Node,
	knownInfo *FileInfo,
	sourceResourceID string,
) error {
	externalID := fetchedNodeResourceID(spaceID, node.ID)
	if s.seenFileIDs[node.ID] {
		return nil
	}
	s.seenFileIDs[node.ID] = true
	s.seenDocs[externalID] = true

	info := knownInfo
	var err error
	if info == nil {
		info, err = s.client.GetFileInfo(ctx, node.ID)
	}
	if err != nil {
		return s.emit(ctx, failedFetchedItem(
			externalID, node.Title, spaceID, node, sourceResourceID,
			withFileStage("fetch_metadata", fmt.Errorf("get Tencent Docs resource info: %w", err)),
		))
	}
	if s.incremental && s.previous != nil && s.previous.DocumentTimes[externalID] == info.ModifiedAt && info.ModifiedAt != 0 {
		s.next.DocumentTimes[externalID] = info.ModifiedAt
		return s.checkpoint(ctx)
	}

	var task *ExportTask
	if pending := s.next.FileRetries[externalID]; pending.ExportTaskID != "" {
		task = &ExportTask{ID: pending.ExportTaskID}
	} else {
		if pending.ExportStartUncertain {
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID, withFileStage("export_start", errors.New("previous export start outcome unknown; manual review required")))
		}
		// Persist intent before a non-idempotent call. If the worker dies before
		// saving its returned task ID, never blindly issue a second export.
		pending.Node, pending.SpaceID, pending.SourceResourceID = node, spaceID, sourceResourceID
		pending.ExportStartUncertain = true
		s.next.FileRetries[externalID] = pending
		if err = s.checkpoint(ctx); err != nil {
			return err
		}
		task, err = s.client.StartExport(ctx, node.ID)
	}
	if err != nil {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
			withFileStage("export_start", fmt.Errorf("start Tencent Docs resource export: %w", err)))
	}
	if task == nil || task.ID == "" {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID, withFileStage("export_start", errors.New("empty export task ID")))
	}
	pending := s.next.FileRetries[externalID]
	pending.Node, pending.SpaceID, pending.SourceResourceID, pending.ExportTaskID = node, spaceID, sourceResourceID, task.ID
	pending.ExportStartUncertain = false
	s.next.FileRetries[externalID] = pending
	if err = s.checkpoint(ctx); err != nil {
		return err
	}
	exportCtx, cancel := context.WithTimeout(ctx, exportTimeout)
	defer cancel()
	var status *ExportStatus
	for {
		status, err = s.client.GetExportProgress(exportCtx, task.ID)
		if err != nil {
			if isUnsupportedResourceExportError(err) {
				title := firstNonEmpty(info.Title, node.Title)
				extension := strings.ToLower(strings.TrimSpace(node.DocumentType))
				fingerprint := resourceFingerprint(nil, "unsupported-export:"+extension, title)
				s.next.ResourceFingerprints[externalID] = fingerprint
				s.next.DocumentTimes[externalID] = info.ModifiedAt
				if s.incremental && s.previous != nil &&
					s.previous.ResourceFingerprints[externalID] == fingerprint {
					delete(s.next.FileRetries, externalID)
					return s.checkpoint(ctx)
				}
				return s.emit(ctx, skippedFetchedItem(
					externalID, title, spaceID, node, sourceResourceID,
					"unsupported_file_type", extension, title,
				))
			}
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
				withExportTask(task.ID, withFileStage("export", fmt.Errorf("poll Tencent Docs resource export: %w", err))))
		}
		if status.Error != "" {
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
				withFileStage("export", fmt.Errorf("Tencent Docs resource export failed: %s", status.Error)))
		}
		if status.Progress >= 100 {
			break
		}
		select {
		case <-exportCtx.Done():
			return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
				withExportTask(task.ID, withFileStage("export", fmt.Errorf("Tencent Docs resource export timed out: %w", exportCtx.Err()))))
		case <-time.After(exportPollInterval):
		}
	}
	if strings.TrimSpace(status.FileURL) == "" {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID,
			withFileStage("export", errors.New("Tencent Docs resource export returned no download URL")))
	}
	fileName := exportFileName(status.FileName, status.FileURL, info.Title, node.Title)
	if fileName == "" {
		fileName = "tencent-docs-resource"
	}
	title := firstNonEmpty(info.Title, node.Title, fileName)
	extension := strings.TrimPrefix(strings.ToLower(filepath.Ext(fileName)), ".")
	if !types.IsSupportedKnowledgeFileExtension(extension) {
		fingerprint := resourceFingerprint(nil, fileName, title)
		s.next.ResourceFingerprints[externalID] = fingerprint
		s.next.DocumentTimes[externalID] = info.ModifiedAt
		if s.incremental && s.previous != nil &&
			s.previous.ResourceFingerprints[externalID] == fingerprint {
			delete(s.next.FileRetries, externalID)
			return s.checkpoint(ctx)
		}
		return s.emit(ctx, skippedFetchedItem(
			externalID, title, spaceID, node, sourceResourceID,
			"unsupported_file_type", extension, fileName,
		))
	}
	data, err := s.client.DownloadExport(ctx, status.FileURL)
	if err != nil {
		return s.emitResourceFailure(ctx, externalID, spaceID, node, info, sourceResourceID, withExportTask(task.ID, withFileStage("download", err)))
	}
	fingerprint := resourceFingerprint(data, fileName, title)
	s.next.ResourceFingerprints[externalID] = fingerprint
	if s.incremental && s.previous != nil && info.ModifiedAt == 0 &&
		s.previous.ResourceFingerprints[externalID] == fingerprint {
		s.next.DocumentTimes[externalID] = info.ModifiedAt
		delete(s.next.FileRetries, externalID)
		return s.checkpoint(ctx)
	}
	s.next.DocumentTimes[externalID] = info.ModifiedAt
	metadata := map[string]string{
		"channel":   types.ChannelTencentDocs,
		"space_id":  spaceID,
		"file_id":   node.ID,
		"node_type": node.Type,
	}
	if spaceID == "" {
		metadata["location"] = "personal_home"
	}
	return s.emit(ctx, types.FetchedItem{
		ExternalID:       externalID,
		Title:            title,
		Content:          data,
		ContentType:      "application/octet-stream",
		FileName:         fileName,
		URL:              firstNonEmpty(info.URL, node.URL),
		UpdatedAt:        timestamp(info.ModifiedAt),
		SourceResourceID: sourceResourceID,
		Metadata:         metadata,
	})
}

func skippedFetchedItem(
	externalID, title, spaceID string,
	node Node,
	sourceResourceID, reason, fileExtension, fileName string,
) types.FetchedItem {
	metadata := map[string]string{
		"channel":        types.ChannelTencentDocs,
		"space_id":       spaceID,
		"file_id":        node.ID,
		"node_type":      node.Type,
		"skip_reason":    reason,
		"file_extension": fileExtension,
	}
	if spaceID == "" {
		metadata["location"] = "personal_home"
	}
	return types.FetchedItem{
		ExternalID:       externalID,
		Title:            title,
		FileName:         fileName,
		SourceResourceID: sourceResourceID,
		Metadata:         metadata,
	}
}

func resourceFingerprint(data []byte, fileName, title string) string {
	hash := sha256.New()
	_, _ = hash.Write(data)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(fileName))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(title))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func fetchedNodeResourceID(spaceID, nodeID string) string {
	if spaceID == "" {
		return encodeHomeNodeResourceID(nodeID)
	}
	return encodeNodeResourceID(spaceID, nodeID)
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
	metadata := map[string]string{
		"channel":   types.ChannelTencentDocs,
		"space_id":  spaceID,
		"file_id":   node.ID,
		"node_type": node.Type,
		"error":     err.Error(),
	}
	addFileFailureMetadata(metadata, err)
	metadata["document_type"] = node.DocumentType
	metadata["retry_node_url"] = node.URL
	if spaceID == "" {
		metadata["location"] = "personal_home"
	}
	return types.FetchedItem{
		ExternalID:       externalID,
		Title:            title,
		SourceResourceID: sourceResourceID,
		Metadata:         metadata,
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
	resourceKindHomeRoot
	resourceKindHomeNode
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

func encodeHomeNodeResourceID(nodeID string) string {
	return homeNodeIDPrefix + encodeIDPart(nodeID)
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
	if value == homeRootResourceID {
		return resourceRef{kind: resourceKindHomeRoot}, nil
	}
	if strings.HasPrefix(value, homeNodeIDPrefix) {
		nodeID, err := decodeIDPart(strings.TrimPrefix(value, homeNodeIDPrefix))
		return resourceRef{kind: resourceKindHomeNode, nodeID: nodeID}, err
	}
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
