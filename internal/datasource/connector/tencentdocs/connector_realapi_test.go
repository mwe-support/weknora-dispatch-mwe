package tencentdocs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// TestConnectorRealMCP is an opt-in, read-only integration test. It validates
// exactly the path used by the data-source UI: token -> spaces -> lazy tree ->
// one selected document -> fetched Markdown. No remote content is modified.
func TestConnectorRealMCP(t *testing.T) {
	token := os.Getenv("TENCENT_DOCS_TOKEN")
	if token == "" {
		t.Skip("TENCENT_DOCS_TOKEN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeTencentDocs,
		Credentials: map[string]interface{}{"mcp_token": token},
	}
	connector := NewConnector()
	if err := connector.Validate(ctx, config); err != nil {
		t.Fatalf("Validate() error: %v", err)
	}
	spaces, err := connector.ListResources(ctx, config, "")
	if err != nil {
		t.Fatalf("ListResources(root) error: %v", err)
	}
	if len(spaces) == 0 {
		t.Fatal("token can connect but exposes no Tencent Docs spaces")
	}
	if wantedSpaceID := os.Getenv("TENCENT_DOCS_TEST_SPACE_ID"); wantedSpaceID != "" {
		wantedResourceID := encodeSpaceResourceID(wantedSpaceID)
		filtered := spaces[:0]
		for _, space := range spaces {
			if space.ExternalID == wantedResourceID {
				filtered = append(filtered, space)
			}
		}
		if len(filtered) == 0 {
			t.Fatalf("TENCENT_DOCS_TEST_SPACE_ID %q is not visible to this token", wantedSpaceID)
		}
		spaces = filtered
	}

	document, ok := findFirstRealDocument(ctx, t, connector, config, spaces, 0)
	if !ok {
		t.Skipf("listed %d space(s), but none contains a document visible to this token", len(spaces))
	}
	items, err := connector.FetchAll(ctx, config, []string{document.ExternalID})
	if err != nil {
		t.Fatalf("FetchAll(selected document) error: %v", err)
	}
	if len(items) != 1 || len(items[0].Content) == 0 {
		t.Fatalf("FetchAll(selected document) returned %d item(s), content bytes=%d", len(items), firstContentLength(items))
	}
	t.Logf("real MCP verified: spaces=%d selected=%q markdown_bytes=%d", len(spaces), document.Name, len(items[0].Content))
}

func findFirstRealDocument(
	ctx context.Context,
	t *testing.T,
	connector *Connector,
	config *types.DataSourceConfig,
	parents []types.Resource,
	depth int,
) (types.Resource, bool) {
	t.Helper()
	if depth > 16 {
		return types.Resource{}, false
	}
	for _, parent := range parents {
		if parent.Type != resourceTypeSpace && isSyncableResourceType(parent.Type) {
			return parent, true
		}
		if !parent.HasChildren {
			continue
		}
		children, err := connector.ListResources(ctx, config, parent.ExternalID)
		if err != nil {
			t.Fatalf("ListResources(%q) error: %v", parent.Name, err)
		}
		if document, ok := findFirstRealDocument(ctx, t, connector, config, children, depth+1); ok {
			return document, true
		}
	}
	return types.Resource{}, false
}

func isSyncableResourceType(resourceType string) bool {
	switch resourceType {
	case "", "folder", "wiki_folder", "space", "link", "shortcut", resourceTypeSpace:
		return false
	default:
		return true
	}
}

func firstContentLength(items []types.FetchedItem) int {
	if len(items) == 0 {
		return 0
	}
	return len(items[0].Content)
}
