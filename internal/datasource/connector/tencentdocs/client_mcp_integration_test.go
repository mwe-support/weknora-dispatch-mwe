package tencentdocs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
	protocol "github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

func TestTencentDocsMCPClientUsesStreamableHTTPWithRawAuthorization(t *testing.T) {
	mcpServer := mcpserver.NewMCPServer("tencent-docs-test", "1.0.0")
	mcpServer.AddTool(
		protocol.Tool{Name: toolQuerySpaceList},
		func(context.Context, protocol.CallToolRequest) (*protocol.CallToolResult, error) {
			return protocol.NewToolResultText(`{
				"spaces":[{"space_id":"space-live","title":"线上空间"}],
				"has_next":false,
				"error":"",
				"trace_id":"trace-live"
			}`), nil
		},
	)
	streamable := mcpserver.NewStreamableHTTPServer(mcpServer, mcpserver.WithStateLess(true))
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "mcp-secret" {
			t.Errorf("Authorization = %q, want raw MCP token", got)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		streamable.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	client, err := newTencentDocsMCPClient(
		MCPClientConfig{Token: "mcp-secret"},
		func(config *internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
			config.Service.URL = &httpServer.URL
			return internalmcp.NewMCPClient(config)
		},
	)
	if err != nil {
		t.Fatalf("newTencentDocsMCPClient() error: %v", err)
	}
	defer client.Close()

	spaces, err := client.ListSpaces(context.Background())
	if err != nil {
		t.Fatalf("ListSpaces() error: %v", err)
	}
	if len(spaces) != 1 || spaces[0].ID != "space-live" || spaces[0].Title != "线上空间" {
		t.Fatalf("spaces = %+v", spaces)
	}
}

func TestTencentDocsMCPClientClassifiesHTTPUnauthorized(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer httpServer.Close()

	client, err := newTencentDocsMCPClient(
		MCPClientConfig{Token: "wrong-secret"},
		func(config *internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
			config.Service.URL = &httpServer.URL
			return internalmcp.NewMCPClient(config)
		},
	)
	if err != nil {
		t.Fatalf("newTencentDocsMCPClient() error: %v", err)
	}
	defer client.Close()

	_, err = client.ListSpaces(context.Background())
	if !errors.Is(err, datasource.ErrInvalidCredentials) {
		t.Fatalf("ListSpaces() error = %v, want ErrInvalidCredentials", err)
	}
}

func TestTencentDocsMCPClientClassifiesHTTPForbidden(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer httpServer.Close()

	client, err := newTencentDocsMCPClient(
		MCPClientConfig{Token: "wrong-secret"},
		func(config *internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
			config.Service.URL = &httpServer.URL
			return internalmcp.NewMCPClient(config)
		},
	)
	if err != nil {
		t.Fatalf("newTencentDocsMCPClient() error: %v", err)
	}
	defer client.Close()

	_, err = client.ListSpaces(context.Background())
	if !errors.Is(err, datasource.ErrInvalidCredentials) {
		t.Fatalf("ListSpaces() error = %v, want ErrInvalidCredentials", err)
	}
}

func TestTencentDocsMCPClientDoesNotMisclassifyOtherHTTP4xx(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer httpServer.Close()

	client, err := newTencentDocsMCPClient(
		MCPClientConfig{Token: "mcp-secret"},
		func(config *internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
			config.Service.URL = &httpServer.URL
			return internalmcp.NewMCPClient(config)
		},
	)
	if err != nil {
		t.Fatalf("newTencentDocsMCPClient() error: %v", err)
	}
	defer client.Close()

	_, err = client.ListSpaces(context.Background())
	if err == nil {
		t.Fatal("ListSpaces() error = nil")
	}
	if errors.Is(err, datasource.ErrInvalidCredentials) {
		t.Fatalf("ListSpaces() error = %v, should not be ErrInvalidCredentials", err)
	}
}
