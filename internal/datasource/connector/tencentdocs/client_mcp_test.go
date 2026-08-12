package tencentdocs

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
	"github.com/Tencent/WeKnora/internal/types"
)

type fakeMCPClient struct {
	connected       bool
	connectCalls    int
	initializeCalls int
	disconnectCalls int
	connectErr      error
	initializeErr   error
	callTool        func(name string, args map[string]interface{}) (*internalmcp.CallToolResult, error)
}

func (f *fakeMCPClient) Connect(context.Context) error {
	f.connectCalls++
	if f.connectErr != nil {
		return f.connectErr
	}
	f.connected = true
	return nil
}

func (f *fakeMCPClient) Disconnect() error {
	f.disconnectCalls++
	f.connected = false
	return nil
}

func (f *fakeMCPClient) Initialize(context.Context) (*internalmcp.InitializeResult, error) {
	f.initializeCalls++
	if f.initializeErr != nil {
		return nil, f.initializeErr
	}
	return &internalmcp.InitializeResult{ProtocolVersion: "2025-03-26"}, nil
}

func (f *fakeMCPClient) ListTools(context.Context) ([]*types.MCPTool, error) { return nil, nil }

func (f *fakeMCPClient) ListResources(context.Context) ([]*types.MCPResource, error) {
	return nil, nil
}

func (f *fakeMCPClient) CallTool(
	_ context.Context,
	name string,
	args map[string]interface{},
) (*internalmcp.CallToolResult, error) {
	if f.callTool == nil {
		return nil, errors.New("unexpected tool call")
	}
	return f.callTool(name, args)
}

func (f *fakeMCPClient) ReadResource(context.Context, string) (*internalmcp.ReadResourceResult, error) {
	return nil, nil
}

func (f *fakeMCPClient) IsConnected() bool { return f.connected }

func (f *fakeMCPClient) GetServiceID() string { return "tencent-docs" }

func toolJSON(t *testing.T, value interface{}) *internalmcp.CallToolResult {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal tool response: %v", err)
	}
	return &internalmcp.CallToolResult{
		Content: []internalmcp.ContentItem{{Type: "text", Text: string(b)}},
	}
}

func newTestClient(t *testing.T, transport *fakeMCPClient) *TencentDocsMCPClient {
	t.Helper()
	client, err := newTencentDocsMCPClient(
		MCPClientConfig{Token: "mcp-secret", Timeout: 45 * time.Second},
		func(config *internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
			if config.Service.URL == nil || *config.Service.URL != DefaultMCPEndpoint {
				t.Fatalf("endpoint = %v, want %q", config.Service.URL, DefaultMCPEndpoint)
			}
			if config.Service.TransportType != types.MCPTransportHTTPStreamable {
				t.Fatalf("transport = %q", config.Service.TransportType)
			}
			if config.Service.AuthConfig == nil ||
				config.Service.AuthConfig.AuthType != types.MCPAuthAPIKey ||
				config.Service.AuthConfig.APIKeyHeader != "Authorization" ||
				config.Service.AuthConfig.APIKey != "mcp-secret" {
				t.Fatalf("unexpected auth config: %+v", config.Service.AuthConfig)
			}
			if config.Service.AdvancedConfig == nil || config.Service.AdvancedConfig.Timeout != 45 {
				t.Fatalf("timeout config = %+v", config.Service.AdvancedConfig)
			}
			return transport, nil
		},
	)
	if err != nil {
		t.Fatalf("newTencentDocsMCPClient() error: %v", err)
	}
	return client
}

func TestNewTencentDocsMCPClientRejectsMissingToken(t *testing.T) {
	_, err := NewTencentDocsMCPClient(MCPClientConfig{})
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("error = %v, want missing token error", err)
	}
}

func TestClientConnectionFailuresAreRecoverable(t *testing.T) {
	t.Run("initialize failure disconnects", func(t *testing.T) {
		transport := &fakeMCPClient{initializeErr: errors.New("bad handshake")}
		client := newTestClient(t, transport)
		if _, err := client.ListSpaces(context.Background()); err == nil ||
			!strings.Contains(err.Error(), "bad handshake") {
			t.Fatalf("ListSpaces() error = %v", err)
		}
		if transport.disconnectCalls != 1 || transport.connected {
			t.Fatalf("disconnect calls/connected = %d/%v, want 1/false", transport.disconnectCalls, transport.connected)
		}
	})

	t.Run("close allows reconnect", func(t *testing.T) {
		first := &fakeMCPClient{}
		second := &fakeMCPClient{}
		second.callTool = func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
			return toolJSON(t, map[string]interface{}{"spaces": []Space{}, "has_next": false}), nil
		}
		transports := []*fakeMCPClient{first, second}
		factoryCalls := 0
		client, err := newTencentDocsMCPClient(
			MCPClientConfig{Token: "mcp-secret"},
			func(*internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
				transport := transports[factoryCalls]
				factoryCalls++
				return transport, nil
			},
		)
		if err != nil {
			t.Fatalf("newTencentDocsMCPClient() error: %v", err)
		}
		if err := client.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		if _, err := client.ListSpaces(context.Background()); err != nil {
			t.Fatalf("ListSpaces() after Close error: %v", err)
		}
		if factoryCalls != 2 || first.connectCalls != 0 || second.connectCalls != 1 || second.initializeCalls != 1 {
			t.Fatalf(
				"factory/first-connect/second-connect-init = %d/%d/%d-%d, want 2/0/1-1",
				factoryCalls, first.connectCalls, second.connectCalls, second.initializeCalls,
			)
		}
	})

	t.Run("tool transport loss reconnects next operation", func(t *testing.T) {
		first := &fakeMCPClient{}
		first.callTool = func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
			first.connected = false
			return nil, errors.New("connection lost")
		}
		second := &fakeMCPClient{}
		second.callTool = func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
			return toolJSON(t, map[string]interface{}{"spaces": []Space{}, "has_next": false}), nil
		}
		transports := []*fakeMCPClient{first, second}
		factoryCalls := 0
		client, err := newTencentDocsMCPClient(
			MCPClientConfig{Token: "mcp-secret"},
			func(*internalmcp.ClientConfig) (internalmcp.MCPClient, error) {
				transport := transports[factoryCalls]
				factoryCalls++
				return transport, nil
			},
		)
		if err != nil {
			t.Fatalf("newTencentDocsMCPClient() error: %v", err)
		}
		if _, err := client.ListSpaces(context.Background()); err == nil {
			t.Fatal("first ListSpaces() error = nil")
		}
		if _, err := client.ListSpaces(context.Background()); err != nil {
			t.Fatalf("second ListSpaces() error: %v", err)
		}
		if factoryCalls != 2 || first.connectCalls != 1 || second.connectCalls != 1 {
			t.Fatalf("factory/connection calls = %d/%d/%d, want 2/1/1", factoryCalls, first.connectCalls, second.connectCalls)
		}
	})
}

func TestMethodsRejectEmptyIdentifiersBeforeConnecting(t *testing.T) {
	transport := &fakeMCPClient{}
	client := newTestClient(t, transport)

	checks := []struct {
		name string
		call func() error
	}{
		{"space", func() error { _, err := client.ListNodes(context.Background(), "", ""); return err }},
		{"file info", func() error { _, err := client.GetFileInfo(context.Background(), " "); return err }},
		{"content", func() error { _, err := client.GetContent(context.Background(), ""); return err }},
		{"export", func() error { _, err := client.StartExport(context.Background(), ""); return err }},
		{"progress", func() error { _, err := client.GetExportProgress(context.Background(), ""); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); err == nil || !strings.Contains(err.Error(), "required") {
				t.Fatalf("error = %v, want required error", err)
			}
		})
	}
	if transport.connectCalls != 0 {
		t.Fatalf("invalid inputs connected %d times", transport.connectCalls)
	}
}

func TestListSpacesPaginatesAndInitializesOnce(t *testing.T) {
	transport := &fakeMCPClient{}
	var pages []int
	transport.callTool = func(name string, args map[string]interface{}) (*internalmcp.CallToolResult, error) {
		if name != toolQuerySpaceList {
			t.Fatalf("tool = %q, want %q", name, toolQuerySpaceList)
		}
		page := args["num"].(int)
		pages = append(pages, page)
		if page == 0 {
			return toolJSON(t, map[string]interface{}{
				"spaces": []map[string]interface{}{{
					"space_id": "space-1", "title": "采购部", "file_cnt": 7,
				}},
				"has_next": true,
			}), nil
		}
		return toolJSON(t, map[string]interface{}{
			"spaces": []map[string]interface{}{{
				"space_id": "space-2", "title": "财务部", "is_owner": true,
			}},
			"has_next": false,
		}), nil
	}

	client := newTestClient(t, transport)
	spaces, err := client.ListSpaces(context.Background())
	if err != nil {
		t.Fatalf("ListSpaces() error: %v", err)
	}
	if !reflect.DeepEqual(pages, []int{0, 1}) {
		t.Fatalf("pages = %v, want [0 1]", pages)
	}
	if len(spaces) != 2 || spaces[0].ID != "space-1" || spaces[1].Title != "财务部" {
		t.Fatalf("spaces = %+v", spaces)
	}
	if transport.connectCalls != 1 || transport.initializeCalls != 1 {
		t.Fatalf("connect/init calls = %d/%d, want 1/1", transport.connectCalls, transport.initializeCalls)
	}

	if err := client.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error: %v", err)
	}
	if transport.connectCalls != 1 || transport.initializeCalls != 1 {
		t.Fatalf("second operation reinitialized client: %d/%d", transport.connectCalls, transport.initializeCalls)
	}
}

func TestListNodesPaginatesWithParent(t *testing.T) {
	transport := &fakeMCPClient{}
	var pages []int
	transport.callTool = func(name string, args map[string]interface{}) (*internalmcp.CallToolResult, error) {
		if name != toolQuerySpaceNode {
			t.Fatalf("tool = %q, want %q", name, toolQuerySpaceNode)
		}
		if args["space_id"] != "space-1" || args["parent_id"] != "folder-1" {
			t.Fatalf("args = %#v", args)
		}
		page := args["num"].(int)
		pages = append(pages, page)
		return toolJSON(t, map[string]interface{}{
			"children": []map[string]interface{}{{
				"node_id": "node-" + string(rune('1'+page)),
				"title":   "制度", "node_type": "wiki_file", "doc_type": "smartcanvas",
				"has_child": page == 0, "url": "https://docs.qq.com/doc/example",
			}},
			"has_next": page == 0,
		}), nil
	}

	client := newTestClient(t, transport)
	nodes, err := client.ListNodes(context.Background(), "space-1", "folder-1")
	if err != nil {
		t.Fatalf("ListNodes() error: %v", err)
	}
	if !reflect.DeepEqual(pages, []int{0, 1}) || len(nodes) != 2 {
		t.Fatalf("pages/nodes = %v/%+v", pages, nodes)
	}
	if nodes[0].ID != "node-1" || !nodes[0].HasChildren || nodes[0].DocumentType != "smartcanvas" {
		t.Fatalf("node[0] = %+v", nodes[0])
	}
}

func TestListHomeNodesPaginatesFromRoot(t *testing.T) {
	transport := &fakeMCPClient{}
	var starts []int
	transport.callTool = func(name string, args map[string]interface{}) (*internalmcp.CallToolResult, error) {
		if name != toolManageFolderList {
			t.Fatalf("tool = %q, want %q", name, toolManageFolderList)
		}
		if _, ok := args["folder_id"]; ok {
			t.Fatalf("root listing must omit folder_id: %#v", args)
		}
		start := args["start"].(int)
		starts = append(starts, start)
		if start == 0 {
			return toolJSON(t, map[string]interface{}{
				"list": []map[string]interface{}{{
					"id": "home-folder-1", "title": "制度", "is_folder": true,
				}},
				"finish": false,
			}), nil
		}
		return toolJSON(t, map[string]interface{}{
			"list": []map[string]interface{}{{
				"id": "home-doc-1", "title": "报销制度", "url": "https://docs.qq.com/doc/home-doc-1",
			}},
			"finish": true,
		}), nil
	}

	client := newTestClient(t, transport)
	nodes, err := client.ListHomeNodes(context.Background(), "")
	if err != nil {
		t.Fatalf("ListHomeNodes() error: %v", err)
	}
	if !reflect.DeepEqual(starts, []int{0, 1}) {
		t.Fatalf("starts = %v, want [0 1]", starts)
	}
	if len(nodes) != 2 || nodes[0].ID != "home-folder-1" || !nodes[0].IsFolder || nodes[1].ID != "home-doc-1" {
		t.Fatalf("home nodes = %+v", nodes)
	}
}

func TestListHomeNodesTreatsRepeatedEmptyUnfinishedTailAsComplete(t *testing.T) {
	transport := &fakeMCPClient{}
	var starts []int
	transport.callTool = func(name string, args map[string]interface{}) (*internalmcp.CallToolResult, error) {
		if name != toolManageFolderList {
			t.Fatalf("tool = %q, want %q", name, toolManageFolderList)
		}
		start := args["start"].(int)
		starts = append(starts, start)
		if start == 0 {
			items := make([]map[string]interface{}, 55)
			for i := range items {
				items[i] = map[string]interface{}{
					"id": "home-doc-" + strconv.Itoa(i+1), "title": "首页文档",
				}
			}
			return toolJSON(t, map[string]interface{}{
				"list": items, "finish": false,
			}), nil
		}
		return toolJSON(t, map[string]interface{}{
			"list": []map[string]interface{}{}, "finish": false,
		}), nil
	}

	client := newTestClient(t, transport)
	nodes, err := client.ListHomeNodes(context.Background(), "")
	if err != nil {
		t.Fatalf("ListHomeNodes() error: %v", err)
	}
	if len(nodes) != 55 {
		t.Fatalf("len(nodes) = %d, want 55", len(nodes))
	}
	if !reflect.DeepEqual(starts, []int{0, 55, 55}) {
		t.Fatalf("starts = %v, want [0 55 55]", starts)
	}
}

func TestTypedTencentDocsTools(t *testing.T) {
	tests := []struct {
		name     string
		tool     string
		args     map[string]interface{}
		response map[string]interface{}
		assert   func(t *testing.T, client *TencentDocsMCPClient)
	}{
		{
			name: "file info", tool: toolQueryFileInfo,
			args: map[string]interface{}{"file_id": "file-1"},
			response: map[string]interface{}{
				"file_id": "file-1", "title": "采购流程", "type": "smartcanvas",
				"create_time": "1713600000000", "last_modify_time": "1713686400000",
				"space_id": "space-1", "is_folder": false,
			},
			assert: func(t *testing.T, client *TencentDocsMCPClient) {
				info, err := client.GetFileInfo(context.Background(), "file-1")
				if err != nil || info.ID != "file-1" || info.CreatedAt != 1713600000000 || info.ModifiedAt != 1713686400000 {
					t.Fatalf("GetFileInfo() = %+v, %v", info, err)
				}
			},
		},
		{
			name: "content", tool: toolGetContent,
			args:     map[string]interface{}{"file_id": "file-1"},
			response: map[string]interface{}{"content": "# 采购流程", "trace_id": "trace-content"},
			assert: func(t *testing.T, client *TencentDocsMCPClient) {
				content, err := client.GetContent(context.Background(), "file-1")
				if err != nil || content.Text != "# 采购流程" || content.TraceID != "trace-content" {
					t.Fatalf("GetContent() = %+v, %v", content, err)
				}
			},
		},
		{
			name: "start export", tool: toolExportFile,
			args:     map[string]interface{}{"file_id": "file-1"},
			response: map[string]interface{}{"task_id": "task-1", "trace_id": "trace-export"},
			assert: func(t *testing.T, client *TencentDocsMCPClient) {
				task, err := client.StartExport(context.Background(), "file-1")
				if err != nil || task.ID != "task-1" || task.TraceID != "trace-export" {
					t.Fatalf("StartExport() = %+v, %v", task, err)
				}
			},
		},
		{
			name: "export progress", tool: toolExportProgress,
			args: map[string]interface{}{"task_id": "task-1"},
			response: map[string]interface{}{
				"progress": 100, "status": "done", "file_name": "采购流程.docx",
				"file_url": "https://example.invalid/signed", "trace_id": "trace-progress",
			},
			assert: func(t *testing.T, client *TencentDocsMCPClient) {
				status, err := client.GetExportProgress(context.Background(), "task-1")
				if err != nil || status.Progress != 100 || status.FileName != "采购流程.docx" {
					t.Fatalf("GetExportProgress() = %+v, %v", status, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &fakeMCPClient{}
			transport.callTool = func(name string, args map[string]interface{}) (*internalmcp.CallToolResult, error) {
				if name != tt.tool || !reflect.DeepEqual(args, tt.args) {
					t.Fatalf("tool call = %q %#v, want %q %#v", name, args, tt.tool, tt.args)
				}
				return toolJSON(t, tt.response), nil
			}
			client := newTestClient(t, transport)
			tt.assert(t, client)
		})
	}
}

func TestToolErrorsRemainDiagnostic(t *testing.T) {
	tests := []struct {
		name   string
		result *internalmcp.CallToolResult
		want   []string
	}{
		{
			name:   "payload error",
			result: toolJSON(t, map[string]interface{}{"error": "Token 鉴权失败", "trace_id": "trace-400006"}),
			want:   []string{"query_space_list", "Token 鉴权失败", "trace-400006"},
		},
		{
			name: "mcp tool error",
			result: &internalmcp.CallToolResult{
				IsError: true,
				Content: []internalmcp.ContentItem{{Type: "text", Text: "permission denied"}},
			},
			want: []string{"query_space_list", "permission denied"},
		},
		{
			name: "invalid json",
			result: &internalmcp.CallToolResult{
				Content: []internalmcp.ContentItem{{Type: "text", Text: "not-json"}},
			},
			want: []string{"query_space_list", "JSON"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &fakeMCPClient{
				callTool: func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
					return tt.result, nil
				},
			}
			client := newTestClient(t, transport)
			_, err := client.ListSpaces(context.Background())
			if err == nil {
				t.Fatal("ListSpaces() error = nil")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %q, want substring %q", err, want)
				}
			}
		})
	}
}

func TestInvalidTencentDocsCredentialsAreClassified(t *testing.T) {
	transport := &fakeMCPClient{
		callTool: func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
			return toolJSON(t, map[string]interface{}{
				"error":    "Token 鉴权失败",
				"code":     400006,
				"trace_id": "trace-auth",
			}), nil
		},
	}
	client := newTestClient(t, transport)
	_, err := client.ListSpaces(context.Background())
	if !errors.Is(err, datasource.ErrInvalidCredentials) {
		t.Fatalf("ListSpaces() error = %v, want ErrInvalidCredentials", err)
	}
	if !strings.Contains(err.Error(), "400006") || !strings.Contains(err.Error(), "trace-auth") {
		t.Fatalf("ListSpaces() error = %v, want code and trace ID", err)
	}
}

func TestValidateExportURLRejectsUntrustedOrInsecureHosts(t *testing.T) {
	for _, rawURL := range []string{
		"http://docs.qq.com/export/file.pdf",
		"https://example.com/file.pdf",
		"file:///etc/passwd",
	} {
		if err := validateExportURL(rawURL); err == nil {
			t.Fatalf("validateExportURL(%q) expected rejection", rawURL)
		}
	}
	for _, rawURL := range []string{
		"https://docs-import-export.cos.ap-guangzhou.myqcloud.com/export/file.pdf",
		"https://docs.qq.com/export/file.pdf",
	} {
		if err := validateExportURL(rawURL); err != nil {
			t.Fatalf("validateExportURL(%q) error: %v", rawURL, err)
		}
	}
}
