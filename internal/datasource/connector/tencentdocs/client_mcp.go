package tencentdocs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/mark3labs/mcp-go/client/transport"
)

const (
	// DefaultMCPEndpoint is Tencent Docs' official Streamable HTTP MCP endpoint.
	DefaultMCPEndpoint = "https://docs.qq.com/openapi/mcp"

	toolQuerySpaceList   = "query_space_list"
	toolQuerySpaceNode   = "query_space_node"
	toolManageFolderList = "manage.folder_list"
	toolQueryFileInfo    = "manage.query_file_info"
	toolGetContent       = "get_content"
	toolSheetGetInfo     = "sheet.get_sheet_info"
	toolSheetGetCells    = "sheet.get_cell_data"
	toolExportFile       = "manage.export_file"
	toolExportProgress   = "manage.export_progress"

	defaultMCPTimeout = 30 * time.Second
	maxMCPPagination  = 10000
	maxExportBytes    = 100 << 20
)

// MCPClientConfig configures access to the official Tencent Docs MCP service.
// Token is the MCP-specific token obtained from Tencent Docs' OpenClaw flow.
type MCPClientConfig struct {
	Token   string
	Timeout time.Duration
}

type mcpClientFactory func(*internalmcp.ClientConfig) (internalmcp.MCPClient, error)

// TencentDocsMCPClient is a typed adapter over Tencent Docs' MCP tools.
// It deliberately reuses WeKnora's shared MCP transport for protocol and
// session handling rather than implementing JSON-RPC or SSE itself.
type TencentDocsMCPClient struct {
	config  MCPClientConfig
	factory mcpClientFactory
	client  internalmcp.MCPClient

	readyMu      sync.Mutex
	ready        bool
	needsRebuild bool
	generation   uint64
}

// MCPToolError is returned when the MCP call succeeds but the Tencent Docs tool
// reports a business error in its JSON payload or as an MCP tool error.
type MCPToolError struct {
	Tool    string
	Code    int
	Message string
	TraceID string
}

func (e *MCPToolError) Error() string {
	detail := e.Message
	if e.Code != 0 {
		detail = fmt.Sprintf("%s (code=%d)", detail, e.Code)
	}
	if e.TraceID != "" {
		return fmt.Sprintf("Tencent Docs MCP tool %s failed: %s (trace_id=%s)", e.Tool, detail, e.TraceID)
	}
	return fmt.Sprintf("Tencent Docs MCP tool %s failed: %s", e.Tool, detail)
}

// NewTencentDocsMCPClient constructs a client for Tencent Docs' official MCP
// endpoint. The token is sent as the raw Authorization header value, matching
// Tencent Docs' MCP authorization contract (it is not prefixed with "Bearer").
func NewTencentDocsMCPClient(config MCPClientConfig) (*TencentDocsMCPClient, error) {
	return newTencentDocsMCPClient(config, internalmcp.NewMCPClient)
}

func newTencentDocsMCPClient(
	config MCPClientConfig,
	factory mcpClientFactory,
) (*TencentDocsMCPClient, error) {
	if strings.TrimSpace(config.Token) == "" {
		return nil, fmt.Errorf("%w: Tencent Docs MCP token is required", datasource.ErrInvalidCredentials)
	}
	if factory == nil {
		return nil, errors.New("MCP client factory is required")
	}

	if config.Timeout <= 0 {
		config.Timeout = defaultMCPTimeout
	}
	transportClient, err := buildMCPTransport(config, factory)
	if err != nil {
		return nil, err
	}

	return &TencentDocsMCPClient{
		config:     config,
		factory:    factory,
		client:     transportClient,
		generation: 1,
	}, nil
}

func buildMCPTransport(
	config MCPClientConfig,
	factory mcpClientFactory,
) (internalmcp.MCPClient, error) {
	timeoutSeconds := int((config.Timeout + time.Second - 1) / time.Second)
	endpoint := DefaultMCPEndpoint
	transportClient, err := factory(&internalmcp.ClientConfig{
		HTTPClient: &http.Client{
			Timeout:   config.Timeout,
			Transport: preserveForbiddenTransport{base: http.DefaultTransport},
		},
		Service: &types.MCPService{
			ID:            "tencent-docs-datasource",
			Name:          "Tencent Docs data source",
			Enabled:       true,
			TransportType: types.MCPTransportHTTPStreamable,
			URL:           &endpoint,
			AuthConfig: &types.MCPAuthConfig{
				AuthType:     types.MCPAuthAPIKey,
				APIKey:       config.Token,
				APIKeyHeader: "Authorization",
			},
			AdvancedConfig: &types.MCPAdvancedConfig{Timeout: timeoutSeconds},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create Tencent Docs MCP transport: %w", err)
	}
	if transportClient == nil {
		return nil, errors.New("create Tencent Docs MCP transport: factory returned nil client")
	}
	return transportClient, nil
}

// Validate verifies the token by initializing the MCP session and issuing one
// minimal, read-only space-list request.
func (c *TencentDocsMCPClient) Validate(ctx context.Context) error {
	if err := c.ensureReady(ctx); err != nil {
		return err
	}
	var response listSpacesResponse
	return c.callToolJSON(ctx, toolQuerySpaceList, map[string]interface{}{"num": 0}, &response)
}

// Close releases the underlying MCP session.
func (c *TencentDocsMCPClient) Close() error {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	c.ready = false
	c.needsRebuild = true
	if c.client == nil {
		return nil
	}
	return c.client.Disconnect()
}

// ListSpaces returns every space visible to the configured MCP token.
func (c *TencentDocsMCPClient) ListSpaces(ctx context.Context) ([]Space, error) {
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}

	spaces := make([]Space, 0)
	for page := 0; page < maxMCPPagination; page++ {
		var response listSpacesResponse
		if err := c.callToolJSON(ctx, toolQuerySpaceList, map[string]interface{}{"num": page}, &response); err != nil {
			return nil, err
		}
		spaces = append(spaces, response.Spaces...)
		if !response.HasNext {
			return spaces, nil
		}
	}
	return nil, fmt.Errorf("Tencent Docs MCP tool %s exceeded %d pages", toolQuerySpaceList, maxMCPPagination)
}

// ListNodes returns all direct children beneath parentID. An empty parentID
// lists the root of the requested space.
func (c *TencentDocsMCPClient) ListNodes(
	ctx context.Context,
	spaceID string,
	parentID string,
) ([]Node, error) {
	if strings.TrimSpace(spaceID) == "" {
		return nil, errors.New("Tencent Docs space ID is required")
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}

	nodes := make([]Node, 0)
	for page := 0; page < maxMCPPagination; page++ {
		args := map[string]interface{}{
			"space_id": spaceID,
			"num":      page,
		}
		if parentID != "" {
			args["parent_id"] = parentID
		}
		var response listNodesResponse
		if err := c.callToolJSON(ctx, toolQuerySpaceNode, args, &response); err != nil {
			return nil, err
		}
		nodes = append(nodes, response.Children...)
		if !response.HasNext {
			return nodes, nil
		}
	}
	return nil, fmt.Errorf("Tencent Docs MCP tool %s exceeded %d pages", toolQuerySpaceNode, maxMCPPagination)
}

// ListHomeNodes returns every direct child in the Tencent Docs personal-home
// hierarchy. An empty folderID lists the account's personal-home root.
func (c *TencentDocsMCPClient) ListHomeNodes(ctx context.Context, folderID string) ([]HomeNode, error) {
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}

	nodes := make([]HomeNode, 0)
	start := 0
	emptyUnfinishedStart := -1
	for page := 0; page < maxMCPPagination; page++ {
		args := map[string]interface{}{"start": start}
		if folderID != "" {
			args["folder_id"] = folderID
		}
		var response listHomeNodesResponse
		if err := c.callToolJSON(ctx, toolManageFolderList, args, &response); err != nil {
			return nil, err
		}
		nodes = append(nodes, response.List...)
		if response.Finish {
			return nodes, nil
		}
		if len(response.List) == 0 {
			// manage.folder_list can report finish=false on the empty page just
			// beyond the real end of a personal-home directory. Retry the same
			// cursor once so a transient empty page cannot silently truncate the
			// listing; a repeated empty response is the service's effective EOF.
			if emptyUnfinishedStart == start {
				return nodes, nil
			}
			emptyUnfinishedStart = start
			continue
		}
		emptyUnfinishedStart = -1
		start += len(response.List)
	}
	return nil, fmt.Errorf("Tencent Docs MCP tool %s exceeded %d pages", toolManageFolderList, maxMCPPagination)
}

// GetFileInfo retrieves metadata for one Tencent Docs file or folder.
func (c *TencentDocsMCPClient) GetFileInfo(ctx context.Context, fileID string) (*FileInfo, error) {
	if err := requireID("file ID", fileID); err != nil {
		return nil, err
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	var info FileInfo
	if err := c.callToolJSON(ctx, toolQueryFileInfo, map[string]interface{}{"file_id": fileID}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// GetContent retrieves the text representation of one Tencent Docs document.
func (c *TencentDocsMCPClient) GetContent(ctx context.Context, fileID string) (*DocumentContent, error) {
	if err := requireID("file ID", fileID); err != nil {
		return nil, err
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	var content DocumentContent
	if err := c.callToolJSON(ctx, toolGetContent, map[string]interface{}{"file_id": fileID}, &content); err != nil {
		return nil, err
	}
	return &content, nil
}

// GetSheetInfo returns every worksheet and its physical dimensions.
func (c *TencentDocsMCPClient) GetSheetInfo(
	ctx context.Context,
	fileID string,
) ([]SheetInfo, error) {
	if err := requireID("file ID", fileID); err != nil {
		return nil, err
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	var response sheetInfoResponse
	if err := c.callToolJSON(
		ctx, toolSheetGetInfo,
		map[string]interface{}{"file_id": fileID},
		&response,
	); err != nil {
		return nil, err
	}
	return append([]SheetInfo(nil), response.Sheets...), nil
}

// GetSheetCells reads one explicit inclusive Sheet range as structured cells.
func (c *TencentDocsMCPClient) GetSheetCells(
	ctx context.Context,
	fileID, sheetID string,
	startRow, endRow, startCol, endCol int,
) ([]SheetCell, error) {
	if err := requireID("file ID", fileID); err != nil {
		return nil, err
	}
	if err := requireID("sheet ID", sheetID); err != nil {
		return nil, err
	}
	if startRow < 0 || startCol < 0 || endRow < startRow || endCol < startCol {
		return nil, fmt.Errorf("invalid Sheet cell range")
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	args := map[string]interface{}{
		"file_id": fileID, "sheet_id": sheetID,
		"start_row": startRow, "end_row": endRow,
		"start_col": startCol, "end_col": endCol,
		"return_csv": false,
	}
	var response sheetCellsResponse
	if err := c.callToolJSON(ctx, toolSheetGetCells, args, &response); err != nil {
		return nil, err
	}
	return append([]SheetCell(nil), response.Cells...), nil
}

// StartExport starts Tencent Docs' asynchronous original-format export.
func (c *TencentDocsMCPClient) StartExport(ctx context.Context, fileID string) (*ExportTask, error) {
	if err := requireID("file ID", fileID); err != nil {
		return nil, err
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	var task ExportTask
	if err := c.callToolJSON(ctx, toolExportFile, map[string]interface{}{"file_id": fileID}, &task); err != nil {
		return nil, err
	}
	if task.ID == "" {
		return nil, fmt.Errorf("Tencent Docs MCP tool %s returned an empty task_id", toolExportFile)
	}
	return &task, nil
}

// GetExportProgress returns the current state and signed download URL, when ready.
func (c *TencentDocsMCPClient) GetExportProgress(
	ctx context.Context,
	taskID string,
) (*ExportStatus, error) {
	if err := requireID("export task ID", taskID); err != nil {
		return nil, err
	}
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	var status ExportStatus
	if err := c.callToolJSON(ctx, toolExportProgress, map[string]interface{}{"task_id": taskID}, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// DownloadExport downloads the short-lived URL returned by export_progress.
// The URL is not authenticated with the MCP token. Restricting scheme, host,
// redirects and size prevents the remote tool response from becoming an SSRF
// or unbounded-download primitive inside WeKnora.
func (c *TencentDocsMCPClient) DownloadExport(ctx context.Context, fileURL string) ([]byte, error) {
	if err := validateExportURL(fileURL); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create Tencent Docs export download: %w", err)
	}
	client := &http.Client{
		Timeout: c.config.Timeout,
		CheckRedirect: func(request *http.Request, _ []*http.Request) error {
			return validateExportURL(request.URL.String())
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("download Tencent Docs export: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("download Tencent Docs export: HTTP status %d", response.StatusCode)
	}
	if response.ContentLength > maxExportBytes {
		return nil, fmt.Errorf("download Tencent Docs export exceeds %d bytes", maxExportBytes)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxExportBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Tencent Docs export: %w", err)
	}
	if len(data) > maxExportBytes {
		return nil, fmt.Errorf("download Tencent Docs export exceeds %d bytes", maxExportBytes)
	}
	return data, nil
}

func validateExportURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil {
		return errors.New("Tencent Docs export URL must be an HTTPS URL")
	}
	host := strings.ToLower(parsed.Hostname())
	allowed := host == "docs.qq.com" || strings.HasSuffix(host, ".qq.com") ||
		strings.HasSuffix(host, ".myqcloud.com") || strings.HasSuffix(host, ".tencentcos.cn")
	if !allowed {
		return fmt.Errorf("Tencent Docs export URL host %q is not allowed", host)
	}
	return nil
}

func (c *TencentDocsMCPClient) ensureReady(ctx context.Context) error {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()

	if c.ready && c.client != nil && c.client.IsConnected() {
		return nil
	}
	if c.ready && (c.client == nil || !c.client.IsConnected()) {
		c.needsRebuild = true
	}
	c.ready = false
	if c.needsRebuild || c.client == nil {
		if c.client != nil {
			_ = c.client.Disconnect()
		}
		rebuilt, err := buildMCPTransport(c.config, c.factory)
		if err != nil {
			return err
		}
		c.client = rebuilt
		c.generation++
		c.needsRebuild = false
	} else if c.client.IsConnected() {
		_ = c.client.Disconnect()
	}
	if err := c.client.Connect(ctx); err != nil {
		c.needsRebuild = true
		return classifyCredentialError(fmt.Errorf("connect Tencent Docs MCP: %w", err))
	}
	if _, err := c.client.Initialize(ctx); err != nil {
		_ = c.client.Disconnect()
		c.needsRebuild = true
		return classifyCredentialError(fmt.Errorf("initialize Tencent Docs MCP: %w", err))
	}
	c.ready = true
	return nil
}

func (c *TencentDocsMCPClient) callToolJSON(
	ctx context.Context,
	tool string,
	args map[string]interface{},
	out interface{},
) error {
	client, generation := c.currentTransport()
	result, err := client.CallTool(ctx, tool, args)
	if err != nil {
		if !client.IsConnected() {
			c.markNeedsRebuild(generation)
		}
		if toolErr := parseTransportBusinessError(tool, err); toolErr != nil {
			return classifyCredentialError(toolErr)
		}
		return classifyCredentialError(fmt.Errorf("call Tencent Docs MCP tool %s: %w", tool, err))
	}
	if result == nil {
		return fmt.Errorf("Tencent Docs MCP tool %s returned no result", tool)
	}
	if result.IsError {
		return classifyCredentialError(&MCPToolError{Tool: tool, Message: toolResultText(result)})
	}

	var decodeErr error
	for _, item := range result.Content {
		if item.Type != "text" || strings.TrimSpace(item.Text) == "" {
			continue
		}
		var envelope toolEnvelope
		if err := json.Unmarshal([]byte(item.Text), &envelope); err != nil {
			decodeErr = err
			continue
		}
		if envelope.Error != "" {
			toolErr := &MCPToolError{
				Tool: tool, Code: envelope.businessCode(),
				Message: envelope.Error, TraceID: envelope.TraceID,
			}
			return classifyCredentialError(toolErr)
		}
		if err := json.Unmarshal([]byte(item.Text), out); err != nil {
			return fmt.Errorf("decode JSON response from Tencent Docs MCP tool %s: %w", tool, err)
		}
		return nil
	}
	if decodeErr != nil {
		return fmt.Errorf("decode JSON response from Tencent Docs MCP tool %s: %w", tool, decodeErr)
	}
	return fmt.Errorf("Tencent Docs MCP tool %s returned no JSON text content", tool)
}

// parseTransportBusinessError normalizes the structured diagnostic currently
// returned by Tencent Docs as an MCP transport error instead of a CallTool
// result. Matching both the requested tool and the complete business prefix
// keeps this conservative: unrelated transport, permission, and network
// failures retain their original error path.
func parseTransportBusinessError(tool string, err error) *MCPToolError {
	if err == nil {
		return nil
	}
	raw := err.Error()
	if !strings.Contains(raw, "(tool: "+tool+")") {
		return nil
	}
	const prefix = "type:business, code:"
	start := strings.Index(raw, prefix)
	if start < 0 {
		return nil
	}
	payload := raw[start+len(prefix):]
	const messageMarker = ", msg:"
	messageStart := strings.Index(payload, messageMarker)
	if messageStart < 0 {
		return nil
	}
	code, parseErr := strconv.Atoi(strings.TrimSpace(payload[:messageStart]))
	if parseErr != nil {
		return nil
	}
	messageAndTrace := payload[messageStart+len(messageMarker):]
	message := messageAndTrace
	traceID := ""
	const traceMarker = ", trace_id:"
	if traceStart := strings.LastIndex(messageAndTrace, traceMarker); traceStart >= 0 {
		message = messageAndTrace[:traceStart]
		traceFields := strings.Fields(strings.TrimSpace(messageAndTrace[traceStart+len(traceMarker):]))
		if len(traceFields) > 0 {
			traceID = strings.Trim(traceFields[0], "()[]{}")
		}
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return nil
	}
	return &MCPToolError{Tool: tool, Code: code, Message: message, TraceID: traceID}
}

func (c *TencentDocsMCPClient) currentTransport() (internalmcp.MCPClient, uint64) {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()
	return c.client, c.generation
}

func (c *TencentDocsMCPClient) markNeedsRebuild(generation uint64) {
	c.readyMu.Lock()
	if c.generation == generation {
		c.ready = false
		c.needsRebuild = true
	}
	c.readyMu.Unlock()
}

func classifyCredentialError(err error) error {
	if err == nil || errors.Is(err, datasource.ErrInvalidCredentials) {
		return err
	}
	var authErr *transport.AuthorizationRequiredError
	var statusErr *mcpHTTPStatusError
	var toolErr *MCPToolError
	message := strings.ToLower(err.Error())
	if errors.As(err, &authErr) ||
		errors.Is(err, transport.ErrAuthorizationRequired) ||
		(errors.As(err, &statusErr) && (statusErr.StatusCode == http.StatusUnauthorized || statusErr.StatusCode == http.StatusForbidden)) ||
		(errors.As(err, &toolErr) && toolErr.Code == 400006) ||
		strings.Contains(message, "token 鉴权失败") {
		return fmt.Errorf("%w: %v", datasource.ErrInvalidCredentials, err)
	}
	return err
}

type mcpHTTPStatusError struct {
	StatusCode int
}

func (e *mcpHTTPStatusError) Error() string {
	return fmt.Sprintf("Tencent Docs MCP returned HTTP status %d", e.StatusCode)
}

type preserveForbiddenTransport struct {
	base http.RoundTripper
}

func (t preserveForbiddenTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusForbidden {
		_ = response.Body.Close()
		return nil, &mcpHTTPStatusError{StatusCode: response.StatusCode}
	}
	return response, nil
}

func toolResultText(result *internalmcp.CallToolResult) string {
	var messages []string
	for _, item := range result.Content {
		if item.Type == "text" && strings.TrimSpace(item.Text) != "" {
			messages = append(messages, strings.TrimSpace(item.Text))
		}
	}
	if len(messages) == 0 {
		return "unknown tool error"
	}
	return strings.Join(messages, "; ")
}

func requireID(name string, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("Tencent Docs %s is required", name)
	}
	return nil
}

type toolEnvelope struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Ret     int    `json:"ret"`
	TraceID string `json:"trace_id"`
}

func (e toolEnvelope) businessCode() int {
	if e.Code != 0 {
		return e.Code
	}
	return e.Ret
}

type listSpacesResponse struct {
	Spaces  []Space `json:"spaces"`
	HasNext bool    `json:"has_next"`
}

type listNodesResponse struct {
	Children []Node `json:"children"`
	HasNext  bool   `json:"has_next"`
}

type listHomeNodesResponse struct {
	List   []HomeNode `json:"list"`
	Finish bool       `json:"finish"`
}
