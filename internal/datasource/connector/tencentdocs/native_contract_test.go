package tencentdocs

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
	"github.com/stretchr/testify/require"
)

func TestNativeContractRejectsNestedBusinessFailureWithoutErrorText(t *testing.T) {
	for _, body := range []string{`{"code":60007,"trace_id":"root"}`, `{"data":{"code":60007,"message":"no permission"},"trace_id":"nested"}`, `{"data":"{\"ret\":60007,\"message\":\"no permission\"}","trace_id":"string"}`} {
		transport := &fakeMCPClient{callTool: func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
			return &internalmcp.CallToolResult{Content: []internalmcp.ContentItem{{Type: "text", Text: body}}}, nil
		}}
		client := newTestClient(t, transport)
		_, err := client.GetContent(context.Background(), "synthetic")
		var toolErr *MCPToolError
		require.ErrorAs(t, err, &toolErr)
		require.Equal(t, 60007, toolErr.Code)
	}
}

func TestNativeContractExportRateLimitIsNotAutomaticallyReissued(t *testing.T) {
	calls := 0
	transport := &fakeMCPClient{callTool: func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
		calls++
		return nil, errors.New("HTTP 429")
	}}
	client := newTestClient(t, transport)
	client.retrySleep = func(context.Context, time.Duration) error { return nil }
	_, err := client.StartExport(context.Background(), "synthetic")
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestNativeContractManagedReadHasNoInnerRetry(t *testing.T) {
	calls := 0
	transport := &fakeMCPClient{callTool: func(string, map[string]interface{}) (*internalmcp.CallToolResult, error) {
		calls++
		return nil, errors.New("HTTP 429")
	}}
	client := newTestClient(t, transport)
	client.retrySleep = func(context.Context, time.Duration) error { return nil }
	_, err := client.GetContent(WithManagedRetries(context.Background()), "synthetic")
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestNativeContractResponseBoundBeforeDecode(t *testing.T) {
	reader := &boundedMCPBody{ReadCloser: io.NopCloser(strings.NewReader("123456789tail")), remaining: 8, limit: 8}
	_, err := io.ReadAll(reader)
	var limit *NativeLimitError
	require.ErrorAs(t, err, &limit)
	require.EqualValues(t, 9, limit.ObservedAtLeastBytes)
	reader = &boundedMCPBody{ReadCloser: io.NopCloser(strings.NewReader("12345678")), remaining: 8, limit: 8}
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "12345678", string(data))
}
