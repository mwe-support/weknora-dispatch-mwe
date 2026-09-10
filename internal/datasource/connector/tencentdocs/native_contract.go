package tencentdocs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const maxNativeResponseBytes = 16 << 20
const maxNativeDocumentBytes = 100 << 20

type managedRetriesKey struct{}

// WithManagedRetries makes the durable stage the sole business retry owner.
// Legacy sources retain their existing read retry policy until migrated.
func WithManagedRetries(ctx context.Context) context.Context {
	return context.WithValue(ctx, managedRetriesKey{}, true)
}

type NativeLimitError struct {
	Kind                 string
	LimitBytes           int64
	ActualBytes          *int64
	ObservedAtLeastBytes int64
}

func (e *NativeLimitError) Error() string {
	if e.ActualBytes != nil {
		return fmt.Sprintf("NATIVE_SIZE_EXCEEDED: %s limit_bytes=%d actual_bytes=%d", e.Kind, e.LimitBytes, *e.ActualBytes)
	}
	return fmt.Sprintf("NATIVE_SIZE_EXCEEDED: %s limit_bytes=%d actual_bytes=unknown observed_at_least_bytes=%d", e.Kind, e.LimitBytes, e.ObservedAtLeastBytes)
}

// The transport must stop an unbounded HTTP/SSE body before the MCP decoder
// allocates its full content; checking a decoded string alone is too late.
type boundedMCPBody struct {
	io.ReadCloser
	remaining, limit int64
}

func (b *boundedMCPBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining < 0 {
		return 0, &NativeLimitError{Kind: "response", LimitBytes: b.limit, ObservedAtLeastBytes: b.limit + 1}
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return n, &NativeLimitError{Kind: "response", LimitBytes: b.limit, ObservedAtLeastBytes: b.limit + 1}
	}
	return n, err
}

func tencentEnvelopeError(tool string, data []byte, inheritedTrace string, depth int) error {
	if depth > 3 {
		return errors.New("Tencent Docs envelope nesting exceeds supported contract")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	trace := inheritedTrace
	if value, ok := fields["trace_id"]; ok {
		_ = json.Unmarshal(value, &trace)
	}
	code := 0
	for _, key := range []string{"code", "ret"} {
		if value, ok := fields[key]; ok && string(value) != "null" {
			var number json.Number
			if err := json.Unmarshal(value, &number); err != nil {
				return fmt.Errorf("invalid Tencent Docs business code: %w", err)
			}
			parsed, err := strconv.Atoi(number.String())
			if err != nil {
				return err
			}
			if parsed != 0 {
				code = parsed
				break
			}
		}
	}
	message := ""
	if raw, ok := fields["error"]; ok && string(raw) != "null" && string(raw) != "false" {
		if json.Unmarshal(raw, &message) != nil {
			message = "Tencent Docs tool returned a structured error"
		}
	}
	if code != 0 || message != "" {
		if message == "" {
			_ = json.Unmarshal(fields["message"], &message)
		}
		if message == "" {
			_ = json.Unmarshal(fields["msg"], &message)
		}
		if message == "" {
			message = "Tencent Docs tool reported a business failure"
		}
		return classifyCredentialError(&MCPToolError{Tool: tool, Code: code, Message: message, TraceID: trace})
	}
	nested := fields["data"]
	if len(nested) > 0 && nested[0] == '"' {
		var decoded string
		if json.Unmarshal(nested, &decoded) == nil {
			nested = []byte(strings.TrimSpace(decoded))
		}
	}
	if len(nested) > 0 && nested[0] == '{' {
		return tencentEnvelopeError(tool, nested, trace, depth+1)
	}
	return nil
}

type NativeResponse struct {
	Data    json.RawMessage
	TraceID string
	Bytes   int64
}

// ReadNative exposes only the reviewed read contracts. Export initiation is
// deliberately absent: it needs its own durable intent and reconciliation.
func (c *TencentDocsMCPClient) ReadNative(ctx context.Context, tool string, args map[string]interface{}) (*NativeResponse, error) {
	switch tool {
	case toolQueryFileInfo, "doc.resolve_document_structure", "smartcanvas.get_top_level_pages", "smartcanvas.read", "smartsheet.list_tables", "smartsheet.list_fields", "smartsheet.list_records", toolSheetGetInfo, toolSheetGetCells:
	default:
		return nil, errors.New("unsupported Tencent Docs native read tool")
	}
	id, ok := args["file_id"].(string)
	if !ok || requireID("file ID", id) != nil {
		return nil, errors.New("native read requires a file ID")
	}
	return c.readNativeResponse(ctx, tool, args)
}

func (c *TencentDocsMCPClient) readNativeResponse(ctx context.Context, tool string, args map[string]interface{}) (*NativeResponse, error) {
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err := c.callToolJSON(ctx, tool, args, &raw); err != nil {
		return nil, err
	}
	result := &NativeResponse{Data: raw, Bytes: int64(len(raw))}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(fields["trace_id"], &result.TraceID)
	if data, ok := fields["data"]; ok {
		if len(data) > 0 && data[0] == '"' {
			var decoded string
			if err := json.Unmarshal(data, &decoded); err != nil {
				return nil, err
			}
			data = json.RawMessage(decoded)
		}
		if !json.Valid(data) || string(data) == "null" {
			return nil, errors.New("Tencent Docs native read returned no structured data")
		}
		result.Data = data
	}
	return result, nil
}
