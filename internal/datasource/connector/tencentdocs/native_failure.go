package tencentdocs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// ProcessingFailure preserves machine-readable provider evidence, never its
// arbitrary message (which may contain a private URL, document text or token).
func ProcessingFailure(stage string, err error) types.ProcessingOutcome {
	outcome := types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "internal", ErrorCode: "STAGE_EXECUTION_ERROR", Message: "The stage did not produce a validated result"}
	var wait *MCPBudgetWaitError
	if errors.As(err, &wait) {
		next := time.Now().UTC().Add(max(wait.RetryAfter, time.Second))
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, Result: types.JSON(`{"reason":"capacity"}`)}
	}
	if errors.Is(err, datasource.ErrInvalidCredentials) {
		outcome.ErrorClass, outcome.ErrorCode, outcome.Message = "authentication", "AUTH_REQUIRED", "The source credential is unavailable or invalid"
		return outcome
	}
	var httpStatus *mcpHTTPStatusError
	if errors.As(err, &httpStatus) && (httpStatus.StatusCode == 401 || httpStatus.StatusCode == 403) {
		outcome.ErrorClass, outcome.ErrorCode, outcome.Message = "permission", fmt.Sprintf("HTTP_%d", httpStatus.StatusCode), "The source provider denied access"
		return outcome
	}
	var nativeSize *NativeLimitError
	var quota *types.StorageQuotaExceededError
	if errors.As(err, &quota) {
		outcome.ErrorClass, outcome.ErrorCode, outcome.Message = "quota", "STORAGE_QUOTA_EXCEEDED", "The tenant has insufficient space for the candidate or temporary output"
		return outcome
	}
	var exportSize *ExportSizeExceededError
	if errors.As(err, &nativeSize) || errors.As(err, &exportSize) {
		outcome.ErrorClass, outcome.ErrorCode, outcome.Message = "size", "SOURCE_SIZE_EXCEEDED", "The source exceeds a processing size limit"
		if nativeSize != nil {
			outcome.Result, _ = json.Marshal(nativeSize)
		} else {
			outcome.Result, _ = json.Marshal(exportSize)
		}
		return outcome
	}
	var provider *MCPToolError
	if errors.As(err, &provider) {
		outcome.ErrorClass, outcome.ErrorCode, outcome.Message = "provider", fmt.Sprintf("TENCENT_%d", provider.Code), "The source provider rejected the operation"
		outcome.Result, _ = json.Marshal(map[string]any{"tool": provider.Tool, "provider_code": provider.Code, "provider_trace_id": provider.TraceID})
		if provider.Code == 60007 || provider.Code == 400007 || provider.Code == 400008 {
			outcome.ErrorClass = "permission"
			return outcome
		}
		if provider.Code == 53044 {
			outcome.ErrorClass, outcome.ErrorCode = "identity", "IDENTITY_UNRESOLVED"
			return outcome
		}
		if provider.Code == 11607 || provider.Code == 323908 {
			outcome.ErrorClass = "capability"
			return outcome
		}
	}
	if stage == "export_start" {
		outcome.ErrorClass, outcome.ErrorCode, outcome.Message = "uncertain", "EXPORT_START_UNCERTAIN", "The export request may have been accepted; reconcile its provider task before issuing another request"
		return outcome
	}
	if isRetryableTencentDocsMCPError("", err) {
		outcome.Status, outcome.ErrorClass, outcome.ErrorCode, outcome.Retryable = types.ProcessingFailed, "transient", "UPSTREAM_TEMPORARY", true
		outcome.Message = "The external operation failed temporarily"
		var status *mcpHTTPStatusError
		if errors.As(err, &status) {
			outcome.RetryAfter = status.RetryAfter
		}
		return outcome
	}
	// Our parser/checkpoint errors use bounded uppercase codes. Preserve these
	// exact codes without copying arbitrary provider text into the ledger.
	if err != nil {
		code := strings.SplitN(err.Error(), ":", 2)[0]
		if len(code) > 0 && len(code) <= 64 && strings.Contains(code, "_") && strings.IndexFunc(code, func(r rune) bool { return (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' }) < 0 {
			outcome.ErrorClass, outcome.ErrorCode = "completeness", code
			if code == "MODEL_OUTPUT_EMPTY" && (stage == "summary" || stage == "image_ocr" || stage == "image_caption" || stage == "question") {
				outcome.Status, outcome.ErrorClass, outcome.Retryable = types.ProcessingFailed, "model", true
				outcome.Message = "The model returned no usable text"
			}
			if code == "NATIVE_SIZE_EXCEEDED" {
				outcome.ErrorClass = "size"
			}
		}
	}
	return outcome
}
