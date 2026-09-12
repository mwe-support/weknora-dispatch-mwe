package tencentdocs

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// ExportSizeExceededError distinguishes a local capacity guard from a parser
// or upstream API failure. ObservedAtLeastBytes is not the total file size.
type ExportSizeExceededError struct {
	LimitBytes           int64
	ActualBytes          *int64
	ObservedAtLeastBytes int64
}

var sourceErrorURL = regexp.MustCompile(`https?://[^\s"'<>]+`)
var sourceErrorSecret = regexp.MustCompile(`(?i)(authorization|cookie|password|api[_-]?key|[a-z0-9_-]*token|secret)\s*["']?\s*[:=]\s*[^\r\n,}]+`)

func safeSourceError(message string) string {
	message = sourceErrorURL.ReplaceAllString(message, "[URL_REDACTED]")
	message = sourceErrorSecret.ReplaceAllString(message, "[SECRET_REDACTED]")
	message = strings.Join(strings.Fields(message), " ")
	if len(message) > 1000 {
		message = strings.ToValidUTF8(message[:1000], "")
	}
	return message
}

func (e *ExportSizeExceededError) Error() string {
	if e.ActualBytes != nil {
		return fmt.Sprintf("FILE_SIZE_EXCEEDED: 文件大小超出上限 (limit_bytes=%d, actual_bytes=%d)", e.LimitBytes, *e.ActualBytes)
	}
	return fmt.Sprintf("FILE_SIZE_EXCEEDED: 文件大小超出上限 (limit_bytes=%d, actual_bytes=unknown, observed_at_least_bytes=%d)", e.LimitBytes, e.ObservedAtLeastBytes)
}

// The caller supplies the unchanged 100 MiB production guard; the parameter
// lets tests exercise bounded streams without allocating 100 MiB per test.
func readExportBody(body io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength > limit {
		return nil, &ExportSizeExceededError{LimitBytes: limit, ActualBytes: &contentLength}
	}
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read Tencent Docs export: %w", err)
	}
	if int64(len(data)) > limit {
		// Even a declared length can be wrong. A bounded read proves only a
		// lower bound, never the actual total size of the remaining stream.
		return nil, &ExportSizeExceededError{LimitBytes: limit, ObservedAtLeastBytes: int64(len(data))}
	}
	return data, nil
}

type fileOperationError struct {
	stage string
	err   error
}

func (e *fileOperationError) Error() string { return e.err.Error() }
func (e *fileOperationError) Unwrap() error { return e.err }

func withFileStage(stage string, err error) error {
	return &fileOperationError{stage: stage, err: err}
}

func addFileFailureMetadata(metadata map[string]string, err error) {
	stage := "fetch"
	var operation *fileOperationError
	if errors.As(err, &operation) {
		stage = operation.stage
	}
	category, retryable := fileRetryCategory(err, stage)
	metadata["retryable"] = strconv.FormatBool(retryable)
	metadata["retry_category"] = category
	var notSent *exportNotSentError
	if errors.As(err, &notSent) {
		metadata["export_not_sent"] = "true"
	}
	var wait *MCPBudgetWaitError
	if errors.As(err, &wait) {
		metadata["retry_admission_wait"] = "true"
		metadata["retry_after_ms"] = strconv.FormatInt(wait.RetryAfter.Milliseconds(), 10)
	}
	if errors.Is(err, ErrUnsupportedSourceType) {
		metadata["error_reason_code"], metadata["error_reason"] = "UNSUPPORTED_FILE_TYPE", "不支持的文档类型"
	}
	var task *exportTaskError
	if errors.As(err, &task) {
		metadata["retry_export_task_id"] = task.taskID
	}
	metadata["error_stage"] = stage
	metadata["error_category"] = "SOURCE_FETCH_FAILED"
	if stage == "export" {
		metadata["error_category"] = "SOURCE_EXPORT_FAILED"
	} else if stage == "download" {
		metadata["error_category"] = "SOURCE_DOWNLOAD_FAILED"
	}
	var sizeErr *ExportSizeExceededError
	if errors.As(err, &sizeErr) {
		metadata["error_stage"] = "download"
		metadata["error_category"] = "FILE_SIZE_EXCEEDED"
		metadata["error_reason_code"] = "FILE_SIZE_EXCEEDED"
		metadata["error_reason"] = sizeErr.Error()
		metadata["limit_bytes"] = strconv.FormatInt(sizeErr.LimitBytes, 10)
		if sizeErr.ActualBytes != nil {
			metadata["actual_bytes"] = strconv.FormatInt(*sizeErr.ActualBytes, 10)
		}
		if sizeErr.ObservedAtLeastBytes > 0 {
			metadata["observed_at_least_bytes"] = strconv.FormatInt(sizeErr.ObservedAtLeastBytes, 10)
		}
	}
}
