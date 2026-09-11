package tencentdocs

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNativeFailureKeepsWaitRetryAndUnknownExportDistinct(t *testing.T) {
	wait := ProcessingFailure("native_read", &MCPBudgetWaitError{RetryAfter: 5 * time.Minute})
	require.Equal(t, types.ProcessingWaitingExternal, wait.Status)
	require.False(t, wait.Retryable)
	require.Greater(t, time.Until(*wait.NextRunAt), 299*time.Second)
	read := ProcessingFailure("native_read", io.ErrUnexpectedEOF)
	require.Equal(t, types.ProcessingFailed, read.Status)
	require.True(t, read.Retryable)
	export := ProcessingFailure("export_start", io.ErrUnexpectedEOF)
	require.Equal(t, types.ProcessingBlocked, export.Status)
	require.Equal(t, "EXPORT_START_UNCERTAIN", export.ErrorCode)
	require.False(t, export.Retryable)
	permission := ProcessingFailure("native_read", &MCPToolError{Tool: "doc.resolve_document_structure", Code: 60007, Message: "secret=https://example.test/private?token=not-public", TraceID: "trace"})
	require.Equal(t, "permission", permission.ErrorClass)
	require.False(t, permission.Retryable)
	require.NotContains(t, permission.Message, "secret")
	require.NotContains(t, string(permission.Result), "token")
	changed := ProcessingFailure("native_read", errors.New("SOURCE_CHANGED_DURING_READ"))
	require.Equal(t, "SOURCE_CHANGED_DURING_READ", changed.ErrorCode)
	require.False(t, changed.Retryable)
}

func TestNativeFailureRetriesOnlyEmptyModelOutputInModelStages(t *testing.T) {
	for _, stage := range []string{"summary", "image_ocr", "image_caption", "question"} {
		outcome := ProcessingFailure(stage, errors.New("MODEL_OUTPUT_EMPTY"))
		require.Equal(t, types.ProcessingFailed, outcome.Status)
		require.Equal(t, "MODEL_OUTPUT_EMPTY", outcome.ErrorCode)
		require.True(t, outcome.Retryable)
	}
	for _, stage := range []string{"parse", "export_start", "scan_document"} {
		require.False(t, ProcessingFailure(stage, errors.New("MODEL_OUTPUT_EMPTY")).Retryable)
	}
	for _, code := range []string{"DOCX_TEXT_COVERAGE_INCOMPLETE", "IMAGE_MODEL_OUTPUT_INVALID", "SOURCE_CHANGED_DURING_READ"} {
		require.False(t, ProcessingFailure("image_ocr", errors.New(code)).Retryable)
	}
}
