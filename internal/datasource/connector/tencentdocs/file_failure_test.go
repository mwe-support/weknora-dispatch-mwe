package tencentdocs

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type unreadableBody struct{}

func (unreadableBody) Read([]byte) (int, error) { panic("oversized declared body must not be read") }

func TestExportSizeFailureKnownAndUnknown(t *testing.T) {
	if maxExportBytes != 104857600 {
		t.Fatal("production 100 MiB guard changed")
	}
	_, err := readExportBody(unreadableBody{}, 1457777536, maxExportBytes)
	var size *ExportSizeExceededError
	if !errors.As(err, &size) || size.ActualBytes == nil || *size.ActualBytes != 1457777536 || size.LimitBytes != maxExportBytes {
		t.Fatalf("known length: %#v, %v", size, err)
	}
	for _, declared := range []int64{-1, 3} {
		_, err = readExportBody(strings.NewReader("123456789tail"), declared, 8)
		if !errors.As(err, &size) || size.ActualBytes != nil || size.ObservedAtLeastBytes != 9 {
			t.Fatalf("stream length %d: %#v, %v", declared, size, err)
		}
		item := failedFetchedItem("tdoc:node:synthetic", "synthetic.xlsx", "space", Node{ID: "file"}, "source",
			withFileStage("download", fmt.Errorf("wrapped: %w", err)))
		m := item.Metadata
		if m["error_category"] != "FILE_SIZE_EXCEEDED" || m["error_stage"] != "download" ||
			m["limit_bytes"] != "8" || m["actual_bytes"] != "" || m["observed_at_least_bytes"] != "9" ||
			!strings.Contains(m["error_reason"], "文件大小超出上限") || m["file_id"] != "file" {
			t.Fatalf("metadata: %#v", m)
		}
	}
	data, err := readExportBody(strings.NewReader("12345678"), 8, 8)
	if err != nil || string(data) != "12345678" {
		t.Fatalf("exact limit: %q %v", data, err)
	}
}

func TestTencent111IsNotUnsupportedFileType(t *testing.T) {
	err := &MCPToolError{Tool: toolExportProgress, Code: 111, Message: "Service Error"}
	if isUnsupportedResourceExportError(err) {
		t.Fatal("111 must remain an upstream failure, not an unsupported-file skip")
	}
	item := failedFetchedItem("external", "title", "space", Node{ID: "file"}, "source", withFileStage("export", err))
	if item.Metadata["error_category"] != "SOURCE_EXPORT_FAILED" || item.Metadata["error_stage"] != "export" || item.Metadata["skip_reason"] != "" {
		t.Fatalf("111 metadata: %#v", item.Metadata)
	}
}
