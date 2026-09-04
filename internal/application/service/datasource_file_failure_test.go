package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestFetchFailurePreservesIdentityStageAndSize(t *testing.T) {
	item := &types.FetchedItem{ExternalID: "external", Title: "synthetic.xlsx", SourceResourceID: "resource", Metadata: map[string]string{
		"file_id": "file", "channel": "tencent_docs", "error_stage": "download",
		"error_category": "FILE_SIZE_EXCEEDED", "error_reason_code": "FILE_SIZE_EXCEEDED",
		"error_reason": "文件大小超出上限", "limit_bytes": "104857600", "observed_at_least_bytes": "104857601",
	}}
	got := fetchFailureSyncError(item, "raw fallback")
	if got.ExternalID != "external" || got.FileID != "file" || got.Stage != "download" || got.Source != "tencent_docs" || got.OccurredAt == nil ||
		got.Category != "FILE_SIZE_EXCEEDED" || got.ActualBytes != nil || got.LimitBytes == nil || *got.LimitBytes != 104857600 {
		t.Fatalf("unexpected structured failure: %#v", got)
	}
	b, err := json.Marshal(got)
	if err != nil || strings.Contains(string(b), `"actual_bytes"`) {
		t.Fatalf("unknown size must not be serialized: %s %v", b, err)
	}
	item.Metadata["actual_bytes"] = "1457777536"
	got = fetchFailureSyncError(item, "raw fallback")
	b, _ = json.Marshal(got)
	var restored types.SyncItemError
	if err := json.Unmarshal(b, &restored); err != nil || restored.ActualBytes == nil || *restored.ActualBytes != 1457777536 || restored.FileID != "file" {
		t.Fatalf("round trip failed: %#v %v", restored, err)
	}
}
