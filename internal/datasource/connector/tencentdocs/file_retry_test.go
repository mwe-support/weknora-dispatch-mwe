package tencentdocs

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFileRetryClassifiesPermanentAndTransient(t *testing.T) {
	for _, tc := range []struct {
		err   error
		stage string
		want  bool
	}{
		{errors.New("HTTP 429"), "fetch_metadata", true},
		{errors.New("status 429"), "fetch_metadata", true},
		{errors.New("i/o timeout"), "download", true},
		{errors.New("i/o timeout"), "export_start", false},
		{&ExportSizeExceededError{LimitBytes: 100}, "download", false},
		{&MCPToolError{Code: 323908}, "export", false},
		{&MCPToolError{Code: 400005}, "fetch", false},
		{&MCPToolError{Code: 10012}, "export", true},
		{&MCPToolError{Code: 111}, "export", false},
	} {
		_, got := fileRetryCategory(tc.err, tc.stage)
		if got != tc.want {
			t.Fatalf("%v/%s: %v", tc.err, tc.stage, got)
		}
	}
	if !isRetryableTencentDocsMCPError(toolQueryFileInfo, errors.New("HTTP 429")) {
		t.Fatal("HTTP 429 format missed")
	}
}

type failingRetryCheckpoint struct {
	recordingStreamHandler
	calls  int
	failAt int
}

func (h *failingRetryCheckpoint) Checkpoint(ctx context.Context, cursor *types.SyncCursor) error {
	h.calls++
	if h.calls == h.failAt {
		return errors.New("checkpoint interrupted")
	}
	return h.recordingStreamHandler.Checkpoint(ctx, cursor)
}

func TestExportIntentSurvivesMissingTaskCheckpoint(t *testing.T) {
	root := encodeSpaceResourceID("s")
	client := &fakeConnectorClient{nodes: map[string][]Node{"s/": {{ID: "pdf", Type: "resource"}}}, infos: map[string]*FileInfo{"pdf": {ID: "pdf", Type: "pdf"}}, exportTasks: map[string]*ExportTask{"pdf": {ID: "created-once"}}}
	c := testConnector(client)
	cfg := testDataSourceConfig(root)
	h := &failingRetryCheckpoint{failAt: 2}
	if _, err := c.FetchStream(context.Background(), cfg, nil, h); err == nil {
		t.Fatal("expected interruption")
	}
	if client.exportStarts != 1 || len(h.checkpoints) != 1 {
		t.Fatal("intent not persisted before export")
	}
	restarted := &recordingStreamHandler{}
	_, err := c.FetchStream(context.Background(), cfg, h.checkpoints[0], restarted)
	if err != nil {
		t.Fatal(err)
	}
	if client.exportStarts != 1 || len(restarted.items) != 1 || restarted.items[0].Metadata["retry_state"] != "needs_manual" {
		t.Fatal("ambiguous export was repeated")
	}
}

func TestFileRetryOnlyFetchesFailedFileAndPreservesCursor(t *testing.T) {
	root := encodeSpaceResourceID("s")
	client := &fakeConnectorClient{nodes: map[string][]Node{"s/": {{ID: "ok", Type: "wiki_file", DocumentType: "doc"}, {ID: "bad", Type: "wiki_file", DocumentType: "doc"}}},
		infos:    map[string]*FileInfo{"ok": {ID: "ok", Title: "OK", Type: "doc", ModifiedAt: 1}, "bad": {ID: "bad", Title: "BAD", Type: "doc", ModifiedAt: 1}},
		contents: map[string]*DocumentContent{"ok": {Text: "OK"}, "bad": {Text: "RECOVERED"}}, contentErrs: map[string]error{"bad": errors.New("HTTP 429")}}
	c := testConnector(client)
	cfg := testDataSourceConfig(root)
	h := &recordingStreamHandler{}
	cursor, err := c.FetchStream(context.Background(), cfg, nil, h)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := decodeTencentDocsCursor(cursor)
	id := encodeNodeResourceID("s", "bad")
	if len(p.FileRetries) != 1 || p.FileRetries[id].State != "scheduled" {
		t.Fatalf("retry not recorded: %+v", p.FileRetries)
	}
	r := p.FileRetries[id]
	r.NextAt = time.Now().Add(-time.Minute)
	p.FileRetries[id] = r
	delete(client.contentErrs, "bad")
	client.listCalls = 0
	client.contentGets = 0
	h = &recordingStreamHandler{}
	cursor, err = c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(p), h)
	if err != nil {
		t.Fatal(err)
	}
	end, _ := decodeTencentDocsCursor(cursor)
	if client.listCalls != 0 || client.contentGets != 1 || len(h.items) != 1 || len(end.FileRetries) != 0 || len(end.DocumentTimes) != 2 {
		t.Fatalf("retry repeated successful files or lost cursor: %+v", end)
	}
	if string(h.items[0].Content) != "RECOVERED" {
		t.Fatal("not re-fetched")
	}
	// Scope/credential changes must invalidate delayed work before remote I/O.
	cfg.ResourceIDs = []string{encodeSpaceResourceID("other")}
	if _, err = c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(p), h); err == nil {
		t.Fatal("changed scope accepted")
	}
}

func TestFileRetryReusesExportAndBoundsAttempts(t *testing.T) {
	root := encodeSpaceResourceID("s")
	id := encodeNodeResourceID("s", "pdf")
	client := &fakeConnectorClient{infos: map[string]*FileInfo{"pdf": {ID: "pdf", Type: "pdf"}},
		exportErrs: map[string]error{"original": errors.New("i/o timeout")}}
	c := testConnector(client)
	cfg := testDataSourceConfig(root)
	p := copyTencentDocsCursor(nil)
	p.RetryScope = retryScope(cfg)
	p.FileRetries[id] = fileRetry{Node: Node{ID: "pdf", Type: "resource"}, SpaceID: "s", SourceResourceID: root, ExportTaskID: "original", State: "scheduled", NextAt: time.Now().Add(-time.Minute)}
	for i := 1; i <= maxFileRetries; i++ {
		h := &recordingStreamHandler{}
		cursor, err := c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(p), h)
		if err != nil {
			t.Fatal(err)
		}
		p, _ = decodeTencentDocsCursor(cursor)
		r := p.FileRetries[id]
		if r.Attempt != i || r.ExportTaskID != "original" || client.exportStarts != 0 {
			t.Fatalf("lost attempt/task: %+v", r)
		}
		if i < maxFileRetries {
			r.NextAt = time.Now().Add(-time.Minute)
			p.FileRetries[id] = r
		}
	}
	if p.FileRetries[id].State != "exhausted" {
		t.Fatal("unbounded retry")
	}
	if at, _ := c.NextFileRetry(syncCursorFromTencentDocs(p)); at != nil {
		t.Fatal("exhausted file scheduled")
	}
	b, _ := syncCursorFromTencentDocs(p).ToJSON()
	if strings.Contains(string(b), "mcp_token") {
		t.Fatal("credential in cursor")
	}
}

func TestRetryAfterAndCredentialBudget(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if retryAfter("30", now) != 30*time.Second || retryAfter(now.Add(time.Minute).Format(http.TimeFormat), now) != time.Minute || retryAfter("invalid", now) != 0 {
		t.Fatal("Retry-After parsing")
	}
	a := withCredentialBudget("same-token", http.DefaultTransport).(*budgetTransport)
	b := withCredentialBudget("same-token", http.DefaultTransport).(*budgetTransport)
	c := withCredentialBudget("another-token", http.DefaultTransport).(*budgetTransport)
	if a.budget != b.budget || a.budget == c.budget {
		t.Fatal("budget not credential scoped")
	}
	a.budget.mu.Lock()
	a.budget.next = time.Now().Add(time.Hour)
	a.budget.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://docs.qq.com/", nil)
	if _, err := a.RoundTrip(req); !errors.Is(err, context.Canceled) {
		t.Fatal("budget ignored cancellation")
	}
}

func TestFileRetryInterruptedLastAttemptIsNotReportedAsSuccess(t *testing.T) {
	root := encodeSpaceResourceID("s")
	id := encodeNodeResourceID("s", "doc")
	cfg := testDataSourceConfig(root)
	client := &fakeConnectorClient{}
	c := testConnector(client)
	p := copyTencentDocsCursor(nil)
	p.RetryScope = retryScope(cfg)
	p.FileRetries[id] = fileRetry{Node: Node{ID: "doc", Type: "wiki_file", Title: "Doc"}, SpaceID: "s", SourceResourceID: root, State: "scheduled", Attempt: maxFileRetries, NextAt: time.Now().Add(-time.Minute)}
	h := &recordingStreamHandler{}
	cursor, err := c.FetchRetryStream(context.Background(), cfg, syncCursorFromTencentDocs(p), h)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.items) != 1 || h.items[0].Metadata["retry_state"] != "exhausted" || h.items[0].Metadata["error"] == "" {
		t.Fatal("interrupted exhausted attempt became silent success")
	}
	if next, _ := c.NextFileRetry(cursor); next != nil {
		t.Fatal("exhausted retry rescheduled")
	}
	if client.listCalls != 0 || client.contentGets != 0 || client.exportStarts != 0 {
		t.Fatal("exhausted record triggered I/O")
	}
}
