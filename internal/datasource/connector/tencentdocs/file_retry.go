package tencentdocs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

const maxFileRetries = 3

type fileRetry struct {
	Node                 Node      `json:"node"`
	SpaceID              string    `json:"space_id"`
	SourceResourceID     string    `json:"source_resource_id"`
	Attempt              int       `json:"attempt"`
	State                string    `json:"state"`
	Category             string    `json:"category,omitempty"`
	NextAt               time.Time `json:"next_at,omitempty"`
	ExportTaskID         string    `json:"export_task_id,omitempty"`
	ExportStartUncertain bool      `json:"export_start_uncertain,omitempty"`
}

type exportTaskError struct {
	taskID string
	err    error
}

func (e *exportTaskError) Error() string        { return e.err.Error() }
func (e *exportTaskError) Unwrap() error        { return e.err }
func withExportTask(id string, err error) error { return &exportTaskError{id, err} }

func retryScope(config *types.DataSourceConfig) string {
	ids := slices.Clone(config.ResourceIDs)
	slices.Sort(ids)
	// Include credentials only in a digest; never persist the token in tasks/cursors.
	b, _ := json.Marshal([]any{ids, config.Credentials})
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func isRateLimited(message string) bool {
	m := strings.ToLower(message)
	return strings.Contains(m, "status 429") || strings.Contains(m, "http 429")
}

func fileRetryCategory(err error, stage string) (string, bool) {
	var size *ExportSizeExceededError
	var tool *MCPToolError
	if errors.As(err, &size) {
		return "FILE_SIZE_EXCEEDED", false
	}
	if errors.Is(err, datasource.ErrInvalidCredentials) {
		return "AUTH_REQUIRED", false
	}
	if errors.As(err, &tool) {
		switch tool.Code {
		case 323908:
			return "UNSUPPORTED_FILE_TYPE", false
		case 400005, 400006:
			return "AUTH_REQUIRED", false
		case 10012, 10328:
			if stage != "export_start" {
				return "UPSTREAM_TEMPORARY", true
			}
		}
	}
	if isRateLimited(err.Error()) {
		return "RATE_LIMITED", true
	}
	if stage == "export_start" {
		return "EXPORT_START_UNCERTAIN", false
	}
	m := strings.ToLower(err.Error())
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(m, "timeout") || strings.Contains(m, "timed out") || strings.Contains(m, "connection reset") || strings.Contains(m, "temporarily unavailable") || strings.Contains(m, "http status 503") || strings.Contains(m, "http status 502") || strings.Contains(m, "http status 504") {
		return "TRANSIENT_NETWORK", true
	}
	return "NEEDS_REVIEW", false
}

func (s *fetchState) trackFileRetry(item *types.FetchedItem) {
	if s.next.FileRetries == nil {
		s.next.FileRetries = map[string]fileRetry{}
	}
	if item.Metadata["error"] == "" {
		delete(s.next.FileRetries, item.ExternalID)
		return
	}
	r := s.next.FileRetries[item.ExternalID]
	r.Node = Node{ID: item.Metadata["file_id"], Title: item.Title, Type: item.Metadata["node_type"], DocumentType: item.Metadata["document_type"], URL: item.Metadata["retry_node_url"]}
	r.SpaceID, r.SourceResourceID = item.Metadata["space_id"], item.SourceResourceID
	r.Category = item.Metadata["retry_category"]
	if id := item.Metadata["retry_export_task_id"]; id != "" {
		r.ExportTaskID = id
		r.ExportStartUncertain = false
	}
	if item.Metadata["retry_category"] == "RATE_LIMITED" && item.Metadata["error_stage"] == "export_start" {
		r.ExportStartUncertain = false
	}
	r.State = "needs_manual"
	if r.Attempt >= maxFileRetries {
		r.State = "exhausted"
	}
	r.NextAt = time.Time{}
	// Directory enumeration is not one file: do not silently retry an entire subtree.
	if item.Metadata["retryable"] == "true" && r.Node.ID != "" && r.Node.Type != "folder" && r.Node.Type != "wiki_folder" {
		if r.Attempt < maxFileRetries {
			r.State = "scheduled"
			base := time.Minute * time.Duration(2<<r.Attempt)
			r.NextAt = time.Now().UTC().Add(base + time.Duration(rand.Int64N(int64(base/4))))
		} else {
			r.State = "exhausted"
		}
	}
	s.next.FileRetries[item.ExternalID] = r
	item.Metadata["retry_state"] = r.State
	item.Metadata["retry_attempt"] = strconv.Itoa(r.Attempt)
	if !r.NextAt.IsZero() {
		item.Metadata["next_retry_at"] = r.NextAt.Format(time.RFC3339Nano)
	}
}

func (c *Connector) NextFileRetry(cursor *types.SyncCursor) (*time.Time, error) {
	state, err := decodeTencentDocsCursor(cursor)
	if err != nil || state == nil {
		return nil, err
	}
	var next *time.Time
	for _, r := range state.FileRetries {
		// Coalesce one source's retry batch after all pending files become due.
		if r.State == "scheduled" && !r.NextAt.IsZero() && (next == nil || r.NextAt.After(*next)) {
			at := r.NextAt
			next = &at
		}
	}
	return next, nil
}

func (c *Connector) FetchRetryStream(ctx context.Context, config *types.DataSourceConfig, cursor *types.SyncCursor, h datasource.StreamHandler) (*types.SyncCursor, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: retry config required", datasource.ErrInvalidConfig)
	}
	previous, err := decodeTencentDocsCursor(cursor)
	if err != nil {
		return nil, err
	}
	if previous == nil || previous.RetryScope != retryScope(config) {
		return nil, fmt.Errorf("%w: retry scope/credentials changed", datasource.ErrInvalidConfig)
	}
	if h == nil {
		return nil, errors.New("retry stream handler required")
	}
	client, err := c.newClient(config)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	s := &fetchState{client: client, handler: h, previous: previous, next: copyTencentDocsCursor(previous), seenDocs: map[string]bool{}, seenFileIDs: map[string]bool{}, visitedNodes: map[string]bool{}}
	ids := make([]string, 0, len(previous.FileRetries))
	for id := range previous.FileRetries {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		r := s.next.FileRetries[id]
		if r.State == "exhausted" || r.State == "needs_manual" || (r.State == "scheduled" && r.Attempt >= maxFileRetries) {
			// A crash after consuming the last attempt is not successful recovery.
			// Report the durable terminal state without making any remote call.
			item := failedFetchedItem(id, r.Node.Title, r.SpaceID, r.Node, r.SourceResourceID, errors.New("previous file failure requires manual action"))
			item.Metadata["retryable"] = "false"
			if r.Category != "" {
				item.Metadata["retry_category"] = r.Category
			}
			if err = s.emit(ctx, item); err != nil {
				return nil, err
			}
			continue
		}
		if r.State != "scheduled" || time.Now().Before(r.NextAt) {
			continue
		}
		if !slices.Contains(config.ResourceIDs, r.SourceResourceID) || id != fetchedNodeResourceID(r.SpaceID, r.Node.ID) {
			return nil, fmt.Errorf("%w: invalid retry target", datasource.ErrInvalidConfig)
		}
		r.Attempt++
		s.next.FileRetries[id] = r
		// Persist attempts before I/O so worker crashes cannot reset the retry budget.
		if err = s.checkpoint(ctx); err != nil {
			return nil, err
		}
		if err = s.fetchNode(ctx, r.SpaceID, r.Node, nil, r.SourceResourceID); err != nil {
			return nil, err
		}
	}
	// Keep all unrelated cursor entries. This was never a full traversal.
	return syncCursorFromTencentDocs(s.next), nil
}
