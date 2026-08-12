// Package tencentdocs provides the Tencent Docs data-source integration.
package tencentdocs

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Space is a Tencent Docs knowledge-base space visible to the configured token.
type Space struct {
	ID          string `json:"space_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	IsTop       bool   `json:"is_top"`
	FileCount   int    `json:"file_cnt"`
	MemberCount int    `json:"member_cnt"`
	IsOwner     bool   `json:"is_owner"`
	CreatedAt   uint64 `json:"created_at"`
	UpdatedAt   uint64 `json:"updated_at"`
}

// Node is one file, folder, or link in a Tencent Docs space.
type Node struct {
	ID           string `json:"node_id"`
	Title        string `json:"title"`
	Type         string `json:"node_type"`
	HasChildren  bool   `json:"has_child"`
	DocumentType string `json:"doc_type"`
	URL          string `json:"url"`
}

// HomeNode is one file or folder in the Tencent Docs personal-home hierarchy.
// The folder-list tool does not expose a document type; callers resolve file
// metadata lazily only when the item is selected for synchronization.
type HomeNode struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	IsFolder bool   `json:"is_folder"`
}

// FileInfo contains the metadata used to decide whether a document changed.
type FileInfo struct {
	ID         string `json:"file_id"`
	Title      string `json:"title"`
	URL        string `json:"url"`
	Type       string `json:"type"`
	Status     string `json:"status"`
	CreatedAt  uint64 `json:"create_time"`
	CreatedBy  string `json:"create_name"`
	ModifiedAt uint64 `json:"last_modify_time"`
	ModifiedBy string `json:"last_modify_name"`
	Owner      string `json:"owner_name"`
	SpaceID    string `json:"space_id"`
	IsFolder   bool   `json:"is_folder"`
	TraceID    string `json:"trace_id"`
}

// UnmarshalJSON accepts both the numeric timestamps documented by Tencent
// Docs and the quoted millisecond timestamps returned by the live MCP service.
func (f *FileInfo) UnmarshalJSON(data []byte) error {
	var wire struct {
		ID         string         `json:"file_id"`
		Title      string         `json:"title"`
		URL        string         `json:"url"`
		Type       string         `json:"type"`
		Status     string         `json:"status"`
		CreatedAt  flexibleUint64 `json:"create_time"`
		CreatedBy  string         `json:"create_name"`
		ModifiedAt flexibleUint64 `json:"last_modify_time"`
		ModifiedBy string         `json:"last_modify_name"`
		Owner      string         `json:"owner_name"`
		SpaceID    string         `json:"space_id"`
		IsFolder   bool           `json:"is_folder"`
		TraceID    string         `json:"trace_id"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*f = FileInfo{
		ID: wire.ID, Title: wire.Title, URL: wire.URL, Type: wire.Type, Status: wire.Status,
		CreatedAt: uint64(wire.CreatedAt), CreatedBy: wire.CreatedBy,
		ModifiedAt: uint64(wire.ModifiedAt), ModifiedBy: wire.ModifiedBy,
		Owner: wire.Owner, SpaceID: wire.SpaceID, IsFolder: wire.IsFolder, TraceID: wire.TraceID,
	}
	return nil
}

type flexibleUint64 uint64

func (v *flexibleUint64) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*v = 0
		return nil
	}
	if data[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		if value == "" {
			*v = 0
			return nil
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid Tencent Docs timestamp %q: %w", value, err)
		}
		*v = flexibleUint64(parsed)
		return nil
	}
	var parsed uint64
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*v = flexibleUint64(parsed)
	return nil
}

// DocumentContent is the text representation returned by get_content.
type DocumentContent struct {
	Text    string `json:"content"`
	TraceID string `json:"trace_id"`
}

// ExportTask identifies an asynchronous Tencent Docs export.
type ExportTask struct {
	ID      string `json:"task_id"`
	TraceID string `json:"trace_id"`
}

// ExportStatus describes the current state of an asynchronous export.
type ExportStatus struct {
	Progress int    `json:"progress"`
	Status   string `json:"status"`
	FileName string `json:"file_name"`
	FileURL  string `json:"file_url"`
	Error    string `json:"error"`
	TraceID  string `json:"trace_id"`
}
