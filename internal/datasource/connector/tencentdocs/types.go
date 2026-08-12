// Package tencentdocs provides the Tencent Docs data-source integration.
package tencentdocs

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
	TraceID  string `json:"trace_id"`
}
