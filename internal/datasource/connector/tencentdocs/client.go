package tencentdocs

import "context"

// Client is the transport-independent Tencent Docs contract consumed by the
// data-source connector. TencentDocsMCPClient is the first implementation; an
// Open API implementation can be added later without changing connector logic.
type Client interface {
	Validate(ctx context.Context) error
	ListSpaces(ctx context.Context) ([]Space, error)
	ListNodes(ctx context.Context, spaceID string, parentID string) ([]Node, error)
	GetFileInfo(ctx context.Context, fileID string) (*FileInfo, error)
	GetContent(ctx context.Context, fileID string) (*DocumentContent, error)
	StartExport(ctx context.Context, fileID string) (*ExportTask, error)
	GetExportProgress(ctx context.Context, taskID string) (*ExportStatus, error)
	Close() error
}

var _ Client = (*TencentDocsMCPClient)(nil)
