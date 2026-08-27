package tencentdocs

import "context"

// Client is the transport-independent Tencent Docs contract consumed by the
// data-source connector. TencentDocsMCPClient is the first implementation; an
// Open API implementation can be added later without changing connector logic.
type Client interface {
	Validate(ctx context.Context) error
	ListSpaces(ctx context.Context) ([]Space, error)
	ListNodes(ctx context.Context, spaceID string, parentID string) ([]Node, error)
	ListHomeNodes(ctx context.Context, folderID string) ([]HomeNode, error)
	GetFileInfo(ctx context.Context, fileID string) (*FileInfo, error)
	GetContent(ctx context.Context, fileID string) (*DocumentContent, error)
	StartExport(ctx context.Context, fileID string) (*ExportTask, error)
	GetExportProgress(ctx context.Context, taskID string) (*ExportStatus, error)
	DownloadExport(ctx context.Context, fileURL string) ([]byte, error)
	Close() error
}

// SheetClient exposes range-based Sheet reads. It is optional so non-Sheet
// connector test doubles and future Open API clients can implement it separately.
type SheetClient interface {
	GetSheetInfo(ctx context.Context, fileID string) ([]SheetInfo, error)
	GetSheetCells(ctx context.Context, fileID, sheetID string,
		startRow, endRow, startCol, endCol int) ([]SheetCell, error)
}

var _ Client = (*TencentDocsMCPClient)(nil)
var _ SheetClient = (*TencentDocsMCPClient)(nil)
