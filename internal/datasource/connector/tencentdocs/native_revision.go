package tencentdocs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
)

// ProbeNativeRevision performs only fresh, bounded reads. DOC metadata mtime
// was observed unchanged after a confirmed edit, so its document version is
// mandatory. This key is also reconstructed from the actual captured pages.
func ProbeNativeRevision(ctx context.Context, fileID, kind string, read NativeReadFunc) (string, error) {
	if err := requireID("file ID", fileID); err != nil {
		return "", err
	}
	if read == nil {
		return "", errors.New("native reader is unavailable")
	}
	if kind == "sheet" {
		snapshot, err := CollectNative(ctx, fileID, kind, func(ctx context.Context, tool string, args map[string]interface{}, _ bool) (*NativeResponse, error) {
			return read(ctx, tool, args, true)
		})
		if err != nil {
			return "", err
		}
		return snapshot.RevisionKey, nil
	}
	c := nativeCollector{snapshot: NativeSnapshot{FileID: fileID, Kind: kind}, read: read}
	var metadata FileInfo
	if err := c.call(ctx, toolQueryFileInfo, map[string]interface{}{}, true, &metadata); err != nil {
		return "", err
	}
	c.snapshot.FileID = metadata.ID
	tool, args, err := nativeRevisionRequest(kind)
	if err != nil {
		return "", err
	}
	var detail json.RawMessage
	if err := c.call(ctx, tool, args, true, &detail); err != nil {
		return "", err
	}
	return nativeRevisionKey(metadata, kind, detail)
}

func nativeRevisionRequest(kind string) (string, map[string]interface{}, error) {
	switch kind {
	case "doc":
		return "doc.resolve_document_structure", map[string]interface{}{"mode": 2, "offset": 0, "limit": 1, "has_text_preview_length": true, "text_preview_length": 0}, nil
	case "smartcanvas":
		return "smartcanvas.get_top_level_pages", map[string]interface{}{}, nil
	case "smartsheet":
		return "smartsheet.list_tables", map[string]interface{}{}, nil
	default:
		return "", nil, errors.New("native revision type is unsupported")
	}
}

func nativeRevisionKey(metadata FileInfo, kind string, detail json.RawMessage) (string, error) {
	if metadata.ID == "" || metadata.Status != "normal" || metadata.ModifiedAt == 0 || metadata.Type != kind {
		return "", errors.New("SOURCE_METADATA_INCOMPLETE_OR_UNAVAILABLE")
	}
	var version any
	switch kind {
	case "doc":
		var page nativeDocPage
		if err := json.Unmarshal(detail, &page); err != nil {
			return "", err
		}
		if len(page.Version) == 0 || string(page.Version) == "null" || string(page.Version) == `""` {
			return "", errors.New("DOC_REVISION_UNAVAILABLE")
		}
		version = page.Version
	case "smartcanvas":
		var catalog nativeCanvasCatalog
		if err := json.Unmarshal(detail, &catalog); err != nil {
			return "", err
		}
		if len(catalog.Pages) == 0 {
			return "", errors.New("MDX_PAGE_CATALOG_EMPTY")
		}
		version = catalog
	case "smartsheet":
		var catalog nativeSmartCatalog
		if err := json.Unmarshal(detail, &catalog); err != nil {
			return "", err
		}
		if len(catalog.Sheets) == 0 {
			return "", errors.New("SMARTSHEET_CATALOG_EMPTY")
		}
		version = catalog
	default:
		return "", errors.New("native revision type is unsupported")
	}
	encoded, err := json.Marshal([]any{metadata.ID, kind, metadata.Title, metadata.ModifiedAt, version})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

func nativeSnapshotRevision(snapshot *NativeSnapshot) (string, error) {
	if snapshot.Kind == "sheet" {
		return nativeSheetSnapshotRevision(snapshot)
	}
	tool, _, err := nativeRevisionRequest(snapshot.Kind)
	if err != nil {
		return "", err
	}
	var metadata FileInfo
	var detail json.RawMessage
	for _, page := range snapshot.Pages {
		if page.Tool == toolQueryFileInfo {
			if err := json.Unmarshal(page.Data, &metadata); err != nil {
				return "", err
			}
		}
		if page.Tool == tool && detail == nil {
			detail = page.Data
		}
	}
	return nativeRevisionKey(metadata, snapshot.Kind, detail)
}
