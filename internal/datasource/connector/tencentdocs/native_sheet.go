package tencentdocs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Reuse the established range renderer and its absolute/relative coordinate
// handling through a capture/replay adapter, with no second cell formatter.
type nativeSheetClient struct {
	call func(context.Context, string, map[string]interface{}, interface{}) error
}

func (c nativeSheetClient) GetSheetInfo(ctx context.Context, file string) ([]SheetInfo, error) {
	var result sheetInfoResponse
	if err := c.call(ctx, toolSheetGetInfo, map[string]interface{}{"file_id": file}, &result); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, sheet := range result.Sheets {
		if sheet.ID == "" || seen[sheet.ID] {
			return nil, errors.New("SHEET_IDENTITY_INVALID")
		}
		seen[sheet.ID] = true
		if sheet.Type != "" && sheet.Type != "worksheet" {
			return nil, errors.New("SHEET_SUBTYPE_UNSUPPORTED")
		}
	}
	return result.Sheets, nil
}
func (c nativeSheetClient) GetSheetCells(ctx context.Context, file, sheet string, startRow, endRow, startCol, endCol int) ([]SheetCell, error) {
	var result sheetCellsResponse
	args := map[string]interface{}{"file_id": file, "sheet_id": sheet, "start_row": startRow, "end_row": endRow, "start_col": startCol, "end_col": endCol, "return_csv": false, "include_formula": true}
	if err := c.call(ctx, toolSheetGetCells, args, &result); err != nil {
		return nil, err
	}
	for i, cell := range result.Cells {
		if cell == (SheetCell{}) {
			continue
		}
		switch strings.ToUpper(cell.ValueType) {
		case "", "NUMBER", "STRING", "BOOL", "FORMULA", "ERROR", "TIME_STRING", "RICH_STRING":
		default:
			return nil, errors.New("SHEET_CELL_TYPE_UNSUPPORTED")
		}
		if cell.Formula != "" {
			value := sheetCellText(cell)
			if value != cell.Formula {
				value += " [formula: " + cell.Formula + "]"
			}
			result.Cells[i].ValueType = "STRING"
			result.Cells[i].StringValue = value
		}
	}
	return result.Cells, nil
}

func (c *nativeCollector) sheet(ctx context.Context) error {
	reader := nativeSheetClient{call: func(ctx context.Context, tool string, args map[string]interface{}, out interface{}) error {
		return c.call(ctx, tool, args, false, out)
	}}
	rendered, err := fetchSheetMarkdown(ctx, reader, c.snapshot.FileID)
	if err != nil {
		return err
	}
	// ponytail: Sheet MCP exposes no immutable revision token. Re-read each
	// captured range; replace these full digest checks with version-pinned reads
	// when the provider makes that contract available.
	for _, page := range c.snapshot.Pages {
		switch page.Tool {
		case toolSheetGetInfo:
			var first, last sheetInfoResponse
			if err := json.Unmarshal(page.Data, &first); err != nil {
				return err
			}
			if err := c.call(ctx, page.Tool, page.Args, true, &last); err != nil {
				return err
			}
			if !reflect.DeepEqual(first, last) {
				return errors.New("SOURCE_CHANGED_DURING_READ")
			}
		case toolSheetGetCells:
			var first, last sheetCellsResponse
			if err := json.Unmarshal(page.Data, &first); err != nil {
				return err
			}
			if err := c.call(ctx, page.Tool, page.Args, true, &last); err != nil {
				return err
			}
			if !reflect.DeepEqual(first, last) {
				return errors.New("SOURCE_CHANGED_DURING_READ")
			}
		}
	}
	c.snapshot.Units, err = strconv.Atoi(rendered.Metadata["non_empty_row_count"])
	return err
}

func normalizeNativeSheet(snapshot *NativeSnapshot) (*sheetMarkdownResult, error) {
	saved := map[string]json.RawMessage{}
	for _, page := range snapshot.Pages {
		if page.Tool != toolSheetGetInfo && page.Tool != toolSheetGetCells {
			continue
		}
		key, err := json.Marshal([]any{page.Tool, page.Args})
		if err != nil {
			return nil, err
		}
		if saved[string(key)] != nil {
			return nil, errors.New("SHEET_DUPLICATE_RANGE")
		}
		saved[string(key)] = page.Data
	}
	reader := nativeSheetClient{call: func(_ context.Context, tool string, args map[string]interface{}, out interface{}) error {
		key, err := json.Marshal([]any{tool, args})
		if err != nil {
			return err
		}
		data := saved[string(key)]
		if data == nil {
			return errors.New("SHEET_CAPTURED_RANGE_MISSING")
		}
		return json.Unmarshal(data, out)
	}}
	return fetchSheetMarkdown(context.Background(), reader, snapshot.FileID)
}

func nativeSheetSnapshotRevision(snapshot *NativeSnapshot) (string, error) {
	var metadata FileInfo
	for _, page := range snapshot.Pages {
		if page.Tool == toolQueryFileInfo {
			if err := json.Unmarshal(page.Data, &metadata); err != nil {
				return "", err
			}
			break
		}
	}
	if metadata.ID == "" || metadata.Status != "normal" || metadata.ModifiedAt == 0 || metadata.Type != "sheet" {
		return "", errors.New("SOURCE_METADATA_INCOMPLETE_OR_UNAVAILABLE")
	}
	rendered, err := normalizeNativeSheet(snapshot)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal([]any{metadata.ID, metadata.Title, metadata.ModifiedAt, "sheet", rendered})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}
