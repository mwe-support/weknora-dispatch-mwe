package tencentdocs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeSheetCapturesAllRangesAndRejectsChangedCells(t *testing.T) {
	for _, fault := range []string{"", "changed", "duplicate-sheet", "unknown-cell"} {
		t.Run(fault, func(t *testing.T) {
			calls := 0
			read := func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
				if tool == toolQueryFileInfo {
					return nativeMetadata(t, "sheet"), nil
				}
				if tool == toolSheetGetInfo {
					sheets := []SheetInfo{{ID: "first", Name: "First", Type: "worksheet", RowCount: 201, ColCount: 2}, {ID: "empty", Name: "Empty", Type: "worksheet", RowCount: 0, ColCount: 2}}
					if fault == "duplicate-sheet" {
						sheets = append(sheets, sheets[0])
					}
					return nativeReply(t, sheetInfoResponse{Sheets: sheets}), nil
				}
				require.Equal(t, toolSheetGetCells, tool)
				require.Equal(t, true, args["include_formula"])
				calls++
				start := args["start_row"].(int)
				var cells []SheetCell
				if start == 0 {
					cells = []SheetCell{{Row: 0, Col: 0, ValueType: "NUMBER", NumberValue: 0}, {Row: 0, Col: 1, ValueType: "BOOL", BoolValue: false}, {}}
				}
				if start == 100 {
					cells = []SheetCell{{Row: 0, Col: 0, ValueType: "NUMBER", NumberValue: 2, Formula: "=1+1"}}
				}
				if start == 200 {
					cells = []SheetCell{{Row: 0, Col: 0, ValueType: "STRING", StringValue: "SHEET-NATIVE-END-7391"}}
				}
				if fault == "changed" && verify && start == 100 {
					cells[0].NumberValue = 9
				}
				if fault == "unknown-cell" && start == 0 {
					cells = append(cells, SheetCell{Row: 1, Col: 1, ValueType: "IMAGE"})
				}
				return nativeReply(t, sheetCellsResponse{Cells: cells}), nil
			}
			snapshot, err := CollectNative(context.Background(), "file", "sheet", read)
			if fault != "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, snapshot.CoverageComplete)
			require.Equal(t, 6, calls, "three captured ranges and three verification ranges")
			normalized, err := NormalizeNativeSnapshot(snapshot)
			require.NoError(t, err)
			require.Contains(t, normalized.Markdown, "| 1 | 0 | false |")
			require.Contains(t, normalized.Markdown, "| 101 | 2 [formula: =1+1] |")
			require.Contains(t, normalized.Markdown, "| 201 | SHEET-NATIVE-END-7391 |")
			require.Contains(t, normalized.Markdown, "Empty worksheet")
			revision, err := ProbeNativeRevision(context.Background(), "file", "sheet", read)
			require.NoError(t, err)
			require.Equal(t, snapshot.RevisionKey, revision)
		})
	}
}
