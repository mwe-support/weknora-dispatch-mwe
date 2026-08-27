package tencentdocs

import (
	"context"
	"reflect"
	"testing"

	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
)

func TestSheetMethodsCallRangeToolsWithExactArguments(t *testing.T) {
	var calls []string
	transport := &fakeMCPClient{}
	transport.callTool = func(
		name string,
		args map[string]interface{},
	) (*internalmcp.CallToolResult, error) {
		calls = append(calls, name)
		switch name {
		case "sheet.get_sheet_info":
			if !reflect.DeepEqual(args, map[string]interface{}{"file_id": "sheet-1"}) {
				t.Fatalf("sheet info args=%#v", args)
			}
			return toolJSON(t, map[string]interface{}{
				"sheets": []map[string]interface{}{{
					"sheet_id": "000001", "sheet_name": "Sheet1",
					"sheet_type": "worksheet", "row_count": 191, "col_count": 26,
				}},
			}), nil
		case "sheet.get_cell_data":
			want := map[string]interface{}{
				"file_id": "sheet-1", "sheet_id": "000001",
				"start_row": 100, "end_row": 190,
				"start_col": 0, "end_col": 25,
				"return_csv": false,
			}
			if !reflect.DeepEqual(args, want) {
				t.Fatalf("sheet cells args=%#v, want %#v", args, want)
			}
			return toolJSON(t, map[string]interface{}{
				"cells": []map[string]interface{}{{
					"row": 118, "col": 1, "value_type": "STRING",
					"string_value": "TARGET-SUPPLIER-119",
				}},
			}), nil
		default:
			t.Fatalf("unexpected tool %q", name)
			return nil, nil
		}
	}

	client := newTestClient(t, transport)
	sheets, err := client.GetSheetInfo(context.Background(), "sheet-1")
	if err != nil {
		t.Fatalf("GetSheetInfo() error: %v", err)
	}
	if len(sheets) != 1 || sheets[0].RowCount != 191 || sheets[0].ColCount != 26 {
		t.Fatalf("GetSheetInfo()=%+v", sheets)
	}

	cells, err := client.GetSheetCells(
		context.Background(), "sheet-1", "000001",
		100, 190, 0, 25,
	)
	if err != nil {
		t.Fatalf("GetSheetCells() error: %v", err)
	}
	if len(cells) != 1 || cells[0].Row != 118 ||
		cells[0].StringValue != "TARGET-SUPPLIER-119" {
		t.Fatalf("GetSheetCells()=%+v", cells)
	}
	if !reflect.DeepEqual(calls, []string{"sheet.get_sheet_info", "sheet.get_cell_data"}) {
		t.Fatalf("calls=%v", calls)
	}
}
