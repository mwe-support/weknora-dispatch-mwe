package tencentdocs

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

type sheetRangeCall struct {
	startRow int
	endRow   int
	startCol int
	endCol   int
}

type fakeSheetConnectorClient struct {
	*fakeConnectorClient
	sheets       map[string][]SheetInfo
	cells        map[string][]SheetCell
	calls        []sheetRangeCall
	relativeRows bool
}

func (f *fakeSheetConnectorClient) GetSheetInfo(_ context.Context, fileID string) ([]SheetInfo, error) {
	return append([]SheetInfo(nil), f.sheets[fileID]...), nil
}

func (f *fakeSheetConnectorClient) GetSheetCells(
	_ context.Context,
	_, sheetID string,
	startRow, endRow, startCol, endCol int,
) ([]SheetCell, error) {
	f.calls = append(f.calls, sheetRangeCall{
		startRow: startRow, endRow: endRow, startCol: startCol, endCol: endCol,
	})
	var result []SheetCell
	for _, cell := range f.cells[sheetID] {
		if cell.Row >= startRow && cell.Row <= endRow &&
			cell.Col >= startCol && cell.Col <= endCol {
			if f.relativeRows {
				cell.Row -= startRow
			}
			result = append(result, cell)
		}
	}
	return result, nil
}

func TestLegacySheetRendererAcceptsPageRelativeSheetRows(t *testing.T) {
	const fileID = "relative-sheet"
	base := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {{
			ID: fileID, Title: "Relative", Type: "wiki_file", DocumentType: "sheet",
		}}},
		infos: map[string]*FileInfo{fileID: {
			ID: fileID, Title: "Relative", Type: "sheet", ModifiedAt: 400,
		}},
	}
	client := &fakeSheetConnectorClient{
		fakeConnectorClient: base,
		relativeRows:        true,
		sheets: map[string][]SheetInfo{fileID: {{
			ID: "sheet-1", Name: "Relative", RowCount: 191, ColCount: 2,
		}}},
		cells: map[string][]SheetCell{"sheet-1": {
			{Row: 0, Col: 0, StringValue: "HEADER"},
			{Row: 118, Col: 0, StringValue: "ROW-119"},
		}},
	}

	items, err := legacySheetItems(context.Background(), client)
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	if len(items) != 1 || !strings.Contains(string(items[0].Content), "| 119 | ROW-119 |") {
		t.Fatalf("items = %+v, want page-relative row normalized to source row 119", items)
	}
}

func TestLegacySheetRendererReadsEverySheetRowBeyondGenericContentLimit(t *testing.T) {
	base := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {{
			ID: "PMJIUYDBGCPD", Title: "供应商列表 2026",
			Type: "wiki_file", DocumentType: "excel",
		}}},
		infos: map[string]*FileInfo{"PMJIUYDBGCPD": {
			ID: "PMJIUYDBGCPD", Title: "供应商列表 2026",
			Type: "tencentsheet", ModifiedAt: 1787746208068,
		}},
		contents: map[string]*DocumentContent{"PMJIUYDBGCPD": {
			Text: "通用内容只有前100行",
		}},
	}
	client := &fakeSheetConnectorClient{
		fakeConnectorClient: base,
		sheets: map[string][]SheetInfo{"PMJIUYDBGCPD": {{
			ID: "000001", Name: "Sheet1", Type: "worksheet",
			RowCount: 191, ColCount: 26,
		}}},
		cells: map[string][]SheetCell{"000001": {
			{Row: 0, Col: 0, ValueType: "STRING", StringValue: "序号"},
			{Row: 0, Col: 1, ValueType: "STRING", StringValue: "供应商名称"},
			{Row: 118, Col: 0, ValueType: "NUMBER", NumberValue: 119},
			{Row: 118, Col: 1, ValueType: "STRING", StringValue: "TARGET-SUPPLIER-119"},
			{Row: 190, Col: 0, ValueType: "NUMBER", NumberValue: 191},
			{Row: 190, Col: 1, ValueType: "STRING", StringValue: "LAST-SUPPLIER"},
		}},
	}

	items, err := legacySheetItems(context.Background(), client)
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("FetchAll() items=%d, want 1", len(items))
	}
	content := string(items[0].Content)
	for _, expected := range []string{
		"TARGET-SUPPLIER-119",
		"LAST-SUPPLIER",
		"| 119 |",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("sheet content missing %q:\n%s", expected, content)
		}
	}
	if base.contentGets != 0 {
		t.Fatalf("generic GetContent calls=%d, want 0 for Sheet", base.contentGets)
	}
	wantCalls := []sheetRangeCall{
		{startRow: 0, endRow: 99, startCol: 0, endCol: 25},
		{startRow: 100, endRow: 190, startCol: 0, endCol: 25},
	}
	if !reflect.DeepEqual(client.calls, wantCalls) {
		t.Fatalf("sheet calls=%+v, want %+v", client.calls, wantCalls)
	}
	if got := items[0].Metadata["source_row_count"]; got != strconv.Itoa(191) {
		t.Fatalf("source_row_count=%q, want 191", got)
	}
	if got := items[0].Metadata["exported_row_count"]; got != strconv.Itoa(191) {
		t.Fatalf("exported_row_count=%q, want 191", got)
	}
	for key, want := range map[string]string{
		"scanned_row_count":        "191",
		"sheet_range_call_count":   "2",
		"paginated_sheet_count":    "1",
		"single_range_sheet_count": "0",
	} {
		if got := items[0].Metadata[key]; got != want {
			t.Fatalf("%s=%q, want %q", key, got, want)
		}
	}
}

func TestLegacySheetRendererScansSparseTailAndReportsEmittedRows(t *testing.T) {
	const fileID = "sparse-sheet"
	base := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {{
			ID: fileID, Title: "Sparse", Type: "wiki_file", DocumentType: "sheet",
		}}},
		infos: map[string]*FileInfo{fileID: {
			ID: fileID, Title: "Sparse", Type: "sheet", ModifiedAt: 200,
		}},
	}
	client := &fakeSheetConnectorClient{
		fakeConnectorClient: base,
		sheets: map[string][]SheetInfo{fileID: {{
			ID: "sheet-1", Name: "Sparse", Type: "worksheet",
			RowCount: 570, ColCount: 26,
		}}},
		cells: map[string][]SheetCell{"sheet-1": {
			{Row: 0, Col: 0, ValueType: "STRING", StringValue: "HEADER"},
			{Row: 569, Col: 2, ValueType: "STRING", StringValue: "TAIL-570"},
		}},
	}

	items, err := legacySheetItems(context.Background(), client)
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("FetchAll() items=%d, want 1", len(items))
	}
	if len(client.calls) != 6 || client.calls[5].startRow != 500 || client.calls[5].endRow != 569 {
		t.Fatalf("sheet calls=%+v, want six calls ending at row 569", client.calls)
	}
	content := string(items[0].Content)
	if !strings.Contains(content, "TAIL-570") || !strings.Contains(content, "| 570 |") {
		t.Fatalf("sparse tail missing from content:\n%s", content)
	}
	for key, want := range map[string]string{
		"scanned_row_count":           "570",
		"emitted_non_empty_row_count": "2",
		"max_non_empty_source_row":    "570",
	} {
		if got := items[0].Metadata[key]; got != want {
			t.Fatalf("%s=%q, want %q", key, got, want)
		}
	}
}

func TestLegacySheetRendererOmitsEmptyWorksheetTablesAndUnusedColumns(t *testing.T) {
	const fileID = "trimmed-sheet"
	base := &fakeConnectorClient{
		nodes: map[string][]Node{"space-1/": {{
			ID: fileID, Title: "Trimmed", Type: "wiki_file", DocumentType: "sheet",
		}}},
		infos: map[string]*FileInfo{fileID: {
			ID: fileID, Title: "Trimmed", Type: "sheet", ModifiedAt: 300,
		}},
	}
	client := &fakeSheetConnectorClient{
		fakeConnectorClient: base,
		sheets: map[string][]SheetInfo{fileID: {
			{ID: "used", Name: "Used", Type: "worksheet", RowCount: 10, ColCount: 26},
			{ID: "empty", Name: "Empty", Type: "worksheet", RowCount: 200, ColCount: 26},
		}},
		cells: map[string][]SheetCell{
			"used": {
				{Row: 0, Col: 0, ValueType: "STRING", StringValue: "A1"},
				{Row: 5, Col: 2, ValueType: "STRING", StringValue: "C6"},
			},
		},
	}

	items, err := legacySheetItems(context.Background(), client)
	if err != nil {
		t.Fatalf("FetchAll() error: %v", err)
	}
	content := string(items[0].Content)
	if !strings.Contains(content, "| Source row | A | B | C |\n") {
		t.Fatalf("used-column header missing:\n%s", content)
	}
	if strings.Contains(content, "| Source row | A | B | C | D |") {
		t.Fatalf("unused columns were emitted:\n%s", content)
	}
	if strings.Count(content, "| Source row |") != 1 {
		t.Fatalf("table header count=%d, want 1:\n%s", strings.Count(content, "| Source row |"), content)
	}
	if !strings.Contains(content, "## Sheet: Empty\n\n_Empty worksheet_\n") {
		t.Fatalf("empty worksheet marker missing:\n%s", content)
	}
	for key, want := range map[string]string{
		"empty_sheet_count":     "1",
		"max_used_column_count": "3",
		"declared_column_count": "52",
		"emitted_column_count":  "3",
	} {
		if got := items[0].Metadata[key]; got != want {
			t.Fatalf("%s=%q, want %q", key, got, want)
		}
	}
}

// Historical native Sheet jobs still use this renderer. New source syncs
// export XLSX instead and are covered by TestSourceExportsFourTypesWithoutReadingBody.
func legacySheetItems(ctx context.Context, client *fakeSheetConnectorClient) ([]types.FetchedItem, error) {
	for id := range client.sheets {
		result, err := fetchSheetMarkdown(ctx, client, id)
		if err != nil {
			return nil, err
		}
		return []types.FetchedItem{{Content: []byte(result.Text), Metadata: result.Metadata}}, nil
	}
	return nil, nil
}
