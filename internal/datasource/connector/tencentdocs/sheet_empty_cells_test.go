package tencentdocs

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// Return captured pages verbatim: the old fake filters out invalid coordinates,
// hiding the zero-value entries returned by Tencent's real Sheet endpoint.
type recordedSheetClient struct {
	sheets []SheetInfo
	pages  map[string][]SheetCell
	calls  []recordedSheetRange
}

type recordedSheetRange struct{ startRow, endRow, startCol, endCol int }

func (c *recordedSheetClient) GetSheetInfo(context.Context, string) ([]SheetInfo, error) {
	return c.sheets, nil
}

func (c *recordedSheetClient) GetSheetCells(_ context.Context, _, sheet string, start, end, colStart, colEnd int) ([]SheetCell, error) {
	c.calls = append(c.calls, recordedSheetRange{start, end, colStart, colEnd})
	key := fmt.Sprintf("%s/%d", sheet, start)
	cells, ok := c.pages[key]
	if !ok {
		return nil, fmt.Errorf("unrecorded page %s", key)
	}
	return append([]SheetCell(nil), cells...), nil
}

func TestSheetEmptyPlaceholderRegression(t *testing.T) {
	for _, rows := range []int{100, 191} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			client := &recordedSheetClient{
				sheets: []SheetInfo{{ID: "s", Name: "S", RowCount: rows, ColCount: 2}},
				pages: map[string][]SheetCell{
					"s/0":   {{Row: 0, Col: 0, ValueType: "STRING", StringValue: "HEADER"}, {}},
					"s/100": {{Row: 118, Col: 1, ValueType: "STRING", StringValue: "ROW119"}, {}},
				},
			}
			result, err := fetchSheetMarkdown(context.Background(), client, "fixture")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(result.Text, "HEADER") {
				t.Fatal("empty placeholder overwrote A1")
			}
			if rows > 100 && !strings.Contains(result.Text, "| 119 |  | ROW119 |") {
				t.Fatal("second-page source row lost")
			}
		})
	}
}

func TestSheetEmptyPlaceholderOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(20260904))
	for i := 0; i < 100; i++ {
		cells := []SheetCell{{}, {}, {},
			{Row: 0, ValueType: "STRING", StringValue: "HEADER"},
			{Row: 1, ValueType: "NUMBER"},
			{Row: 2, ValueType: "BOOL"},
		}
		rng.Shuffle(len(cells), func(i, j int) { cells[i], cells[j] = cells[j], cells[i] })
		client := &recordedSheetClient{
			sheets: []SheetInfo{{ID: "s", Name: "S", RowCount: 100, ColCount: 1}},
			pages:  map[string][]SheetCell{"s/0": cells},
		}
		result, err := fetchSheetMarkdown(context.Background(), client, "fixture")
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"| 1 | HEADER |", "| 2 | 0 |", "| 3 | false |"} {
			if !strings.Contains(result.Text, want) {
				t.Fatalf("shuffle %d lost %q", i, want)
			}
		}
		if result.Metadata["sheet_empty_placeholder_count"] != "3" || result.Metadata["omitted_empty_row_count"] != "97" {
			t.Fatalf("shuffle %d incorrect exclusion metrics", i)
		}
	}
}

func TestSheetAblation(t *testing.T) {
	var fixtures []struct {
		Name   string
		Sheets []struct {
			SheetInfo
			ExpectedRows []int `json:"expected_rows"`
		}
		Pages []struct {
			SheetID string `json:"sheet_id"`
			Start   int
			Cells   []SheetCell
		}
	}
	data, err := os.ReadFile("testdata/sheet_empty_ablation.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		t.Run(f.Name, func(t *testing.T) {
			client := &recordedSheetClient{pages: map[string][]SheetCell{}}
			wantRows := map[string][]int{}
			wantCells := map[string][]SheetCell{}
			for _, s := range f.Sheets {
				client.sheets = append(client.sheets, s.SheetInfo)
				wantRows[s.Name] = s.ExpectedRows
			}
			for _, p := range f.Pages {
				client.pages[fmt.Sprintf("%s/%d", p.SheetID, p.Start)] = p.Cells
				for _, s := range f.Sheets {
					if s.ID == p.SheetID {
						wantCells[s.Name] = append(wantCells[s.Name], p.Cells...)
					}
				}
			}
			checkSheetAblation(t, client, wantRows, wantCells, false)
		})
	}
	for _, scenario := range []string{"typed_values", "sparse_tail", "relative", "true_row", "negative_row", "true_col", "typed_blank", "typed_zero", "typed_false", "untyped_value", "negative_blank"} {
		t.Run(scenario, func(t *testing.T) {
			client := &recordedSheetClient{
				sheets: []SheetInfo{{ID: "s", Name: "S", RowCount: 191, ColCount: 2}},
				pages: map[string][]SheetCell{
					"s/0":   {{Row: 0, ValueType: "STRING", StringValue: "HEADER"}, {}},
					"s/100": {{Row: 118, ValueType: "STRING", StringValue: "TAIL"}, {}},
				},
			}
			wantRows := map[string][]int{"S": {1, 119}}
			wantCells := map[string][]SheetCell{"S": {client.pages["s/0"][0], client.pages["s/100"][0]}}
			wantError := false
			switch scenario {
			case "typed_values":
				more := []SheetCell{
					{Row: 1, ValueType: "NUMBER", NumberValue: 0},
					{Row: 2, ValueType: "BOOL", BoolValue: false},
					{Row: 3, ValueType: "FORMULA", Formula: "=0"},
					{Row: 4, ValueType: "ERROR", StringValue: "#DIV/0!"},
					{Row: 5, ValueType: "STRING", StringValue: " \t "},
					{Row: 6, StringValue: "LEGACY-TEXT"},
					{Row: 7, ValueType: "STRING", StringValue: "0"},
					{Row: 8, ValueType: "STRING", StringValue: "false"},
					{Row: 9, ValueType: "RICH_STRING", StringValue: "RICH"},
					{Row: 10, ValueType: "TIME_STRING", StringValue: "12:00"},
				}
				client.pages["s/0"] = append(client.pages["s/0"], more...)
				wantCells["S"] = append(wantCells["S"], more...)
				wantRows["S"] = []int{1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 119}
			case "sparse_tail":
				client.sheets[0].RowCount = 301
				client.pages["s/100"] = []SheetCell{{}, {}}
				client.pages["s/200"] = nil
				client.pages["s/300"] = []SheetCell{{Row: 300, ValueType: "STRING", StringValue: "TAIL"}, {}}
				wantRows["S"] = []int{1, 301}
				wantCells["S"][1].Row = 300
			case "relative":
				client.pages["s/100"][0].Row = 18
			default:
				wantError = true
				bad := SheetCell{Row: 99, ValueType: "STRING", StringValue: "MUST-NOT-BE-DROPPED"}
				switch scenario {
				case "negative_row":
					bad.Row = -1
				case "true_col":
					bad.Row, bad.Col = 118, 2
				case "typed_blank":
					bad = SheetCell{ValueType: "STRING"}
				case "typed_zero":
					bad = SheetCell{ValueType: "NUMBER"}
				case "typed_false":
					bad = SheetCell{ValueType: "BOOL"}
				case "untyped_value":
					bad = SheetCell{StringValue: "MUST-NOT-BE-DROPPED"}
				case "negative_blank":
					bad = SheetCell{Row: -1}
				}
				client.pages["s/100"] = append(client.pages["s/100"], bad)
			}
			checkSheetAblation(t, client, wantRows, wantCells, wantError)
		})
	}
}

func checkSheetAblation(t *testing.T, client *recordedSheetClient, wantRows map[string][]int, wantCells map[string][]SheetCell, wantError bool) {
	t.Helper()
	result, err := fetchSheetMarkdown(context.Background(), client, "fixture")
	report := map[string]interface{}{"case": t.Name(), "calls": len(client.calls), "expect_rejection": wantError}
	defer func() {
		report["pass"] = !t.Failed()
		data, _ := json.Marshal(report)
		t.Log("ABLATION_JSON " + string(data))
	}()
	if wantError {
		if err == nil || !strings.Contains(err.Error(), "out-of-range") {
			t.Errorf("expected real coordinate violation, got %v", err)
		}
		return
	}
	if err != nil {
		report["error"] = err.Error()
		t.Fatal(err)
	}
	actual := map[string]map[int][]string{}
	actualRows := map[string][]int{}
	name := ""
	rowCount := 0
	for _, line := range strings.Split(result.Text, "\n") {
		if strings.HasPrefix(line, "## Sheet: ") {
			name = strings.TrimPrefix(line, "## Sheet: ")
			actual[name] = map[int][]string{}
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) < 3 {
			continue
		}
		row, e := strconv.Atoi(strings.TrimSpace(fields[1]))
		if e != nil {
			continue
		}
		rowCount++
		actualRows[name] = append(actualRows[name], row)
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
		actual[name][row] = fields[2 : len(fields)-1]
	}
	report["output_rows"] = rowCount
	report["metadata"] = result.Metadata
	for name, rows := range wantRows {
		if !reflect.DeepEqual(actualRows[name], rows) {
			t.Errorf("%s source row coverage differs: got %d rows, want %d", name, len(actualRows[name]), len(rows))
		}
		for _, cell := range wantCells[name] {
			want := strings.TrimSpace(sheetCellText(cell))
			if want == "" {
				continue
			}
			row := actual[name][cell.Row+1]
			if cell.Col >= len(row) || row[cell.Col] != want {
				t.Errorf("%s cell (%d,%d) lost or changed", name, cell.Row, cell.Col)
				break
			}
		}
	}
	wantCalls, scanned := 0, 0
	for _, s := range client.sheets {
		wantCalls += len(planSheetRowRanges(s.RowCount, s.ColCount))
		scanned += s.RowCount
	}
	if len(client.calls) != wantCalls || result.Metadata["scanned_row_count"] != strconv.Itoa(scanned) {
		t.Error("did not scan every declared page")
	}
	entries, placeholders, nonEmpty := 0, 0, 0
	for _, cells := range client.pages {
		entries += len(cells)
		for _, cell := range cells {
			if cell == (SheetCell{}) {
				placeholders++
			}
		}
	}
	for _, rows := range wantRows {
		nonEmpty += len(rows)
	}
	for key, want := range map[string]int{
		"sheet_cell_entry_count":        entries,
		"sheet_empty_placeholder_count": placeholders,
		"omitted_empty_row_count":       scanned - nonEmpty,
	} {
		if result.Metadata[key] != strconv.Itoa(want) {
			t.Errorf("%s=%s, want %d", key, result.Metadata[key], want)
		}
	}
}
