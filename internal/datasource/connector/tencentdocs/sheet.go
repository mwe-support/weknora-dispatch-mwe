package tencentdocs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

const (
	maxSheetCellsPerRequest   = 20000
	preferredSheetRowsPerCall = 100
)

type sheetMarkdownResult struct {
	Text     string
	Metadata map[string]string
}

func isTencentDocsSheetType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "sheet", "excel", "tencentsheet":
		return true
	default:
		return false
	}
}

func fetchOnlineDocumentContent(
	ctx context.Context,
	client Client,
	fileID string,
	documentType string,
) (string, map[string]string, error) {
	if !isTencentDocsSheetType(documentType) {
		content, err := client.GetContent(ctx, fileID)
		if err != nil {
			return "", nil, err
		}
		return content.Text, nil, nil
	}

	sheetClient, ok := client.(SheetClient)
	if !ok {
		return "", nil, errorsNewSheetClientUnavailable()
	}
	result, err := fetchSheetMarkdown(ctx, sheetClient, fileID)
	if err != nil {
		return "", nil, err
	}
	return result.Text, result.Metadata, nil
}

func errorsNewSheetClientUnavailable() error {
	return fmt.Errorf("Tencent Docs Sheet range client is unavailable")
}

type sheetRowRange struct {
	start int
	end   int
}

type sheetOutputRow struct {
	sourceRow int
	cells     []string
}

func fetchSheetMarkdown(
	ctx context.Context,
	client SheetClient,
	fileID string,
) (*sheetMarkdownResult, error) {
	sheets, err := client.GetSheetInfo(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("get Sheet metadata: %w", err)
	}
	if len(sheets) == 0 {
		return nil, fmt.Errorf("get Sheet metadata: no worksheets returned")
	}

	var builder strings.Builder
	sourceRows := 0
	scannedRows := 0
	nonEmptyRows := 0
	maxNonEmptySourceRow := 0
	totalRangeCalls := 0
	paginatedSheets := 0
	singleRangeSheets := 0
	emptySheets := 0
	declaredColumns := 0
	emittedColumns := 0
	maxUsedColumns := 0

	for _, sheet := range sheets {
		if sheet.ID == "" {
			return nil, fmt.Errorf("get Sheet metadata: worksheet has empty ID")
		}
		if sheet.RowCount < 0 || sheet.ColCount < 0 {
			return nil, fmt.Errorf("get Sheet metadata: invalid dimensions for %q", sheet.Name)
		}
		sourceRows += sheet.RowCount
		declaredColumns += sheet.ColCount

		builder.WriteString("## Sheet: ")
		builder.WriteString(escapeMarkdownCell(firstNonEmpty(sheet.Name, sheet.ID)))
		builder.WriteString("\n\n")

		if sheet.RowCount == 0 || sheet.ColCount == 0 {
			// With zero columns every declared row is necessarily empty. Count
			// those rows as inspected so coverage metadata remains exact.
			scannedRows += sheet.RowCount
			emptySheets++
			builder.WriteString("_Empty worksheet_\n\n")
			continue
		}
		if sheet.ColCount > maxSheetCellsPerRequest {
			return nil, fmt.Errorf(
				"worksheet %q has %d columns, exceeding the per-request cell limit",
				sheet.Name, sheet.ColCount,
			)
		}

		ranges := planSheetRowRanges(sheet.RowCount, sheet.ColCount)
		totalRangeCalls += len(ranges)
		if len(ranges) > 1 {
			paginatedSheets++
		} else if len(ranges) == 1 {
			singleRangeSheets++
		}

		rowsToEmit := make([]sheetOutputRow, 0)
		sheetMaxUsedCol := -1
		for _, rowRange := range ranges {
			cells, err := client.GetSheetCells(
				ctx, fileID, sheet.ID,
				rowRange.start, rowRange.end, 0, sheet.ColCount-1,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"get Sheet cells %s rows %d-%d: %w",
					sheet.ID, rowRange.start+1, rowRange.end+1, err,
				)
			}

			rows := make([][]string, rowRange.end-rowRange.start+1)
			for i := range rows {
				rows[i] = make([]string, sheet.ColCount)
			}
			for _, cell := range cells {
				if cell.Row < rowRange.start || cell.Row > rowRange.end ||
					cell.Col < 0 || cell.Col >= sheet.ColCount {
					return nil, fmt.Errorf(
						"get Sheet cells %s returned out-of-range cell (%d,%d)",
						sheet.ID, cell.Row, cell.Col,
					)
				}
				rows[cell.Row-rowRange.start][cell.Col] = sheetCellText(cell)
			}

			for offset, row := range rows {
				scannedRows++
				lastUsedCol := lastNonEmptyColumn(row)
				if lastUsedCol < 0 {
					continue
				}
				sourceRow := rowRange.start + offset + 1
				nonEmptyRows++
				if sourceRow > maxNonEmptySourceRow {
					maxNonEmptySourceRow = sourceRow
				}
				if lastUsedCol > sheetMaxUsedCol {
					sheetMaxUsedCol = lastUsedCol
				}
				rowsToEmit = append(rowsToEmit, sheetOutputRow{
					sourceRow: sourceRow,
					cells:     append([]string(nil), row[:lastUsedCol+1]...),
				})
			}
		}

		if len(rowsToEmit) == 0 {
			emptySheets++
			builder.WriteString("_Empty worksheet_\n\n")
			continue
		}
		usedColumns := sheetMaxUsedCol + 1
		emittedColumns += usedColumns
		if usedColumns > maxUsedColumns {
			maxUsedColumns = usedColumns
		}
		writeSheetTableHeader(&builder, usedColumns)
		for _, row := range rowsToEmit {
			writeSheetTableRow(&builder, row.sourceRow, row.cells, usedColumns)
		}
		builder.WriteString("\n")
	}

	if scannedRows != sourceRows {
		return nil, fmt.Errorf(
			"Sheet scan incomplete: scanned %d of %d declared rows",
			scannedRows, sourceRows,
		)
	}
	return &sheetMarkdownResult{
		Text: builder.String(),
		Metadata: map[string]string{
			"sheet_export_mode":             "cell_ranges",
			"sheet_pagination_strategy":     "complete_declared_range",
			"source_sheet_count":            strconv.Itoa(len(sheets)),
			"source_row_count":              strconv.Itoa(sourceRows),
			"exported_row_count":            strconv.Itoa(scannedRows),
			"scanned_row_count":             strconv.Itoa(scannedRows),
			"non_empty_row_count":           strconv.Itoa(nonEmptyRows),
			"emitted_non_empty_row_count":   strconv.Itoa(nonEmptyRows),
			"max_non_empty_source_row":      strconv.Itoa(maxNonEmptySourceRow),
			"sheet_range_call_count":        strconv.Itoa(totalRangeCalls),
			"paginated_sheet_count":         strconv.Itoa(paginatedSheets),
			"single_range_sheet_count":      strconv.Itoa(singleRangeSheets),
			"empty_sheet_count":             strconv.Itoa(emptySheets),
			"declared_column_count":         strconv.Itoa(declaredColumns),
			"emitted_column_count":          strconv.Itoa(emittedColumns),
			"max_used_column_count":         strconv.Itoa(maxUsedColumns),
			"sheet_cells_per_limit":         strconv.Itoa(maxSheetCellsPerRequest),
			"preferred_sheet_rows_per_call": strconv.Itoa(preferredSheetRowsPerCall),
		},
	}, nil
}

func planSheetRowRanges(rowCount, colCount int) []sheetRowRange {
	if rowCount <= 0 || colCount <= 0 {
		return nil
	}
	rowsPerCall := maxSheetCellsPerRequest / colCount
	if rowsPerCall > preferredSheetRowsPerCall {
		rowsPerCall = preferredSheetRowsPerCall
	}
	if rowsPerCall < 1 {
		rowsPerCall = 1
	}
	ranges := make([]sheetRowRange, 0, (rowCount+rowsPerCall-1)/rowsPerCall)
	for start := 0; start < rowCount; start += rowsPerCall {
		end := start + rowsPerCall - 1
		if end >= rowCount {
			end = rowCount - 1
		}
		ranges = append(ranges, sheetRowRange{start: start, end: end})
	}
	return ranges
}

func writeSheetTableHeader(builder *strings.Builder, colCount int) {
	builder.WriteString("| Source row |")
	for col := 0; col < colCount; col++ {
		builder.WriteString(" ")
		builder.WriteString(sheetColumnName(col))
		builder.WriteString(" |")
	}
	builder.WriteString("\n| --- |")
	for col := 0; col < colCount; col++ {
		builder.WriteString(" --- |")
	}
	builder.WriteString("\n")
}

func writeSheetTableRow(builder *strings.Builder, sourceRow int, row []string, colCount int) {
	builder.WriteString("| ")
	builder.WriteString(strconv.Itoa(sourceRow))
	builder.WriteString(" |")
	for col := 0; col < colCount; col++ {
		value := ""
		if col < len(row) {
			value = row[col]
		}
		builder.WriteString(" ")
		builder.WriteString(escapeMarkdownCell(value))
		builder.WriteString(" |")
	}
	builder.WriteString("\n")
}

func lastNonEmptyColumn(row []string) int {
	for col := len(row) - 1; col >= 0; col-- {
		if strings.TrimSpace(row[col]) != "" {
			return col
		}
	}
	return -1
}
func sheetCellText(cell SheetCell) string {
	switch strings.ToUpper(cell.ValueType) {
	case "NUMBER":
		return strconv.FormatFloat(cell.NumberValue, 'f', -1, 64)
	case "BOOL":
		return strconv.FormatBool(cell.BoolValue)
	case "FORMULA":
		if cell.StringValue != "" {
			return cell.StringValue
		}
		return cell.Formula
	default:
		if cell.StringValue != "" {
			return cell.StringValue
		}
		return cell.Formula
	}
}

func escapeMarkdownCell(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	value = strings.ReplaceAll(value, "\n", "<br>")
	value = strings.ReplaceAll(value, "|", "\\|")
	return strings.TrimSpace(value)
}

func sheetColumnName(index int) string {
	if index < 0 {
		return ""
	}
	index++
	var name []byte
	for index > 0 {
		index--
		name = append([]byte{byte('A' + index%26)}, name...)
		index /= 26
	}
	return string(name)
}
