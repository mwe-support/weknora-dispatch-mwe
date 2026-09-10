package service

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/xuri/excelize/v2"
)

func validateFAQConnector(kbType, connectorType string) error {
	if kbType == types.KnowledgeBaseTypeFAQ && connectorType != types.ConnectorTypeTencentDocs {
		return fmt.Errorf("%w: FAQ knowledge bases currently support only Tencent Docs data sources", datasource.ErrInvalidConfig)
	}
	return nil
}

type faqSourceGuardKey struct{}

// Synchronous source imports retain this guard across parsing and model calls.
// Manual/queued FAQ imports have no source scope and keep their existing path.
func checkFAQSourcePublication(ctx context.Context) error {
	if check, ok := ctx.Value(faqSourceGuardKey{}).(func(context.Context) error); ok {
		return check(ctx)
	}
	return nil
}

func (s *DataSourceService) applyFAQFetchedItem(ctx context.Context, ds *types.DataSource, item *types.FetchedItem, result *types.SyncResult) {
	ctx = context.WithValue(ctx, faqSourceGuardKey{}, func(ctx context.Context) error {
		return s.checkTencentCandidateScope(ctx, ds)
	})
	entries, err := parseFAQFetchedItem(item)
	stage, code := "faq_validate", "FAQ_FORMAT_INVALID"
	if err == nil {
		err = checkFAQSourcePublication(ctx)
	}
	if err == nil {
		stage, code = "faq_import", "FAQ_IMPORT_FAILED"
		var taskID string
		taskID, err = s.knowledgeService.UpsertFAQEntries(ctx, ds.KnowledgeBaseID, &types.FAQBatchUpsertPayload{
			Entries: entries, Mode: types.FAQBatchModeAppend, Synchronous: true,
		})
		if err == nil {
			var progress *types.FAQImportProgress
			progress, err = s.knowledgeService.GetFAQImportProgress(ctx, taskID)
			if err == nil && (progress == nil || progress.Status != types.FAQImportStatusCompleted) {
				err = fmt.Errorf("FAQ import did not complete (task_id=%s)", taskID)
			}
			if err == nil && (progress.FailedCount > 0 || progress.PartialFailedCount > 0) {
				err = fmt.Errorf("FAQ import task_id=%s: failed=%d, partial_failed=%d; inspect FAQ import results", taskID, progress.FailedCount, progress.PartialFailedCount)
				if len(progress.FailedEntries) > 0 {
					first := progress.FailedEntries[0]
					err = fmt.Errorf("%w; entry %d: %s", err, first.Index+1, first.Reason)
				}
			}
		}
	}
	if err == nil {
		err = checkFAQSourcePublication(ctx)
	}
	if err != nil {
		failure := syncItemIdentity(item)
		failure.Code, failure.Category, failure.Stage = strings.ToLower(code), code, stage
		failure.Message = err.Error()
		result.Failed++
		recordSyncError(result, failure)
		logger.Warnf(ctx, "FAQ source failed: ds=%s external_id=%s stage=%s category=%s reason=%s", ds.ID, item.ExternalID, stage, code, failure.Message)
		return
	}
	// Append uses the official standard-question merge semantics; never replace
	// the entire FAQ KB or delete manually maintained entries on source removal.
	// ponytail: KB-wide question merge; source-owned deletion needs provenance.
	if item.ExternalID != "" {
		if result.FAQCompleted == nil {
			result.FAQCompleted = map[string]time.Time{}
		}
		result.FAQCompleted[item.ExternalID] = time.Now().UTC()
	}
	result.Updated++
}

func parseFAQFetchedItem(item *types.FetchedItem) ([]types.FAQEntryPayload, error) {
	if item.Metadata["skip_reason"] == "unsupported_file_type" {
		return nil, fmt.Errorf("unsupported FAQ source file type %q; use FAQ-template CSV/TSV/XLSX/JSON or a Tencent Docs FAQ table", item.Metadata["file_extension"])
	}
	if len(item.Content) == 0 {
		return nil, fmt.Errorf("FAQ source has no fetched content")
	}
	var entries []types.FAQEntryPayload
	var err error
	switch strings.ToLower(filepath.Ext(item.FileName)) {
	case ".json":
		err = json.Unmarshal(bytes.TrimPrefix(item.Content, []byte("\xef\xbb\xbf")), &entries)
	case ".csv", ".tsv":
		r := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(item.Content, []byte("\xef\xbb\xbf"))))
		if strings.EqualFold(filepath.Ext(item.FileName), ".tsv") || (!bytes.Contains(bytes.SplitN(item.Content, []byte("\n"), 2)[0], []byte(",")) && bytes.Contains(item.Content, []byte("\t"))) {
			r.Comma = '\t'
		}
		r.FieldsPerRecord = -1
		var rows [][]string
		rows, err = r.ReadAll()
		if err == nil {
			entries, err = parseFAQRows(rows, item.FileName)
		}
	case ".xlsx":
		var book *excelize.File
		book, err = excelize.OpenReader(bytes.NewReader(item.Content), excelize.Options{UnzipSizeLimit: 64 << 20, UnzipXMLSizeLimit: 16 << 20})
		if err != nil {
			break
		}
		defer book.Close()
		for _, sheet := range book.GetSheetList() {
			var rows [][]string
			rows, err = book.GetRows(sheet)
			if err != nil {
				break
			}
			if len(rows) == 0 {
				continue
			}
			var part []types.FAQEntryPayload
			part, err = parseFAQRows(rows, sheet)
			if err != nil {
				break
			}
			entries = append(entries, part...)
		}
	case ".md":
		entries, err = parseFAQMarkdown(item)
	default:
		return nil, fmt.Errorf("unsupported FAQ file type %q; use FAQ-template CSV/TSV/XLSX/JSON or a Tencent Docs FAQ table (legacy XLS must be saved as XLSX)", filepath.Ext(item.FileName))
	}
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("FAQ source contains no valid FAQ entries")
	}
	for i := range entries {
		if err := validateFAQSourceEntry(&entries[i]); err != nil {
			return nil, fmt.Errorf("entry %d: %w", i+1, err)
		}
	}
	return entries, nil
}

func validateFAQSourceEntry(entry *types.FAQEntryPayload) error {
	entry.StandardQuestion = strings.TrimSpace(entry.StandardQuestion)
	// Match manual JSON import: internal IDs from an exported KB are not portable.
	entry.ID, entry.TagID = nil, 0
	if err := validateFAQEntryPayloadBasic(entry); err != nil {
		return err
	}
	if entry.AnswerStrategy != nil && *entry.AnswerStrategy != types.AnswerStrategyAll && *entry.AnswerStrategy != types.AnswerStrategyRandom {
		return fmt.Errorf("answer_strategy must be all or random")
	}
	return nil
}

var faqHeaderNote = regexp.MustCompile(`\([^)]*\)|（[^）]*）`)

func faqHeader(value string) string {
	value = strings.ToLower(strings.TrimSpace(faqHeaderNote.ReplaceAllString(value, "")))
	// DOC export preserves emphasis in table headings. Strip paired wrappers
	// from headings only; questions and answers keep their original formatting.
	for _, marker := range []string{"**", "__", "`", "*", "_"} {
		if len(value) > 2*len(marker) && strings.HasPrefix(value, marker) && strings.HasSuffix(value, marker) {
			value = strings.TrimSpace(value[len(marker) : len(value)-len(marker)])
		}
	}
	switch value {
	case "问题", "question":
		return "standard_question"
	case "机器人回答":
		return "answers"
	case "标签", "分类":
		return "tag_name"
	case "相似问题":
		return "similar_questions"
	case "反例问题":
		return "negative_questions"
	}
	return value
}
func faqNonEmpty(row []string) bool {
	for _, v := range row {
		if strings.TrimSpace(v) != "" {
			return true
		}
	}
	return false
}

func parseFAQRows(rows [][]string, source string, sourceRows ...[]int) ([]types.FAQEntryPayload, error) {
	var headers []string
	var entries []types.FAQEntryPayload
	for i, row := range rows {
		rowNumber := i + 1
		if len(sourceRows) > 0 && i < len(sourceRows[0]) {
			rowNumber = sourceRows[0][i]
		}
		if !faqNonEmpty(row) {
			continue
		}
		if headers == nil {
			headers = make([]string, len(row))
			seen := map[string]bool{}
			for j, v := range row {
				headers[j] = faqHeader(v)
				if headers[j] != "" && seen[headers[j]] {
					return nil, fmt.Errorf("%s row %d: duplicate column %s", source, rowNumber, headers[j])
				}
				seen[headers[j]] = true
			}
			if !(seen["问题"] || seen["standard_question"] || seen["question"]) || !(seen["机器人回答"] || seen["answers"]) {
				return nil, fmt.Errorf("%s row %d: FAQ headers require 问题/standard_question and 机器人回答/answers", source, rowNumber)
			}
			continue
		}
		entry, err := faqRow(headers, row)
		if err != nil {
			return nil, fmt.Errorf("%s row %d: %w", source, rowNumber, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func faqRow(headers, row []string) (types.FAQEntryPayload, error) {
	entry := types.FAQEntryPayload{}
	if len(row) > len(headers) && faqNonEmpty(row[len(headers):]) {
		return entry, fmt.Errorf("more cells than header columns")
	}
	record := map[string]string{}
	for i, h := range headers {
		if i < len(row) {
			record[h] = strings.TrimSpace(row[i])
		}
	}
	value := func(keys ...string) string {
		for _, k := range keys {
			if record[k] != "" {
				return record[k]
			}
		}
		return ""
	}
	split := func(v string) []string {
		var out []string
		for _, s := range strings.Split(v, "##") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	entry.StandardQuestion = value("问题", "standard_question", "question")
	entry.Answers = split(value("机器人回答", "answers"))
	entry.SimilarQuestions = split(value("相似问题", "similar_questions"))
	entry.NegativeQuestions = split(value("反例问题", "negative_questions"))
	entry.TagName = value("标签", "分类", "tag_name")
	for _, field := range []string{"是否停用", "是否禁止被推荐", "是否全部回复", "is_enabled", "is_recommended"} {
		v := record[field]
		if v == "" {
			continue
		}
		var b bool
		switch strings.ToLower(v) {
		case "true", "1", "是", "yes":
			b = true
		case "false", "0", "否", "no":
			b = false
		default:
			return entry, fmt.Errorf("column %s: invalid boolean (use TRUE/FALSE)", field)
		}
		switch field {
		case "是否停用":
			b = !b
			entry.IsEnabled = &b
		case "is_enabled":
			entry.IsEnabled = &b
		case "是否禁止被推荐":
			b = !b
			entry.IsRecommended = &b
		case "is_recommended":
			entry.IsRecommended = &b
		case "是否全部回复":
			strategy := types.AnswerStrategyRandom
			if b {
				strategy = types.AnswerStrategyAll
			}
			entry.AnswerStrategy = &strategy
		}
	}
	if v := record["answer_strategy"]; v != "" {
		strategy := types.AnswerStrategy(v)
		entry.AnswerStrategy = &strategy
	}
	return entry, validateFAQSourceEntry(&entry)
}

// Tencent Sheet exports have a synthetic Source row/A/B header, followed by
// the actual template header. Other Markdown tables use their own first row.
func parseFAQMarkdown(item *types.FetchedItem) ([]types.FAQEntryPayload, error) {
	var entries []types.FAQEntryPayload
	var rows [][]string
	var rowNumbers []int
	source := item.FileName
	sheetMode := item.Metadata["sheet_export_mode"] == "cell_ranges"
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		part, err := parseFAQRows(rows, source, rowNumbers)
		if err != nil {
			return err
		}
		entries = append(entries, part...)
		rows, rowNumbers = nil, nil
		return nil
	}
	for lineNo, line := range strings.Split(string(item.Content), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "|") && (len(line) < 2 || !strings.HasSuffix(line, "|")) {
			return nil, fmt.Errorf("%s line %d: malformed FAQ table row (missing closing pipe)", source, lineNo+1)
		}
		if strings.HasPrefix(line, "## Sheet: ") {
			if err := flush(); err != nil {
				return nil, err
			}
			source = strings.TrimPrefix(line, "## Sheet: ")
			continue
		}
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			if !sheetMode {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			continue
		}
		var row []string
		var cell strings.Builder
		body := line[1 : len(line)-1]
		for i := 0; i < len(body); i++ {
			if body[i] == '\\' && i+1 < len(body) && body[i+1] == '|' {
				cell.WriteByte('|')
				i++
				continue
			}
			if body[i] == '|' {
				row = append(row, strings.TrimSpace(cell.String()))
				cell.Reset()
			} else {
				cell.WriteByte(body[i])
			}
		}
		row = append(row, strings.TrimSpace(cell.String()))
		separator := true
		for _, c := range row {
			if strings.Trim(c, " :-") != "" {
				separator = false
			}
		}
		if separator {
			continue
		}
		if sheetMode {
			if row[0] == "Source row" {
				continue
			}
			n, err := strconv.Atoi(row[0])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("%s line %d: invalid source row number", source, lineNo+1)
			}
			rowNumbers = append(rowNumbers, n)
			row = row[1:]
		} else {
			rowNumbers = append(rowNumbers, lineNo+1)
		}
		for i := range row {
			row[i] = strings.ReplaceAll(row[i], "<br>", "\n")
		}
		rows = append(rows, row)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return entries, nil
}
