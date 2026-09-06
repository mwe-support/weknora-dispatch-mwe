package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestFAQSourceFormats(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       int
		bad        string
	}{
		{"faq.csv", "标签(必填),问题(必填),机器人回答(必填),是否停用\n服务,如何使用,答案一##答案二,FALSE\n", 1, ""},
		{"faq.tsv", "question\tanswers\n123\tfirst, second; keep punctuation\n", 1, ""},
		{"faq.json", `[{"id":42,"standard_question":"Q","answers":["A"],"is_recommended":false}]`, 1, ""},
		{"faq.md", "| question | answers |\n| --- | --- |\n| 123 | a\\|b |\n", 1, ""},
		{"faq.csv", "question,answers\ngood,yes\nbad,\n", 0, "row 3"},
		{"faq.csv", "question,answers,是否停用\nQ,A,maybe\n", 0, "invalid boolean"},
		{"faq.csv", "question,question,answers\nQ,Q,A\n", 0, "duplicate column"},
		{"faq.csv", "问题,question,answers\nQ,Q,A\n", 0, "duplicate column"},
		{"faq.csv", "question,answers\nQ,A,extra\n", 0, "more cells"},
		{"faq.json", `[{"standard_question":"Q","answers":[]}]`, 0, "entry 1"},
		{"faq.pdf", "%PDF-1.7", 0, "unsupported FAQ file type"},
		{"faq.xls", "binary", 0, "legacy XLS"},
		{"faq.md", "An ordinary document is not a FAQ template", 0, "no valid"},
		{"faq.md", "|", 0, "malformed FAQ table row"},
		{"faq.md", "| question | answers |\n|---|---|\n|Q|A", 0, "malformed FAQ table row"},
		{"faq.md", "| question | answers |\n|---|---|\n|Q|A|\n\n|name|cost|\n|---|---|\n|x|1|", 0, "FAQ headers"},
	} {
		t.Run(tc.name+tc.bad, func(t *testing.T) {
			entries, err := parseFAQFetchedItem(&types.FetchedItem{FileName: tc.name, Content: []byte(tc.body)})
			if tc.bad != "" {
				require.ErrorContains(t, err, tc.bad)
				require.Nil(t, entries)
			} else {
				require.NoError(t, err)
				require.Len(t, entries, tc.want)
			}
		})
	}
}

func TestFAQSourceSheetRowNumbersAndAllSheets(t *testing.T) {
	item := &types.FetchedItem{FileName: "FAQ.md", Metadata: map[string]string{"sheet_export_mode": "cell_ranges"}, Content: []byte("## Sheet: Main\n\n| Source row | A | B |\n|---|---|---|\n| 3 | question | answers |\n| 119 | Q | A<br>line |\n\n## Sheet: Other\n\n| Source row | A | B |\n|---|---|---|\n| 1 | question | answers |\n| 201 | Broken | |\n")}
	entries, err := parseFAQFetchedItem(item)
	require.Nil(t, entries)
	require.ErrorContains(t, err, "Other row 201")
	item.Content = []byte(strings.ReplaceAll(string(item.Content), "| 201 | Broken | |", "| 201 | Fixed | Answer |"))
	entries, err = parseFAQFetchedItem(item)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "A\nline", entries[0].Answers[0])
}

func TestFAQSourceXLSX(t *testing.T) {
	book := excelize.NewFile()
	defer book.Close()
	require.NoError(t, book.SetSheetRow("Sheet1", "A1", &[]string{"问题", "机器人回答"}))
	require.NoError(t, book.SetSheetRow("Sheet1", "A2", &[]string{"Q", "A"}))
	_, err := book.NewSheet("Second")
	require.NoError(t, err)
	require.NoError(t, book.SetSheetRow("Second", "A1", &[]string{"question", "answers"}))
	require.NoError(t, book.SetSheetRow("Second", "A2", &[]string{"Q2", "A2"}))
	buf, err := book.WriteToBuffer()
	require.NoError(t, err)
	entries, err := parseFAQFetchedItem(&types.FetchedItem{FileName: "FAQ.xlsx", Content: buf.Bytes()})
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

type faqSourceKnowledge struct {
	interfaces.KnowledgeService
	calls    int
	progress *types.FAQImportProgress
	err      error
}

func (s *faqSourceKnowledge) UpsertFAQEntries(_ context.Context, _ string, p *types.FAQBatchUpsertPayload) (string, error) {
	if !p.Synchronous || p.Mode != types.FAQBatchModeAppend {
		panic("must wait for official append import")
	}
	s.calls++
	return "test-task", s.err
}
func (s *faqSourceKnowledge) GetFAQImportProgress(context.Context, string) (*types.FAQImportProgress, error) {
	return s.progress, nil
}

func TestFAQSourceStreamRejectsInvalidAndPartialImports(t *testing.T) {
	k := &faqSourceKnowledge{progress: &types.FAQImportProgress{Status: types.FAQImportStatusCompleted}}
	r := &types.SyncResult{}
	h := &streamSyncHandler{faq: true, svc: &DataSourceService{knowledgeService: k}, ds: &types.DataSource{ID: "ds", Type: types.ConnectorTypeTencentDocs}, result: r}
	bad := types.FetchedItem{ExternalID: "bad", FileName: "bad.csv", Title: "bad", Content: []byte("question,answers\nQ,\n"), Metadata: map[string]string{"file_id": "f", "source_path": "Folder/bad.csv"}}
	require.NoError(t, h.Emit(context.Background(), bad))
	require.True(t, h.ItemRejected("bad"))
	require.Zero(t, k.calls)
	require.Equal(t, "FAQ_FORMAT_INVALID", r.Errors[0].Category)
	require.Equal(t, "Folder/bad.csv", r.Errors[0].SourcePath)
	good := types.FetchedItem{ExternalID: "good", FileName: "good.csv", Content: []byte("question,answers\nQ,A\n")}
	require.NoError(t, h.Emit(context.Background(), good))
	require.False(t, h.ItemRejected("good"))
	require.Equal(t, 1, r.Updated)
	require.Contains(t, r.FAQCompleted, "good")
	require.NotContains(t, r.FAQCompleted, "bad")
	k.progress.PartialFailedCount = 1
	good.ExternalID = "partial"
	require.NoError(t, h.Emit(context.Background(), good))
	require.True(t, h.ItemRejected("partial"))
	k.err = errors.New("embedding unavailable")
	good.ExternalID = "embedding"
	require.NoError(t, h.Emit(context.Background(), good))
	require.True(t, h.ItemRejected("embedding"))
	require.Equal(t, 3, r.Failed)
	require.NoError(t, validateFAQConnector("faq", "tencent_docs"))
	require.Error(t, validateFAQConnector("faq", "notion"))
	require.NoError(t, validateFAQConnector("document", "notion"))
}
