package tencentdocs

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeNormalizeRecordedSmartSheetKeepsZeroFalseAndComputedValues(t *testing.T) {
	data, err := os.ReadFile("testdata/lifecycle-native-pages.json")
	require.NoError(t, err)
	var fixture map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fixture))
	snapshot := NativeSnapshot{FileID: "synthetic", Kind: "smartsheet", CoverageComplete: true, Pages: []NativePage{
		{Tool: "smartsheet.list_tables", Data: json.RawMessage(`{"sheets":[{"sheet_id":"one","title":"合成工作表"}]}`)},
		{Tool: "smartsheet.list_fields", Args: map[string]interface{}{"sheet_id": "one"}, Data: fixture["smartSheetFields"]},
		{Tool: "smartsheet.list_records", Args: map[string]interface{}{"sheet_id": "one"}, Data: fixture["smartSheetRecords"]},
	}}
	// This fixture covers normalization of three real rows, not whole-document
	// collection. Full offset/total completeness is tested separately.
	normalized, err := NormalizeNativeSnapshot(&snapshot)
	require.NoError(t, err)
	require.Empty(t, normalized.Unsupported)
	require.Contains(t, normalized.Markdown, "SMART-ROW-1 | 0 |")
	require.Contains(t, normalized.Markdown, "false | 0 |")
	require.Contains(t, normalized.Markdown, "true | 4 |")
	require.Contains(t, normalized.Markdown, "false | 6 |")
}

func TestNativeNormalizeRejectsAmbiguousFieldsAndMissingComputedResult(t *testing.T) {
	for _, fields := range []string{
		`{"fields":[{"field_id":"a","field_title":"同名","field_type":"text"},{"field_id":"b","field_title":"同名","field_type":"text"}]}`,
		`{"fields":[{"field_id":"a","field_title":"同名","field_type":"formula"}]}`,
	} {
		snapshot := NativeSnapshot{Kind: "smartsheet", CoverageComplete: true, Pages: []NativePage{
			{Tool: "smartsheet.list_tables", Data: json.RawMessage(`{"sheets":[{"sheet_id":"one","title":"One"}]}`)},
			{Tool: "smartsheet.list_fields", Args: map[string]interface{}{"sheet_id": "one"}, Data: json.RawMessage(fields)},
			{Tool: "smartsheet.list_records", Args: map[string]interface{}{"sheet_id": "one"}, Data: json.RawMessage(`{"records":[{"record_id":"r","field_values":[{"field":"同名","string_value":"raw"}]}]}`)},
		}}
		_, err := NormalizeNativeSnapshot(&snapshot)
		require.Error(t, err)
	}
}

func TestNativeNormalizeImageReferenceMustResolveBeforeCompleteness(t *testing.T) {
	result := &NativeNormalized{}
	text, err := nativeSmartValue(map[string]json.RawMessage{"image_value": json.RawMessage(`{"items":[{"image_id":"opaque"}]}`)}, "row/field", result)
	require.NoError(t, err)
	require.NotEmpty(t, text)
	require.Len(t, result.Unsupported, 1)
	result = &NativeNormalized{}
	_, err = nativeSmartValue(map[string]json.RawMessage{"image_value": json.RawMessage(`{"items":[{"image_id":"https://docimg5.docs.qq.com/image/a.png"},{"image_id":"https://docimg5.docs.qq.com/image/b.png"}]}`)}, "row/field", result)
	require.NoError(t, err)
	require.Len(t, result.Assets, 2)
	require.Empty(t, result.Unsupported)
}
