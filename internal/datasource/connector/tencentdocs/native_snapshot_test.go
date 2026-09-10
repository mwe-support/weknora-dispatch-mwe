package tencentdocs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func nativeReply(t *testing.T, value interface{}) *NativeResponse {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return &NativeResponse{Data: data, Bytes: int64(len(data))}
}
func nativeMetadata(t *testing.T, kind string) *NativeResponse {
	return nativeReply(t, map[string]interface{}{"file_id": "canonical", "type": kind, "status": "normal", "last_modify_time": "7391", "title": "synthetic"})
}

func TestNativeSnapshotDOCFullPaginationNeverCertifiesPreviewAsBody(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		result, err := CollectNative(context.Background(), "locator", "doc", func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
			if tool == toolQueryFileInfo {
				return nativeMetadata(t, "doc"), nil
			}
			require.Equal(t, "canonical", args["file_id"])
			start, limit := args["offset"].(int), args["limit"].(int)
			end := min(start+limit, 151)
			nodes := make([]map[string]interface{}, 0, end-start)
			for i := start; i < end; i++ {
				nodes = append(nodes, map[string]interface{}{"paragraph_index": i + 1, "type": "Paragraph", "text_preview": strings.Repeat("P", 100)})
			}
			return nativeReply(t, map[string]interface{}{"nodes": nodes, "version": "1", "pagination": map[string]interface{}{"total_nodes": 151, "returned_nodes": len(nodes), "has_more": end < 151 && !truncate}}), nil
		})
		if truncate {
			require.ErrorContains(t, err, "TRUNCATED")
			continue
		}
		require.NoError(t, err)
		require.True(t, result.CoverageComplete)
		require.True(t, result.RequiresExport)
		require.Equal(t, 151, result.Units)
		require.Equal(t, "locator", result.LocatorID)
		require.Equal(t, "canonical", result.FileID)
	}
}

func TestNativeSnapshotMDXAllPagesAndNodeCoverage(t *testing.T) {
	for _, fault := range []string{"", "missing", "duplicate", "cursor", "changed"} {
		t.Run(fault, func(t *testing.T) {
			result, err := CollectNative(context.Background(), "file", "smartcanvas", func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
				if tool == toolQueryFileInfo {
					return nativeMetadata(t, "smartcanvas"), nil
				}
				if tool == "smartcanvas.get_top_level_pages" {
					children := []string{}
					for i := 0; i < 21; i++ {
						children = append(children, fmt.Sprint("block", i))
					}
					version := 1
					if verify && fault == "changed" {
						version = 2
					}
					return nativeReply(t, map[string]interface{}{"top_level_pages": []interface{}{
						map[string]interface{}{"id": "page1", "children": children, "version": version},
						map[string]interface{}{"id": "page2", "children": []string{"last"}, "version": 1},
					}}), nil
				}
				content, next := "", ""
				if args["page_id"] == "page2" {
					content = `<Paragraph id="last">Last page</Paragraph>`
				} else if args["next_token"] == "" {
					for i := 0; i < 20; i++ {
						content += fmt.Sprintf(`<Paragraph id="block%d">text</Paragraph>`, i)
					}
					next = "next"
				} else {
					content = `<Image id="block20" src="https://docs.qq.com/synthetic.png" />`
					if fault == "missing" {
						content = ""
					}
					if fault == "duplicate" {
						content += `<Paragraph id="block1">duplicate</Paragraph>`
					}
					if fault == "cursor" {
						next = "next"
					}
				}
				return nativeReply(t, map[string]interface{}{"content": content, "next_token": next}), nil
			})
			if fault != "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 22, result.Units)
			require.True(t, result.CoverageComplete)
		})
	}
}

func TestNativeSnapshotSmartSheetPaginatesAllTablesFieldsAndComputedRows(t *testing.T) {
	result, err := CollectNative(context.Background(), "file", "smartsheet", func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
		if tool == toolQueryFileInfo {
			return nativeMetadata(t, "smartsheet"), nil
		}
		if tool == "smartsheet.list_tables" {
			return nativeReply(t, map[string]interface{}{"sheets": []interface{}{
				map[string]interface{}{"sheet_id": "first", "title": "first"}, map[string]interface{}{"sheet_id": "second", "title": "second"},
			}}), nil
		}
		start, limit := args["offset"].(int), args["limit"].(int)
		total, key, idKey := 101, "fields", "field_id"
		if tool == "smartsheet.list_records" {
			total, key, idKey = 135, "records", "record_id"
			require.Equal(t, true, args["include_computed_values"])
		}
		if args["sheet_id"] == "second" {
			total = 1
		}
		end := min(start+min(limit, 60), total) // Server may cap a requested 100 to fewer entries.
		items := []map[string]interface{}{}
		for i := start; i < end; i++ {
			items = append(items, map[string]interface{}{idKey: fmt.Sprint(key, i), "number_value": 0, "bool_value": false})
		}
		return nativeReply(t, map[string]interface{}{key: items, "total": total, "next": end, "has_more": end < total}), nil
	})
	require.NoError(t, err)
	require.Equal(t, 136, result.Units)
	require.True(t, result.CoverageComplete)
}

func TestNativeSnapshotCumulativeLimitCountsRawResponses(t *testing.T) {
	c := nativeCollector{read: func(context.Context, string, map[string]interface{}, bool) (*NativeResponse, error) {
		return &NativeResponse{Data: json.RawMessage(`{}`), Bytes: maxNativeResponseBytes}, nil
	}}
	var out map[string]interface{}
	for i := 0; i < 6; i++ {
		require.NoError(t, c.call(context.Background(), "synthetic", map[string]interface{}{}, false, &out))
	}
	err := c.call(context.Background(), "synthetic", map[string]interface{}{}, false, &out)
	var limit *NativeLimitError
	require.ErrorAs(t, err, &limit)
	require.Nil(t, limit.ActualBytes)
	require.EqualValues(t, 112<<20, limit.ObservedAtLeastBytes)
}
