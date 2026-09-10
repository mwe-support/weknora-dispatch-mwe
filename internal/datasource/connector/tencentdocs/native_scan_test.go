package tencentdocs

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeScanPagesPreserveScopeAndDoNotFollowLinks(t *testing.T) {
	ctx := context.Background()
	rootID := encodeSpaceResourceID("synthetic-space")
	roots, err := NativeScanRoots([]string{rootID, rootID})
	require.NoError(t, err)
	require.Len(t, roots, 1)
	read := func(_ context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
		require.Equal(t, toolQuerySpaceNode, tool)
		require.Equal(t, "synthetic-space", args["space_id"])
		require.False(t, verify)
		var payload string
		switch {
		case args["parent_id"] == "folder":
			payload = `{"children":[{"node_id":"tail","title":"SYNTHETIC-SCAN-TAIL","node_type":"wiki_file","doc_type":"sheet"}],"has_next":false}`
		case args["num"] == 0:
			payload = `{"children":[{"node_id":"folder","title":"Folder/A","node_type":"wiki_folder","has_child":true},{"node_id":"doc","title":"Body","node_type":"wiki_file","doc_type":"doc"},{"node_id":"link","title":"External","node_type":"link","doc_type":"doc","has_child":true,"url":"https://example.invalid/private"}],"has_next":true}`
		default:
			payload = `{"children":[],"has_next":false}`
		}
		return &NativeResponse{Data: json.RawMessage(payload)}, nil
	}
	page, err := ReadNativeScanPage(ctx, roots[0], read)
	require.NoError(t, err)
	require.Len(t, page.Entries, 3)
	require.Len(t, page.Children, 1)
	require.Equal(t, "link", page.Entries[2].Disposition)
	require.Equal(t, encodeNodeResourceID("synthetic-space", "doc"), page.Entries[1].ExternalID)
	require.Equal(t, "Folder／A", page.Children[0].FolderPath)
	require.NotNil(t, page.Next)
	require.Equal(t, 1, page.Next.Offset)
	child, err := ReadNativeScanPage(ctx, page.Children[0], read)
	require.NoError(t, err)
	require.Equal(t, "SYNTHETIC-SCAN-TAIL", child.Entries[0].Title)
	require.Nil(t, child.Next)
	end, err := ReadNativeScanPage(ctx, *page.Next, read)
	require.NoError(t, err)
	require.Empty(t, end.Entries)
	require.Nil(t, end.Next)

	for _, bad := range []string{`{}`, `{"children":[],"has_next":true}`, `{"children":[{"node_id":"a"},{"node_id":"a"}],"has_next":false}`} {
		_, err := ReadNativeScanPage(ctx, roots[0], func(context.Context, string, map[string]interface{}, bool) (*NativeResponse, error) {
			return &NativeResponse{Data: json.RawMessage(bad)}, nil
		})
		require.Error(t, err)
	}
	cycle := page.Children[0]
	_, err = ReadNativeScanPage(ctx, cycle, func(context.Context, string, map[string]interface{}, bool) (*NativeResponse, error) {
		return &NativeResponse{Data: json.RawMessage(`{"children":[{"node_id":"folder","node_type":"wiki_folder","has_child":true}],"has_next":false}`)}, nil
	})
	require.ErrorContains(t, err, "SCAN_CYCLE")
	_, err = ReadNativeScanPage(ctx, *page.Next, readSameScanPage(page.Response))
	require.ErrorContains(t, err, "SCAN_PAGE_REPEATED")

	home, err := NativeScanRoots([]string{homeRootResourceID})
	require.NoError(t, err)
	h, err := ReadNativeScanPage(ctx, home[0], readSameScanPage(NativeResponse{Data: json.RawMessage(`{"list":[{"id":"one","title":"Personal"}],"finish":false}`)}))
	require.NoError(t, err)
	require.Equal(t, 1, h.Next.Offset)
	require.Equal(t, encodeHomeNodeResourceID("one"), h.Entries[0].ExternalID)
	_, err = ReadNativeScanPage(ctx, *h.Next, readSameScanPage(NativeResponse{Data: json.RawMessage(`{"list":[],"finish":false}`)}))
	require.ErrorContains(t, err, "SCAN_EMPTY_UNFINISHED")

	selected, err := NativeScanRoots([]string{encodeNodeResourceID("synthetic-space", "folder")})
	require.NoError(t, err)
	require.True(t, selected[0].Metadata)
	folder, err := ReadNativeScanPage(ctx, selected[0], readSameScanPage(NativeResponse{Data: json.RawMessage(`{"file_id":"folder","title":"Selected","space_id":"synthetic-space","is_folder":true,"status":"normal"}`)}))
	require.NoError(t, err)
	require.Len(t, folder.Children, 1)
	require.False(t, folder.Children[0].Metadata)
	require.Equal(t, []string{"folder"}, folder.Children[0].Ancestors)
	_, err = ReadNativeScanPage(ctx, selected[0], readSameScanPage(NativeResponse{Data: json.RawMessage(`{"file_id":"other","type":"doc","status":"normal"}`)}))
	require.ErrorContains(t, err, "SCAN_IDENTITY_MISMATCH")
}

func readSameScanPage(response NativeResponse) NativeReadFunc {
	return func(context.Context, string, map[string]interface{}, bool) (*NativeResponse, error) {
		return &response, nil
	}
}
