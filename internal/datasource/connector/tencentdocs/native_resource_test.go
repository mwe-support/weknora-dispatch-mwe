package tencentdocs

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeResourceRequiresFreshScopedListing(t *testing.T) {
	ctx := context.Background()
	root := NativeScanRequest{ResourceID: encodeHomeNodeResourceID("folder"), ParentID: "folder", Ancestors: []string{"folder"}}
	body := `{"list":[{"id":"pdf","title":"sample.pdf","url":"https://docs.qq.com/pdf/Synthetic?signature=private"}],"finish":false}`
	page, err := ReadNativeScanPage(ctx, root, readSameScanPage(NativeResponse{Data: json.RawMessage(body)}))
	require.NoError(t, err)
	entry := page.Entries[0]
	require.Equal(t, "https://docs.qq.com/pdf/Synthetic", entry.URL)
	unsupported := &MCPToolError{Tool: toolQueryFileInfo, Code: fileInfoUnsupportedTypeCode, Message: fileInfoUnsupportedTypeMessage}
	require.True(t, NativeResourceFallback(entry, unsupported))
	require.False(t, NativeResourceFallback(entry, &MCPToolError{Tool: toolQueryFileInfo, Code: 60007, Message: "permission denied"}))
	missing := entry
	missing.Listing = nil
	require.False(t, NativeResourceFallback(missing, unsupported))
	require.ErrorContains(t, VerifyNativeResource(ctx, missing, nil), "RESOURCE_LISTING_PROOF_REQUIRED")
	for _, changed := range []string{"", "renamed", "removed", "repeated", "denied"} {
		t.Run(changed, func(t *testing.T) {
			calls := 0
			err := VerifyNativeResource(ctx, entry, func(_ context.Context, request NativeScanRequest) (*NativeResponse, error) {
				calls++
				require.Equal(t, root.ResourceID, request.ResourceID)
				require.Equal(t, "folder", request.ParentID)
				if changed == "denied" {
					return nil, &MCPToolError{Tool: toolManageFolderList, Code: 60007}
				}
				data := `{"list":[{"id":"other","title":"other.pdf"}],"finish":false}`
				if request.Offset > 0 && changed != "repeated" {
					data = body
					if changed == "renamed" {
						data = `{"list":[{"id":"pdf","title":"renamed.pdf"}],"finish":true}`
					}
					if changed == "removed" {
						data = `{"list":[],"finish":true}`
					}
				}
				return &NativeResponse{Data: json.RawMessage(data)}, nil
			})
			if changed == "" {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestNativeMembershipTraversesCurrentRootInsteadOfOldReadableParent(t *testing.T) {
	for _, scenario := range []string{"member", "moved-parent", "trashed-root", "wrong-identity"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			read := func(_ context.Context, request NativeScanRequest) (*NativeResponse, error) {
				calls++
				body := `{"list":[],"finish":true}`
				switch {
				case request.Metadata:
					body = `{"file_id":"root","title":"Root","status":"normal","is_folder":true}`
					if scenario == "trashed-root" {
						body = `{"file_id":"root","title":"Root","status":"trash","is_folder":true}`
					}
				case request.ParentID == "root" && scenario != "moved-parent":
					body = `{"list":[{"id":"parent","title":"Parent","is_folder":true}],"finish":true}`
				case request.ParentID == "parent" && request.Offset == 0:
					body = `{"list":[{"id":"other","title":"Other"}],"finish":false}`
				case request.ParentID == "parent":
					body = `{"list":[{"id":"file","title":"File"}],"finish":true}`
				}
				return &NativeResponse{Data: json.RawMessage(body)}, nil
			}
			external := encodeHomeNodeResourceID("file")
			if scenario == "wrong-identity" {
				external = encodeHomeNodeResourceID("other")
			}
			entry, err := ReadNativeMembership(context.Background(), encodeHomeNodeResourceID("root"), "file", external, read)
			if scenario == "member" {
				require.NoError(t, err)
				require.Equal(t, "Root/Parent", entry.FolderPath)
				require.Equal(t, []string{"root", "parent"}, entry.Listing.Ancestors)
				require.Equal(t, 4, calls)
			} else {
				require.Error(t, err)
				if scenario == "moved-parent" {
					require.Equal(t, 2, calls, "a still-readable old parent must not be followed after it leaves the root")
				}
			}
		})
	}
}
