package tencentdocs

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opt-in only. The test token arrives on standard input and is never written
// to a fixture, command line, log, or container configuration. These IDs are
// the isolated synthetic corpus, outside all production source selections.
func TestNativeLiveAcceptanceCorpus(t *testing.T) {
	if os.Getenv("PROCESSING_NATIVE_LIVE") != "1" {
		t.Skip("live synthetic corpus is opt-in")
	}
	var credentials struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.NewDecoder(os.Stdin).Decode(&credentials))
	require.NotEmpty(t, credentials.Token)
	client, err := NewTencentDocsMCPClient(MCPClientConfig{Token: credentials.Token, Timeout: 45 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Run("scan", func(t *testing.T) {
		requests, err := NativeScanRoots([]string{encodeSpaceResourceID("XbrGSWLWVGqO")})
		require.NoError(t, err)
		seen := map[string]bool{}
		pages := 0
		for len(requests) > 0 {
			request := requests[0]
			requests = requests[1:]
			page, err := ReadNativeScanPage(ctx, request, func(ctx context.Context, _ string, _ map[string]interface{}, _ bool) (*NativeResponse, error) {
				return client.ReadScan(WithManagedRetries(ctx), request)
			})
			require.NoError(t, err)
			pages++
			require.Less(t, pages, 100, "isolated corpus scan unexpectedly expanded")
			requests = append(requests, page.Children...)
			if page.Next != nil {
				requests = append(requests, *page.Next)
			}
			for _, entry := range page.Entries {
				if entry.Disposition == "document" {
					seen[entry.FileID] = true
				}
			}
		}
		for _, id := range []string{"XpKxoPOCEmwt", "XxGoJdYEHCjF", "XUIXKvaewTnf", "XGHqAAMQHokx", "XrSLOJNFcOPo"} {
			require.True(t, seen[id], "synthetic file missing from complete scan: %s", id)
		}
		require.Len(t, seen, 5)
		t.Logf("verified isolated scan pages=%d documents=%d", pages, len(seen))
	})
	for _, item := range []struct{ kind, id, tail string }{
		{"doc", "XpKxoPOCEmwt", "DOC-END-7391"},
		{"smartcanvas", "XxGoJdYEHCjF", "MDX-END-7391"},
		{"smartsheet", "XUIXKvaewTnf", "SMART-SECOND-TABLE-7391"},
		{"sheet", "XGHqAAMQHokx", "SHEET-END-7391"},
	} {
		t.Run(item.kind, func(t *testing.T) {
			read := func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
				return client.ReadNative(WithManagedRetries(ctx), tool, args)
			}
			revision, err := ProbeNativeRevision(ctx, item.id, item.kind, read)
			require.NoError(t, err)
			snapshot, err := CollectNative(ctx, item.id, item.kind, read)
			require.NoError(t, err)
			require.Equal(t, revision, snapshot.RevisionKey)
			require.True(t, snapshot.CoverageComplete)
			normalized, err := NormalizeNativeSnapshot(snapshot)
			require.NoError(t, err)
			if item.kind == "doc" {
				require.True(t, snapshot.RequiresExport)
				require.GreaterOrEqual(t, snapshot.Units, 172)
			} else {
				require.Contains(t, normalized.Markdown, item.tail)
			}
			if item.kind == "smartcanvas" {
				require.Len(t, normalized.Assets, 1)
				require.Len(t, normalized.Unsupported, 2)
			}
			if item.kind == "smartsheet" {
				require.Empty(t, normalized.Unsupported)
				require.Equal(t, 136, snapshot.Units)
				require.Contains(t, normalized.Markdown, "SMART-ROW-130")
			}
			t.Logf("verified kind=%s file=%s revision=%d pages=%d units=%d native_bytes=%d text_bytes=%d assets=%d unsupported=%d export_required=%v",
				item.kind, item.id, snapshot.SourceRevision, len(snapshot.Pages), snapshot.Units, snapshot.RawBytes, len(normalized.Markdown), len(normalized.Assets), len(normalized.Unsupported), snapshot.RequiresExport)
		})
	}
}
