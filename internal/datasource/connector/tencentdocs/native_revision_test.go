package tencentdocs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeRevisionDetectsDOCEditWithUnchangedMetadataTimestamp(t *testing.T) {
	version := "1"
	read := func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*NativeResponse, error) {
		require.True(t, verify)
		if tool == toolQueryFileInfo {
			return nativeMetadata(t, "doc"), nil
		}
		return nativeReply(t, map[string]interface{}{"version": version}), nil
	}
	first, err := ProbeNativeRevision(context.Background(), "file", "doc", read)
	require.NoError(t, err)
	version = "2"
	second, err := ProbeNativeRevision(context.Background(), "file", "doc", read)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}
