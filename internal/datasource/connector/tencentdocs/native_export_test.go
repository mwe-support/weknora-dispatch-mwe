package tencentdocs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNativeExportPreservesFilenameAndHonorsEarlierSignatureExpiry(t *testing.T) {
	received := time.Date(2026, 9, 10, 9, 5, 0, 0, time.UTC)
	status := &ExportStatus{FileURL: "https://docs.qq.com/synthetic?response-content-disposition=attachment%3Bfilename%3Dtest.docx&X-Amz-Date=20260910T090000Z&X-Amz-Expires=600"}
	require.Equal(t, "test.docx", status.ResolvedFileName())
	require.Equal(t, received.Add(4*time.Minute), status.DownloadExpiresAt(received))
	status.FileURL = "https://docs.qq.com/synthetic?X-Amz-Date=invalid&X-Amz-Expires=600"
	require.Equal(t, received, status.DownloadExpiresAt(received))
	status.FileURL = "https://docs.qq.com/synthetic"
	require.Equal(t, received.Add(25*time.Minute), status.DownloadExpiresAt(received))
}
