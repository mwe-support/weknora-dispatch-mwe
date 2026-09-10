package service

import (
	"context"
	"mime/multipart"
	"strings"
	"testing"

	werrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/stretchr/testify/require"
)

func TestProcessingSharedFileIngressLimitPrecedesStorageAndBoundsUnknownBody(t *testing.T) {
	t.Setenv("MAX_FILE_SIZE_MB", "200")
	s := &knowledgeService{}
	for _, file := range []*multipart.FileHeader{nil, {Filename: "huge.txt", Size: 100<<20 + 1}} {
		_, err := s.CreateKnowledgeFromFile(context.Background(), "kb", file, nil, nil, "", nil, "", nil)
		require.Error(t, err)
	}
	t.Setenv("MAX_FILE_SIZE_MB", "1")
	file := newMultipartFileHeader(t, "synthetic.txt", strings.Repeat("x", 2<<20))
	file.Size = 1 // A direct caller's metadata is not proof of actual body size.
	_, err := calculateFileHash(file)
	var limit *werrors.AppError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "FILE_SIZE_EXCEEDED", limit.Message)
	require.Equal(t, int64(1<<20+1), limit.Details.(map[string]any)["observed_at_least_bytes"])
}
