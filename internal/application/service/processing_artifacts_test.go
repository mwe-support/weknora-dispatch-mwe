package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestProcessingArtifactsEncryptValidateAndBindSourceIdentity(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	dir := t.TempDir()
	fs := files.NewLocalFileService(dir, "")
	store := NewProcessingArtifacts(fs, nil)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	job := types.ProcessingJob{ID: "job", TenantID: 1, Generation: 2}
	step := types.ProcessingStep{ID: "step", InputFingerprint: "input"}
	data := []byte("synthetic page; private URL https://example.test/download?signature=private")
	ref, digest, err := store.Save(ctx, job, step, "native_page", data)
	require.NoError(t, err)
	require.NotContains(t, ref, "signature")
	read, err := store.Read(ctx, job, step, "native_page", ref, digest)
	require.NoError(t, err)
	require.Equal(t, data, read)
	old := job
	old.Generation--
	_, err = store.Read(ctx, old, step, "native_page", ref, digest)
	require.Error(t, err)
	_, err = store.Read(context.WithValue(ctx, types.TenantIDContextKey, uint64(2)), job, step, "native_page", ref, digest)
	require.Error(t, err)
	var stored string
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			stored = path
		}
		return nil
	}))
	ciphertext, err := os.ReadFile(stored)
	require.NoError(t, err)
	require.NotContains(t, string(ciphertext), "signature=private")
	ciphertext[len(ciphertext)-5] ^= 1
	require.NoError(t, os.WriteFile(stored, ciphertext, 0600))
	_, err = store.Read(ctx, job, step, "native_page", ref, digest)
	require.Error(t, err)
	t.Setenv("SYSTEM_AES_KEY", "")
	_, _, err = store.Save(ctx, job, step, "native_page", data)
	require.Error(t, err) // A missing key must never store plaintext.
}

type processingStreamOnlyFiles struct{ interfaces.FileService }

func (s processingStreamOnlyFiles) SaveBytes(context.Context, []byte, uint64, string, bool) (string, error) {
	return "", errors.New("artifact storage must stream to SaveFile")
}

func TestProcessingArtifactsStreamWithoutWholeCiphertextCopies(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	fs := files.NewLocalFileService(t.TempDir(), "")
	store := NewProcessingArtifacts(processingStreamOnlyFiles{fs}, nil)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	job := types.ProcessingJob{ID: "job", TenantID: 1, Generation: 2}
	step := types.ProcessingStep{ID: "step", InputFingerprint: "input"}
	data := bytes.Repeat([]byte("synthetic-stream"), 600000)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	ref, digest, err := store.Save(ctx, job, step, "native_page", data)
	require.NoError(t, err)
	runtime.ReadMemStats(&after)
	t.Logf("artifact plaintext bytes=%d, save and verify allocations=%d", len(data), after.TotalAlloc-before.TotalAlloc)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(len(data)), "stream encryption and readback verification must not allocate another whole body")
	read, err := store.Read(ctx, job, step, "native_page", ref, digest)
	require.NoError(t, err)
	require.Equal(t, data, read)
	reader, err := fs.GetFile(ctx, ref)
	require.NoError(t, err)
	ciphertext, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.True(t, bytes.HasPrefix(ciphertext, []byte("WKPA3\n")))
	for name, damaged := range map[string][]byte{"truncated": ciphertext[:len(ciphertext)-1], "trailing": append(bytes.Clone(ciphertext), 1)} {
		path, err := fs.SaveBytes(ctx, damaged, 1, name+".enc", false)
		require.NoError(t, err)
		_, err = store.Read(ctx, job, step, "native_page", path, digest)
		require.Error(t, err)
	}
	ref, digest, err = store.Save(ctx, job, step, "empty", nil)
	require.NoError(t, err)
	read, err = store.Read(ctx, job, step, "empty", ref, digest)
	require.NoError(t, err)
	require.Empty(t, read)
	legacyHeader, err := processingArtifactIdentity(ctx, job, step, "legacy")
	require.NoError(t, err)
	legacyBody := []byte("previously confirmed synthetic artifact")
	legacyHeader.Bytes = len(legacyBody)
	headerBytes, _ := json.Marshal(legacyHeader)
	legacyPlain := append(append(headerBytes, '\n'), legacyBody...)
	legacyCipher, err := utils.EncryptAESGCM(string(legacyPlain), utils.GetAESKey())
	require.NoError(t, err)
	legacyPath, err := fs.SaveBytes(ctx, []byte(legacyCipher), 1, "legacy.enc", false)
	require.NoError(t, err)
	read, err = store.Read(ctx, job, step, "legacy", legacyPath, fmt.Sprintf("%x", sha256.Sum256(legacyPlain)))
	require.NoError(t, err)
	require.Equal(t, legacyBody, read)
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	_, err = store.saveStream(ctx, 1, "failed.enc", 1024, func(writer io.Writer) error {
		_, _ = writer.Write([]byte("incomplete encrypted frame"))
		return io.ErrUnexpectedEOF
	})
	require.Error(t, err)
	entries, err := os.ReadDir(temporary)
	require.NoError(t, err)
	require.Empty(t, entries, "failed encryption must release its temporary file")
}
