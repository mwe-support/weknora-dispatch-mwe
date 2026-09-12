package tencentdocs

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/utils"
)

// ZIP timestamps/compression and generated Office properties do not change
// document content. Bound decompression; unusual archives use the raw digest.
func exportContentDigest(data []byte) [32]byte {
	raw := sha256.Sum256(data)
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return raw
	}
	office := false
	for _, f := range z.File {
		if f.Name == "word/document.xml" || f.Name == "xl/workbook.xml" {
			office = true
		}
	}
	if !office {
		return raw
	}
	sort.Slice(z.File, func(i, j int) bool { return z.File[i].Name < z.File[j].Name })
	h := sha256.New()
	remaining := int64(maxExportBytes)
	for _, f := range z.File {
		if f.FileInfo().IsDir() || f.Name == "docProps/core.xml" || f.Name == "docProps/app.xml" {
			continue
		}
		if f.UncompressedSize64 > uint64(remaining) {
			return raw
		}
		r, err := f.Open()
		if err != nil {
			return raw
		}
		_, _ = h.Write([]byte(f.Name + "\x00"))
		n, err := io.Copy(h, io.LimitReader(r, remaining+1))
		_ = r.Close()
		if err != nil || n > remaining {
			return raw
		}
		remaining -= n
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

var ErrUnsupportedSourceType = errors.New("UNSUPPORTED_FILE_TYPE: 不支持的文档类型")

type exportNotSentError struct{ err error }

func (e *exportNotSentError) Error() string { return "export request was not sent: " + e.err.Error() }
func (e *exportNotSentError) Unwrap() error { return e.err }

func sourceExportFormat(kind string) string {
	switch strings.ToLower(kind) {
	case "doc", "word", "tencentdoc", "smartcanvas":
		return "docx"
	case "sheet", "excel", "tencentsheet", "smartsheet":
		return "xlsx"
	default:
		return ""
	}
}

// The persistent refusal survives later sync runs. Only an explicit reset or
// credential/scope change can reopen it; no provider request is made here.
func (s *fetchState) skipTerminalFile(ctx context.Context, space string, node Node, root string) (bool, error) {
	id := fetchedNodeResourceID(space, node.ID)
	r, exists := s.next.FileRetries[id]
	if !exists {
		for oldID, saved := range s.next.FileRetries {
			if saved.Node.ID == node.ID {
				r, exists = saved, true
				delete(s.next.FileRetries, oldID)
				s.next.FileRetries[id] = r
				break
			}
		}
	}
	if exists && r.State == "running" && r.Attempt >= maxFileRetries {
		r.State = "exhausted"
		r.ErrorAt = time.Now().UTC()
		r.Error = "Final source retry was interrupted before recording a result"
		if r.ExportStartUncertain {
			r.Category = "EXPORT_START_UNCERTAIN"
		}
		s.next.FileRetries[id] = r
	}
	if !exists || (r.State != "needs_manual" && r.State != "exhausted" && r.State != "scheduled") {
		return false, nil
	}
	if s.seenFileIDs[node.ID] {
		return true, nil
	}
	s.seenFileIDs[node.ID], s.seenDocs[id] = true, true
	item := failedFetchedItem(id, firstNonEmpty(node.Title, r.Node.Title), space, r.Node, root, errors.New(firstNonEmpty(r.Error, "Previous source failure requires manual action")))
	item.Metadata["retry_category"], item.Metadata["error_stage"] = r.Category, r.Stage
	item.Metadata["retryable"] = "false"
	item.Metadata["retained_failure"] = "true"
	// Preserve the pending retry instead of turning it into a permanent error.
	if r.State == "scheduled" && r.Attempt < maxFileRetries {
		item.Metadata["retryable"] = "true"
		if delay := time.Until(r.NextAt); delay > 0 {
			item.Metadata["retry_after_ms"] = fmt.Sprint(delay.Milliseconds())
		}
	}
	return true, s.emit(ctx, item)
}

func (s *fetchState) saveExportURL(ctx context.Context, id string, status *ExportStatus, name string) error {
	if s.handler == nil {
		return nil
	}
	key := utils.GetAESKey()
	if key == nil {
		return errors.New("EXPORT_CHECKPOINT_KEY_UNAVAILABLE")
	}
	previous := s.next.FileRetries[id]
	if previous.ExportURL != "" {
		old, err := utils.DecryptStoredSecret(previous.ExportURL)
		if err != nil {
			return err
		}
		if old == status.FileURL {
			return nil
		} // Do not extend a cached URL's original expiry.
	}
	ciphertext, err := utils.EncryptAESGCM(status.FileURL, key)
	if err != nil {
		return err
	}
	r := s.next.FileRetries[id]
	r.ExportURL, r.ExportFileName, r.ExportURLExpiresAt = ciphertext, name, status.DownloadExpiresAt(time.Now().UTC())
	s.next.FileRetries[id] = r
	return s.checkpoint(ctx)
}

func savedExportStatus(r fileRetry) (*ExportStatus, error) {
	if r.ExportURL == "" || !time.Now().Before(r.ExportURLExpiresAt) {
		return nil, nil
	}
	url, err := utils.DecryptStoredSecret(r.ExportURL)
	if err != nil {
		return nil, err
	}
	if err := validateExportURL(url); err != nil {
		return nil, err
	}
	return &ExportStatus{Progress: 100, FileURL: url, FileName: r.ExportFileName}, nil
}
