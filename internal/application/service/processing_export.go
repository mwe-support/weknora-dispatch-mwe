package service

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"path"
	"strings"
	"time"
	"unicode"

	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
)

type processingExportReceipt struct {
	TaskID       string    `json:"task_id,omitempty"`
	Request      string    `json:"request"`
	StartedAt    time.Time `json:"started_at"`
	NotSent      bool      `json:"not_sent,omitempty"`
	FileName     string    `json:"file_name,omitempty"`
	URL          string    `json:"url,omitempty"`
	URLExpiresAt time.Time `json:"url_expires_at,omitempty"`
}

func (e *processingDocumentExecution) parseExport(ctx context.Context) (types.ProcessingOutcome, error) {
	data, err := e.dependencyBytes(ctx, "download", "body", "source_file")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var receipt processingExportReceipt
	if err := e.dependency(ctx, "normalize", "body", "normalize", &receipt); err != nil {
		return types.ProcessingOutcome{}, err
	}
	ext := strings.TrimPrefix(path.Ext(receipt.FileName), ".")
	if !types.IsSupportedKnowledgeFileExtension(ext) || receipt.FileName != "source."+ext {
		return types.ProcessingOutcome{}, errors.New("EXPORT_FORMAT_UNSUPPORTED")
	}
	if e.document.Kind == "doc" && ext != "docx" {
		return types.ProcessingOutcome{}, errors.New("DOC_FULL_EXPORT_REQUIRED")
	}
	var textRuns map[string]int
	images := 0
	if ext == "docx" {
		textRuns, images, err = processingDOCXInventory(data)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
	} else if ext == "pdf" && !bytes.HasPrefix(data, []byte("%PDF-")) {
		return types.ProcessingOutcome{}, errors.New("PDF_HEADER_INVALID")
	}
	config := ResolveProcessConfig(e.kb, nil)
	overrides := e.s.getParserEngineOverridesFromContext(ctx)
	applyParserRuleOverrides(overrides, config.ChunkingConfig, ext)
	engine := config.ChunkingConfig.ResolveParserEngine(ext)
	reader := e.s.resolveDocReader(ctx, engine, ext, false, overrides)
	if reader == nil {
		return types.ProcessingOutcome{}, errors.New("DOCREADER_UNAVAILABLE")
	}
	result, err := e.s.callDocReaderWithTimeout(ctx, reader, &types.ReadRequest{FileContent: data, FileName: receipt.FileName, FileType: ext, Title: e.document.Title,
		ParserEngine: engine, RequestID: e.lease.Step.ID, ParserEngineOverrides: overrides})
	if errors.Is(err, docparser.ErrMinerUTransient) {
		next := time.Now().UTC().Add(30 * time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, Result: types.JSON(`{"reason":"capacity"}`)}, nil
	}
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if result == nil || result.Error != "" || result.IsAudio || len(result.MarkdownContent) > processingArtifactLimit {
		return types.ProcessingOutcome{}, errors.New("DOCREADER_RESULT_INVALID")
	}
	if ext == "docx" {
		if err := validateProcessingDOCXText(textRuns, images, result); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	if result.Metadata == nil {
		result.Metadata = map[string]string{}
	}
	result.Metadata["parser_engine"], result.Metadata["source_type"] = engine, e.document.Kind
	if ext == "docx" {
		result.Metadata["verified_export_text_runs"], result.Metadata["export_images"] = fmt.Sprint(len(textRuns)), fmt.Sprint(images)
	}
	parsed, err := e.prepareParsedImages(ctx, *result)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	outcome, err := e.success(ctx, "parse", parsed)
	if len(result.ImageRefs) == 0 {
		outcome.Completeness = "complete"
	}
	return outcome, err
}

// Check the actual archive stream before giving it to a remote parser. XML
// text runs include table cells, text boxes, footnotes and endnotes; previews
// and a parser's success flag cannot substitute for their presence in output.
func processingDOCXInventory(data []byte) (map[string]int, int, error) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, 0, errors.New("DOCX_ARCHIVE_INVALID")
	}
	if len(archive.File) > 10000 {
		return nil, 0, errors.New("DOCX_ARCHIVE_ENTRY_LIMIT")
	}
	const maxExpanded = 200 << 20
	total, images := int64(0), 0
	textRuns := map[string]int{}
	seen := map[string]bool{}
	for _, file := range archive.File {
		name := file.Name
		if seen[name] || strings.Contains(name, "\\") || strings.HasPrefix(name, "/") || strings.HasPrefix(name, "../") || path.Clean(name) != strings.TrimSuffix(name, "/") {
			return nil, 0, errors.New("DOCX_ARCHIVE_PATH_INVALID")
		}
		seen[name] = true
		if file.UncompressedSize64 > uint64(maxExpanded-total) {
			return nil, 0, errors.New("DOCX_EXPANDED_SIZE_EXCEEDED")
		}
		stream, err := file.Open()
		if err != nil {
			return nil, 0, errors.New("DOCX_ARCHIVE_INVALID")
		}
		body, readErr := io.ReadAll(io.LimitReader(stream, maxExpanded-total+1))
		_ = stream.Close()
		total += int64(len(body))
		if total > maxExpanded {
			return nil, 0, errors.New("DOCX_EXPANDED_SIZE_EXCEEDED")
		}
		if readErr != nil {
			return nil, 0, errors.New("DOCX_ARCHIVE_INVALID")
		}
		if strings.HasPrefix(name, "word/media/") && len(body) > 0 {
			images++
		}
		if !strings.HasPrefix(name, "word/") || !strings.HasSuffix(name, ".xml") {
			continue
		}
		decoder := xml.NewDecoder(bytes.NewReader(body))
		for {
			token, err := decoder.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, 0, errors.New("DOCX_XML_INVALID")
			}
			start, ok := token.(xml.StartElement)
			if !ok {
				continue
			}
			if start.Name.Space == "http://schemas.openxmlformats.org/wordprocessingml/2006/main" {
				switch start.Name.Local {
				case "altChunk", "object":
					return nil, 0, errors.New("DOCX_EMBEDDED_OBJECT_UNSUPPORTED")
				case "t":
					var text string
					if err := decoder.DecodeElement(&text, &start); err != nil {
						return nil, 0, errors.New("DOCX_XML_INVALID")
					}
					text = processingDOCXText(text)
					if text != "" {
						textRuns[text]++
					}
				}
			}
		}
	}
	if !seen["word/document.xml"] {
		return nil, 0, errors.New("DOCX_MAIN_DOCUMENT_MISSING")
	}
	return textRuns, images, nil
}

func processingDOCXText(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, html.UnescapeString(text))
}

func validateProcessingDOCXText(textRuns map[string]int, images int, result *types.ReadResult) error {
	text := processingDOCXText(result.MarkdownContent)
	for run, expected := range textRuns {
		if strings.Count(text, run) < expected {
			return errors.New("DOCX_TEXT_COVERAGE_INCOMPLETE")
		}
	}
	if len(result.ImageRefs) < images {
		return errors.New("DOCX_IMAGE_COVERAGE_INCOMPLETE")
	}
	return nil
}

func (e *processingDocumentExecution) exportStage(ctx context.Context, client tencentdocs.Client, verify func(context.Context) error) (types.ProcessingOutcome, error) {
	ctx = tencentdocs.WithManagedRetries(ctx)
	if err := verify(ctx); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if e.lease.Step.Stage == "export_start" {
		request := fmt.Sprintf("%x", sha256.Sum256([]byte(e.lease.Job.ID+"/"+e.document.FileID+"/"+e.lease.Job.SourceRevision)))
		var receipt processingExportReceipt
		if e.lease.Step.CheckpointRef != "" {
			if err := json.Unmarshal([]byte(e.lease.Step.CheckpointRef), &receipt); err != nil || receipt.Request != request {
				return types.ProcessingOutcome{}, errors.New("EXPORT_INTENT_INVALID")
			}
			if receipt.TaskID != "" {
				return e.success(ctx, "export_task", receipt)
			}
			if !receipt.NotSent {
				return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "uncertain", ErrorCode: "EXPORT_START_UNCERTAIN", Message: "Reconcile the recorded export request before starting another export"}, nil
			}
		}
		receipt = processingExportReceipt{Request: request, StartedAt: time.Now().UTC()}
		checkpoint, _ := json.Marshal(receipt)
		if err := e.repo.Heartbeat(ctx, e.lease.Job.TenantID, e.lease, 2*time.Minute, string(checkpoint)); err != nil {
			return types.ProcessingOutcome{}, err
		}
		task, err := client.StartExport(ctx, e.document.FileID)
		if err != nil {
			var wait *tencentdocs.MCPBudgetWaitError
			if errors.As(err, &wait) {
				// Admission proves that no export RPC was issued. Persist this
				// proof with the wait so waking up can safely try admission again.
				receipt.NotSent = true
				checkpoint, _ = json.Marshal(receipt)
				outcome := tencentdocs.ProcessingFailure("export_start", err)
				outcome.CheckpointRef = string(checkpoint)
				return outcome, nil
			}
			return types.ProcessingOutcome{}, err
		}
		if task == nil || task.ID == "" {
			return types.ProcessingOutcome{}, errors.New("EXPORT_TASK_ID_MISSING")
		}
		receipt.TaskID = task.ID
		checkpoint, _ = json.Marshal(receipt)
		if err := e.repo.Heartbeat(ctx, e.lease.Job.TenantID, e.lease, 2*time.Minute, string(checkpoint)); err != nil {
			return types.ProcessingOutcome{}, err
		}
		return e.success(ctx, "export_task", receipt)
	}
	var receipt processingExportReceipt
	if err := e.dependency(ctx, "export_start", "body", "export_task", &receipt); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if receipt.TaskID == "" || receipt.StartedAt.IsZero() {
		return types.ProcessingOutcome{}, errors.New("EXPORT_TASK_INVALID")
	}
	var ready processingExportReceipt
	if e.lease.Step.Stage == "download" {
		if err := e.dependency(ctx, "export_poll", "body", "export_ready", &ready); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if ready.TaskID != receipt.TaskID {
			return types.ProcessingOutcome{}, errors.New("EXPORT_RESULT_CHANGED")
		}
		if e.lease.Step.CheckpointRef != "" {
			var saved processingArtifactRef
			if err := json.Unmarshal([]byte(e.lease.Step.CheckpointRef), &saved); err != nil {
				return types.ProcessingOutcome{}, errors.New("EXPORT_CHECKPOINT_INVALID")
			}
			data, err := e.artifacts.Read(ctx, e.lease.Job, e.lease.Step, "export_ready", saved.Path, saved.Digest)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			var fresh processingExportReceipt
			if err := json.Unmarshal(data, &fresh); err != nil || fresh.TaskID != ready.TaskID || fresh.FileName != ready.FileName {
				return types.ProcessingOutcome{}, errors.New("EXPORT_CHECKPOINT_INVALID")
			}
			ready = fresh
		}
		if ready.URL != "" && time.Now().Before(ready.URLExpiresAt) {
			outcome, err := e.downloadExport(ctx, client, ready, verify)
			if !errors.Is(err, tencentdocs.ErrExportDownloadURLExpired) {
				return outcome, err
			}
		}
	}
	if e.lease.Step.Stage == "export_poll" && e.lease.Step.CheckpointRef != "" {
		var saved processingArtifactRef
		if err := json.Unmarshal([]byte(e.lease.Step.CheckpointRef), &saved); err != nil {
			return types.ProcessingOutcome{}, errors.New("EXPORT_CHECKPOINT_INVALID")
		}
		data, err := e.artifacts.Read(ctx, e.lease.Job, e.lease.Step, "export_ready", saved.Path, saved.Digest)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if err := json.Unmarshal(data, &ready); err != nil || ready.TaskID != receipt.TaskID {
			return types.ProcessingOutcome{}, errors.New("EXPORT_CHECKPOINT_INVALID")
		}
		return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: saved.Path, OutputDigest: saved.Digest}, nil
	}
	status, err := client.GetExportProgress(ctx, receipt.TaskID)
	if err != nil {
		if e.lease.Step.Stage == "download" {
			var provider *tencentdocs.MCPToolError
			if errors.As(err, &provider) && (provider.Code == 404 || provider.Code == 323908) {
				return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "capability", ErrorCode: "EXPORT_DOWNLOAD_URL_EXPIRED", Message: "The saved download address expired and the provider no longer exposes the original export task"}, nil
			}
		}
		return types.ProcessingOutcome{}, err
	}
	if status == nil || status.Error != "" || status.Progress < 0 || status.Progress > 100 {
		return types.ProcessingOutcome{}, errors.New("EXPORT_RESULT_INVALID")
	}
	if status.Progress != 100 {
		if time.Since(receipt.StartedAt) >= 45*time.Minute {
			return types.ProcessingOutcome{}, errors.New("EXPORT_DEADLINE_EXCEEDED")
		}
		next := time.Now().UTC().Add(3 * time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next}, nil
	}
	ext := strings.TrimPrefix(strings.ToLower(path.Ext(status.ResolvedFileName())), ".")
	if status.FileURL == "" || !types.IsSupportedKnowledgeFileExtension(ext) {
		return types.ProcessingOutcome{}, errors.New("EXPORT_FORMAT_UNSUPPORTED")
	}
	if e.document.Kind == "doc" && ext != "docx" {
		return types.ProcessingOutcome{}, errors.New("DOC_FULL_EXPORT_REQUIRED")
	}
	receipt.FileName = "source." + ext
	receipt.URL, receipt.URLExpiresAt = status.FileURL, status.DownloadExpiresAt(time.Now().UTC())
	if e.lease.Step.Stage == "export_poll" {
		// Live Tencent exports can disappear after the first completed poll.
		// Save the first URL in an authenticated encrypted artifact immediately;
		// only its object reference enters the DB checkpoint or delivery payload.
		outcome, err := e.success(ctx, "export_ready", receipt)
		if err != nil {
			return outcome, err
		}
		checkpoint, _ := json.Marshal(processingArtifactRef{Path: outcome.OutputManifestRef, Digest: outcome.OutputDigest})
		if err := e.repo.Heartbeat(ctx, e.lease.Job.TenantID, e.lease, 2*time.Minute, string(checkpoint)); err != nil {
			return types.ProcessingOutcome{}, err
		}
		return outcome, nil
	}
	if e.lease.Step.Stage != "download" {
		return types.ProcessingOutcome{}, errors.New("EXPORT_STAGE_INVALID")
	}
	if ready.TaskID != receipt.TaskID || ready.FileName != receipt.FileName {
		return types.ProcessingOutcome{}, errors.New("EXPORT_RESULT_CHANGED")
	}
	// A refreshed URL may also be returned only once. Persist it before the
	// download, so a transport failure reuses this receipt on the next attempt.
	outcome, err := e.success(ctx, "export_ready", receipt)
	if err != nil {
		return outcome, err
	}
	checkpoint, _ := json.Marshal(processingArtifactRef{Path: outcome.OutputManifestRef, Digest: outcome.OutputDigest})
	if err := e.repo.Heartbeat(ctx, e.lease.Job.TenantID, e.lease, 2*time.Minute, string(checkpoint)); err != nil {
		return types.ProcessingOutcome{}, err
	}
	return e.downloadExport(ctx, client, receipt, verify)
}

func (e *processingDocumentExecution) downloadExport(ctx context.Context, client tencentdocs.Client, ready processingExportReceipt, verify func(context.Context) error) (types.ProcessingOutcome, error) {
	data, err := client.DownloadExport(ctx, ready.URL)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if len(data) == 0 {
		return types.ProcessingOutcome{}, errors.New("EXPORT_BODY_EMPTY")
	}
	if len(data) > processingArtifactLimit {
		size := int64(len(data))
		return types.ProcessingOutcome{}, &tencentdocs.NativeLimitError{Kind: "download", LimitBytes: processingArtifactLimit, ActualBytes: &size}
	}
	if err := verify(ctx); err != nil {
		return types.ProcessingOutcome{}, err
	}
	ref, digest, err := e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, "source_file", data)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	result, _ := json.Marshal(map[string]any{"bytes": len(data), "file_type": strings.TrimPrefix(path.Ext(ready.FileName), "."), "body_sha256": fmt.Sprintf("%x", sha256.Sum256(data))})
	return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: ref, OutputDigest: digest, Result: result}, nil
}
