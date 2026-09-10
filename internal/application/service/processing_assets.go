package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
)

const processingImageLimit = 10 << 20

// Only the encrypted parse artifact contains temporary provider addresses.
// Steps carry a stable source identity; renderable Markdown uses owned objects.
type processingAsset struct {
	ID        string                `json:"id"`
	URL       string                `json:"url,omitempty"`
	Source    processingArtifactRef `json:"source,omitempty"`
	StoredURL string                `json:"stored_url,omitempty"`
	Digest    string                `json:"digest,omitempty"`
	Bytes     int64                 `json:"bytes,omitempty"`
}
type processingParsed struct {
	types.ReadResult
	Assets []processingAsset `json:"assets,omitempty"`
}

func processingAssetUnit(id string) string  { return fmt.Sprintf("%x", sha256.Sum256([]byte(id))) }
func processingAssetToken(id string) string { return "asset:" + processingAssetUnit(id) }

func (e *processingDocumentExecution) prepareParsedImages(ctx context.Context, result types.ReadResult) (processingParsed, error) {
	parsed := processingParsed{ReadResult: result}
	// Inline binary objects get independent encrypted files, keeping base64
	// overhead and hundreds of images out of the text/checkpoint manifest.
	parsed.ImageRefs = nil
	parsed.ImageDirPath = ""
	targets := map[string]string{}
	var total int64
	for _, ref := range result.ImageRefs {
		if ref.OriginalRef == "" || targets[ref.OriginalRef] != "" {
			return parsed, errors.New("IMAGE_REFERENCE_DUPLICATE")
		}
		if len(ref.ImageData) == 0 {
			return parsed, errors.New("IMAGE_INLINE_DATA_MISSING")
		}
		if _, err := processingImageFormat(ref.ImageData); err != nil {
			return parsed, err
		}
		total += int64(len(ref.ImageData))
		if total > processingArtifactLimit {
			return parsed, &tencentdocs.NativeLimitError{Kind: "media", LimitBytes: processingArtifactLimit, ObservedAtLeastBytes: total}
		}
		id := "docreader/" + processingAssetUnit(ref.OriginalRef)
		path, digest, err := e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, "asset_source", ref.ImageData)
		if err != nil {
			return parsed, err
		}
		parsed.Assets = append(parsed.Assets, processingAsset{ID: id, Source: processingArtifactRef{Path: path, Digest: digest}})
		targets[ref.OriginalRef] = processingAssetToken(id)
	}
	var err error
	parsed.MarkdownContent, err = docparser.RewriteProcessingImages(result.MarkdownContent, targets)
	return parsed, err
}

func processingNativeParsed(normalized tencentdocs.NativeNormalized) (processingParsed, error) {
	parsed := processingParsed{ReadResult: types.ReadResult{MarkdownContent: normalized.Markdown, Metadata: map[string]string{"parser": "tencent-native-v2"}}}
	seen := map[string]bool{}
	for _, asset := range normalized.Assets {
		if asset.ID == "" || seen[asset.ID] {
			return parsed, errors.New("ASSET_IDENTITY_INVALID")
		}
		seen[asset.ID] = true
		ref := strings.NewReplacer(" ", "%20", "(", "%28", ")", "%29").Replace(asset.URL)
		if !strings.Contains(parsed.MarkdownContent, "]("+ref+")") {
			return parsed, errors.New("ASSET_REFERENCE_MISSING")
		}
		parsed.MarkdownContent = strings.Replace(parsed.MarkdownContent, "]("+ref+")", "]("+processingAssetToken(asset.ID)+")", 1)
		parsed.Assets = append(parsed.Assets, processingAsset{ID: asset.ID, URL: asset.URL})
	}
	return parsed, nil
}

func (e *processingDocumentExecution) assetBarrier(ctx context.Context) (types.ProcessingOutcome, error) {
	var parsed processingParsed
	if err := e.dependency(ctx, "parse", "body", "parse", &parsed); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if !e.lease.Step.PlanSealed && len(parsed.Assets) > 0 {
		var specs []types.ProcessingStepSpec
		for _, asset := range parsed.Assets {
			input, _ := json.Marshal(map[string]string{"asset_id": asset.ID})
			specs = append(specs, types.ProcessingStepSpec{Stage: "asset_download", UnitKey: processingAssetUnit(asset.ID), Phase: e.lease.Step.Phase, Input: input, InputFingerprint: fmt.Sprintf("%x", sha256.Sum256(input))})
		}
		next := time.Now().UTC().Add(time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}, nil
	}
	var total int64
	for i, asset := range parsed.Assets {
		var stored processingAsset
		if err := e.dependency(ctx, "asset_download", processingAssetUnit(asset.ID), "asset", &stored); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if stored.ID != asset.ID || stored.Bytes <= 0 || stored.Bytes > processingImageLimit || stored.StoredURL == "" || len(stored.Digest) != 64 {
			return types.ProcessingOutcome{}, errors.New("ASSET_MANIFEST_INVALID")
		}
		total += stored.Bytes
		if total > processingArtifactLimit {
			return types.ProcessingOutcome{}, &tencentdocs.NativeLimitError{Kind: "media", LimitBytes: processingArtifactLimit, ObservedAtLeastBytes: total}
		}
		// Confirm that stored media still exists and matches its manifest.
		if _, err := e.readStoredAsset(ctx, stored); err != nil {
			return types.ProcessingOutcome{}, err
		}
		parsed.MarkdownContent = strings.ReplaceAll(parsed.MarkdownContent, processingAssetToken(asset.ID), stored.StoredURL)
		parsed.Assets[i] = stored
	}
	if strings.Contains(parsed.MarkdownContent, "](asset:") {
		return types.ProcessingOutcome{}, errors.New("ASSET_COVERAGE_INCOMPLETE")
	}
	out, err := e.success(ctx, "assets", parsed)
	out.SealPlan = !e.lease.Step.PlanSealed
	out.Completeness = "complete"
	out.Result, _ = json.Marshal(map[string]any{"images": len(parsed.Assets), "media_bytes": total, "ocr_requested": e.kb.IsMultimodalEnabled()})
	return out, err
}

func processingImageFormat(data []byte) (string, error) {
	if len(data) > processingImageLimit {
		n := int64(len(data))
		return "", &tencentdocs.NativeLimitError{Kind: "image", LimitBytes: processingImageLimit, ActualBytes: &n}
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil && http.DetectContentType(data) == "image/webp" {
		return "webp", nil
	}
	if err != nil || config.Width <= 0 || config.Height <= 0 {
		return "", errors.New("IMAGE_FORMAT_INVALID")
	}
	if int64(config.Width)*int64(config.Height) > 100000000 {
		return "", errors.New("IMAGE_PIXEL_LIMIT_EXCEEDED")
	}
	switch format {
	case "png", "gif", "jpeg", "webp":
		return format, nil
	}
	return "", errors.New("IMAGE_FORMAT_UNSUPPORTED")
}

func (e *processingDocumentExecution) readStoredAsset(ctx context.Context, asset processingAsset) ([]byte, error) {
	stream, err := e.artifacts.files.GetFile(ctx, asset.StoredURL)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, processingImageLimit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != asset.Bytes || fmt.Sprintf("%x", sha256.Sum256(data)) != asset.Digest {
		return nil, errors.New("ASSET_CHECKSUM_MISMATCH")
	}
	return data, nil
}

func (e *processingDocumentExecution) downloadAsset(ctx context.Context, fetch func(context.Context, processingAsset) ([]byte, error)) (types.ProcessingOutcome, error) {
	var input struct {
		ID string `json:"asset_id"`
	}
	if err := json.Unmarshal(e.lease.Step.Input, &input); err != nil || input.ID == "" || processingAssetUnit(input.ID) != e.lease.Step.UnitKey {
		return types.ProcessingOutcome{}, errors.New("ASSET_INPUT_INVALID")
	}
	var parsed processingParsed
	if err := e.dependency(ctx, "parse", "body", "parse", &parsed); err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, asset := range parsed.Assets {
		if asset.ID != input.ID {
			continue
		}
		data, err := fetch(ctx, asset)
		if err != nil {
			var status *docparser.ImageDownloadStatusError
			if errors.As(err, &status) && (status.StatusCode == 403 || status.StatusCode == 404 || status.StatusCode == 410) {
				return types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "transient", ErrorCode: "ASSET_URL_EXPIRED", Message: "The temporary image address must be resolved again from its source identity", Retryable: true}, nil
			}
			return types.ProcessingOutcome{}, err
		}
		format, err := processingImageFormat(data)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(data))
		stored, err := e.artifacts.files.SaveBytes(ctx, data, e.lease.Job.TenantID, "processing-image-"+digest+"."+format, false)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if e.artifacts.catalog != nil {
			if err := e.artifacts.catalog.Bind(ctx, stored, "processing_job", e.lease.Job.ID, "source_image"); err != nil {
				return types.ProcessingOutcome{}, err
			}
		}
		asset = processingAsset{ID: asset.ID, StoredURL: stored, Digest: digest, Bytes: int64(len(data))}
		if _, err := e.readStoredAsset(ctx, asset); err != nil {
			return types.ProcessingOutcome{}, err
		}
		out, err := e.success(ctx, "asset", asset)
		out.Result, _ = json.Marshal(map[string]any{"asset_id": asset.ID, "bytes": asset.Bytes, "sha256": digest})
		return out, err
	}
	return types.ProcessingOutcome{}, errors.New("ASSET_IDENTITY_MISSING")
}

func (e *processingDocumentExecution) fetchAsset(ctx context.Context, asset processingAsset) ([]byte, error) {
	if asset.Source.Path != "" {
		for _, step := range e.steps {
			if step.Stage == "parse" && step.UnitKey == "body" && step.Status == types.ProcessingSucceeded {
				return e.artifacts.Read(ctx, e.lease.Job, step, "asset_source", asset.Source.Path, asset.Source.Digest)
			}
		}
		return nil, errors.New("ASSET_SOURCE_UNCONFIRMED")
	}
	address := asset.URL
	if e.lease.Ref.Attempt > 1 {
		// Temporary addresses are resolved from the same source revision on retry.
		source, err := e.sources.FindByID(ctx, e.lease.Job.DataSourceID)
		if err != nil {
			return nil, err
		}
		if source.TenantID != e.lease.Job.TenantID || source.KnowledgeBaseID != e.kb.ID {
			return nil, errors.New("ASSET_SOURCE_SCOPE_INVALID")
		}
		cfg, err := source.ParseConfig()
		if err != nil {
			return nil, err
		}
		client, err := tencentdocs.NewConfiguredMCPClient(cfg)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		snapshot, err := tencentdocs.CollectNative(ctx, e.document.FileID, e.document.Kind, func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
			return client.ReadNative(tencentdocs.WithManagedRetries(ctx), tool, args)
		})
		if err != nil {
			return nil, err
		}
		if snapshot.RevisionKey != e.lease.Job.SourceRevision {
			return nil, errors.New("SOURCE_CHANGED_DURING_READ")
		}
		normalized, err := tencentdocs.NormalizeNativeSnapshot(snapshot)
		if err != nil {
			return nil, err
		}
		address = ""
		for _, fresh := range normalized.Assets {
			if fresh.ID == asset.ID {
				if address != "" {
					return nil, errors.New("ASSET_IDENTITY_INVALID")
				}
				address = fresh.URL
			}
		}
		if address == "" {
			return nil, errors.New("ASSET_IDENTITY_MISSING")
		}
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "https" {
		return nil, errors.New("ASSET_URL_INVALID")
	}
	data, err := docparser.DownloadProcessingImage(ctx, address)
	var limit *docparser.ImageDownloadLimitError
	if errors.As(err, &limit) {
		return nil, &tencentdocs.NativeLimitError{Kind: "image", LimitBytes: processingImageLimit, ActualBytes: limit.ActualBytes, ObservedAtLeastBytes: limit.ObservedAtLeastBytes}
	}
	return data, err
}
