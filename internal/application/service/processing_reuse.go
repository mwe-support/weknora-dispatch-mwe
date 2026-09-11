package service

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/Tencent/WeKnora/internal/types"
)

func (e *processingDocumentExecution) reusableBytes(ctx context.Context, kind string) ([]byte, types.ProcessingJob, types.ProcessingStep, bool, error) {
	if e.repo == nil || e.lease.Job.Generation < 2 {
		return nil, types.ProcessingJob{}, types.ProcessingStep{}, false, nil
	}
	job, step, found, err := e.repo.ReusableArtifact(ctx, e.lease.Job.TenantID, e.lease)
	if err != nil || !found {
		return nil, job, step, found, err
	}
	data, err := e.artifacts.Read(ctx, job, step, kind, step.OutputManifestRef, step.OutputDigest)
	return data, job, step, true, err
}

// Copy plaintext only after authenticating its original identity. New files
// carry the consumer's identity; retained references protect nested media.
// IDs and external writes are rebuilt for the current generation separately.
func (e *processingDocumentExecution) reuseOutput(ctx context.Context) (types.ProcessingOutcome, bool, error) {
	kind := ""
	switch e.lease.Step.Stage {
	case "native_read":
		kind = "native_snapshot"
	case "export_start":
		kind = "export_task"
	case "export_poll":
		kind = "export_ready"
	case "download":
		kind = "source_file"
	case "parse":
		kind = "parse"
	case "asset_download":
		kind = "asset"
	case "image_ocr", "image_caption":
		kind = e.lease.Step.Stage
	default:
		return types.ProcessingOutcome{}, false, nil
	}
	data, producer, step, found, err := e.reusableBytes(ctx, kind)
	if err != nil || !found {
		return types.ProcessingOutcome{}, found, err
	}
	if kind == "parse" {
		var parsed processingParsed
		if json.Unmarshal(data, &parsed) != nil {
			return types.ProcessingOutcome{}, true, errors.New("REUSE_PARSE_INVALID")
		}
		for i := range parsed.Assets {
			asset := &parsed.Assets[i]
			if asset.Source.Path == "" {
				continue
			}
			body, err := e.artifacts.Read(ctx, producer, step, "asset_source", asset.Source.Path, asset.Source.Digest)
			if err != nil {
				return types.ProcessingOutcome{}, true, err
			}
			asset.Source.Path, asset.Source.Digest, err = e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, "asset_source", body)
			if err != nil {
				return types.ProcessingOutcome{}, true, err
			}
		}
		data, err = json.Marshal(parsed)
		if err != nil {
			return types.ProcessingOutcome{}, true, err
		}
	}
	if kind == "asset" {
		var asset processingAsset
		if json.Unmarshal(data, &asset) != nil || processingAssetUnit(asset.ID) != e.lease.Step.UnitKey {
			return types.ProcessingOutcome{}, true, errors.New("REUSE_ASSET_INVALID")
		}
		if _, err := e.readStoredAsset(ctx, asset); err != nil {
			return types.ProcessingOutcome{}, true, err
		}
		if e.artifacts.catalog != nil {
			if err := e.artifacts.catalog.Bind(ctx, asset.StoredURL, "processing_job", e.lease.Job.ID, "source_image"); err != nil {
				return types.ProcessingOutcome{}, true, err
			}
		}
	}
	ref, digest, err := e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, kind, data)
	if err != nil {
		return types.ProcessingOutcome{}, true, err
	}
	result := map[string]any{}
	if len(step.Result) > 0 {
		_ = json.Unmarshal(step.Result, &result)
	}
	if result == nil {
		result = map[string]any{}
	}
	result["reused_from_job"], result["reused_from_step"] = producer.ID, step.ID
	encoded, _ := json.Marshal(result)
	return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: ref, OutputDigest: digest, Result: encoded, CopiedArtifact: true}, true, nil
}

func (e *processingDocumentExecution) reuseEmbeddings(ctx context.Context, items []*types.IndexInfo) (bool, error) {
	data, _, _, found, err := e.reusableBytes(ctx, e.lease.Step.Stage)
	if err != nil || !found {
		return found, err
	}
	var saved []*types.IndexInfo
	if json.Unmarshal(data, &saved) != nil || len(saved) != len(items) {
		return false, errors.New("REUSE_EMBEDDING_INVALID")
	}
	dimension := 0
	if e.kb.IsVectorEnabled() {
		model, err := e.s.modelService.GetEmbeddingModel(ctx, e.kb.EmbeddingModelID)
		if err != nil {
			return false, err
		}
		dimension = model.GetDimensions()
	}
	for i, item := range items {
		if item == nil || saved[i] == nil || saved[i].Content != item.Content || len(saved[i].PreparedEmbedding) != dimension {
			return false, errors.New("REUSE_EMBEDDING_INPUT_MISMATCH")
		}
		for _, value := range saved[i].PreparedEmbedding {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return false, errors.New("REUSE_EMBEDDING_INVALID")
			}
		}
		item.PreparedEmbedding = saved[i].PreparedEmbedding
	}
	return true, nil
}
