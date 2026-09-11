package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/models/utils/ollama"
	"github.com/Tencent/WeKnora/internal/models/vlm"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

// Keep all OCR/caption text while bounding each embedding input. Plain rune
// slices cannot drop Markdown/table content or split a UTF-8 character.
// ponytail: fixed rune boundaries; add semantic overlap if retrieval needs it.
func processingImageTextParts(text string) []string {
	runes := []rune(text)
	var parts []string
	for start := 0; start < len(runes); start += 4096 {
		parts = append(parts, string(runes[start:min(start+4096, len(runes))]))
	}
	return parts
}

func (e *processingDocumentExecution) imageText(ctx context.Context) (types.ProcessingOutcome, error) {
	var input struct {
		ID string `json:"asset_id"`
	}
	if err := json.Unmarshal(e.lease.Step.Input, &input); err != nil || input.ID == "" || processingAssetUnit(input.ID) != e.lease.Step.UnitKey {
		return types.ProcessingOutcome{}, errors.New("ASSET_INPUT_INVALID")
	}
	var asset processingAsset
	if err := e.dependency(ctx, "asset_download", e.lease.Step.UnitKey, "asset", &asset); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if asset.ID != input.ID {
		return types.ProcessingOutcome{}, errors.New("ASSET_IDENTITY_INVALID")
	}
	data, err := e.readStoredAsset(ctx, asset)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	cfg := ResolveProcessConfig(e.kb, nil).VLMConfig
	if !cfg.IsEnabled() {
		return types.ProcessingOutcome{}, errors.New("VLM_NOT_ENABLED")
	}
	var model vlm.VLM
	if cfg.ModelID != "" {
		model, err = e.s.modelService.GetVLMModel(ctx, cfg.ModelID)
	} else {
		var local *ollama.OllamaService
		if models, ok := e.s.modelService.(*modelService); ok {
			local = models.ollamaService
		}
		model, err = vlm.NewVLMFromLegacyConfig(cfg, local)
	}
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	prompt := buildVLMCaptionPrompt(ctx, cfg)
	if e.lease.Step.Stage == "image_ocr" {
		prompt = types.AppendCustomPromptInstructions(vlmOCRPrompt, cfg.CustomInstructions, "image_ocr")
	}
	text, err := model.Predict(ctx, [][]byte{data}, prompt)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return types.ProcessingOutcome{}, errors.New("MODEL_OUTPUT_EMPTY")
	}
	if len(text) > 16<<20 {
		return types.ProcessingOutcome{}, errors.New("IMAGE_MODEL_OUTPUT_INVALID")
	}
	empty := false
	if e.lease.Step.Stage == "image_ocr" {
		// Only the prompt's explicit no-text answer proves an image contains no
		// text. Malformed/empty model output cannot silently pass as complete.
		empty = strings.EqualFold(strings.TrimSuffix(text, "."), "No text content")
		text = sanitizeOCRText(text)
		if text == "" && !empty {
			return types.ProcessingOutcome{}, errors.New("IMAGE_OCR_OUTPUT_INVALID")
		}
	}
	out, err := e.success(ctx, e.lease.Step.Stage, text)
	out.Result, _ = json.Marshal(map[string]any{"asset_id": asset.ID, "characters": len([]rune(text)), "no_text": empty})
	return out, err
}

func (e *processingDocumentExecution) imageBarrier(ctx context.Context) (types.ProcessingOutcome, error) {
	var parsed processingParsed
	if err := e.dependency(ctx, "assets", "body", "assets", &parsed); err != nil {
		return types.ProcessingOutcome{}, err
	}
	enabled := e.kb.IsMultimodalEnabled()
	if !e.lease.Step.PlanSealed && enabled && len(parsed.Assets) > 0 {
		var specs []types.ProcessingStepSpec
		for _, asset := range parsed.Assets {
			input, _ := json.Marshal(map[string]string{"asset_id": asset.ID})
			for _, stage := range []string{"image_ocr", "image_caption"} {
				specs = append(specs, types.ProcessingStepSpec{Stage: stage, UnitKey: processingAssetUnit(asset.ID), Phase: e.lease.Step.Phase, Input: input, InputFingerprint: processingFingerprint(e.lease.Step.InputFingerprint, stage, asset.ID, asset.Digest)})
			}
		}
		next := time.Now().UTC().Add(time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}, nil
	}
	var results []*types.Chunk
	if enabled && len(parsed.Assets) > 0 {
		texts, err := e.indexChunks(ctx, "chunk")
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		for _, asset := range parsed.Assets {
			parent := ""
			for _, chunk := range texts {
				if strings.Contains(chunk.Content, asset.StoredURL) {
					parent = chunk.ID
					break
				}
			}
			if parent == "" {
				return types.ProcessingOutcome{}, errors.New("IMAGE_CHUNK_MAPPING_MISSING")
			}
			var ocr, caption string
			unit := processingAssetUnit(asset.ID)
			if err := e.dependency(ctx, "image_ocr", unit, "image_ocr", &ocr); err != nil {
				return types.ProcessingOutcome{}, err
			}
			if err := e.dependency(ctx, "image_caption", unit, "image_caption", &caption); err != nil {
				return types.ProcessingOutcome{}, err
			}
			info, _ := json.Marshal([]types.ImageInfo{{URL: asset.StoredURL, OriginalURL: asset.StoredURL, OCRText: ocr, Caption: caption}})
			stamp, _ := json.Marshal(map[string]string{"processing_job_id": e.lease.Job.ID, "processing_step_id": e.lease.Step.ID, "processing_attempt": fmt.Sprint(e.lease.Ref.Attempt), "asset_id": asset.ID})
			for _, item := range []struct{ kind, text string }{{types.ChunkTypeImageOCR, ocr}, {types.ChunkTypeImageCaption, caption}} {
				if item.text == "" {
					continue
				}
				parts := processingImageTextParts(item.text)
				for partIndex, part := range parts {
					key := fmt.Sprintf("%s/%d/%s/%s", e.lease.Step.ID, e.lease.Ref.Attempt, asset.ID, item.kind)
					partInfo := info
					if len(parts) > 1 {
						key += fmt.Sprintf("/%d", partIndex)
						image := types.ImageInfo{URL: asset.StoredURL, OriginalURL: asset.StoredURL}
						if item.kind == types.ChunkTypeImageOCR {
							image.OCRText = part
						} else {
							image.Caption = part
						}
						partInfo, _ = json.Marshal([]types.ImageInfo{image})
					}
					id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(key)).String()
					results = append(results, &types.Chunk{ID: id, TenantID: e.lease.Job.TenantID, KnowledgeBaseID: e.kb.ID, KnowledgeID: e.lease.Job.KnowledgeID,
						Content: part, SourceContent: part, ChunkIndex: partIndex, ChunkType: item.kind, ParentChunkID: parent, ImageInfo: string(partInfo), IsEnabled: true, Flags: types.ChunkFlagRecommended,
						Status: int(types.ChunkStatusStored), IndexStatus: "processing", Metadata: stamp})
				}
			}
		}
	}
	out, err := e.success(ctx, "images", results)
	out.Chunks = results
	out.SealPlan = !e.lease.Step.PlanSealed
	out.Result, _ = json.Marshal(map[string]any{"images": len(parsed.Assets), "chunks": len(results), "requested": enabled})
	return out, err
}
