package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func processingLegacyFixture() *repository.ProcessingLegacySnapshot {
	k := types.Knowledge{ID: "legacy", TenantID: 1, KnowledgeBaseID: "kb", Title: "Title"}
	chunk := func(id, kind, text, parent string) *types.Chunk {
		return &types.Chunk{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("legacy-fixture/"+id)).String(), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkType: kind, Content: text, ParentChunkID: parent}
	}
	image := types.ImageInfo{URL: "minio://owned/synthetic.png", OCRText: "Image text", Caption: "Description"}
	info, _ := json.Marshal([]types.ImageInfo{image})
	parent := chunk("parent", types.ChunkTypeParentText, "Text ![image]("+image.URL+")", "")
	text := chunk("text", types.ChunkTypeText, parent.Content, parent.ID)
	ocr := chunk("ocr", types.ChunkTypeImageOCR, image.OCRText, text.ID)
	ocr.ImageInfo = string(info)
	caption := chunk("caption", types.ChunkTypeImageCaption, image.Caption, text.ID)
	caption.ImageInfo = string(info)
	snapshot := &repository.ProcessingLegacySnapshot{Knowledge: k, Chunks: []*types.Chunk{parent, text, ocr, caption}, Attempt: 1, Digest: strings.Repeat("a", 64)}
	snapshot.Spans = []types.KnowledgeProcessingSpan{
		{Kind: types.SpanKindRoot, Status: types.SpanStatusRunning},
		{Kind: types.SpanKindStage, Name: types.StageDocReader, Status: types.SpanStatusDone},
		{Kind: types.SpanKindStage, Name: types.StageChunking, Status: types.SpanStatusDone, Output: types.JSONMap{"chunks_written": 2}},
		{Kind: types.SpanKindStage, Name: types.StageEmbedding, Status: types.SpanStatusDone, Input: types.JSONMap{"chunks_to_embed": 1, "model_id": "embed", "dim": 2}, Output: types.JSONMap{"vectors_written": 1}},
		{Kind: types.SpanKindStage, Name: types.StageMultimodal, Status: types.SpanStatusDone, Input: types.JSONMap{"image_count": 1}},
		{Kind: types.SpanKindStage, Name: types.StagePostProcess, Status: types.SpanStatusSkipped},
		{Kind: types.SpanKindGeneration, Name: "multimodal.image[0]", Status: types.SpanStatusDone, Input: types.JSONMap{"image_url": image.URL}, Output: types.JSONMap{"chunks_created": 2}},
	}
	return snapshot
}

func TestProcessingLegacySnapshotUsesExactOldIndexAndMediaCoverage(t *testing.T) {
	snapshot := processingLegacyFixture()
	items, assets, err := processingLegacyInputs(snapshot, true)
	require.NoError(t, err)
	require.NoError(t, repository.VerifyLegacyStages(snapshot, true))
	require.Len(t, items, 3)
	require.Equal(t, "Title\nText ![image](minio://owned/synthetic.png)", items[0].Content)
	require.Equal(t, "Image text", items[1].Content)
	require.False(t, items[1].IsEnabled)
	require.Equal(t, []string{"minio://owned/synthetic.png"}, assets)
	for _, name := range []string{"missing-text", "missing-image", "changed-image-count", "missing-parent", "unregistered-reference", "missing-stage", "late-projection", "wrong-tenant", "extra-registered-image", "failed-descendant"} {
		t.Run(name, func(t *testing.T) {
			s := processingLegacyFixture()
			switch name {
			case "extra-registered-image":
				ref := "minio://owned/second.png"
				s.Chunks[0].Content += " ![extra](" + ref + ")"
				data, _ := json.Marshal([]types.ImageInfo{{URL: ref}})
				s.Chunks[0].ImageInfo = string(data)
			case "failed-descendant":
				s.Spans = append(s.Spans, types.KnowledgeProcessingSpan{Kind: types.SpanKindSubSpan, Name: "postprocess.graph.chunk[0]", Status: types.SpanStatusFailed})
			case "missing-text":
				s.Chunks = append(s.Chunks[:1], s.Chunks[2:]...)
			case "missing-image":
				s.Chunks = s.Chunks[:3]
			case "changed-image-count":
				s.Spans[4].Input["image_count"] = 2
			case "missing-parent":
				s.Chunks[1].ParentChunkID = "unknown"
			case "unregistered-reference":
				s.Chunks[0].Content += " ![x](minio://owned/other.png)"
			case "missing-stage":
				s.Spans = s.Spans[1:]
			case "late-projection":
				s.Chunks = append(s.Chunks, &types.Chunk{ID: "summary", TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "legacy", ChunkType: types.ChunkTypeSummary, Content: "summary"})
			case "wrong-tenant":
				s.Chunks[0].TenantID = 2
			}
			if name == "missing-stage" || name == "failed-descendant" {
				require.Error(t, repository.VerifyLegacyStages(s, true))
				return
			}
			_, _, err := processingLegacyInputs(s, true)
			require.Error(t, err)
		})
	}
}
