package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"

	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func processingTestImage(t *testing.T, n int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.RGBA{uint8(n), 21, 39, 255})
	var b bytes.Buffer
	require.NoError(t, png.Encode(&b, img))
	return b.Bytes()
}

func TestProcessingDOCXImagesKeepSeparateBinaryArtifacts(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	e := processingDocumentExecution{artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil), lease: types.ProcessingLease{Job: types.ProcessingJob{ID: "job", Generation: 1, TenantID: 1}, Step: types.ProcessingStep{ID: "parse", Stage: "parse", UnitKey: "body", InputFingerprint: "fixed"}}}
	pixels := processingTestImage(t, 13)
	input := types.ReadResult{MarkdownContent: `![one](<images/one a.png> "caption")`, ImageDirPath: "/private/parser/temp", ImageRefs: []types.ImageRef{{OriginalRef: "images/one a.png", ImageData: pixels}}}
	parsed, err := e.prepareParsedImages(ctx, input)
	require.NoError(t, err)
	require.Len(t, parsed.Assets, 1)
	require.Empty(t, parsed.ImageRefs)
	require.Empty(t, parsed.ImageDirPath)
	got, err := e.artifacts.Read(ctx, e.lease.Job, e.lease.Step, "asset_source", parsed.Assets[0].Source.Path, parsed.Assets[0].Source.Digest)
	require.NoError(t, err)
	require.Equal(t, pixels, got)
	require.Contains(t, parsed.MarkdownContent, processingAssetToken(parsed.Assets[0].ID))
	require.NotContains(t, parsed.MarkdownContent, "images/")
	input.ImageRefs[0].ImageData = nil
	_, err = e.prepareParsedImages(ctx, input)
	require.EqualError(t, err, "IMAGE_INLINE_DATA_MISSING")
	_, err = processingImageFormat(append(pixels, make([]byte, processingImageLimit)...))
	var limit *tencentdocs.NativeLimitError
	require.ErrorAs(t, err, &limit)
	require.NotNil(t, limit.ActualBytes)
}

func TestProcessingAssetsPreserve48ImagesAndResumeOnlyFailedDownload(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	e := processingDocumentExecution{kb: &types.KnowledgeBase{}, artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil),
		lease: types.ProcessingLease{Job: types.ProcessingJob{ID: "job", Generation: 1, TenantID: 1}, Step: types.ProcessingStep{ID: "parse", Stage: "parse", UnitKey: "body", InputFingerprint: "fixed"}}}
	normalized := tencentdocs.NativeNormalized{}
	for i := 0; i < 48; i++ {
		id := fmt.Sprintf("img-%02d", i)
		u := fmt.Sprintf("https://docs.qq.com/synthetic/%s?signature=private", id)
		normalized.Assets = append(normalized.Assets, tencentdocs.NativeAsset{ID: id, URL: u, Caption: id})
		normalized.Markdown += fmt.Sprintf("![%s](%s)\n", id, u)
	}
	parsed, err := processingNativeParsed(normalized)
	require.NoError(t, err)
	require.NotContains(t, parsed.MarkdownContent, "signature")
	save := func(out types.ProcessingOutcome) {
		require.Equal(t, types.ProcessingSucceeded, out.Status)
		step := e.lease.Step
		step.Status = out.Status
		step.OutputManifestRef = out.OutputManifestRef
		step.OutputDigest = out.OutputDigest
		e.steps = append(e.steps, step)
	}
	out, err := e.success(ctx, "parse", parsed)
	require.NoError(t, err)
	save(out)
	barrier := types.ProcessingStep{ID: "assets", Stage: "assets", UnitKey: "body", InputFingerprint: "fixed", Phase: types.ProcessingPhasePrepare}
	e.lease.Step = barrier
	out, err = e.assetBarrier(ctx)
	require.NoError(t, err)
	require.True(t, out.SealPlan)
	require.Len(t, out.ChildSteps, 48)
	specs := out.ChildSteps
	calls := map[string]int{}
	fetch := func(_ context.Context, asset processingAsset) ([]byte, error) {
		calls[asset.ID]++
		if asset.ID == "img-31" && calls[asset.ID] == 1 {
			return nil, context.DeadlineExceeded
		}
		var i int
		_, err := fmt.Sscanf(asset.ID, "img-%02d", &i)
		require.NoError(t, err)
		return processingTestImage(t, i), nil
	}
	for _, spec := range specs {
		require.NotContains(t, string(spec.Input), "signature")
		require.NotContains(t, string(spec.Input), "https:")
		e.lease.Step = types.ProcessingStep{ID: spec.UnitKey, Stage: spec.Stage, UnitKey: spec.UnitKey, ParentStepID: "assets", Input: spec.Input, InputFingerprint: spec.InputFingerprint}
		out, err = e.downloadAsset(ctx, fetch)
		if spec.UnitKey == processingAssetUnit("img-31") {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			out, err = e.downloadAsset(ctx, fetch)
		}
		require.NoError(t, err)
		save(out)
	}
	e.lease.Step = barrier
	e.lease.Step.PlanSealed = true
	out, err = e.assetBarrier(ctx)
	require.NoError(t, err)
	require.Equal(t, "complete", out.Completeness)
	data, err := e.artifacts.Read(ctx, e.lease.Job, e.lease.Step, "assets", out.OutputManifestRef, out.OutputDigest)
	require.NoError(t, err)
	var result processingParsed
	require.NoError(t, json.Unmarshal(data, &result))
	require.Len(t, result.Assets, 48)
	require.NotContains(t, result.MarkdownContent, "asset:")
	require.NotContains(t, result.MarkdownContent, "signature")
	require.Equal(t, 48, strings.Count(result.MarkdownContent, "!["))
	for id, n := range calls {
		if id == "img-31" {
			require.Equal(t, 2, n)
		} else {
			require.Equal(t, 1, n)
		}
	}
	e.steps = e.steps[:len(e.steps)-1]
	_, err = e.assetBarrier(ctx)
	require.Error(t, err, "missing image cannot be complete")
	normalized.Assets = append(normalized.Assets, normalized.Assets[0])
	_, err = processingNativeParsed(normalized)
	require.Error(t, err, "duplicate identity cannot hide an asset")
}
