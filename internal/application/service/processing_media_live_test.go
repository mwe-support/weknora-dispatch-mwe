package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingMediaLive48Assets(t *testing.T) {
	if os.Getenv("PROCESSING_MEDIA_LIVE") != "1" {
		t.Skip("live synthetic media is opt-in")
	}
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	var credentials struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.NewDecoder(os.Stdin).Decode(&credentials))
	client, err := tencentdocs.NewTencentDocsMCPClient(tencentdocs.MCPClientConfig{Token: credentials.Token, Timeout: 45 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, types.TenantIDContextKey, uint64(1))
	snapshot, err := tencentdocs.CollectNative(ctx, "XrSLOJNFcOPo", "smartcanvas", func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
		return client.ReadNative(tencentdocs.WithManagedRetries(ctx), tool, args)
	})
	if err != nil {
		t.Fatal(tencentdocs.ProcessingFailure("native_read", err).ErrorCode)
	}
	normalized, err := tencentdocs.NormalizeNativeSnapshot(snapshot)
	require.NoError(t, err)
	require.Empty(t, normalized.Unsupported)
	require.Len(t, normalized.Assets, 48)
	require.Contains(t, normalized.Markdown, "MDX-MEDIA-END-2-7391")
	hashes := map[string]string{}
	expected, err := os.ReadFile("internal/datasource/connector/tencentdocs/testdata/lifecycle-media-hashes.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(expected, &hashes))
	e := processingDocumentExecution{kb: &types.KnowledgeBase{}, document: ProcessingDocumentSpec{FileID: "XrSLOJNFcOPo", Kind: "smartcanvas"}, artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil),
		lease: types.ProcessingLease{Job: types.ProcessingJob{ID: "synthetic-media-live", Generation: 1, TenantID: 1, SourceRevision: snapshot.RevisionKey}, Step: types.ProcessingStep{ID: "parse", Stage: "parse", UnitKey: "body", InputFingerprint: snapshot.RevisionKey}}}
	parsed, err := processingNativeParsed(*normalized)
	require.NoError(t, err)
	out, err := e.success(ctx, "parse", parsed)
	require.NoError(t, err)
	step := e.lease.Step
	step.Status = types.ProcessingSucceeded
	step.OutputManifestRef = out.OutputManifestRef
	step.OutputDigest = out.OutputDigest
	e.steps = append(e.steps, step)
	total := int64(0)
	for i, asset := range parsed.Assets {
		input, _ := json.Marshal(map[string]string{"asset_id": asset.ID})
		e.lease.Step = types.ProcessingStep{ID: processingAssetUnit(asset.ID), Stage: "asset_download", UnitKey: processingAssetUnit(asset.ID), Input: input, InputFingerprint: snapshot.RevisionKey}
		out, err = e.downloadAsset(ctx, func(ctx context.Context, asset processingAsset) ([]byte, error) {
			data, err := e.fetchAsset(ctx, asset)
			if err != nil {
				return nil, err
			}
			actual := fmt.Sprintf("%x", sha256.Sum256(data))
			require.Equal(t, hashes[normalized.Assets[i].Caption], actual, "provider must preserve the synthetic image bytes")
			total += int64(len(data))
			return data, nil
		})
		if err != nil {
			t.Logf("asset fetch error type=%T detail=%s", err, regexp.MustCompile(`https?://[^\s]+`).ReplaceAllString(err.Error(), "[URL]"))
			t.Fatal(tencentdocs.ProcessingFailure("asset_download", err).ErrorCode)
		}
		if out.Status != types.ProcessingSucceeded {
			t.Fatalf("asset outcome=%s code=%s", out.Status, out.ErrorCode)
		}
		step := e.lease.Step
		step.Status = types.ProcessingSucceeded
		step.OutputManifestRef = out.OutputManifestRef
		step.OutputDigest = out.OutputDigest
		e.steps = append(e.steps, step)
		if (i+1)%12 == 0 {
			t.Logf("verified media images=%d bytes=%d", i+1, total)
		}
	}
	e.lease.Step = types.ProcessingStep{ID: "assets", Stage: "assets", UnitKey: "body", InputFingerprint: snapshot.RevisionKey, PlanSealed: true}
	out, err = e.assetBarrier(ctx)
	require.NoError(t, err)
	require.Equal(t, "complete", out.Completeness)
	t.Logf("verified real native pages=%d assets=%d bytes=%d source_units=%d", len(snapshot.Pages), len(parsed.Assets), total, snapshot.Units)
}
