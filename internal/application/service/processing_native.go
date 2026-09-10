package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
)

type processingArtifactRef struct {
	Path   string `json:"path"`
	Digest string `json:"digest"`
}

// Checkpoints are a linked sequence of immutable page objects. Appending one
// page does not rewrite all previous pages or grow the DB checkpoint field.
type processingNativePage struct {
	Previous processingArtifactRef      `json:"previous"`
	Revision string                     `json:"revision"`
	Request  string                     `json:"request"`
	Response tencentdocs.NativeResponse `json:"response"`
}

// ReadProcessingNative resumes only pages validated against this fixed source
// revision. Every revision/end-of-read verification still reaches the provider.
func ReadProcessingNative(ctx context.Context, repo *repository.ProcessingRepository, artifacts *ProcessingArtifacts,
	lease types.ProcessingLease, fileID, kind string, read tencentdocs.NativeReadFunc) (types.ProcessingOutcome, error) {
	var outcome types.ProcessingOutcome
	revision, err := tencentdocs.ProbeNativeRevision(ctx, fileID, kind, read)
	if err != nil {
		return outcome, err
	}
	if revision != lease.Job.SourceRevision {
		return outcome, errors.New("SOURCE_CHANGED_DURING_READ")
	}
	var checkpoint processingArtifactRef
	if lease.Step.CheckpointRef != "" {
		if err := json.Unmarshal([]byte(lease.Step.CheckpointRef), &checkpoint); err != nil {
			return outcome, errors.New("invalid native checkpoint reference")
		}
	}
	cached := map[string]tencentdocs.NativeResponse{}
	seen := map[string]bool{}
	cursor, savedBytes := checkpoint, int64(0)
	for cursor.Path != "" {
		if seen[cursor.Path] || len(seen) >= 10000 {
			return outcome, errors.New("invalid native checkpoint chain")
		}
		seen[cursor.Path] = true
		data, err := artifacts.Read(ctx, lease.Job, lease.Step, "native_checkpoint", cursor.Path, cursor.Digest)
		if err != nil {
			return outcome, err
		}
		var page processingNativePage
		if err := json.Unmarshal(data, &page); err != nil {
			return outcome, errors.New("invalid native checkpoint page")
		}
		if page.Revision != revision || page.Request == "" || !json.Valid(page.Response.Data) {
			return outcome, errors.New("native checkpoint input mismatch")
		}
		if _, exists := cached[page.Request]; exists {
			return outcome, errors.New("duplicate native checkpoint request")
		}
		savedBytes += max(int64(len(page.Response.Data)), page.Response.Bytes)
		if savedBytes > processingArtifactLimit {
			return outcome, errors.New("NATIVE_SIZE_EXCEEDED")
		}
		cached[page.Request] = page.Response
		cursor = page.Previous
	}
	snapshot, err := tencentdocs.CollectNative(ctx, fileID, kind, func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
		if verify {
			return read(ctx, tool, args, true)
		}
		encoded, err := json.Marshal([]any{tool, args})
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%x", sha256.Sum256(encoded))
		if page, exists := cached[key]; exists {
			return &page, nil
		}
		page, err := read(ctx, tool, args, false)
		if err != nil {
			return nil, err
		}
		if page == nil || !json.Valid(page.Data) {
			return nil, errors.New("native provider returned no valid page")
		}
		pageBytes := max(int64(len(page.Data)), page.Bytes)
		if pageBytes > 16<<20 || savedBytes+pageBytes > processingArtifactLimit {
			return nil, errors.New("NATIVE_SIZE_EXCEEDED")
		}
		encoded, err = json.Marshal(processingNativePage{Previous: checkpoint, Revision: revision, Request: key, Response: *page})
		if err != nil {
			return nil, err
		}
		path, digest, err := artifacts.Save(ctx, lease.Job, lease.Step, "native_checkpoint", encoded)
		if err != nil {
			return nil, err
		}
		next := processingArtifactRef{Path: path, Digest: digest}
		ref, err := json.Marshal(next)
		if err != nil {
			return nil, err
		}
		if err := repo.Heartbeat(ctx, lease.Job.TenantID, lease, 2*time.Minute, string(ref)); err != nil {
			return nil, err
		}
		checkpoint = next
		cached[key] = *page
		savedBytes += pageBytes
		return page, nil
	})
	if err != nil {
		return outcome, err
	}
	if snapshot.RevisionKey != revision {
		return outcome, errors.New("SOURCE_CHANGED_DURING_READ")
	}
	// A second fresh revision probe also covers edits between the initial probe
	// and the collector's first request, including edits with stale DOC mtime.
	after, err := tencentdocs.ProbeNativeRevision(ctx, fileID, kind, read)
	if err != nil {
		return outcome, err
	}
	if after != revision {
		return outcome, errors.New("SOURCE_CHANGED_DURING_READ")
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return outcome, err
	}
	path, digest, err := artifacts.Save(ctx, lease.Job, lease.Step, "native_snapshot", data)
	if err != nil {
		return outcome, err
	}
	result, err := json.Marshal(map[string]any{"kind": kind, "pages": len(snapshot.Pages), "units": snapshot.Units,
		"raw_bytes": snapshot.RawBytes, "revision": revision, "requires_export": snapshot.RequiresExport})
	if err != nil {
		return outcome, err
	}
	return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: path, OutputDigest: digest, Result: result}, nil
}
