package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
)

func (e *processingDocumentExecution) scan(ctx context.Context) (types.ProcessingOutcome, error) {
	if e.lease.Step.Stage == "legacy_retry_coverage" {
		return e.legacyRetryCoverage(ctx)
	}
	if e.lease.Step.Stage == "scan_scope" {
		return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "SCAN_SCOPE_INVALID", Message: "Select a valid Tencent Docs source scope and start a new sync"}, nil
	}
	source, err := e.sources.FindByID(ctx, e.lease.Job.DataSourceID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if source.TenantID != e.lease.Job.TenantID || source.KnowledgeBaseID != e.lease.Job.KnowledgeBaseID {
		return types.ProcessingOutcome{}, repository.ErrProcessingScope
	}
	configuration, err := source.ParseConfig()
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	client, err := tencentdocs.NewConfiguredMCPClient(configuration)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	defer client.Close()
	ctx = tencentdocs.WithManagedRetries(ctx)
	read := func(ctx context.Context, tool string, args map[string]interface{}, _ bool) (*tencentdocs.NativeResponse, error) {
		return client.ReadNative(ctx, tool, args)
	}
	if e.lease.Step.Stage == "scan_document" {
		return e.scanDocument(ctx, read, client.ReadScan)
	}
	if e.lease.Step.Stage != "scan_page" {
		return types.ProcessingOutcome{}, errors.New("SCAN_STAGE_INVALID")
	}
	var request tencentdocs.NativeScanRequest
	if json.Unmarshal(e.lease.Step.Input, &request) != nil || !slices.Contains(configuration.ResourceIDs, request.ResourceID) {
		return types.ProcessingOutcome{}, errors.New("SCAN_SCOPE_INVALID")
	}
	return e.scanPage(ctx, request, func(ctx context.Context, _ string, _ map[string]interface{}, _ bool) (*tencentdocs.NativeResponse, error) {
		return client.ReadScan(ctx, request)
	})
}

func (e *processingDocumentExecution) scanPage(ctx context.Context, request tencentdocs.NativeScanRequest, read tencentdocs.NativeReadFunc) (types.ProcessingOutcome, error) {
	if e.lease.Step.PlanSealed {
		return e.success(ctx, "scan_coverage", map[string]any{"units": e.lease.Step.ExpectedUnits, "page_plan": e.lease.Step.PlanDigest})
	}
	var page *tencentdocs.NativeScanPage
	checkpoint := e.lease.Step.CheckpointRef
	if checkpoint != "" {
		var ref processingArtifactRef
		if json.Unmarshal([]byte(checkpoint), &ref) != nil {
			return types.ProcessingOutcome{}, errors.New("SCAN_CHECKPOINT_INVALID")
		}
		data, err := e.artifacts.Read(ctx, e.lease.Job, e.lease.Step, "scan_page", ref.Path, ref.Digest)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if err = json.Unmarshal(data, &page); err != nil || page == nil {
			return types.ProcessingOutcome{}, errors.New("SCAN_CHECKPOINT_INVALID")
		}
	} else {
		var err error
		page, err = tencentdocs.ReadNativeScanPage(ctx, request, read)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		encoded, _ := json.Marshal(page)
		path, digest, err := e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, "scan_page", encoded)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		ref, _ := json.Marshal(processingArtifactRef{Path: path, Digest: digest})
		checkpoint = string(ref)
		if err := e.repo.Heartbeat(ctx, e.lease.Job.TenantID, e.lease, 2*time.Minute, checkpoint); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	var children []types.ProcessingStepSpec
	var items []types.SyncRunItem
	for _, child := range page.Children {
		children = append(children, processingScanSpec("scan_page", e.lease.Step.ID, child, true))
	}
	if page.Next != nil {
		children = append(children, processingScanSpec("scan_page", e.lease.Step.ID, *page.Next, true))
	}
	for _, entry := range page.Entries {
		if entry.Disposition == "document" {
			allowed, err := e.legacyRetryAllows(entry)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			if !allowed {
				continue
			}
		}
		items = append(items, types.SyncRunItem{Kind: entry.Disposition, ExternalID: entry.ExternalID})
		if entry.Disposition == "document" {
			children = append(children, processingScanSpec("scan_document", e.lease.Step.ID, entry, false))
		}
	}
	if len(children) == 0 {
		outcome, err := e.success(ctx, "scan_coverage", map[string]any{"page_digest": page.Digest, "units": 0})
		outcome.SealPlan, outcome.DiscoveredItems, outcome.CheckpointRef = true, items, checkpoint
		return outcome, err
	}
	next := time.Now().UTC().Add(time.Second)
	return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: children, DiscoveredItems: items, CheckpointRef: checkpoint}, nil
}

func (e *processingDocumentExecution) scanDocument(ctx context.Context, read tencentdocs.NativeReadFunc, listing ...tencentdocs.NativeScanReadFunc) (types.ProcessingOutcome, error) {
	var entry tencentdocs.NativeScanEntry
	if json.Unmarshal(e.lease.Step.Input, &entry) != nil || entry.FileID == "" || entry.ExternalID == "" || entry.Disposition != "document" {
		return types.ProcessingOutcome{}, errors.New("SCAN_DOCUMENT_INPUT_INVALID")
	}
	if allowed, err := e.legacyRetryAllows(entry); err != nil || !allowed {
		return types.ProcessingOutcome{}, repository.ErrProcessingScope
	}
	response, err := read(ctx, "manage.query_file_info", map[string]interface{}{"file_id": entry.FileID}, true)
	if err != nil {
		if tencentdocs.NativeResourceFallback(entry, err) && len(listing) == 1 {
			return e.scanResource(ctx, entry, listing[0])
		}
		return types.ProcessingOutcome{}, err
	}
	var info tencentdocs.FileInfo
	if response == nil || json.Unmarshal(response.Data, &info) != nil || info.ID != entry.FileID || info.Status != "normal" || info.IsFolder {
		return types.ProcessingOutcome{}, errors.New("SCAN_IDENTITY_MISMATCH")
	}
	if (info.Type == "resource" || (types.IsSupportedKnowledgeFileExtension(info.Type) && !slices.Contains([]string{"doc", "sheet", "smartcanvas", "smartsheet"}, strings.ToLower(info.Type)))) && len(listing) == 1 {
		return e.scanResource(ctx, entry, listing[0])
	}
	plan, err := e.documentPlan(ctx, info.Type)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	latest := info
	revision, err := tencentdocs.ProbeNativeRevision(ctx, entry.FileID, info.Type, func(ctx context.Context, tool string, args map[string]interface{}, verify bool) (*tencentdocs.NativeResponse, error) {
		response, err := read(ctx, tool, args, verify)
		if err == nil && tool == "manage.query_file_info" {
			if response == nil || json.Unmarshal(response.Data, &latest) != nil {
				return nil, errors.New("SCAN_METADATA_INVALID")
			}
		}
		return response, err
	})
	versionlessDOC := info.Type == "doc" && errors.Is(err, tencentdocs.ErrDOCRevisionUnavailable)
	if err != nil && !versionlessDOC {
		return types.ProcessingOutcome{}, err
	}
	if latest.ID != info.ID || latest.Type != info.Type {
		return types.ProcessingOutcome{}, errors.New("SCAN_IDENTITY_MISMATCH")
	}
	// Keep the user-facing document location without copying provider tracking
	// parameters, fragments or signed download credentials into the job ledger.
	sourceURL := ""
	if parsed, err := url.Parse(latest.URL); err == nil && parsed.Scheme == "https" && parsed.Host == "docs.qq.com" && parsed.User == nil {
		query := url.Values{}
		for _, key := range []string{"resourceId", "mode"} {
			if value := parsed.Query().Get(key); value != "" {
				query.Set(key, value)
			}
		}
		parsed.RawQuery, parsed.Fragment = query.Encode(), ""
		sourceURL = parsed.String()
	}
	document := ProcessingDocumentSpec{FileID: entry.FileID, Kind: info.Type, Title: latest.Title, FolderPath: entry.FolderPath, URL: sourceURL}
	if versionlessDOC {
		// Fresh blank DOCs omit the provider version. Do not infer a stable
		// content revision from mtime: export once per scan and hash full bytes.
		document.RevisionMode, document.IdentityRevision = "export_snapshot", processingDOCIdentity(latest)
		revision = processingFingerprint("doc-export-snapshot", e.lease.Job.OriginRunID, e.lease.Job.ID, document.IdentityRevision)
	}
	metadata, _ := json.Marshal(document)
	outcome, err := e.success(ctx, "scan_document", map[string]string{"external_id": entry.ExternalID, "revision": revision})
	if err != nil {
		return outcome, err
	}
	outcome.AdmitDocuments = []types.ProcessingAdmission{{Job: types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: e.lease.Job.TenantID, KnowledgeBaseID: e.lease.Job.KnowledgeBaseID, DataSourceID: e.lease.Job.DataSourceID,
		ExternalID: entry.ExternalID, SourceRevision: revision, SourceDigest: fmt.Sprintf("%x", sha256.Sum256(metadata)), PipelineFingerprint: e.lease.Job.PipelineFingerprint, Metadata: metadata}, Steps: plan}}
	return outcome, nil
}

func processingDOCIdentity(metadata tencentdocs.FileInfo) string {
	return processingFingerprint(metadata.ID, metadata.Type, metadata.Title, metadata.ModifiedAt)
}

func (e *processingDocumentExecution) scanResource(ctx context.Context, entry tencentdocs.NativeScanEntry, read tencentdocs.NativeScanReadFunc) (types.ProcessingOutcome, error) {
	if err := tencentdocs.VerifyNativeResource(ctx, entry, read); err != nil {
		return types.ProcessingOutcome{}, err
	}
	plan, err := e.documentPlan(ctx, "resource")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	metadata, _ := json.Marshal(ProcessingDocumentSpec{FileID: entry.FileID, Kind: "resource", Title: entry.Title, FolderPath: entry.FolderPath, URL: entry.URL, Resource: &entry, RevisionMode: "export_snapshot"})
	// The provider exposes no content version for uploaded resources. A fresh
	// scan must export once; retries keep the same task and confirmed file.
	revision := fmt.Sprintf("%x", sha256.Sum256(append([]byte(e.lease.Job.OriginRunID+"\x00"), metadata...)))
	outcome, err := e.success(ctx, "scan_document", map[string]string{"external_id": entry.ExternalID, "revision": revision, "revision_mode": "export_snapshot"})
	if err != nil {
		return outcome, err
	}
	outcome.AdmitDocuments = []types.ProcessingAdmission{{Job: types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: e.lease.Job.TenantID, KnowledgeBaseID: e.lease.Job.KnowledgeBaseID, DataSourceID: e.lease.Job.DataSourceID, ExternalID: entry.ExternalID, SourceRevision: revision, SourceDigest: fmt.Sprintf("%x", sha256.Sum256(metadata)), PipelineFingerprint: e.lease.Job.PipelineFingerprint, Metadata: metadata}, Steps: plan}}
	return outcome, nil
}
