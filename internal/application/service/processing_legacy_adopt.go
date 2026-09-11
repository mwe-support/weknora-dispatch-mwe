package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func processingLegacyPlan(kb *types.KnowledgeBase, fingerprint string) ([]types.ProcessingStepSpec, error) {
	ordinary, err := ProcessingDocumentPlan(kb, "resource", fingerprint)
	if err != nil {
		return nil, err
	}
	plan := []types.ProcessingStepSpec{
		{Stage: "legacy_snapshot", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: processingFingerprint(fingerprint, "legacy_snapshot"), RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "legacy_indexes", UnitKey: "body", Kind: "barrier", Phase: types.ProcessingPhasePrepare, InputFingerprint: processingFingerprint(fingerprint, "legacy_indexes"), DependsOn: []string{"legacy_snapshot/body"}, RequiredForReady: true, RequiredForCompletion: true},
	}
	for _, step := range ordinary {
		if step.Phase == types.ProcessingPhasePrepare {
			continue
		}
		if step.Stage == "publish" {
			step.DependsOn = []string{"legacy_indexes/body"}
		}
		if step.Stage == "retire_previous" {
			step.DependsOn = append(step.DependsOn, "legacy_retire_indexes/body")
		}
		plan = append(plan, step)
	}
	plan = append(plan, types.ProcessingStepSpec{Stage: "legacy_retire_indexes", UnitKey: "body", Phase: types.ProcessingPhaseProjection, InputFingerprint: processingFingerprint(fingerprint, "legacy_retire_indexes"), DependsOn: []string{"publish/body"}, RequiredForCompletion: true})
	return plan, nil
}

func AdoptLegacyCandidate(ctx context.Context, knowledge interfaces.KnowledgeService, repo *repository.ProcessingRepository, evidence types.ProcessingLegacyEvidence) (*types.ProcessingJob, error) {
	s, ok := knowledge.(*knowledgeService)
	if !ok || evidence.Action != "candidate_adopted" {
		return nil, errors.New("LEGACY_ADOPTION_UNAVAILABLE")
	}
	if result, err := repo.LegacyAdoptionForOperation(ctx, evidence); err != nil || result != nil {
		return result, err
	}
	source, err := repo.LegacySource(ctx, evidence.TenantID, evidence.KnowledgeBaseID, evidence.DataSourceID)
	if err != nil {
		return nil, err
	}
	if !ProcessingLifecycleEnabled(source) {
		return nil, errors.New("LEGACY_SOURCE_NOT_ENROLLED")
	}
	original, err := repo.InspectLegacyError(ctx, evidence.ProcessingLegacyIdentity)
	if err != nil {
		return nil, err
	}
	if original.Identity != evidence.ProcessingLegacyIdentity {
		return nil, repository.ErrProcessingConflict
	}
	configuration, err := source.ParseConfig()
	if err != nil {
		return nil, err
	}
	if original.Error.SourceResourceID == "" || !slices.Contains(configuration.ResourceIDs, original.Error.SourceResourceID) {
		return nil, errors.New("LEGACY_SOURCE_SELECTOR_UNVERIFIED")
	}
	scope, auth, err := repository.ProcessingSourceRevisions(source)
	if err != nil {
		return nil, err
	}
	before, err := repo.ConfigurationRevision(ctx, evidence.TenantID, evidence.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	evidence.ConfigurationRevision = before
	snapshot, err := repo.LegacySnapshot(ctx, evidence.ProcessingLegacyIdentity, evidence.KnowledgeID, evidence.SourceRevision, evidence.Attempt)
	if err != nil {
		return nil, err
	}
	if snapshot.Digest != evidence.SnapshotDigest {
		return nil, repository.ErrProcessingConflict
	}
	member, err := readLegacyMembership(ctx, configuration, original.Error.SourceResourceID, snapshot.Knowledge.GetMetadata()["file_id"], snapshot.Knowledge.GetMetadata()["external_id"])
	if err != nil {
		return nil, err
	}
	if member.Title != snapshot.Knowledge.Title || types.NormalizeKnowledgeFolderPath(member.FolderPath) != snapshot.Knowledge.FolderPath {
		return nil, errors.New("LEGACY_SOURCE_METADATA_CHANGED")
	}
	ctx = context.WithValue(ctx, types.TenantIDContextKey, evidence.TenantID)
	tenant, err := s.tenantRepo.GetTenantByID(ctx, evidence.TenantID)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, evidence.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	manifest, _, err := s.verifyLegacySnapshot(ctx, repo, kb, snapshot, true)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	evidence.ArtifactDigest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	k := snapshot.Knowledge
	document := ProcessingDocumentSpec{FileID: k.GetMetadata()["file_id"], Kind: "resource", Title: k.Title, FolderPath: k.FolderPath, RevisionMode: "legacy_snapshot", Language: types.LanguageFromContextOrDefault(ctx), LegacyResourceID: original.Error.SourceResourceID, LegacyMembership: member}
	document.LegacyIndexDestination = &manifest.LegacyDestination
	if document.FileID == "" || (evidence.FileID != "" && document.FileID != evidence.FileID) {
		return nil, repository.ErrProcessingScope
	}
	metadata, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: evidence.TenantID, KnowledgeBaseID: kb.ID, DataSourceID: source.ID, ExternalID: k.GetMetadata()["external_id"], SourceRevision: evidence.SourceRevision, SourceDigest: evidence.ArtifactDigest, ConfigurationRevision: before, PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: metadata}
	input.ScopeRevision, input.AuthRevision = scope, auth
	input.IndexDestination, err = json.Marshal(manifest.Destination)
	if err != nil {
		return nil, err
	}
	resources := []types.ProcessingLegacyResource{{Reference: k.FilePath, Bytes: manifest.FileBytes, Digest: manifest.FileDigest, Kind: "file"}}
	for _, asset := range manifest.Assets {
		resources = append(resources, types.ProcessingLegacyResource{Reference: asset.StoredURL, Bytes: asset.Bytes, Digest: asset.Digest, Kind: "image"})
	}
	plan, err := processingLegacyPlan(kb, processingFingerprint(input.PipelineFingerprint, before, evidence.ArtifactDigest))
	if err != nil {
		return nil, err
	}
	return repo.AdoptLegacyCandidate(ctx, evidence, input, plan, resources)
}

func (e *processingDocumentExecution) snapshotLegacy(ctx context.Context) (types.ProcessingOutcome, error) {
	if err := e.verifyLegacyMembership(ctx); err != nil {
		return types.ProcessingOutcome{}, err
	}
	snapshot, evidence, err := e.repo.AdoptedLegacySnapshot(ctx, e.lease)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	manifest, body, err := e.s.verifyLegacySnapshot(ctx, e.repo, e.kb, snapshot, true)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(encoded)) != evidence.ArtifactDigest {
		return types.ProcessingOutcome{}, errors.New("LEGACY_ARTIFACTS_CHANGED_AFTER_ADMISSION")
	}
	if err := e.repo.RecordIndexDestination(ctx, e.lease.Job.TenantID, e.lease, manifest.Destination); err != nil {
		return types.ProcessingOutcome{}, err
	}
	manifest.SourceFile.Path, manifest.SourceFile.Digest, err = e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, "source_file", body)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if err := e.bindLegacyObject(ctx, snapshot.Knowledge.FilePath, manifest.FileBytes, manifest.FileDigest, "file"); err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, asset := range manifest.Assets {
		if err := e.bindLegacyObject(ctx, asset.StoredURL, asset.Bytes, asset.Digest, "image"); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	out, err := e.success(ctx, "legacy_snapshot", manifest)
	out.Completeness = "complete"
	out.Result, _ = json.Marshal(map[string]any{"verification": "legacy_artifacts", "chunks": len(manifest.Chunks), "indexes": len(manifest.Indexes), "images": len(manifest.Assets), "bytes": manifest.FileBytes, "sha256": manifest.FileDigest})
	return out, err
}

func readLegacyMembership(ctx context.Context, configuration *types.DataSourceConfig, root, file, external string) (*tencentdocs.NativeScanEntry, error) {
	if root == "" || !slices.Contains(configuration.ResourceIDs, root) {
		return nil, errors.New("LEGACY_SOURCE_SELECTOR_UNVERIFIED")
	}
	client, err := tencentdocs.NewConfiguredMCPClient(configuration)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return tencentdocs.ReadNativeMembership(tencentdocs.WithManagedRetries(ctx), root, file, external, client.ReadScan)
}

func (e *processingDocumentExecution) verifyLegacyMembership(ctx context.Context) error {
	if e.document.LegacyMembership == nil {
		return errors.New("LEGACY_MEMBERSHIP_PROOF_REQUIRED")
	}
	source, err := e.repo.LegacySource(ctx, e.lease.Job.TenantID, e.kb.ID, e.lease.Job.DataSourceID)
	if err != nil {
		return err
	}
	configuration, err := source.ParseConfig()
	if err != nil {
		return err
	}
	actual, err := readLegacyMembership(ctx, configuration, e.document.LegacyResourceID, e.document.FileID, e.lease.Job.ExternalID)
	if err != nil {
		return err
	}
	expected, _ := json.Marshal(e.document.LegacyMembership)
	current, _ := json.Marshal(actual)
	if string(expected) != string(current) {
		return errors.New("LEGACY_SOURCE_METADATA_CHANGED")
	}
	return e.repo.ValidateLease(ctx, e.lease.Job.TenantID, e.lease)
}

func (e *processingDocumentExecution) bindLegacyObject(ctx context.Context, address string, size int64, digest, kind string) error {
	if e.artifacts.catalog == nil {
		return errors.New("LEGACY_RESOURCE_CATALOG_REQUIRED")
	}
	return e.repo.BindLegacyResource(ctx, e.lease, address, size, digest, kind)
}

func (e *processingDocumentExecution) retireLegacyIndexes(ctx context.Context) (types.ProcessingOutcome, error) {
	if !e.lease.Job.IsPublished || e.lease.Step.ExpectedPublicationEpoch != e.lease.Job.PublicationEpoch {
		return types.ProcessingOutcome{}, repository.ErrProcessingConflict
	}
	var manifest processingLegacyManifest
	if err := e.dependency(ctx, "legacy_snapshot", "body", "legacy_snapshot", &manifest); err != nil {
		return types.ProcessingOutcome{}, err
	}
	engine, err := processingIndexEngine(ctx, e.s, e.lease.Job.TenantID, manifest.LegacyDestination)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var ids []string
	for _, item := range manifest.Indexes {
		if item == nil || item.SourceID != item.ChunkID || item.KnowledgeID != e.lease.Job.KnowledgeID || item.KnowledgeBaseID != e.kb.ID {
			return types.ProcessingOutcome{}, errors.New("LEGACY_RETIREMENT_IDENTITY_INVALID")
		}
		ids = append(ids, item.SourceID)
	}
	for start := 0; start < len(ids); start += 128 {
		if err := engine.DeleteBySourceIDList(ctx, ids[start:min(start+128, len(ids))], manifest.LegacyDestination.Dimension, manifest.LegacyDestination.KnowledgeType); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	return e.success(ctx, "legacy_retire_indexes", map[string]any{"retired_indexes": len(ids)})
}
