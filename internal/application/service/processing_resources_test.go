package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingResourceOwnershipFencesLateWritesAndPreservesOtherBindings(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}))
	require.NoError(t, db.Create(&types.Tenant{ID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.AutoMigrate(&types.StoredResource{}, &types.ResourceBinding{}))
	r := repository.NewProcessingRepository(db)
	catalog := NewResourceCatalog(repository.NewResourceRepository(db))
	fs := files.NewResourceCatalogFileService(files.NewLocalFileService(t.TempDir(), ""), catalog)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"}
	job, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "input"}}))
	claim := func(job *types.ProcessingJob, step types.ProcessingStep) *types.ProcessingLease {
		t.Helper()
		lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		return lease
	}
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	lease := claim(job, steps[0])
	work := types.WithProcessingLease(ctx, *lease)
	private, err := fs.SaveBytes(work, []byte("private synthetic output"), 1, "private.txt", false)
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(work, private, "processing_job", job.ID, "stage_artifact"))
	resource, err := catalog.Resolve(ctx, private)
	require.NoError(t, err)
	require.Equal(t, job.ID, resource.CreationJobID, "registration must retain ownership even if a worker crashes before Bind")
	shared, err := fs.SaveBytes(work, []byte("shared synthetic output"), 1, "shared.txt", false)
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(work, shared, "processing_job", job.ID, "source_image"))
	require.NoError(t, catalog.Bind(ctx, shared, "knowledge", "another-knowledge", "attachment"))
	input.SourceRevision = "v2"
	_, err = r.EnsureJob(ctx, input)
	require.NoError(t, err)
	job, _ = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "retire", "operator"))
	steps, err = r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	var cleanup *types.ProcessingLease
	for _, step := range steps {
		if step.Stage == "retire" {
			cleanup = claim(job, step)
		}
	}
	require.NotNil(t, cleanup)
	_, err = fs.SaveBytes(work, []byte("late output"), 1, "late.txt", false)
	require.Error(t, err, "a stopped producer cannot register a late file")
	require.Error(t, catalog.Bind(work, private, "processing_job", job.ID, "late_relation"))
	pending, err := r.RetirementResources(ctx, 1, *cleanup)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, resource.ID, pending[0].ID)
	require.Error(t, catalog.Bind(ctx, private, "knowledge", "too-late", "attachment"), "deletion state excludes every new reference")
	reader, err := fs.GetFile(ctx, shared)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.NoError(t, fs.DeleteFile(ctx, pending[0].PhysicalPath))
	require.NoError(t, r.RetirementResourceDeleted(ctx, 1, *cleanup, pending[0].ID))
	require.NoError(t, r.RetirementResourceDeleted(ctx, 1, *cleanup, pending[0].ID))
	pending, err = r.RetirementResources(ctx, 1, *cleanup)
	require.NoError(t, err)
	require.Empty(t, pending)
	require.NoError(t, r.FinishStep(ctx, 1, *cleanup, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "ledger:retirement", OutputDigest: "verified"}))
	resource, err = catalog.Resolve(ctx, shared)
	require.NoError(t, err)
	require.Equal(t, types.ResourceStateActive, resource.State)
}

func TestProcessingRetainedVersionsKeepStorageAndIndexDestinations(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.StorageBackend{}, &types.Tenant{}))
	ctx := context.Background()
	backend := &types.StorageBackend{ID: "old-storage", TenantID: 1, Name: "synthetic", Provider: "local", Status: types.StorageBackendStatusActive}
	require.NoError(t, db.Create(backend).Error)
	resource := &types.StoredResource{ID: "old-resource", Handle: "1234567890123456789012", TenantID: 1, Provider: "local", PhysicalPath: "local://1/synthetic", StorageBackendID: backend.ID, State: "deleting"}
	require.NoError(t, db.Create(resource).Error)
	storage := NewStorageBackendService(repository.NewStorageBackendRepository(db), db)
	require.Error(t, storage.Delete(ctx, 1, backend.ID), "cleanup in progress must retain its backend")
	backend.Status = types.StorageBackendStatusDisabled
	require.Error(t, storage.Update(ctx, backend))
	require.NoError(t, db.Model(resource).Update("state", types.ResourceStateDeleted).Error)
	require.NoError(t, storage.Delete(ctx, 1, backend.ID))

	vector, vectorDB, _ := newGuardTestService(t)
	storeID := "retained-vector"
	require.NoError(t, vectorDB.Create(&types.VectorStore{ID: storeID, TenantID: 1, Name: "synthetic", EngineType: types.PostgresRetrieverEngineType}).Error)
	encoded, _ := json.Marshal(types.ProcessingIndexDestination{VectorStoreID: &storeID, Dimension: 2, Kinds: []types.RetrieverType{types.VectorRetrieverType}, KnowledgeType: types.KnowledgeBaseTypeDocument})
	job := &types.ProcessingJob{ID: "retained-job", TenantID: 1, RetirementState: "deleting", IndexDestination: encoded}
	require.NoError(t, vectorDB.Create(job).Error)
	require.Error(t, vector.DeleteStore(ctx, 1, storeID))
	require.NoError(t, vectorDB.Model(job).Update("retirement_state", "deleted").Error)
	require.NoError(t, vector.DeleteStore(ctx, 1, storeID))
}
