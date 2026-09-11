package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestProcessingQuotaReservesBeforeConcurrentPhysicalWrites(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.StorageBackend{}))
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&types.Tenant{ID: 1, Name: "synthetic", StorageQuota: 100}).Error)
	r := repository.NewProcessingRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "quota", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "input"}}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	step := steps[0]
	lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	require.NoError(t, err)
	work := types.WithProcessingLease(ctx, *lease)
	catalog := NewResourceCatalog(repository.NewResourceRepository(db))
	directory := t.TempDir()
	physicalFiles := files.NewLocalFileService(directory, "")
	fs := files.NewResourceCatalogFileService(physicalFiles, catalog)
	start := make(chan struct{})
	var wg sync.WaitGroup
	refs, errs := make([]string, 2), make([]error, 2)
	for i, name := range []string{"one.bin", "two.bin"} {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			<-start
			refs[i], errs[i] = fs.SaveBytes(work, make([]byte, 60), 1, name, false)
		}(i, name)
	}
	close(start)
	wg.Wait()
	winners := 0
	var saved string
	for i, err := range errs {
		if err == nil {
			winners++
			saved = refs[i]
		} else {
			var quota *types.StorageQuotaExceededError
			require.True(t, errors.As(err, &quota), "%v", err)
		}
	}
	require.Equal(t, 1, winners, "both candidates must not spend the same remaining quota")
	var tenant types.Tenant
	require.NoError(t, db.First(&tenant, 1).Error)
	require.EqualValues(t, 60, tenant.StorageUsed)
	resource, err := catalog.Resolve(ctx, saved)
	require.NoError(t, err)
	require.NoError(t, fs.DeleteFile(ctx, saved))
	require.NoError(t, repository.NewResourceRepository(db).MarkDeleted(ctx, resource.ID))
	require.NoError(t, db.First(&tenant, 1).Error)
	require.Zero(t, tenant.StorageUsed)
	backend := &types.StorageBackend{ID: "quota-backend", TenantID: 1, Name: "synthetic", Provider: "local", Status: types.StorageBackendStatusActive}
	require.NoError(t, db.Create(backend).Error)
	reservation, err := catalog.ReserveStorage(work, 1, 1, false, backend.ID)
	require.NoError(t, err)
	storage := NewStorageBackendService(repository.NewStorageBackendRepository(db), db)
	require.ErrorContains(t, storage.Delete(ctx, 1, backend.ID), "reserved writes")
	backend.Status = types.StorageBackendStatusDisabled
	require.ErrorContains(t, storage.Update(ctx, backend), "reserved writes")
	require.NoError(t, catalog.ReleaseStorage(ctx, 1, reservation))
	require.NoError(t, catalog.ReleaseStorage(ctx, 1, reservation))
	require.NoError(t, storage.Delete(ctx, 1, backend.ID))

	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	artifacts := NewProcessingArtifacts(fs, catalog)
	_, _, err = artifacts.Save(work, lease.Job, lease.Step, "quota", make([]byte, 1000))
	var quota *types.StorageQuotaExceededError
	require.True(t, errors.As(err, &quota), "the temporary file also requires a reservation")
	entries, err := os.ReadDir(temporary)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, db.Model(&types.Tenant{}).Where("id = ?", 1).Update("storage_quota", 5000).Error)
	stored, _, err := artifacts.Save(work, lease.Job, lease.Step, "quota", make([]byte, 1000))
	require.NoError(t, err)
	resource, err = catalog.Resolve(ctx, stored)
	require.NoError(t, err)
	require.NoError(t, db.First(&tenant, 1).Error)
	require.Equal(t, resource.Size, tenant.StorageUsed, "temporary charge is released; retained ciphertext is charged once")
	entries, err = os.ReadDir(temporary)
	require.NoError(t, err)
	require.Empty(t, entries)
	before := tenant.StorageUsed
	uncertainFiles := files.NewResourceCatalogFileService(&processingQuotaUncertainFiles{FileService: physicalFiles}, catalog)
	_, err = uncertainFiles.SaveBytes(work, make([]byte, 40), 1, "uncertain.bin", false)
	require.Error(t, err)
	require.NoError(t, db.First(&tenant, 1).Error)
	require.Equal(t, before+40, tenant.StorageUsed, "an unknown write must retain its quota until its physical outcome is reconciled")
	var pending []types.ProcessingStorageReservation
	require.NoError(t, db.Where("state = ?", "reserved").Find(&pending).Error)
	require.Len(t, pending, 1)
	require.EqualValues(t, 40, pending[0].Bytes)
	require.Empty(t, pending[0].PhysicalPath)
	filesOnDisk := 0
	require.NoError(t, filepath.WalkDir(directory, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			filesOnDisk++
		}
		return err
	}))
	require.Equal(t, 2, filesOnDisk, "only the accepted ciphertext and unknown write remain; the over-quota candidate wrote no file")
}

type processingQuotaUncertainFiles struct{ interfaces.FileService }

func (s *processingQuotaUncertainFiles) SaveBytes(ctx context.Context, data []byte, tenant uint64, name string, temporary bool) (string, error) {
	if _, err := s.FileService.SaveBytes(ctx, data, tenant, name, temporary); err != nil {
		return "", err
	}
	return "", errors.New("synthetic lost write acknowledgment")
}
