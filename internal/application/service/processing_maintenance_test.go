package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestProcessingGarbageProtectsActivePinnedReferencedAndUncertainJobs(t *testing.T) {
	db := processingServiceTestDatabase(t)
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	old := time.Now().Add(-8 * 24 * time.Hour)
	for _, name := range []string{"active", "published", "pinned", "reference", "running", "blocked", "young", "uncertain", "orphan"} {
		job := types.ProcessingJob{ID: name, Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: name, Generation: 1, SourceRevision: "synthetic", PipelineFingerprint: "p1", Status: types.ProcessingSuperseded, RetirementState: "retained", CreatedAt: old, UpdatedAt: old}
		switch name {
		case "active":
			job.IsCurrent = true
		case "published":
			job.IsPublished = true
		case "pinned":
			job.RollbackPin = true
		case "running":
			job.Status = types.ProcessingRunning
		case "blocked":
			job.Status = types.ProcessingBlocked
		case "young":
			job.UpdatedAt = time.Now()
		}
		require.NoError(t, db.Create(&job).Error)
		if name == "reference" {
			require.NoError(t, db.Create(&types.ProcessingArtifactReference{ID: "held", TenantID: 1, ProducerJobID: job.ID, ProducerStepID: "output", ConsumerJobID: "active", ConsumerStepID: "input", Attempt: 1, Digest: "verified"}).Error)
		}
		if name == "uncertain" {
			require.NoError(t, db.Create(&types.ProcessingStep{ID: "unknown", JobID: job.ID, Stage: "export_start", UnitKey: "body", Phase: types.ProcessingPhasePrepare, Status: types.ProcessingBlocked, ErrorClass: "uncertain", ErrorCode: "EXPORT_START_UNCERTAIN"}).Error)
		}
	}
	cursor := ""
	for i := 0; i < 10; i++ {
		var err error
		cursor, err = r.CollectProcessingGarbage(ctx, cursor, 1)
		require.NoError(t, err)
		if cursor == "" {
			break
		}
	}
	var steps []types.ProcessingStep
	require.NoError(t, db.Where("phase = ?", types.ProcessingPhaseRetire).Find(&steps).Error)
	require.Len(t, steps, 1)
	require.Equal(t, "orphan", steps[0].JobID)
	require.Equal(t, types.ProcessingEnqueuePending, steps[0].Status)
	_, err := r.CollectProcessingGarbage(ctx, "", 100)
	require.NoError(t, err)
	var count int64
	require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("event_type = ?", "retirement_requested").Count(&count).Error)
	require.EqualValues(t, 1, count)
	// A pin acquired after enumeration/planning still excludes the delete lease.
	orphan, err := r.GetJob(ctx, 1, "orphan")
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, orphan.ID, orphan.Revision, true, "pin-after-gc-plan", "operator"))
	step := steps[0]
	_, err = r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: orphan.ID, Generation: orphan.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
	require.ErrorIs(t, err, repository.ErrProcessingConflict)
}

func TestProcessingFAQResourceGarbageKeepsLiveAnswersAndFencesRestoration(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}))
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&types.Tenant{ID: 1, Name: "synthetic"}).Error)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	local := files.NewLocalFileService(t.TempDir(), "")
	path, err := local.SaveBytes(ctx, []byte("synthetic answer image"), 1, "answer.png", false)
	require.NoError(t, err)
	old := time.Now().Add(-8 * 24 * time.Hour)
	require.NoError(t, db.Create(&types.ProcessingJob{ID: "retired", Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", Generation: 1, Status: types.ProcessingSuperseded, RetirementState: "deleted"}).Error)
	resource := &types.StoredResource{ID: "image", Handle: "AbCdEfGhIjKlMnOpQrStUv", TenantID: 1, CreationJobID: "retired", Provider: "local", PhysicalPath: path, State: types.ResourceStateActive, CreatedAt: old, UpdatedAt: old}
	require.NoError(t, db.Create(resource).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "canonical", TenantID: 1, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeFAQ}).Error)
	chunks := repository.NewChunkRepository(db)
	reference := types.BuildResourcePath(resource.Handle)
	var rows []*types.Chunk
	for i := 0; i < 2; i++ {
		row := &types.Chunk{ID: fmt.Sprint("faq-", i), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "canonical", ChunkType: types.ChunkTypeFAQ, IsEnabled: true, Status: int(types.ChunkStatusIndexed)}
		require.NoError(t, row.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: fmt.Sprint("Question ", i), Answers: []string{"![image](" + reference + ")"}}))
		require.NoError(t, chunks.CreateChunks(ctx, []*types.Chunk{row}))
		rows = append(rows, row)
	}
	r := repository.NewProcessingRepository(db)
	garbage, _, err := r.ClaimProcessingResourceGarbage(ctx, "", 20)
	require.NoError(t, err)
	require.Empty(t, garbage)
	removeImage := func(row *types.Chunk) {
		t.Helper()
		meta, err := row.FAQMetadata()
		require.NoError(t, err)
		meta.Answers = []string{"Updated answer without an image"}
		require.NoError(t, row.SetFAQMetadata(meta))
		require.NoError(t, chunks.SaveChunks(ctx, []*types.Chunk{row}))
	}
	removeImage(rows[0])
	garbage, _, err = r.ClaimProcessingResourceGarbage(ctx, "", 20)
	require.NoError(t, err)
	require.Empty(t, garbage)
	require.NoError(t, db.Model(resource).Update("updated_at", old).Error)
	garbage, _, err = r.ClaimProcessingResourceGarbage(ctx, "", 20)
	require.NoError(t, err)
	require.Empty(t, garbage, "the other answer still uses the image")
	removeImage(rows[1])
	garbage, _, err = r.ClaimProcessingResourceGarbage(ctx, "", 20)
	require.NoError(t, err)
	require.Empty(t, garbage, "detaching the last answer starts a fresh grace period")
	var held int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&held).Error)
	require.Zero(t, held)
	require.NoError(t, db.Model(resource).Update("updated_at", old).Error)
	garbage, _, err = r.ClaimProcessingResourceGarbage(ctx, "", 20)
	require.NoError(t, err)
	require.Len(t, garbage, 1)
	meta, err := rows[1].FAQMetadata()
	require.NoError(t, err)
	meta.Answers = []string{"![image](" + reference + ")"}
	require.NoError(t, rows[1].SetFAQMetadata(meta))
	require.ErrorIs(t, chunks.SaveChunks(ctx, []*types.Chunk{rows[1]}), repository.ErrChunkRevisionConflict, "a delayed edit cannot reference an object claimed by GC")
	unchanged, err := chunks.GetChunkByID(ctx, 1, rows[1].ID)
	require.NoError(t, err)
	unchangedMeta, err := unchanged.FAQMetadata()
	require.NoError(t, err)
	require.Equal(t, []string{"Updated answer without an image"}, unchangedMeta.Answers)
	fault := &processingRetirementFaultFiles{FileService: local}
	s := &knowledgeService{tenantRepo: repository.NewTenantRepository(db), fileSvc: fault}
	_, err = s.collectProcessingResourceGarbage(ctx, r, "", 20)
	require.Error(t, err)
	require.NoError(t, db.Unscoped().First(resource, "id = ?", resource.ID).Error)
	require.Equal(t, "deleting", resource.State)
	_, err = s.collectProcessingResourceGarbage(ctx, r, "", 20)
	require.NoError(t, err)
	require.NoError(t, db.Unscoped().First(resource, "id = ?", resource.ID).Error)
	require.Equal(t, types.ResourceStateDeleted, resource.State)
	require.Equal(t, 2, fault.calls)
	_, err = local.GetFile(ctx, path)
	require.Error(t, err, "the physical file must actually be removed")
}

type processingGarbageFaultIndex struct {
	*processingPipelineIndex
	fail bool
}

func (r *processingGarbageFaultIndex) DeleteBySourceIDList(ctx context.Context, ids []string, dimension int, kind string) error {
	if err := r.processingPipelineIndex.DeleteBySourceIDList(ctx, ids, dimension, kind); err != nil {
		return err
	}
	if r.fail {
		r.fail = false
		return context.DeadlineExceeded
	}
	return nil
}

func TestProcessingFAQGarbageUsesExactOriginalDestinationAndRetriesLostACK(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}))
	tenant := &types.Tenant{ID: 1, Name: "synthetic", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}}}}
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(tenant).Error)
	kb := &types.KnowledgeBase{}
	require.NoError(t, db.First(kb).Error)
	kb.Type = types.KnowledgeBaseTypeFAQ
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true}
	require.NoError(t, db.Save(kb).Error)
	index := &processingGarbageFaultIndex{processingPipelineIndex: &processingPipelineIndex{vectors: true}, fail: true}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
	s := &knowledgeService{tenantRepo: repository.NewTenantRepository(db), chunkRepo: repository.NewChunkRepository(db), retrieveEngine: registry}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	destination, err := knowledgeIndexDestination(ctx, s, kb, 2)
	require.NoError(t, err)
	encoded, _ := json.Marshal(destination)
	require.NoError(t, db.Create(&types.Knowledge{ID: "canonical", TenantID: 1, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeFAQ}).Error)
	chunk := &types.Chunk{ID: "faq", TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "canonical", ChunkType: types.ChunkTypeFAQ, Content: "synthetic", IsEnabled: true, Status: int(types.ChunkStatusIndexed)}
	require.NoError(t, s.chunkRepo.CreateChunks(ctx, []*types.Chunk{chunk}))
	var liveKeys, garbageKeys []string
	var orphanID string
	for i := 0; i < 2; i++ {
		items := []*types.IndexInfo{{SourceID: "question", KnowledgeID: "canonical"}}
		manifest, err := types.VersionFAQIndexes(fmt.Sprint(i), chunk, items)
		require.NoError(t, err)
		var m types.FAQIndexManifest
		require.NoError(t, json.Unmarshal([]byte(manifest), &m))
		keys, _ := json.Marshal(m.SourceIDs)
		write := types.FAQIndexWrite{ID: m.WriteIDs[0], TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "canonical", ChunkID: chunk.ID, BaseRevision: chunk.ContentRevision, ContentDigest: m.ContentDigest, SourceIDs: keys, Destination: encoded}
		require.NoError(t, s.chunkRepo.RegisterFAQIndexWrites(ctx, []types.FAQIndexWrite{write}))
		require.NoError(t, s.chunkRepo.ConfirmFAQIndexWrites(ctx, 1, []string{write.ID}))
		index.writes = append(index.writes, items...)
		if i == 0 {
			chunk.FAQIndexManifest = manifest
			require.NoError(t, s.chunkRepo.SaveChunks(ctx, []*types.Chunk{chunk}))
			liveKeys = m.SourceIDs
		} else {
			garbageKeys = m.SourceIDs
			orphanID = write.ID
		}
	}
	require.NoError(t, db.Model(&types.FAQIndexWrite{}).Where("tenant_id = ?", 1).Update("updated_at", time.Now().Add(-8*24*time.Hour)).Error)
	r := repository.NewProcessingRepository(db)
	_, err = s.collectFAQIndexGarbage(context.Background(), r, "", 20)
	require.Error(t, err)
	var write types.FAQIndexWrite
	require.NoError(t, db.First(&write, "id = ?", orphanID).Error)
	require.Equal(t, "deleting", write.State)
	require.Equal(t, garbageKeys, index.compensated)
	require.Len(t, index.writes, 1)
	require.Equal(t, liveKeys[0], index.writes[0].SourceID)
	_, err = s.collectFAQIndexGarbage(context.Background(), r, "", 20)
	require.NoError(t, err)
	require.NoError(t, db.First(&write, "id = ?", orphanID).Error)
	require.Equal(t, "deleted", write.State)
	require.Zero(t, index.deleteCalls)
	require.Equal(t, append(append([]string{}, garbageKeys...), garbageKeys...), index.compensated)
}

func TestProcessingRetiredIndexesReconcileLateWritesWithoutTouchingLiveVersions(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}))
	tenant := &types.Tenant{ID: 1, Name: "synthetic", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}}}}
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(tenant).Error)
	index := &processingPipelineIndex{vectors: true}
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
	s := &knowledgeService{tenantRepo: repository.NewTenantRepository(db), retrieveEngine: registry}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 1, Type: types.KnowledgeBaseTypeDocument, IndexingStrategy: types.IndexingStrategy{VectorEnabled: true}}
	destination, err := knowledgeIndexDestination(ctx, s, kb, 2)
	require.NoError(t, err)
	encoded, _ := json.Marshal(destination)
	old := time.Now().Add(-8 * 24 * time.Hour)
	for _, id := range []string{"retired", "live", "fresh"} {
		job := types.ProcessingJob{ID: id, Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: id, Generation: 1, KnowledgeID: types.ProcessingKnowledgeID(id), RetirementState: "deleted", IndexDestination: encoded, CreatedAt: old, UpdatedAt: old}
		if id == "live" {
			job.RetirementState, job.IsCurrent, job.IsPublished = "retained", true, true
		}
		if id == "fresh" {
			job.UpdatedAt = time.Now()
		}
		require.NoError(t, db.Create(&job).Error)
		index.writes = append(index.writes, &types.IndexInfo{KnowledgeID: job.KnowledgeID, SourceID: id})
	}
	r := repository.NewProcessingRepository(db)
	_, err = s.collectRetiredIndexGarbage(ctx, r, "", 20)
	require.Error(t, err, "lost deletion ACK must remain eligible")
	require.Len(t, index.writes, 2)
	_, err = s.collectRetiredIndexGarbage(ctx, r, "", 20)
	require.NoError(t, err)
	index.writes = append(index.writes, &types.IndexInfo{KnowledgeID: types.ProcessingKnowledgeID("retired"), SourceID: "late-provider-write"})
	_, err = s.collectRetiredIndexGarbage(ctx, r, "", 20)
	require.NoError(t, err)
	require.Len(t, index.writes, 3, "successful reconciliation starts the seven-day interval")
	require.NoError(t, db.Model(&types.ProcessingJob{}).Where("id = ?", "retired").Update("updated_at", old).Error)
	_, err = s.collectRetiredIndexGarbage(ctx, r, "", 20)
	require.NoError(t, err)
	require.Len(t, index.writes, 2)
	require.Equal(t, []int{2, 2, 2}, index.deletedDimensions)
	var count int64
	require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("event_type = ?", "retired_indexes_reconciled").Count(&count).Error)
	require.EqualValues(t, 2, count)
}

func TestProcessingCompletedScanArtifactsRetireAndReplayKeepsIdentity(t *testing.T) {
	for _, status := range []string{types.ProcessingSucceeded, types.ProcessingCanceled, types.ProcessingSuperseded} {
		t.Run(status, func(t *testing.T) {
			db := processingServiceTestDatabase(t)
			require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.SyncLog{}, &types.SyncRunItem{}))
			tenant := &types.Tenant{ID: 1, Name: "synthetic"}
			require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(tenant).Error)
			run := &types.SyncLog{ID: "completed-scan", TenantID: 1, DataSourceID: "source", Status: types.SyncLogStatusRunning}
			require.NoError(t, db.Create(run).Error)
			r := repository.NewProcessingRepository(db)
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			input := types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source",
				ExternalID: "run:" + run.ID, OriginRunID: run.ID, SourceRevision: run.ID, PipelineFingerprint: "p1"}
			plan := []types.ProcessingStepSpec{{Stage: "scan_page", UnitKey: "root", Phase: types.ProcessingPhaseScan, InputFingerprint: "scan-input", RequiredForCompletion: true}}
			job, err := r.BeginScan(ctx, input, plan)
			require.NoError(t, err)
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			claim := func(step types.ProcessingStep) *types.ProcessingLease {
				t.Helper()
				lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
				require.NoError(t, err)
				return lease
			}
			lease := claim(steps[0])
			local := files.NewLocalFileService(t.TempDir(), "")
			path, err := local.SaveBytes(ctx, []byte("private synthetic scan output"), 1, "scan.enc", false)
			require.NoError(t, err)
			resource := &types.StoredResource{ID: "scan-output", Handle: "AbCdEfGhIjKlMnOpQrStUv", TenantID: 1,
				CreationJobID: job.ID, Provider: "local", PhysicalPath: path, State: types.ResourceStateActive}
			require.NoError(t, db.Create(resource).Error)
			require.NoError(t, db.Create(&types.ResourceBinding{ID: "scan-binding", TenantID: 1, ResourceID: resource.ID, OwnerType: "processing_job", OwnerID: job.ID, Relation: "stage_artifact"}).Error)
			require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: path, OutputDigest: "verified"}))
			_, err = r.CollectProcessingGarbage(ctx, "", 100)
			require.NoError(t, err)
			steps, err = r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			require.Len(t, steps, 1, "fresh scan artifacts stay available")
			require.NoError(t, db.Model(&types.ProcessingJob{}).Where("id = ?", job.ID).Update("status", status).Error)
			require.NoError(t, db.Model(&types.ProcessingJob{}).Where("id = ?", job.ID).Update("updated_at", time.Now().Add(-8*24*time.Hour)).Error)
			_, err = r.CollectProcessingGarbage(ctx, "", 100)
			require.NoError(t, err)
			steps, err = r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			require.Len(t, steps, 2, "completed scan artifacts need a retirement stage")
			for _, step := range steps {
				if step.Phase != types.ProcessingPhaseRetire {
					continue
				}
				cleanup := claim(step)
				s := &knowledgeService{fileSvc: local}
				outcome, err := s.retireProcessingVersion(ctx, r, *cleanup, tenant)
				require.NoError(t, err)
				require.NoError(t, r.FinishStep(ctx, 1, *cleanup, outcome))
			}
			_, err = local.GetFile(ctx, path)
			require.Error(t, err, "the old scan's physical artifact is removed")
			replay, err := r.BeginScan(ctx, input, plan)
			require.NoError(t, err)
			require.Equal(t, job.ID, replay.ID)
			require.Equal(t, "deleted", replay.RetirementState)
			snapshot, err := r.RunSnapshot(ctx, 1, run.ID)
			require.NoError(t, err)
			require.Equal(t, status == types.ProcessingSucceeded, snapshot.DiscoveryComplete)
			require.Equal(t, job.ID, snapshot.ScanJobID)
		})
	}
}
