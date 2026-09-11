package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func resolveAdoptedLegacyJob(tx *gorm.DB, job *types.ProcessingJob) error {
	var document struct {
		EvidenceID string `json:"legacy_evidence_id"`
	}
	if json.Unmarshal(job.Metadata, &document) != nil || document.EvidenceID == "" {
		return nil
	}
	evidence, err := NewProcessingRepository(tx).LegacyEvidence(tx.Statement.Context, job.TenantID, job.KnowledgeBaseID, document.EvidenceID)
	if err != nil {
		return err
	}
	if evidence.Action != "candidate_adopted" || evidence.JobID != job.ID || evidence.KnowledgeID != job.KnowledgeID || !job.IsPublished || job.Status != types.ProcessingSucceeded {
		return ErrProcessingConflict
	}
	evidence.Action = "recovered"
	evidence.OperationRequestID = "legacy-recovered:" + job.ID
	evidence.Actor = "processing:" + job.ID
	evidence.Reason = "Verified adopted processing job completed"
	evidence.EvidenceReference = "processing-job:" + job.ID
	evidence.EvidenceDigest = fmt.Sprintf("%x", sha256.Sum256([]byte(job.PlanDigest+"/"+job.ActiveIndexManifest)))
	evidence.ArtifactDigest = fmt.Sprintf("%x", sha256.Sum256([]byte(job.ActiveIndexManifest)))
	_, err = appendLegacyEvidence(tx, *evidence)
	return err
}

func (r *ProcessingRepository) LegacySource(ctx context.Context, tenant uint64, kbID, id string) (*types.DataSource, error) {
	var source types.DataSource
	if err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?", id, tenant, kbID, types.ConnectorTypeTencentDocs).Take(&source).Error; err != nil {
		return nil, err
	}
	return &source, nil
}

func (r *ProcessingRepository) LegacyAdoptionForOperation(ctx context.Context, request types.ProcessingLegacyEvidence) (*types.ProcessingJob, error) {
	var old types.ProcessingLegacyEvidence
	err := r.db.WithContext(ctx).Where("tenant_id = ? AND operation_request_id = ?", request.TenantID, request.OperationRequestID).Take(&old).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if old.Action != "candidate_adopted" || old.JobID == "" {
		return nil, ErrProcessingConflict
	}
	request.JobID, request.ArtifactDigest = old.JobID, old.ArtifactDigest
	request.ConfigurationRevision = old.ConfigurationRevision
	if _, err := appendLegacyEvidence(r.db.WithContext(ctx), request); err != nil {
		return nil, err
	}
	return r.GetJob(ctx, request.TenantID, old.JobID)
}

func (r *ProcessingRepository) LegacyEvidence(ctx context.Context, tenant uint64, kbID, id string) (*types.ProcessingLegacyEvidence, error) {
	var evidence types.ProcessingLegacyEvidence
	if err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", id, tenant, kbID).Take(&evidence).Error; err != nil {
		return nil, err
	}
	return &evidence, nil
}

func (r *ProcessingRepository) AdoptedLegacySnapshot(ctx context.Context, lease types.ProcessingLease) (*ProcessingLegacySnapshot, *types.ProcessingLegacyEvidence, error) {
	if err := r.ValidateLease(ctx, lease.Job.TenantID, lease); err != nil {
		return nil, nil, err
	}
	var spec struct {
		EvidenceID string `json:"legacy_evidence_id"`
	}
	if json.Unmarshal(lease.Job.Metadata, &spec) != nil || spec.EvidenceID == "" {
		return nil, nil, ErrProcessingConflict
	}
	evidence, err := r.LegacyEvidence(ctx, lease.Job.TenantID, lease.Job.KnowledgeBaseID, spec.EvidenceID)
	if err != nil {
		return nil, nil, err
	}
	if evidence.Action != "candidate_adopted" || evidence.JobID != lease.Job.ID || evidence.KnowledgeID != lease.Job.KnowledgeID || evidence.SourceRevision != lease.Job.SourceRevision {
		return nil, nil, ErrProcessingConflict
	}
	original, err := r.InspectLegacyError(ctx, evidence.ProcessingLegacyIdentity)
	if err != nil || original.Identity != evidence.ProcessingLegacyIdentity {
		return nil, nil, ErrProcessingConflict
	}
	snapshot, err := r.legacySnapshot(ctx, evidence.ProcessingLegacyIdentity, evidence.KnowledgeID, evidence.SourceRevision, evidence.Attempt, lease.Job.ID)
	if err != nil {
		return nil, nil, err
	}
	if snapshot.Digest != evidence.SnapshotDigest {
		return nil, nil, ErrProcessingConflict
	}
	return snapshot, evidence, nil
}

// The verified candidate, evidence, DAG and first outbox delivery acquire
// ownership together. No old sync log, span or completed legacy row is edited.
func (r *ProcessingRepository) AdoptLegacyCandidate(ctx context.Context, evidence types.ProcessingLegacyEvidence, input types.ProcessingJob, specs []types.ProcessingStepSpec, resources []types.ProcessingLegacyResource) (*types.ProcessingJob, error) {
	if len(resources) == 0 || len(resources) > 10001 || len(input.IndexDestination) == 0 {
		return nil, ErrProcessingScope
	}
	if evidence.Action != "candidate_adopted" || input.Kind != types.ProcessingJobDocument || input.TenantID != evidence.TenantID || input.KnowledgeBaseID != evidence.KnowledgeBaseID || input.DataSourceID != evidence.DataSourceID || (evidence.ExternalID != "" && input.ExternalID != evidence.ExternalID) || input.SourceRevision != evidence.SourceRevision || input.SourceDigest != evidence.ArtifactDigest || input.OriginRunID != "" {
		return nil, ErrProcessingScope
	}
	var result *types.ProcessingJob
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		configuration, err := lockProcessingConfiguration(tx, input.TenantID, input.KnowledgeBaseID)
		if err != nil {
			return err
		}
		if configuration != input.ConfigurationRevision || configuration != evidence.ConfigurationRevision {
			return ErrProcessingScope
		}
		var source types.DataSource
		query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", input.DataSourceID, input.TenantID, input.KnowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.Take(&source).Error; err != nil {
			return err
		}
		if source.Status != types.DataSourceStatusActive {
			return ErrProcessingScope
		}
		repo := NewProcessingRepository(tx)
		var previous types.ProcessingLegacyEvidence
		err = tx.Where("tenant_id = ? AND operation_request_id = ?", evidence.TenantID, evidence.OperationRequestID).Take(&previous).Error
		if err == nil {
			evidence.JobID = previous.JobID
			if _, err := appendLegacyEvidence(tx, evidence); err != nil {
				return err
			}
			result, err = repo.GetJob(ctx, evidence.TenantID, previous.JobID)
			if err != nil {
				return err
			}
			if result.PipelineFingerprint != input.PipelineFingerprint {
				return ErrProcessingConflict
			}
			_, digest, err := processingPlan(result.ID, specs)
			if err != nil {
				return err
			}
			if result.PlanDigest != digest {
				return ErrProcessingConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := requireLegacyDrain(tx, &source, configuration, evidence.DrainID); err != nil {
			return err
		}
		original, err := repo.InspectLegacyError(ctx, evidence.ProcessingLegacyIdentity)
		if err != nil {
			return err
		}
		if original.Identity != evidence.ProcessingLegacyIdentity || original.Error.Stage != "ingest" || original.Error.Code == "EXPORT_START_UNCERTAIN" || original.Error.Category == "EXPORT_START_UNCERTAIN" ||
			(original.KnowledgeID != "" && original.KnowledgeID != evidence.KnowledgeID) || (original.SourceRevision != "" && original.SourceRevision != evidence.SourceRevision) || (original.Attempt > 0 && original.Attempt != evidence.Attempt) {
			return ErrProcessingConflict
		}
		var currentCount int64
		if err := processingLogicalQuery(tx, input).Where("is_current = ? OR is_published = ?", true, true).Count(&currentCount).Error; err != nil {
			return err
		}
		if currentCount != 0 {
			return ErrProcessingConflict
		}
		snapshot, err := repo.LegacySnapshot(ctx, evidence.ProcessingLegacyIdentity, evidence.KnowledgeID, evidence.SourceRevision, evidence.Attempt)
		if err != nil {
			return err
		}
		k := snapshot.Knowledge
		if k.GetMetadata()["external_id"] != input.ExternalID {
			return ErrProcessingScope
		}
		if snapshot.Digest != evidence.SnapshotDigest || !k.IsDataSourceCandidate() || k.EnableStatus != "disabled" || k.GetMetadata()["datasource_index_ready"] != "true" || k.GetMetadata()["datasource_processing_failed"] != "" || k.PendingSubtasksCount != 0 {
			return ErrProcessingConflict
		}
		if k.ParseStatus != types.ParseStatusProcessing && k.ParseStatus != types.ParseStatusFinalizing && !(k.ParseStatus == types.ParseStatusFailed && strings.HasSuffix(k.ErrorMessage, types.HousekeepingRecoveryErrorSuffix)) {
			return ErrProcessingConflict
		}
		if err := VerifyLegacyStages(snapshot, true); err != nil {
			return err
		}
		if resources[0].Kind != "file" || resources[0].Reference != k.FilePath || resources[0].Bytes != k.FileSize {
			return ErrProcessingScope
		}
		var destination types.ProcessingIndexDestination
		var documentDestinations struct {
			Legacy types.ProcessingIndexDestination `json:"legacy_index_destination"`
		}
		if json.Unmarshal(input.IndexDestination, &destination) != nil || json.Unmarshal(input.Metadata, &documentDestinations) != nil {
			return ErrProcessingScope
		}
		if err := lockProcessingVectorStores(tx, input.TenantID, destination, documentDestinations.Legacy); err != nil {
			return err
		}
		job, err := repo.EnsureJob(ctx, input)
		if err != nil {
			return err
		}
		if job.KnowledgeID != "" {
			return ErrProcessingConflict
		}
		evidence.JobID = job.ID
		stored, err := appendLegacyEvidence(tx, evidence)
		if err != nil {
			return err
		}
		var document map[string]any
		if json.Unmarshal(job.Metadata, &document) != nil {
			return ErrProcessingConflict
		}
		document["legacy_evidence_id"] = stored.ID
		job.Metadata, err = json.Marshal(document)
		if err != nil {
			return err
		}
		job.KnowledgeID, job.IndexDestination = k.ID, input.IndexDestination
		if err := tx.Model(job).Updates(map[string]any{"knowledge_id": k.ID, "metadata": job.Metadata, "index_destination": job.IndexDestination}).Error; err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, resource := range resources {
			if seen[resource.Reference] {
				return ErrProcessingConflict
			}
			seen[resource.Reference] = true
			if err := bindLegacyResource(tx, job, resource); err != nil {
				return err
			}
		}
		var metadata map[string]any
		if json.Unmarshal(k.Metadata, &metadata) != nil {
			return ErrProcessingConflict
		}
		metadata["processing_protocol"], metadata["processing_job_id"] = "2", job.ID
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		update := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", k.ID, k.TenantID).Scopes(LegacyKnowledge).Update("metadata", types.JSON(encoded))
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return ErrProcessingConflict
		}
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "legacy_candidate_adopted", Actor: evidence.Actor, Detail: types.JSON(`{"verification":"current_legacy_artifacts"}`)}); err != nil {
			return err
		}
		if err := repo.PlanSteps(ctx, job.TenantID, job.ID, specs); err != nil {
			return err
		}
		if k.StorageSize < 0 {
			return ErrProcessingConflict
		}
		if k.StorageSize > 0 {
			owner, err := lockProcessingStorageTenant(tx, job.TenantID)
			if err != nil {
				return err
			}
			if owner.StorageUsed < k.StorageSize {
				return errors.New("LEGACY_STORAGE_ACCOUNTING_MISMATCH")
			}
			var step types.ProcessingStep
			if err := tx.Where("job_id = ? AND stage = ? AND unit_key = ?", job.ID, "legacy_snapshot", "body").Take(&step).Error; err != nil {
				return err
			}
			// Transfer the existing legacy text-index charge without charging it
			// again. The original knowledge field remains immutable evidence.
			charge := types.ProcessingStorageReservation{ID: legacyIndexChargeID(job.ID), TenantID: job.TenantID, JobID: job.ID, StepID: step.ID, Attempt: 1, Kind: "index", State: "reserved", Bytes: k.StorageSize, PhysicalPath: k.ID}
			if err := tx.Create(&charge).Error; err != nil {
				return err
			}
		}
		result = job
		return nil
	})
	return result, err
}

func legacyIndexChargeID(jobID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(jobID+"/legacy-index-charge")).String()
}

func releaseLegacyIndexCharge(tx *gorm.DB, job *types.ProcessingJob) error {
	var charge types.ProcessingStorageReservation
	err := tx.Where("id = ? AND tenant_id = ? AND job_id = ?", legacyIndexChargeID(job.ID), job.TenantID, job.ID).Take(&charge).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return releaseProcessingStorage(tx, job.TenantID, charge.ID, false)
}
