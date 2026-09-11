package repository

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Reuse is limited to one source/KB. Its existing lock serializes reference
// acquisition with deletion and excludes cross-tenant or scope transplantation.
// Consumers release their references only after their own retirement succeeds.
func (r *ProcessingRepository) ReferenceArtifact(ctx context.Context, tenant uint64, producerID, producerStepID, consumerID, consumerStepID string) error {
	if producerID == consumerID {
		return ErrProcessingConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		producer, err := lockProcessingRetirement(tx, tenant, producerID)
		if err != nil {
			return err
		}
		if producer.RetirementState != "retained" {
			return ErrProcessingConflict
		}
		var consumer types.ProcessingJob
		if err := tx.Where("id = ? AND tenant_id = ? AND datasource_id = ? AND knowledge_base_id = ?", consumerID, tenant, producer.DataSourceID, producer.KnowledgeBaseID).Take(&consumer).Error; err != nil {
			return err
		}
		if consumer.RetirementState != "retained" || (!consumer.IsCurrent && !consumer.IsPublished) || consumer.ScopeRevision != producer.ScopeRevision || consumer.AuthRevision != producer.AuthRevision {
			return ErrProcessingScope
		}
		if consumer.ExternalID != producer.ExternalID || consumer.SourceRevision != producer.SourceRevision || consumer.SourceDigest != producer.SourceDigest || consumer.Kind != producer.Kind {
			return ErrProcessingScope
		}
		var source types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ? AND status NOT IN ?", consumer.DataSourceID, tenant, []string{types.DataSourceStatusPaused, types.DataSourceStatusDeleted}).Take(&source).Error; err != nil {
			return err
		}
		scope, auth, err := ProcessingSourceRevisions(&source)
		if err != nil {
			return err
		}
		if scope != consumer.ScopeRevision || auth != consumer.AuthRevision {
			return ErrProcessingScope
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", consumer.KnowledgeBaseID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		var output, input types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ? AND status = ?", producerStepID, producerID, types.ProcessingSucceeded).Take(&output).Error; err != nil {
			return err
		}
		if err := tx.Where("id = ? AND job_id = ?", consumerStepID, consumerID).Take(&input).Error; err != nil {
			return err
		}
		if output.OutputManifestRef == "" || output.OutputDigest == "" || output.InputFingerprint != input.InputFingerprint {
			return ErrProcessingConflict
		}
		if !processingStepEligible(&consumer, &input) {
			return ErrProcessingConflict
		}
		ref := types.ProcessingArtifactReference{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(producerStepID+"/"+consumerStepID)).String(), TenantID: tenant,
			ProducerJobID: producerID, ProducerStepID: producerStepID, ConsumerJobID: consumerID, ConsumerStepID: consumerStepID, Attempt: output.Attempt, Digest: output.OutputDigest}
		var prior types.ProcessingArtifactReference
		err = tx.Where("id = ?", ref.ID).Take(&prior).Error
		if err == nil {
			if prior.Attempt != ref.Attempt || prior.Digest != ref.Digest {
				return ErrProcessingConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&ref).Error; err != nil {
			return err
		}
		return appendProcessingEvent(tx, producer, types.ProcessingEvent{Type: "artifact_referenced", StepID: output.ID, Attempt: output.Attempt, Message: consumer.ID})
	})
}

// Acquire the retention reference while the consumer's exact lease is valid.
// The source lock excludes deletion between lookup and reference creation.
func (r *ProcessingRepository) ReusableArtifact(ctx context.Context, tenant uint64, lease types.ProcessingLease) (producer types.ProcessingJob, output types.ProcessingStep, found bool, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		consumer, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if consumer.Kind != types.ProcessingJobDocument {
			return nil
		}
		q := tx.Table("processing_steps AS s").Select("s.*").Joins("JOIN processing_jobs AS j ON j.id = s.job_id").
			Where("j.tenant_id = ? AND j.knowledge_base_id = ? AND j.datasource_id = ? AND j.external_id = ? AND j.kind = ? AND j.source_revision = ? AND j.scope_revision = ? AND j.auth_revision = ? AND j.generation < ? AND j.retirement_state = ?", tenant, consumer.KnowledgeBaseID, consumer.DataSourceID, consumer.ExternalID, consumer.Kind, consumer.SourceRevision, consumer.ScopeRevision, consumer.AuthRevision, consumer.Generation, "retained").
			Where("s.stage = ? AND s.input_fingerprint = ? AND s.status = ? AND s.output_manifest_ref <> '' AND s.output_digest <> ''", lease.Step.Stage, lease.Step.InputFingerprint, types.ProcessingSucceeded).
			Order("j.generation DESC, s.id").Limit(1)
		q = q.Where("j.source_digest = ?", consumer.SourceDigest)
		if err = q.Take(&output).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		if err = tx.Where("id = ? AND tenant_id = ?", output.JobID, tenant).Take(&producer).Error; err != nil {
			return err
		}
		if err = NewProcessingRepository(tx).ReferenceArtifact(ctx, tenant, producer.ID, output.ID, consumer.ID, lease.Step.ID); err != nil {
			return err
		}
		found = true
		return nil
	})
	return
}
