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
