package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func processingGraphChunk(tx *gorm.DB, job *types.ProcessingJob, id string, revision int) error {
	q := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND content_revision = ? AND is_enabled = ?", id, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID, revision, true)
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "SHARE"})
	}
	var chunk types.Chunk
	return q.Take(&chunk).Error
}

func (r *ProcessingRepository) ReserveGraphWrite(ctx context.Context, tenant uint64, lease types.ProcessingLease, chunkID string, revision int, destination string) (*types.ProcessingGraphWrite, error) {
	if lease.Step.Stage != "graph_apply" || lease.Step.UnitKey != chunkID || len(destination) != 64 {
		return nil, ErrProcessingConflict
	}
	var write types.ProcessingGraphWrite
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if !job.IsPublished || job.PublicationEpoch != lease.Step.ExpectedPublicationEpoch {
			return ErrProcessingConflict
		}
		var changed int64
		if err := tx.Model(&types.ProcessingGraphWrite{}).Where("job_id = ? AND destination_digest <> ?", job.ID, destination).Count(&changed).Error; err != nil {
			return err
		}
		if changed != 0 {
			return errors.New("GRAPH_DESTINATION_CHANGED")
		}
		if err := processingGraphChunk(tx, job, chunkID, revision); err != nil {
			return err
		}
		write = types.ProcessingGraphWrite{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("graph/%s/%d/%d", lease.Step.ID, lease.Ref.Attempt, job.PublicationEpoch))).String(),
			TenantID: tenant, JobID: job.ID, StepID: lease.Step.ID, Attempt: lease.Ref.Attempt, PublicationEpoch: job.PublicationEpoch,
			KnowledgeBaseID: job.KnowledgeBaseID, KnowledgeID: job.KnowledgeID, ChunkID: chunkID, ContentRevision: revision, DestinationDigest: destination, State: "reserved"}
		var existing types.ProcessingGraphWrite
		err = tx.Where("id = ?", write.ID).Take(&existing).Error
		if err == nil {
			if existing.TenantID != tenant || existing.JobID != job.ID || existing.DestinationDigest != destination || existing.ContentRevision != revision || existing.State != "reserved" {
				return ErrProcessingConflict
			}
			write = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return tx.Create(&write).Error
	})
	return &write, err
}

func commitProcessingGraph(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, id string) error {
	if step.Stage != "graph_apply" || id == "" || !job.IsPublished || step.ExpectedPublicationEpoch != job.PublicationEpoch {
		return ErrProcessingConflict
	}
	var write types.ProcessingGraphWrite
	if err := tx.Where("id = ? AND tenant_id = ? AND job_id = ? AND step_id = ? AND step_attempt = ? AND publication_epoch = ? AND state = ?", id, job.TenantID, job.ID, step.ID, step.Attempt, job.PublicationEpoch, "reserved").Take(&write).Error; err != nil {
		return err
	}
	if step.UnitKey != write.ChunkID {
		return ErrProcessingConflict
	}
	if err := processingGraphChunk(tx, job, write.ChunkID, write.ContentRevision); err != nil {
		return err
	}
	return tx.Model(&write).Updates(map[string]any{"state": "confirmed", "output_digest": step.OutputDigest}).Error
}

// The caller has already authorized this KB/file, as in FilterIndexes. Owner
// tenant is read from that scope so shared KBs retain their normal access rules.
// Both graph endpoints must belong to these visible immutable contributions.
func (r *ProcessingRepository) VisibleGraph(ctx context.Context, namespace types.NameSpace, destination string) (contributions, legacy []string, err error) {
	if namespace.KnowledgeBase == "" {
		return nil, nil, ErrProcessingScope
	}
	var kb types.KnowledgeBase
	if err = r.db.WithContext(ctx).Where("id = ?", namespace.KnowledgeBase).Take(&kb).Error; err != nil {
		return
	}
	q := r.db.WithContext(ctx).Select("id", "enable_status", "metadata").Where("knowledge_base_id = ? AND tenant_id = ?", kb.ID, kb.TenantID)
	if namespace.Knowledge != "" {
		q = q.Where("id = ?", namespace.Knowledge)
	}
	var knowledge []types.Knowledge
	if err = q.Find(&knowledge).Error; err != nil {
		return
	}
	managed := map[string]string{}
	for _, item := range knowledge {
		if item.EnableStatus != "enabled" {
			continue
		}
		if item.GetMetadata()["processing_protocol"] == "2" {
			managed[item.ID] = item.GetMetadata()["processing_job_id"]
		} else {
			legacy = append(legacy, item.ID)
		}
	}
	var jobs []types.ProcessingJob
	if len(managed) == 0 {
		return
	}
	if err = r.db.WithContext(ctx).Where("tenant_id = ? AND knowledge_base_id = ? AND is_published = ? AND retirement_state = ?", kb.TenantID, kb.ID, true, "retained").Find(&jobs).Error; err != nil {
		return
	}
	for _, job := range jobs {
		if managed[job.KnowledgeID] != job.ID {
			continue
		}
		var manifest map[string]types.ProcessingArtifactVersion
		if err = json.Unmarshal([]byte(job.ActiveIndexManifest), &manifest); err != nil {
			return
		}
		var writes []types.ProcessingGraphWrite
		if err = r.db.WithContext(ctx).Table("processing_graph_writes AS w").Select("w.*").
			Joins("JOIN chunks c ON c.id = w.chunk_id AND c.tenant_id = w.tenant_id AND c.knowledge_id = w.knowledge_id AND c.content_revision = w.content_revision AND c.deleted_at IS NULL AND c.is_enabled = ?", true).
			Where("w.job_id = ? AND w.tenant_id = ? AND w.publication_epoch = ? AND w.state = ? AND w.destination_digest = ?", job.ID, kb.TenantID, job.PublicationEpoch, "confirmed", destination).Find(&writes).Error; err != nil {
			return
		}
		for _, write := range writes {
			version, ok := manifest[write.StepID]
			if ok && version.Attempt == write.Attempt && version.Digest == write.OutputDigest {
				contributions = append(contributions, write.ID)
			}
		}
	}
	return
}

func (r *ProcessingRepository) RetirementGraphWrites(ctx context.Context, tenant uint64, lease types.ProcessingLease) (writes []types.ProcessingGraphWrite, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if lease.Step.Phase != types.ProcessingPhaseRetire || job.RetirementState != "deleting" {
			return ErrProcessingConflict
		}
		return tx.Where("job_id = ? AND tenant_id = ? AND state <> ?", job.ID, tenant, "deleted").Order("id").Find(&writes).Error
	})
	return
}

func (r *ProcessingRepository) RetirementGraphDeleted(ctx context.Context, tenant uint64, lease types.ProcessingLease, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		if lease.Step.Phase != types.ProcessingPhaseRetire || job.RetirementState != "deleting" {
			return ErrProcessingConflict
		}
		return tx.Model(&types.ProcessingGraphWrite{}).Where("id = ? AND job_id = ? AND tenant_id = ?", id, job.ID, tenant).Update("state", "deleted").Error
	})
}
