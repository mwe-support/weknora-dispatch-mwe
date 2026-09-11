package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func validateLegacyDrainInventory(encoded types.JSON) error {
	var inventory types.ProcessingLegacyDrainInventory
	if len(encoded) > 1<<20 || json.Unmarshal(encoded, &inventory) != nil || !inventory.Complete || len(inventory.Workers) == 0 || len(inventory.Workers) > 256 {
		return errors.New("LEGACY_WORKER_INVENTORY_REQUIRED")
	}
	workers := map[string]bool{}
	for _, worker := range inventory.Workers {
		if worker.OldInstanceID == "" || len(worker.OldInstanceID) > 128 || workers[worker.OldInstanceID] || worker.ReplacementID == "" || len(worker.ReplacementID) > 128 || worker.ReplacementID == worker.OldInstanceID || !worker.ExitConfirmed || worker.GuardProtocol != types.ProcessingProtocol || !legacyDigest(worker.ImageDigest) {
			return errors.New("LEGACY_WORKER_DRAIN_UNVERIFIED")
		}
		workers[worker.OldInstanceID] = true
	}
	queues := map[string]bool{}
	for _, queue := range inventory.Queues {
		if queues[queue.Queue] || !legacyDigest(queue.InventoryDigest) || len(queue.Tasks) > 10000 {
			return errors.New("LEGACY_QUEUE_INVENTORY_INVALID")
		}
		queues[queue.Queue] = true
		tasks := map[string]bool{}
		for _, task := range queue.Tasks {
			if task.TaskID == "" || len(task.TaskID) > 256 || tasks[task.TaskID] || !legacyDigest(task.PayloadDigest) {
				return errors.New("LEGACY_DELIVERY_PROOF_INVALID")
			}
			switch task.State {
			case "canceled", "completed", "archived", "absent":
			default:
				return errors.New("LEGACY_DELIVERY_STILL_LIVE")
			}
			tasks[task.TaskID] = true
		}
	}
	for _, queue := range types.QueueDefinitions() {
		if !queues[queue.Name] {
			return errors.New("LEGACY_QUEUE_INVENTORY_INCOMPLETE")
		}
	}
	return nil
}

func (r *ProcessingRepository) RecordLegacyDrain(ctx context.Context, input types.ProcessingLegacyDrain) (*types.ProcessingLegacyDrain, error) {
	if input.TenantID == 0 || input.KnowledgeBaseID == "" || input.DataSourceID == "" || !legacyDigest(input.ScopeRevision) || !legacyDigest(input.ConfigurationRevision) || !legacyDigest(input.EvidenceDigest) ||
		strings.TrimSpace(input.Actor) == "" || len(input.Actor) > 128 || strings.TrimSpace(input.OperationRequestID) == "" || len(input.OperationRequestID) > 128 || strings.TrimSpace(input.EvidenceReference) == "" || len(input.EvidenceReference) > 256 || strings.ContainsAny(input.EvidenceReference, "\r\n?#") || strings.Contains(input.EvidenceReference, "://") {
		return nil, errors.New("LEGACY_DRAIN_ATTESTATION_REQUIRED")
	}
	if err := validateLegacyDrainInventory(input.Inventory); err != nil {
		return nil, err
	}
	var inventory types.ProcessingLegacyDrainInventory
	_ = json.Unmarshal(input.Inventory, &inventory)
	input.Inventory, _ = json.Marshal(inventory)
	input.ID, input.AuthRevision, input.RequestDigest = "", "", ""
	input.CreatedAt, input.ExpiresAt = time.Time{}, time.Time{}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	input.RequestDigest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	var result *types.ProcessingLegacyDrain
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		configuration, err := lockProcessingConfiguration(tx, input.TenantID, input.KnowledgeBaseID)
		if err != nil {
			return err
		}
		var source types.DataSource
		query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?", input.DataSourceID, input.TenantID, input.KnowledgeBaseID, types.ConnectorTypeTencentDocs)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.Take(&source).Error; err != nil {
			return err
		}
		var old types.ProcessingLegacyDrain
		err = tx.Where("tenant_id = ? AND operation_request_id = ?", input.TenantID, input.OperationRequestID).Take(&old).Error
		if err == nil {
			if old.RequestDigest != input.RequestDigest {
				return ErrProcessingConflict
			}
			result = &old
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		scope, auth, err := ProcessingSourceRevisions(&source)
		if err != nil {
			return err
		}
		if source.Status == types.DataSourceStatusDeleted || input.ScopeRevision != scope || input.ConfigurationRevision != configuration {
			return ErrProcessingScope
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		if input.CheckedAt.After(now.Add(5*time.Second)) || input.CheckedAt.Before(now.Add(-2*time.Minute)) {
			return errors.New("LEGACY_DRAIN_OBSERVATION_STALE")
		}
		input.ID, input.AuthRevision, input.CreatedAt, input.ExpiresAt = uuid.NewString(), auth, now, now.Add(5*time.Minute)
		if err := tx.Create(&input).Error; err != nil {
			return err
		}
		result = &input
		return nil
	})
	return result, err
}

func requireLegacyDrain(tx *gorm.DB, source *types.DataSource, configuration, id string) error {
	if id == "" {
		return errors.New("LEGACY_DRAIN_REQUIRED")
	}
	var drain types.ProcessingLegacyDrain
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ?", id, source.TenantID, source.KnowledgeBaseID, source.ID).Take(&drain).Error; err != nil {
		return err
	}
	scope, auth, err := ProcessingSourceRevisions(source)
	if err != nil {
		return err
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return err
	}
	if !drain.ExpiresAt.After(now) || drain.ScopeRevision != scope || drain.AuthRevision != auth || drain.ConfigurationRevision != configuration {
		return errors.New("LEGACY_DRAIN_EXPIRED_OR_CHANGED")
	}
	return validateLegacyDrainInventory(drain.Inventory)
}
