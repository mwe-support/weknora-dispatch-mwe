package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// JSONB changes whitespace and key order. Queue and archive verification use
// the same canonical semantic payload, preserving unknown fields and numbers.
func ProcessingLegacyPayloadDigest(payload []byte) (string, error) {
	var value map[string]any
	d := json.NewDecoder(bytes.NewReader(payload))
	d.UseNumber()
	if len(payload) > 1<<20 || d.Decode(&value) != nil || value == nil {
		return "", errors.New("LEGACY_PAYLOAD_INVALID")
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return "", errors.New("LEGACY_PAYLOAD_INVALID")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

func legacyRetryMetadata(job *types.ProcessingJob) (*types.ProcessingLegacyRetry, error) {
	if len(job.Metadata) == 0 {
		return nil, nil
	}
	var spec types.ProcessingLegacyRetryScan
	if json.Unmarshal(job.Metadata, &spec) != nil {
		return nil, ErrProcessingConflict
	}
	return spec.LegacyRetry, nil
}

func legacyRetryRunID(request types.ProcessingLegacyRetry) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%d/legacy-retry/%s", request.TenantID, request.OperationRequestID))).String()
}

func (r *ProcessingRepository) BeginLegacyRetry(ctx context.Context, request types.ProcessingLegacyRetry, input types.ProcessingJob, specs []types.ProcessingStepSpec) (*types.ProcessingJob, error) {
	if request.TenantID == 0 || request.KnowledgeBaseID == "" || request.DataSourceID == "" || request.RunID == "" || request.DeadLetterID < 1 ||
		request.QueueTaskID == "" || len(request.QueueTaskID) > 256 || !legacyDigest(request.PayloadDigest) || request.DrainID == "" || len(request.Errors) == 0 || len(request.Errors) > 100 ||
		strings.TrimSpace(request.Actor) == "" || len(request.Actor) > 128 || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 512 || strings.TrimSpace(request.OperationRequestID) == "" || len(request.OperationRequestID) > 128 ||
		input.Kind != types.ProcessingJobScan || input.TenantID != request.TenantID || input.KnowledgeBaseID != request.KnowledgeBaseID || input.DataSourceID != request.DataSourceID {
		return nil, ErrProcessingScope
	}
	request.Errors = slices.Clone(request.Errors)
	slices.SortFunc(request.Errors, func(a, b types.ProcessingLegacyIdentity) int { return a.ErrorOrdinal - b.ErrorOrdinal })
	seen := map[string]bool{}
	for i, identity := range request.Errors {
		if identity.TenantID != request.TenantID || identity.KnowledgeBaseID != request.KnowledgeBaseID || identity.DataSourceID != request.DataSourceID || identity.RunID != request.RunID || identity.ErrorOrdinal < 1 ||
			!legacyDigest(identity.ErrorDigest) || identity.ExternalID == "" || identity.FileID == "" || seen[identity.ExternalID] || (i > 0 && request.Errors[i-1].ErrorOrdinal == identity.ErrorOrdinal) {
			return nil, ErrProcessingScope
		}
		seen[identity.ExternalID] = true
	}
	encoded, _ := json.Marshal(request)
	metadata := types.ProcessingLegacyRetryScan{LegacyRetry: &request, RequestDigest: fmt.Sprintf("%x", sha256.Sum256(encoded))}
	input.Metadata, _ = json.Marshal(metadata)
	input.OriginRunID = legacyRetryRunID(request)
	input.ExternalID, input.SourceRevision = "run:"+input.OriginRunID, input.OriginRunID
	var result *types.ProcessingJob
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
		var previous types.ProcessingJob
		err = processingLogicalQuery(tx, input).Take(&previous).Error
		if err == nil {
			var old types.ProcessingLegacyRetryScan
			if json.Unmarshal(previous.Metadata, &old) != nil || old.RequestDigest != metadata.RequestDigest {
				return ErrProcessingConflict
			}
			result = &previous
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if source.Status != types.DataSourceStatusActive {
			return ErrProcessingScope
		}
		input.ConfigurationRevision = configuration
		if err := requireLegacyDrain(tx, &source, configuration, request.DrainID); err != nil {
			return err
		}
		if err := verifyLegacyRetryDelivery(tx, request); err != nil {
			return err
		}
		cfg, err := source.ParseConfig()
		if err != nil {
			return err
		}
		repo := NewProcessingRepository(tx)
		for _, identity := range request.Errors {
			original, err := repo.InspectLegacyError(ctx, identity)
			if err != nil {
				return err
			}
			if original.Identity != identity || original.Error.RetryState != "scheduled" || original.Error.SourceResourceID == "" || !slices.Contains(cfg.ResourceIDs, original.Error.SourceResourceID) ||
				original.Error.Code == "EXPORT_START_UNCERTAIN" || original.Error.Category == "EXPORT_START_UNCERTAIN" {
				return errors.New("LEGACY_RETRY_IDENTITY_UNVERIFIED")
			}
			var count int64
			if err := tx.Model(&types.ProcessingLegacyEvidence{}).Where("tenant_id = ? AND run_id = ? AND error_ordinal = ? AND error_digest = ?", identity.TenantID, identity.RunID, identity.ErrorOrdinal, identity.ErrorDigest).Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return errors.New("LEGACY_RETRY_ALREADY_LINKED")
			}
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		run := types.SyncLog{ID: input.OriginRunID, TenantID: input.TenantID, DataSourceID: input.DataSourceID, Status: types.SyncLogStatusRunning, StartedAt: now, CreatedAt: now}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		result, err = repo.BeginScan(ctx, input, specs)
		if err != nil {
			return err
		}
		result, err = repo.GetJob(ctx, input.TenantID, result.ID)
		if err != nil {
			return err
		}
		for _, identity := range request.Errors {
			_, err := appendLegacyEvidence(tx, types.ProcessingLegacyEvidence{ProcessingLegacyIdentity: identity, Action: "retry_requested", JobID: result.ID,
				SnapshotDigest: identity.ErrorDigest, ArtifactDigest: metadata.RequestDigest, ConfigurationRevision: configuration,
				EvidenceReference: fmt.Sprintf("legacy-dead-letter:%d", request.DeadLetterID), EvidenceDigest: request.PayloadDigest, DrainID: request.DrainID,
				Actor: request.Actor, Reason: request.Reason, OperationRequestID: fmt.Sprintf("legacy-retry-request:%s:%d", result.ID, identity.ErrorOrdinal)})
			if err != nil {
				return err
			}
		}
		return appendProcessingEvent(tx, result, types.ProcessingEvent{Type: "legacy_retry_requested", Actor: request.Actor, RunID: run.ID,
			Detail: types.JSON(fmt.Sprintf(`{"old_run_id":%q,"dead_letter_id":%d,"requested_count":%d}`, request.RunID, request.DeadLetterID, len(request.Errors)))})
	})
	return result, err
}

func verifyLegacyRetryDelivery(tx *gorm.DB, request types.ProcessingLegacyRetry) error {
	var dead types.TaskDeadLetter
	if err := tx.Where("id = ? AND tenant_id = ? AND task_type = ? AND scope = ? AND scope_id = ?", request.DeadLetterID, request.TenantID, types.TypeDataSourceSync, types.TaskScopeTenant, strconv.FormatUint(request.TenantID, 10)).Take(&dead).Error; err != nil {
		return err
	}
	var payload types.DataSourceSyncPayload
	digest, err := ProcessingLegacyPayloadDigest(dead.Payload)
	if err != nil || digest != request.PayloadDigest || json.Unmarshal(dead.Payload, &payload) != nil || payload.TenantID != request.TenantID || payload.DataSourceID != request.DataSourceID || payload.SyncLogID != request.RunID || dead.FailCount < 1 {
		return errors.New("LEGACY_DEAD_LETTER_MISMATCH")
	}
	var drain types.ProcessingLegacyDrain
	if err := tx.Where("id = ? AND tenant_id = ?", request.DrainID, request.TenantID).Take(&drain).Error; err != nil {
		return err
	}
	var inventory types.ProcessingLegacyDrainInventory
	if json.Unmarshal(drain.Inventory, &inventory) != nil {
		return ErrProcessingConflict
	}
	matches := 0
	for _, queue := range inventory.Queues {
		for _, task := range queue.Tasks {
			if task.TaskID == request.QueueTaskID {
				if queue.Queue != types.QueueSync || task.PayloadDigest != digest {
					return errors.New("LEGACY_DELIVERY_PAYLOAD_MISMATCH")
				}
				matches++
			}
		}
	}
	if matches != 1 {
		return errors.New("LEGACY_DELIVERY_PROOF_MISSING")
	}
	return nil
}

func (r *ProcessingRepository) LegacyRetryCoverage(ctx context.Context, job *types.ProcessingJob) (bool, error) {
	request, err := legacyRetryMetadata(job)
	if err != nil || request == nil {
		return false, ErrProcessingConflict
	}
	var items []types.SyncRunItem
	if err := r.db.WithContext(ctx).Where("run_id = ? AND tenant_id = ? AND kind = ?", job.OriginRunID, job.TenantID, "document").Find(&items).Error; err != nil {
		return false, err
	}
	if len(items) != len(request.Errors) {
		return false, nil
	}
	for _, original := range request.Errors {
		if !slices.ContainsFunc(items, func(item types.SyncRunItem) bool {
			return item.ExternalID == original.ExternalID && item.JobID != "" && item.SourceRevision != ""
		}) {
			return false, nil
		}
	}
	return true, nil
}

func commitLegacyRetryAdmission(tx *gorm.DB, scan, job *types.ProcessingJob, input types.ProcessingJob) error {
	request, err := legacyRetryMetadata(scan)
	if err != nil || request == nil {
		return err
	}
	for _, identity := range request.Errors {
		if identity.ExternalID != input.ExternalID {
			continue
		}
		var document struct {
			FileID string `json:"file_id"`
		}
		if json.Unmarshal(input.Metadata, &document) != nil || document.FileID != identity.FileID {
			return ErrProcessingScope
		}
		original, err := NewProcessingRepository(tx).InspectLegacyError(tx.Statement.Context, identity)
		if err != nil || original.Identity != identity {
			return ErrProcessingConflict
		}
		evidence := types.ProcessingLegacyEvidence{ProcessingLegacyIdentity: identity, Action: "retry_admitted", JobID: job.ID, SourceRevision: job.SourceRevision,
			SnapshotDigest: identity.ErrorDigest, ArtifactDigest: input.SourceDigest, ConfigurationRevision: scan.ConfigurationRevision,
			EvidenceReference: fmt.Sprintf("legacy-dead-letter:%d", request.DeadLetterID), EvidenceDigest: request.PayloadDigest, DrainID: request.DrainID,
			Actor: request.Actor, Reason: request.Reason, OperationRequestID: fmt.Sprintf("legacy-retry:%s:%d", scan.ID, identity.ErrorOrdinal)}
		if _, err := appendLegacyEvidence(tx, evidence); err != nil {
			return err
		}
		var metadata map[string]any
		if json.Unmarshal(job.Metadata, &metadata) != nil || metadata == nil {
			return ErrProcessingConflict
		}
		metadata["legacy_retry_linked"] = true
		job.Metadata, err = json.Marshal(metadata)
		if err != nil {
			return err
		}
		if err := tx.Model(job).Update("metadata", job.Metadata).Error; err != nil {
			return err
		}
		return resolveRetriedLegacyJob(tx, job)
	}
	return ErrProcessingScope
}

func resolveRetriedLegacyJob(tx *gorm.DB, job *types.ProcessingJob) error {
	empty := job.Status == types.ProcessingSkipped && job.Completeness == "verified_empty" && !job.IsPublished
	if job.Kind != types.ProcessingJobDocument || (!empty && (job.Status != types.ProcessingSucceeded || !job.IsPublished || job.KnowledgeID == "")) {
		return nil
	}
	var metadata struct {
		Linked bool `json:"legacy_retry_linked"`
	}
	if json.Unmarshal(job.Metadata, &metadata) != nil || !metadata.Linked {
		return nil
	}
	var rows []types.ProcessingLegacyEvidence
	if err := tx.Where("job_id = ? AND tenant_id = ? AND action = ?", job.ID, job.TenantID, "retry_admitted").Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		row.Action, row.KnowledgeID = "recovered", job.KnowledgeID
		row.Actor, row.Reason = "processing:"+job.ID, "Verified restricted retry job completed"
		row.OperationRequestID = "legacy-recovered:" + row.ID
		row.EvidenceReference = "processing-job:" + job.ID
		row.EvidenceDigest = fmt.Sprintf("%x", sha256.Sum256([]byte(job.PlanDigest+"/"+job.ActiveIndexManifest)))
		row.ArtifactDigest = fmt.Sprintf("%x", sha256.Sum256([]byte(job.ActiveIndexManifest)))
		if empty {
			var proof types.ProcessingStep
			if err := tx.Where("job_id = ? AND stage = ? AND status = ?", job.ID, "assets", types.ProcessingSkipped).Take(&proof).Error; err != nil {
				return err
			}
			if proof.OutputManifestRef == "" || !legacyDigest(proof.OutputDigest) {
				return ErrProcessingConflict
			}
			row.Action, row.Reason = "policy_skipped", "Fresh source verified empty and skipped by policy"
			row.OperationRequestID = "legacy-policy-skipped:" + row.ID
			row.ArtifactDigest = proof.OutputDigest
		}
		if _, err := appendLegacyEvidence(tx, row); err != nil {
			return err
		}
	}
	return nil
}
