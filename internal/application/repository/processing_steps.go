package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// PlanSteps atomically seals an initial DAG and creates its first deliveries.
// Repeating the identical plan is harmless after a lost response.
func (r *ProcessingRepository) PlanSteps(ctx context.Context, tenant uint64, jobID string, specs []types.ProcessingStepSpec) error {
	steps, digest, err := processingPlan(jobID, specs)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingJob(tx, tenant, jobID)
		if err != nil {
			return err
		}
		if job.PlanSealed {
			if job.PlanDigest == digest {
				return nil
			}
			return ErrProcessingConflict
		}
		if !job.IsCurrent || processingTerminal(job.Status) {
			return ErrProcessingConflict
		}
		now, err := processingDBTime(tx)
		if err != nil {
			return err
		}
		deadline := now.Add(24 * time.Hour)
		for i := range steps {
			steps[i].DeadlineAt = &deadline
		}
		if err := tx.Create(&steps).Error; err != nil {
			return err
		}
		job.PlanSealed, job.PlanDigest = true, digest
		if err := tx.Model(job).Updates(map[string]any{"plan_sealed": true, "plan_digest": digest}).Error; err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "plan_sealed"}); err != nil {
			return err
		}
		return scheduleProcessingSteps(tx, job)
	})
}

func processingPlan(jobID string, specs []types.ProcessingStepSpec) ([]types.ProcessingStep, string, error) {
	if jobID == "" || len(specs) == 0 {
		return nil, "", errors.New("processing plan must contain work")
	}
	ids := make(map[string]string, len(specs))
	for _, spec := range specs {
		key := spec.Stage + "/" + spec.UnitKey
		if spec.Stage == "" || strings.ContainsAny(spec.Stage, "/\x00") || spec.UnitKey == "" || spec.InputFingerprint == "" || len(spec.Stage) > 64 || len(spec.UnitKey) > 512 {
			return nil, "", errors.New("processing step requires a bounded stage, unit and input fingerprint")
		}
		if _, exists := ids[key]; exists {
			return nil, "", errors.New("duplicate processing unit")
		}
		if spec.Kind != "" && spec.Kind != "work" && spec.Kind != "barrier" {
			return nil, "", errors.New("invalid processing step kind")
		}
		switch spec.Phase {
		case types.ProcessingPhasePrepare, types.ProcessingPhasePublish, types.ProcessingPhaseProjection, types.ProcessingPhaseRetire, types.ProcessingPhaseScan:
		default:
			return nil, "", errors.New("invalid processing phase")
		}
		ids[key] = uuid.NewSHA1(uuid.NameSpaceOID, []byte(jobID+"\x00"+key)).String()
	}
	steps := make([]types.ProcessingStep, 0, len(specs))
	for _, spec := range specs {
		dependencies := make([]string, 0, len(spec.DependsOn))
		for _, key := range spec.DependsOn {
			id, exists := ids[key]
			if !exists {
				return nil, "", errors.New("processing dependency is absent from plan")
			}
			dependencies = append(dependencies, id)
		}
		slices.Sort(dependencies)
		dependencies = slices.Compact(dependencies)
		encoded, err := json.Marshal(dependencies)
		if err != nil {
			return nil, "", err
		}
		kind := spec.Kind
		if kind == "" {
			kind = "work"
		}
		steps = append(steps, types.ProcessingStep{ID: ids[spec.Stage+"/"+spec.UnitKey], JobID: jobID,
			Stage: spec.Stage, UnitKey: spec.UnitKey, Phase: spec.Phase, Kind: kind, Dependencies: encoded,
			Input: spec.Input, InputFingerprint: spec.InputFingerprint, Status: types.ProcessingPlanned,
			Attempt: 1, MaxRetries: 4, RequiredForReady: spec.RequiredForReady, RequiredForCompletion: spec.RequiredForCompletion})
	}
	// Plans contain stages/work batches. Native page cursors live in manifests.
	visited := map[string]bool{}
	for len(visited) < len(steps) {
		before := len(visited)
		for _, step := range steps {
			if visited[step.ID] {
				continue
			}
			var dependencies []string
			if err := json.Unmarshal(step.Dependencies, &dependencies); err != nil {
				return nil, "", err
			}
			ready := true
			for _, id := range dependencies {
				if !visited[id] {
					ready = false
					break
				}
			}
			if ready {
				visited[step.ID] = true
			}
		}
		if before == len(visited) {
			return nil, "", errors.New("processing dependency cycle")
		}
	}
	slices.SortFunc(steps, func(a, b types.ProcessingStep) int { return strings.Compare(a.ID, b.ID) })
	encoded, err := json.Marshal(steps)
	if err != nil {
		return nil, "", err
	}
	return steps, fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

// Mutations follow KB -> source -> job -> step lock order. No network I/O is allowed
// inside these transactions; external artifacts are written before commit.
func lockProcessingJob(tx *gorm.DB, tenant uint64, id string, stepIDs ...string) (*types.ProcessingJob, error) {
	if len(stepIDs) > 0 {
		var step types.ProcessingStep
		if err := tx.Where("id = ? AND job_id = ?", stepIDs[0], id).Take(&step).Error; err != nil {
			return nil, err
		}
		if step.Phase == types.ProcessingPhaseRetire {
			return lockProcessingRetirement(tx, tenant, id)
		}
	}
	var job types.ProcessingJob
	if err := tx.Where("id = ? AND tenant_id = ?", id, tenant).Take(&job).Error; err != nil {
		return nil, err
	}
	configuration, err := lockProcessingConfiguration(tx, tenant, job.KnowledgeBaseID)
	if err != nil {
		return nil, err
	}
	if configuration != job.ConfigurationRevision {
		return nil, ErrProcessingScope
	}
	query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", job.DataSourceID, tenant, job.KnowledgeBaseID)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var source types.DataSource
	if err := query.Take(&source).Error; err != nil {
		return nil, err
	}
	if source.Status == types.DataSourceStatusPaused || source.Status == types.DataSourceStatusDeleted {
		return nil, ErrProcessingScope
	}
	scope, auth, err := ProcessingSourceRevisions(&source)
	if err != nil {
		return nil, err
	}
	if scope != job.ScopeRevision || auth != job.AuthRevision {
		return nil, ErrProcessingScope
	}
	query = tx.Where("id = ? AND tenant_id = ?", id, tenant)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.Take(&job).Error; err != nil {
		return nil, err
	}
	return &job, nil
}

func processingStepEligible(job *types.ProcessingJob, step *types.ProcessingStep) bool {
	if step.Phase == types.ProcessingPhaseRetire {
		return !job.IsCurrent && !job.IsPublished && !job.RollbackPin && step.ExpectedPublicationEpoch == job.PublicationEpoch && job.RetirementState != "deleted"
	}
	if job.Status == types.ProcessingCanceled || job.RetirementState == "deleted" {
		return false
	}
	switch step.Phase {
	case types.ProcessingPhaseScan:
		return job.Kind == types.ProcessingJobScan && job.Status != types.ProcessingSucceeded && job.Status != types.ProcessingSuperseded
	case types.ProcessingPhasePrepare:
		return job.IsCurrent && job.Status != types.ProcessingSucceeded && job.Status != types.ProcessingSuperseded
	case types.ProcessingPhasePublish:
		return job.IsCurrent && job.Readiness == "ready" && job.Completeness == "complete"
	case types.ProcessingPhaseProjection:
		return job.IsPublished && step.ExpectedPublicationEpoch == job.PublicationEpoch && job.RetirementState == "retained"
	default:
		return false
	}
}

func processingDependenciesDone(step *types.ProcessingStep, steps []types.ProcessingStep) (bool, error) {
	if step.Kind == "barrier" && step.PlanSealed {
		registered := 0
		for _, child := range steps {
			if child.ParentStepID == step.ID {
				registered++
			}
		}
		if registered != step.ExpectedUnits {
			return false, nil
		}
	}
	var dependencies []string
	if len(step.Dependencies) > 0 {
		if err := json.Unmarshal(step.Dependencies, &dependencies); err != nil {
			return false, err
		}
	}
	for _, id := range dependencies {
		found := false
		for _, candidate := range steps {
			if candidate.ID != id {
				continue
			}
			found = candidate.Status == types.ProcessingSucceeded && (candidate.Kind != "barrier" || candidate.PlanSealed)
			break
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}

func sealProcessingChildren(tx *gorm.DB, job *types.ProcessingJob, parent *types.ProcessingStep, specs []types.ProcessingStepSpec) error {
	if parent.Kind != "barrier" || parent.PlanSealed {
		return errors.New("only an unsealed running barrier may register children")
	}
	var children []types.ProcessingStep
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte("[]")))
	var err error
	if len(specs) > 0 {
		children, digest, err = processingPlan(job.ID, specs)
		if err != nil {
			return err
		}
	}
	var dependencies []string
	if len(parent.Dependencies) > 0 {
		if err := json.Unmarshal(parent.Dependencies, &dependencies); err != nil {
			return err
		}
	}
	for i := range children {
		child := &children[i]
		if child.Phase != parent.Phase {
			return errors.New("child step must preserve its barrier phase")
		}
		child.ParentStepID, child.DeadlineAt = parent.ID, parent.DeadlineAt
		child.ExpectedPublicationEpoch = parent.ExpectedPublicationEpoch
		child.RequiredForReady, child.RequiredForCompletion = parent.RequiredForReady, parent.RequiredForCompletion
		dependencies = append(dependencies, child.ID)
	}
	if len(children) > 0 {
		if parent.Phase == types.ProcessingPhaseScan {
			var pages, units int64
			if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ?", job.ID).Count(&units).Error; err != nil {
				return err
			}
			if err := tx.Model(&types.ProcessingStep{}).Where("job_id = ? AND stage = ?", job.ID, "scan_page").Count(&pages).Error; err != nil {
				return err
			}
			for _, child := range children {
				if child.Stage == "scan_page" {
					pages++
				}
			}
			if units+int64(len(children)) > 100000 || pages > 10000 {
				return errors.New("SCAN_LIMIT_EXCEEDED")
			}
		}
		if err := tx.Create(&children).Error; err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(dependencies)
	if err != nil {
		return err
	}
	parent.PlanSealed, parent.PlanDigest, parent.ExpectedUnits, parent.Dependencies = true, digest, len(children), encoded
	return appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "unit_plan_sealed", StepID: parent.ID, Attempt: parent.Attempt})
}

func scheduleProcessingSteps(tx *gorm.DB, job *types.ProcessingJob) error {
	if !job.PlanSealed {
		return nil
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return err
	}
	var steps []types.ProcessingStep
	if err := tx.Where("job_id = ?", job.ID).Order("id").Find(&steps).Error; err != nil {
		return err
	}
	for i := range steps {
		step := &steps[i]
		if step.Status != types.ProcessingPlanned && step.Status != types.ProcessingRetryWait && step.Status != types.ProcessingWaitingExternal {
			continue
		}
		if step.NextRunAt != nil && step.NextRunAt.After(now) {
			continue
		}
		if step.DeadlineAt != nil && !step.DeadlineAt.After(now) {
			continue
		}
		if !processingStepEligible(job, step) {
			continue
		}
		ready, err := processingDependenciesDone(step, steps)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		from := step.Status
		step.Status, step.DispatchSeq = types.ProcessingEnqueuePending, step.DispatchSeq+1
		payload, err := json.Marshal(processingStepRef(job, step))
		if err != nil {
			return err
		}
		op := types.TaskPendingOp{TenantID: job.TenantID, TaskType: types.TypeProcessingStep, Scope: "processing_job", ScopeID: job.ID,
			Op: "deliver", DedupKey: step.ID, Payload: payload, EnqueuedAt: now, AvailableAt: &now,
			StepID: &step.ID, StepAttempt: &step.Attempt, DispatchSeq: &step.DispatchSeq}
		if err := tx.Create(&op).Error; err != nil {
			return err
		}
		if err := tx.Model(step).Updates(map[string]any{"status": step.Status, "dispatch_seq": step.DispatchSeq}).Error; err != nil {
			return err
		}
		if err := appendProcessingEvent(tx, job, stepEvent(step, "step_scheduled", from)); err != nil {
			return err
		}
	}
	return nil
}

func processingStepRef(job *types.ProcessingJob, step *types.ProcessingStep) types.ProcessingRef {
	return types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, StepID: step.ID,
		Generation: job.Generation, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}
}

func stepEvent(step *types.ProcessingStep, kind, from string) types.ProcessingEvent {
	return types.ProcessingEvent{StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq,
		LeaseToken: step.LeaseToken, Type: kind, FromState: from, ToState: step.Status, QueueTaskID: step.QueueTaskID}
}

func processingRefMatches(job *types.ProcessingJob, step *types.ProcessingStep, ref types.ProcessingRef) bool {
	return ref.Protocol == types.ProcessingProtocol && ref.JobID == job.ID && step.JobID == job.ID && ref.StepID == step.ID &&
		ref.Generation == job.Generation && ref.Attempt == step.Attempt && ref.DispatchSeq == step.DispatchSeq && ref.InputFingerprint == step.InputFingerprint
}
