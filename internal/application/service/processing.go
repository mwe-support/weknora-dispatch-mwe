package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

type ProcessingExecutor func(context.Context, types.ProcessingLease) (types.ProcessingOutcome, error)

// ProcessingService delivers committed outbox entries and executes one leased
// unit. Business failures are committed to the ledger and acknowledged to Asynq.
type ProcessingService struct {
	repo      *repository.ProcessingRepository
	tasks     interfaces.TaskEnqueuer
	execute   ProcessingExecutor
	knowledge *knowledgeService
}

func NewProcessingService(repo *repository.ProcessingRepository, tasks interfaces.TaskEnqueuer, execute ProcessingExecutor, knowledge interfaces.KnowledgeService) *ProcessingService {
	s, _ := knowledge.(*knowledgeService)
	return &ProcessingService{repo: repo, tasks: tasks, execute: execute, knowledge: s}
}

func (s *ProcessingService) Dispatch(ctx context.Context) error {
	ops, err := s.repo.PendingDeliveries(ctx, 100)
	if err != nil {
		return err
	}
	var failures error
	for _, op := range ops {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ref types.ProcessingRef
		if err := json.Unmarshal(op.Payload, &ref); err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		step, err := s.repo.GetStep(ctx, op.TenantID, ref.JobID, ref.StepID)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		var metadata types.JSON
		if step.Stage == "publish" {
			job, err := s.repo.GetJob(ctx, op.TenantID, ref.JobID)
			if err != nil {
				failures = errors.Join(failures, err)
				continue
			}
			metadata = job.Metadata
		}
		queue := types.ProcessingQueue(step.Stage, metadata)
		id := fmt.Sprintf("processing-%s-%d-%d", ref.StepID, ref.Attempt, ref.DispatchSeq)
		payload, err := json.Marshal(types.ProcessingTaskPayload{ProcessingRef: ref, TenantID: op.TenantID})
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		task := asynq.NewTask(types.TypeProcessingStep+":"+queue, payload)
		// No business retry in Asynq. If a delivery or worker is lost, recovery
		// checks durable state before producing another dispatch or attempt.
		info, err := s.tasks.Enqueue(task, asynq.TaskID(id), asynq.Queue(queue), asynq.MaxRetry(0), asynq.Timeout(24*time.Hour))
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			err = nil
		} else if err == nil {
			if info == nil || info.ID == "" {
				err = errors.New("queue returned no delivery receipt")
			} else {
				id = info.ID
			}
		}
		if err == nil {
			err = s.repo.ConfirmDelivery(ctx, op.TenantID, ref, id)
		}
		if err != nil {
			failures = errors.Join(failures, err)
		}
	}
	return failures
}

func (s *ProcessingService) Process(ctx context.Context, task *asynq.Task) error {
	var payload types.ProcessingTaskPayload
	if len(task.Payload()) > 8192 || json.Unmarshal(task.Payload(), &payload) != nil || payload.TenantID == 0 || payload.Protocol != types.ProcessingProtocol {
		return fmt.Errorf("invalid processing delivery: %w", asynq.SkipRetry)
	}
	lease, err := s.repo.ClaimStep(ctx, payload.TenantID, payload.ProcessingRef, 2*time.Minute)
	if errors.Is(err, repository.ErrProcessingConflict) || errors.Is(err, repository.ErrProcessingScope) || errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	workCtx, cancel := context.WithCancel(context.WithValue(ctx, types.TenantIDContextKey, payload.TenantID))
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if s.repo.Heartbeat(workCtx, payload.TenantID, *lease, 2*time.Minute, "") != nil {
					cancel()
					return
				}
			}
		}
	}()
	outcome, runErr := s.run(workCtx, *lease)
	canceled := workCtx.Err() != nil
	cancel()
	<-stopped
	// An interrupted execution has no success receipt. Reconciliation expires
	// its lease; export_start becomes uncertain instead of being issued again.
	if canceled {
		return ctx.Err()
	}
	if runErr != nil {
		outcome = types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "internal", ErrorCode: "EXECUTION_ERROR", Message: "Stage execution failed before producing a validated result"}
		// An executor panic/error after a non-idempotent call may have lost its
		// remote receipt. It must not become manually retryable as a generic error.
		outcome.Retryable = lease.Step.Stage == "export_start"
	}
	err = s.repo.FinishStep(ctx, payload.TenantID, *lease, outcome)
	if errors.Is(err, repository.ErrProcessingConflict) || errors.Is(err, repository.ErrProcessingScope) || errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	return err
}

func (s *ProcessingService) run(ctx context.Context, lease types.ProcessingLease) (outcome types.ProcessingOutcome, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("processing executor panicked")
		}
	}()
	if s.execute == nil {
		return outcome, errors.New("processing executor is not configured")
	}
	return s.execute(ctx, lease)
}

// Run uses a keyset cursor so a permanently blocked source cannot starve later
// jobs. Backend inspection errors never imply that an acknowledged task is lost.
func (s *ProcessingService) Run(ctx context.Context, inspector *asynq.Inspector) {
	if s.knowledge != nil {
		go s.runMaintenance(ctx)
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	cursor := ""
	for {
		jobs, err := s.repo.RecoveryJobs(ctx, cursor, 100)
		if err == nil {
			for _, job := range jobs {
				cursor = job.ID
				if err = s.repo.ReconcileJob(ctx, job.TenantID, job.ID); err != nil {
					continue
				}
				steps, readErr := s.repo.ListSteps(ctx, job.TenantID, job.ID)
				if readErr != nil {
					continue
				}
				for _, step := range steps {
					if step.Status != types.ProcessingQueued || step.QueuedAt == nil || time.Since(*step.QueuedAt) < 2*time.Minute {
						continue
					}
					live := false
					if inspector != nil {
						task, inspectErr := inspector.GetTaskInfo(types.ProcessingQueue(step.Stage, job.Metadata), step.QueueTaskID)
						if inspectErr != nil && !errors.Is(inspectErr, asynq.ErrTaskNotFound) && !errors.Is(inspectErr, asynq.ErrQueueNotFound) {
							continue
						}
						live = task != nil && task.State != asynq.TaskStateArchived && task.State != asynq.TaskStateCompleted
					}
					if !live {
						_ = s.repo.Redeliver(ctx, job.TenantID, job.ID, step)
					}
				}
			}
			if len(jobs) < 100 {
				cursor = ""
			}
		}
		if err = s.Dispatch(ctx); err != nil && ctx.Err() == nil {
			logger.Warnf(ctx, "[Processing] lifecycle delivery deferred: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
