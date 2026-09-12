package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

// Prefix only transport queues, keeping the real dispatcher, task types,
// worker, dependency checks and durable completion receipts under test.
type processingScopedQueue struct {
	client *asynq.Client
	prefix string
}

func (q processingScopedQueue) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	queue := strings.TrimPrefix(task.Type(), types.TypeProcessingStep+":")
	return q.client.Enqueue(task, append(opts, asynq.Queue(q.prefix+queue))...)
}

func TestProcessingReadyDocumentProgressesWhileSourceWorkerIsOccupied(t *testing.T) {
	addr := os.Getenv("PROCESSING_TEST_REDIS")
	if addr == "" {
		t.Skip("isolated Redis is required")
	}
	require.Equal(t, "lifecycle-redis:6379", addr)
	r := processingServiceTestStore(t)
	ctx := context.Background()
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	prefix := "processing-isolation-" + uuid.NewString() + "-"
	queue := processingScopedQueue{client: client, prefix: prefix}
	entered, release := make(chan struct{}), make(chan struct{})
	svc := NewProcessingService(r, queue, func(ctx context.Context, lease types.ProcessingLease) (types.ProcessingOutcome, error) {
		if lease.Step.Stage == "native_read" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "source", ErrorCode: "SYNTHETIC_SOURCE_FAILURE"}, nil
		}
		return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/confirmed", OutputDigest: "confirmed", Completeness: "complete"}, nil
	}, nil)
	mux := asynq.NewServeMux()
	for _, definition := range types.QueueDefinitions() {
		mux.HandleFunc(types.TypeProcessingStep+":"+definition.Name, svc.Process)
	}
	var servers []*asynq.Server
	var queues []string
	t.Cleanup(func() {
		close(release)
		for _, server := range servers {
			server.Shutdown()
		}
		inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr})
		for _, name := range queues {
			_ = inspector.DeleteQueue(name, true)
		}
		_ = inspector.Close()
		_ = client.Close()
	})
	for _, pool := range []string{types.WorkerPoolCore, types.WorkerPoolMaintenance} {
		weights := make(map[string]int)
		for name, weight := range types.QueueWeightsForPool(pool) {
			weights[prefix+name] = weight
			queues = append(queues, prefix+name)
		}
		server := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{Concurrency: 1, Queues: weights, TaskCheckInterval: 50 * time.Millisecond, ShutdownTimeout: time.Second})
		require.NoError(t, server.Start(mux))
		servers = append(servers, server)
	}
	newJob := func(external string, plan []types.ProcessingStepSpec) string {
		job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: external, SourceRevision: "v1", PipelineFingerprint: "p1"})
		require.NoError(t, err)
		require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
		require.NoError(t, svc.Dispatch(ctx))
		return job.ID
	}
	newJob("slow-source", []types.ProcessingStepSpec{{Stage: "native_read", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "slow", RequiredForCompletion: true}})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("source worker did not start")
	}
	ready := newJob("already-fetched", []types.ProcessingStepSpec{
		{Stage: "normalize", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "cached", RequiredForCompletion: true},
		{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parse", DependsOn: []string{"normalize/body"}, RequiredForCompletion: true},
	})
	require.Eventually(t, func() bool {
		if svc.Dispatch(ctx) != nil {
			return false
		}
		steps, err := r.ListSteps(ctx, 1, ready)
		if err != nil {
			return false
		}
		for _, step := range steps {
			if step.Status != types.ProcessingSucceeded {
				return false
			}
		}
		return len(steps) == 2
	}, 3*time.Second, 20*time.Millisecond, "ready normalize/parse must finish without waiting for the occupied source worker")
}

func TestProcessingNormalizationRerouteFencesOldDelivery(t *testing.T) {
	addr := os.Getenv("PROCESSING_TEST_REDIS")
	if addr == "" {
		t.Skip("isolated Redis is required")
	}
	require.Equal(t, "lifecycle-redis:6379", addr)
	db := processingServiceTestDatabase(t)
	r := repository.NewProcessingRepository(db)
	ctx := context.Background()
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr})
	prefix := "processing-reroute-" + uuid.NewString() + "-"
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(prefix+types.QueueSync, true)
		_ = inspector.DeleteQueue(prefix+types.QueueDefault, true)
		_ = inspector.Close()
		_ = client.Close()
	})
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "reroute", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "normalize", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "cached", RequiredForCompletion: true}}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	old := steps[0]
	ref := types.ProcessingRef{Protocol: 2, JobID: job.ID, StepID: old.ID, Generation: job.Generation, Attempt: old.Attempt, DispatchSeq: old.DispatchSeq, InputFingerprint: old.InputFingerprint}
	payload, err := json.Marshal(types.ProcessingTaskPayload{ProcessingRef: ref, TenantID: 1})
	require.NoError(t, err)
	oldTask := asynq.NewTask(types.TypeProcessingStep+":"+types.QueueSync, payload)
	info, err := client.Enqueue(oldTask, asynq.Queue(prefix+types.QueueSync), asynq.TaskID(fmt.Sprintf("processing-%s-%d-%d", old.ID, old.Attempt, old.DispatchSeq)))
	require.NoError(t, err)
	require.NoError(t, r.ConfirmDelivery(ctx, 1, ref, info.ID))
	require.NoError(t, db.Model(&types.ProcessingStep{}).Where("id = ?", old.ID).Update("queued_at", time.Now().Add(-3*time.Minute)).Error)
	steps, err = r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	_, err = inspector.GetTaskInfo(prefix+types.ProcessingQueue("normalize"), info.ID)
	require.True(t, errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound))
	require.NoError(t, r.Redeliver(ctx, 1, job.ID, steps[0]))
	calls := 0
	svc := NewProcessingService(r, processingScopedQueue{client: client, prefix: prefix}, func(context.Context, types.ProcessingLease) (types.ProcessingOutcome, error) {
		calls++
		return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/confirmed", OutputDigest: "confirmed", Completeness: "complete"}, nil
	}, nil)
	require.NoError(t, svc.Dispatch(ctx))
	steps, err = r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, old.Attempt, steps[0].Attempt)
	require.Equal(t, old.DispatchSeq+1, steps[0].DispatchSeq)
	current, err := inspector.GetTaskInfo(prefix+types.QueueDefault, steps[0].QueueTaskID)
	require.NoError(t, err)
	require.NoError(t, svc.Process(ctx, oldTask))
	require.Equal(t, 0, calls, "old source-queue delivery must not execute")
	require.NoError(t, svc.Process(ctx, asynq.NewTask(current.Type, current.Payload)))
	require.NoError(t, svc.Process(ctx, oldTask))
	require.Equal(t, 1, calls)
}
