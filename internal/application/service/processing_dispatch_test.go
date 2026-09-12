package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type processingNotifyingQueue struct{ tasks chan *asynq.Task }

func (q processingNotifyingQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	var p types.ProcessingTaskPayload
	if err := json.Unmarshal(task.Payload(), &p); err != nil {
		return nil, err
	}
	q.tasks <- task
	return &asynq.TaskInfo{ID: fmt.Sprintf("processing-%s-%d-%d", p.StepID, p.Attempt, p.DispatchSeq)}, nil
}

func TestProcessingCompletionDispatchDoesNotWaitForRecoveryScan(t *testing.T) {
	db := processingServiceTestDatabase(t)
	r := repository.NewProcessingRepository(db)
	ctx, cancel := context.WithCancel(context.Background())
	queue := processingNotifyingQueue{tasks: make(chan *asynq.Task, 8)}
	svc := NewProcessingService(r, queue, func(context.Context, types.ProcessingLease) (types.ProcessingOutcome, error) {
		return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/confirmed", OutputDigest: "confirmed", Completeness: "complete"}, nil
	}, nil)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "handoff", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
		{Stage: "normalize", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "cached", RequiredForCompletion: true},
		{Stage: "parse", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "parse", DependsOn: []string{"normalize/body"}, RequiredForCompletion: true},
	}))
	require.NoError(t, svc.Dispatch(ctx))
	first := <-queue.tasks
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	// Block only the recovery scan after it has released its SQL rows. Normal
	// outbox reads and committed document transitions remain available.
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:hold-recovery", func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "retirement_state") && strings.Contains(tx.Statement.SQL.String(), "EXISTS") {
			once.Do(func() { close(entered); <-release })
		}
	}))
	go func() { defer close(stopped); svc.Run(ctx, nil) }()
	t.Cleanup(func() {
		close(release)
		cancel()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Error("processing loops did not stop")
		}
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery scan did not enter the controlled wait")
	}
	// Let the independent dispatcher's initial empty scan finish before the
	// commit, so this exercises the completion wake-up, not only startup.
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	require.NoError(t, svc.Process(ctx, first))
	select {
	case next := <-queue.tasks:
		var payload types.ProcessingTaskPayload
		require.NoError(t, json.Unmarshal(next.Payload(), &payload))
		step, err := r.GetStep(ctx, 1, payload.JobID, payload.StepID)
		require.NoError(t, err)
		require.Equal(t, "parse", step.Stage)
		t.Logf("committed predecessor dispatched its successor in %s while recovery remained blocked", time.Since(start))
	case <-time.After(800 * time.Millisecond):
		t.Fatal("ready successor waited for recovery/polling instead of the committed predecessor")
	}
	// An external producer does not have this service's in-memory wake-up.
	// Its committed outbox must still be delivered while recovery is blocked.
	external, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "external-producer", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, external.ID, []types.ProcessingStepSpec{{Stage: "normalize", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "external", RequiredForCompletion: true}}))
	select {
	case task := <-queue.tasks:
		var p types.ProcessingTaskPayload
		require.NoError(t, json.Unmarshal(task.Payload(), &p))
		require.Equal(t, external.ID, p.JobID)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("fallback did not deliver an external producer's committed outbox")
	}
}
