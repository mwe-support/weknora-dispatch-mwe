package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type processingCrashQueue struct{ client *asynq.Client }

func processingCrashBoundary() {
	fmt.Println("PROCESSING_CRASH_READY")
	time.Sleep(time.Minute) // Parent kills this process, bypassing every defer.
}

func (q processingCrashQueue) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	info, err := q.client.Enqueue(task, opts...)
	if err == nil {
		processingCrashBoundary()
	}
	return info, err
}

// Actual process death complements the unit-level lost-ACK/lease tests. This
// uses only the labeled test PostgreSQL/Redis and one disposable SQL schema.
func TestProcessingProcessCrashRecovery(t *testing.T) {
	dsn, addr := os.Getenv("PROCESSING_TEST_POSTGRES"), os.Getenv("PROCESSING_TEST_REDIS")
	if dsn == "" || addr == "" {
		t.Skip("isolated PostgreSQL and Redis are required")
	}
	require.Contains(t, dsn, "host=lifecycle-pg ")
	require.Contains(t, dsn, "dbname=lifecycle_test ")
	require.Equal(t, "lifecycle-redis:6379", addr)
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	defer client.Close()
	ctx := context.Background()
	plan := []types.ProcessingStepSpec{{Stage: "fetch", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "crash-input", RequiredForCompletion: true}}
	effect := func(db *gorm.DB) ProcessingExecutor {
		return func(ctx context.Context, lease types.ProcessingLease) (types.ProcessingOutcome, error) {
			err := db.WithContext(ctx).Exec("INSERT INTO crash_effects (job_id) VALUES (?) ON CONFLICT DO NOTHING", lease.Job.ID).Error
			return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/confirmed", OutputDigest: "confirmed"}, err
		}
	}
	taskFor := func(r *repository.ProcessingRepository, jobID string) *asynq.Task {
		steps, err := r.ListSteps(ctx, 1, jobID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		step := steps[0]
		data, err := json.Marshal(types.ProcessingTaskPayload{TenantID: 1, ProcessingRef: types.ProcessingRef{Protocol: 2, JobID: jobID, Generation: 1, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}})
		require.NoError(t, err)
		return asynq.NewTask(types.TypeProcessingStep+":"+types.QueueSync, data)
	}
	if boundary := os.Getenv("PROCESSING_CRASH_BOUNDARY"); boundary != "" {
		schema := os.Getenv("PROCESSING_CRASH_SCHEMA")
		require.True(t, strings.HasPrefix(schema, "processing_service_"))
		db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{})
		require.NoError(t, err)
		r := repository.NewProcessingRepository(db)
		jobID := os.Getenv("PROCESSING_CRASH_JOB")
		require.NoError(t, r.PlanSteps(ctx, 1, jobID, plan))
		if boundary == "commit_before_delivery" {
			processingCrashBoundary()
		}
		if boundary == "delivery_before_receipt" {
			require.NoError(t, NewProcessingService(r, processingCrashQueue{client}, nil, nil).Dispatch(ctx))
		}
		s := NewProcessingService(r, client, effect(db), nil)
		require.NoError(t, s.Dispatch(ctx))
		require.NoError(t, s.Process(ctx, taskFor(r, jobID)))
		processingCrashBoundary() // Commit is durable; queue still has no worker ACK.
		return
	}
	for _, boundary := range []string{"commit_before_delivery", "delivery_before_receipt", "completion_before_ack"} {
		t.Run(boundary, func(t *testing.T) {
			db := processingServiceTestDatabase(t)
			require.NoError(t, db.Exec("CREATE TABLE crash_effects (job_id text PRIMARY KEY)").Error)
			var schema string
			require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
			r := repository.NewProcessingRepository(db)
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "crash-test", SourceRevision: "v1", PipelineFingerprint: "p1"})
			require.NoError(t, err)
			executable, err := os.Executable()
			require.NoError(t, err)
			command := exec.Command(executable, "-test.run=^TestProcessingProcessCrashRecovery$")
			command.Env = append(os.Environ(), "PROCESSING_CRASH_BOUNDARY="+boundary, "PROCESSING_CRASH_SCHEMA="+schema, "PROCESSING_CRASH_JOB="+job.ID)
			stdout, err := command.StdoutPipe()
			require.NoError(t, err)
			command.Stderr = io.Discard
			require.NoError(t, command.Start())
			defer command.Process.Kill()
			ready := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					if scanner.Text() == "PROCESSING_CRASH_READY" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case reached := <-ready:
				require.True(t, reached, "child must reach the specified durable boundary")
			case <-time.After(15 * time.Second):
				t.Fatal("child did not reach crash boundary")
			}
			require.NoError(t, command.Process.Kill())
			require.Error(t, command.Wait(), "process must die before normal return or cleanup")
			resumed := NewProcessingService(r, client, effect(db), nil)
			require.NoError(t, resumed.Dispatch(ctx))
			task := taskFor(r, job.ID)
			require.NoError(t, resumed.Process(ctx, task))
			require.NoError(t, resumed.Process(ctx, task))
			var effects, commits int64
			require.NoError(t, db.Table("crash_effects").Where("job_id = ?", job.ID).Count(&effects).Error)
			require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("job_id = ? AND event_type = ?", job.ID, "step_committed").Count(&commits).Error)
			require.EqualValues(t, 1, effects)
			require.EqualValues(t, 1, commits)
			pending, err := r.PendingDeliveries(ctx, 100)
			require.NoError(t, err)
			require.Empty(t, pending)
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			require.Equal(t, types.ProcessingSucceeded, steps[0].Status)
			require.Equal(t, 1, steps[0].Attempt)
			inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr})
			require.NoError(t, inspector.DeleteTask(types.QueueSync, steps[0].QueueTaskID))
			require.NoError(t, inspector.Close())
			t.Logf("killed at %s; durable delivery recovered, effect=1, completion=1, attempt=1", boundary)
		})
	}
}
