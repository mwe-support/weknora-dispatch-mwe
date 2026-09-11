package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type processingTestQueue struct {
	tasks []*asynq.Task
	fail  bool
}

func TestProcessingWikiChildrenUseIsolatedQueue(t *testing.T) {
	r := processingServiceTestStore(t)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	var specs []types.ProcessingStepSpec
	for _, stage := range []string{"wiki_extract", "wiki_dedup", "wiki_cite", "wiki_summary_part", "wiki_summary", "wiki_prepare", "wiki_taxonomy_input", "wiki_taxonomy", "wiki_taxonomy_vectors", "wiki_taxonomy_plan", "wiki_pages", "wiki_page", "wiki_links"} {
		specs = append(specs, types.ProcessingStepSpec{Stage: stage, UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: stage, RequiredForCompletion: true})
	}
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, specs))
	queue := &processingTestQueue{}
	s := NewProcessingService(r, queue, nil, nil)
	require.NoError(t, s.Dispatch(ctx))
	require.Len(t, queue.tasks, len(specs))
	for _, task := range queue.tasks {
		require.Equal(t, types.TypeProcessingStep+":"+types.QueueWiki, task.Type())
	}
	require.Equal(t, types.QueueMaintenance, types.ProcessingQueue("wiki_retire_page"))
	require.Equal(t, types.QueueMaintenance, types.ProcessingQueue("retire_previous"))
	for _, stage := range []string{"embedding", "faq_embedding"} {
		require.Equal(t, types.QueueSummary, types.ProcessingQueue(stage), "model calls require the existing enrichment pool")
	}
	require.Equal(t, types.QueuePostProcess, types.ProcessingQueue("publish"))
}

func (q *processingTestQueue) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	if q.fail {
		return nil, errors.New("synthetic transport failure")
	}
	q.tasks = append(q.tasks, task)
	return &asynq.TaskInfo{ID: "accepted"}, nil
}

func processingServiceTestStore(t *testing.T) *repository.ProcessingRepository {
	return repository.NewProcessingRepository(processingServiceTestDatabase(t))
}

func processingServiceTestDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	var dialector gorm.Dialector = sqlite.Open(":memory:")
	if dsn := os.Getenv("PROCESSING_TEST_POSTGRES"); dsn != "" {
		require.Contains(t, dsn, "host=lifecycle-pg ")
		require.Contains(t, dsn, "dbname=lifecycle_test ")
		admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		require.NoError(t, err)
		schema := "processing_service_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
		t.Cleanup(func() {
			_ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error
			raw, _ := admin.DB()
			_ = raw.Close()
		})
		dialector = postgres.Open(dsn + " search_path=" + schema)
	}
	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.DataSource{}, &types.ProcessingJob{}, &types.ProcessingStep{}, &types.ProcessingEvent{}, &types.TaskPendingOp{}, &types.ProcessingArtifactReference{}, &types.ProcessingStorageReservation{}, &types.ProcessingGraphWrite{}, &types.ProcessingWikiWrite{}, &types.WikiPage{}, &types.StoredResource{}, &types.ResourceBinding{}, &types.Chunk{}, &types.FAQIndexWrite{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.Create(&types.DataSource{ID: "source", TenantID: 1, KnowledgeBaseID: "kb", Name: "synthetic", Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive}).Error)
	return db
}

func TestProcessingDispatcherAndWorkerKeepOneBusinessRetryOwner(t *testing.T) {
	r := processingServiceTestStore(t)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
		{Stage: "fetch", UnitKey: "source", Phase: types.ProcessingPhasePrepare, InputFingerprint: "f1", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "parse", UnitKey: "source", Phase: types.ProcessingPhasePrepare, InputFingerprint: "p1", DependsOn: []string{"fetch/source"}, RequiredForReady: true, RequiredForCompletion: true},
	}))
	calls := map[string]int{}
	q := &processingTestQueue{fail: true}
	svc := NewProcessingService(r, q, func(ctx context.Context, lease types.ProcessingLease) (types.ProcessingOutcome, error) {
		calls[lease.Step.Stage]++
		if lease.Step.Stage == "parse" {
			return types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "transient", ErrorCode: "SYNTHETIC", Retryable: true}, nil
		}
		return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/source", OutputDigest: "digest", Completeness: "complete"}, nil
	}, nil)
	require.Error(t, svc.Dispatch(ctx))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		require.Equal(t, 0, step.RetryCount)
	}
	q.fail = false
	require.NoError(t, svc.Dispatch(ctx))
	require.Len(t, q.tasks, 1)
	var payload types.ProcessingTaskPayload
	require.NoError(t, json.Unmarshal(q.tasks[0].Payload(), &payload))
	require.EqualValues(t, 1, payload.TenantID)
	require.NoError(t, svc.Process(ctx, q.tasks[0]))
	require.NoError(t, svc.Process(ctx, q.tasks[0]))
	require.Equal(t, 1, calls["fetch"])
	require.NoError(t, svc.Dispatch(ctx))
	require.Len(t, q.tasks, 2)
	require.NoError(t, svc.Process(ctx, q.tasks[1]))
	require.NoError(t, svc.Process(ctx, q.tasks[1]))
	require.Equal(t, 1, calls["parse"])
	steps, err = r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		if step.Stage == "fetch" {
			require.Equal(t, types.ProcessingSucceeded, step.Status)
			require.Equal(t, 1, step.Attempt)
		}
		if step.Stage == "parse" {
			require.Equal(t, types.ProcessingRetryWait, step.Status)
			require.Equal(t, 1, step.RetryCount)
			require.Equal(t, 2, step.Attempt)
		}
	}
	// A queue payload cannot select a different tenant, even with a valid job ID.
	payload.TenantID = 2
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, svc.Process(ctx, asynq.NewTask(q.tasks[0].Type(), data)))
	require.Equal(t, 1, calls["fetch"])
}

type processingLostAckQueue struct {
	client *asynq.Client
	lose   bool
}

func (q *processingLostAckQueue) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	info, err := q.client.Enqueue(task, opts...)
	if err == nil && q.lose {
		q.lose = false
		return nil, errors.New("synthetic lost Redis acknowledgement")
	}
	return info, err
}

func TestProcessingRealRedisRedeliveryAfterLostAcknowledgement(t *testing.T) {
	addr := os.Getenv("PROCESSING_TEST_REDIS")
	if addr == "" {
		t.Skip("isolated Redis is not configured")
	}
	require.Equal(t, "lifecycle-redis:6379", addr, "never run this against a business queue")
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping())
	r := processingServiceTestStore(t)
	ctx := context.Background()
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"})
	require.NoError(t, err)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
		{Stage: "fetch", UnitKey: "source", Phase: types.ProcessingPhasePrepare, InputFingerprint: "f1", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "parse", UnitKey: "source", Phase: types.ProcessingPhasePrepare, InputFingerprint: "p1", DependsOn: []string{"fetch/source"}, RequiredForReady: true, RequiredForCompletion: true},
	}))
	svc := NewProcessingService(r, &processingLostAckQueue{client: client, lose: true}, func(ctx context.Context, lease types.ProcessingLease) (types.ProcessingOutcome, error) {
		return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/" + lease.Step.Stage, OutputDigest: "digest", Completeness: "complete"}, nil
	}, nil)
	require.Error(t, svc.Dispatch(ctx))   // Redis accepted it, but the producer lost its receipt.
	require.NoError(t, svc.Dispatch(ctx)) // Task ID conflict is the acknowledgement.
	server := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{Concurrency: 2, Queues: map[string]int{types.QueueSync: 1, types.QueueDefault: 1}, TaskCheckInterval: 25 * time.Millisecond, ShutdownTimeout: time.Second})
	mux := asynq.NewServeMux()
	mux.HandleFunc(types.TypeProcessingStep+":", svc.Process)
	require.NoError(t, server.Start(mux))
	t.Cleanup(server.Shutdown)
	require.Eventually(t, func() bool {
		if svc.Dispatch(ctx) != nil {
			return false
		}
		steps, err := r.ListSteps(ctx, 1, job.ID)
		if err != nil {
			return false
		}
		for _, step := range steps {
			if step.Status != types.ProcessingSucceeded {
				return false
			}
		}
		return len(steps) == 2
	}, 10*time.Second, 25*time.Millisecond)
	events, err := r.ListEvents(ctx, 1, job.ID, 0, 100)
	require.NoError(t, err)
	started, committed := 0, 0
	for _, event := range events {
		if event.Type == "step_started" {
			started++
		}
		if event.Type == "step_committed" {
			committed++
		}
	}
	require.Equal(t, 2, started)
	require.Equal(t, 2, committed)
}
