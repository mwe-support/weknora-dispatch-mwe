package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
)

type fileRetryLogs struct {
	interfaces.SyncLogRepository
	logs    map[string]*types.SyncLog
	creates int
}

func (r *fileRetryLogs) FindByID(_ context.Context, id string) (*types.SyncLog, error) {
	if v := r.logs[id]; v != nil {
		return v, nil
	}
	return nil, errors.New("not found")
}
func (r *fileRetryLogs) Create(_ context.Context, log *types.SyncLog) error {
	r.creates++
	r.logs[log.ID] = log
	return nil
}
func (r *fileRetryLogs) UpdateResult(_ context.Context, log *types.SyncLog) error {
	r.logs[log.ID] = log
	return nil
}

type fileRetryQueue struct {
	calls int
	task  *asynq.Task
	err   error
}

func (q *fileRetryQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.calls++
	q.task = task
	return &asynq.TaskInfo{}, q.err
}

type fileRetryConnector struct {
	datasource.FileRetryConnector
	next *time.Time
}

func (c fileRetryConnector) NextFileRetry(*types.SyncCursor) (*time.Time, error) { return c.next, nil }

func TestFileCompensationScheduleIsDurableAndDeduplicated(t *testing.T) {
	repo := &fileRetryLogs{logs: map[string]*types.SyncLog{}}
	q := &fileRetryQueue{}
	s := &DataSourceService{syncLogRepo: repo, taskEnqueuer: q}
	ds := &types.DataSource{ID: "ds", TenantID: 1, Status: types.DataSourceStatusActive}
	next := time.Now().Add(2 * time.Minute)
	parent := &types.SyncLog{ID: "parent", DataSourceID: "ds", TenantID: 1}
	payload := types.DataSourceSyncPayload{DataSourceID: "ds", TenantID: 1, ForceFull: true}
	result := &types.SyncResult{Failed: 1}
	if err := s.scheduleFileRetry(context.Background(), fileRetryConnector{next: &next}, ds, parent, payload, nil, result); err != nil {
		t.Fatal(err)
	}
	var p types.DataSourceSyncPayload
	if err := json.Unmarshal(q.task.Payload(), &p); err != nil {
		t.Fatal(err)
	}
	if !p.FileRetryOnly || p.ForceFull || p.FileRetryRound != 1 || p.RetryOf != "parent" || p.DataSourceID != "ds" || p.TenantID != 1 {
		t.Fatalf("unsafe payload: %+v", p)
	}
	if repo.logs[p.SyncLogID] == nil || result.RetryState != "scheduled" || result.NextRetryAt.Before(next) {
		t.Fatal("missing durable plan")
	}
	q.err = asynq.ErrTaskIDConflict
	// Different parent runs converging on the same source/cursor due time share
	// the same task ID and child log, instead of creating parallel file passes.
	parent.ID = "another-parent"
	if err := s.scheduleFileRetry(context.Background(), fileRetryConnector{next: &next}, ds, parent, payload, nil, result); err != nil {
		t.Fatal(err)
	}
	if repo.creates != 1 {
		t.Fatal("duplicate retry log")
	}
}

func TestFileCompensationDoesNotSchedulePermanentOrPaused(t *testing.T) {
	s := &DataSourceService{}
	r := &types.SyncResult{Failed: 1}
	ds := &types.DataSource{Status: types.DataSourceStatusActive}
	if err := s.scheduleFileRetry(context.Background(), fileRetryConnector{}, ds, nil, types.DataSourceSyncPayload{}, nil, r); err != nil {
		t.Fatal(err)
	}
	if r.RetryState != "needs_manual" {
		t.Fatal("permanent failure retried")
	}
	next := time.Now()
	ds.Status = types.DataSourceStatusPaused
	if err := s.scheduleFileRetry(context.Background(), fileRetryConnector{next: &next}, ds, nil, types.DataSourceSyncPayload{}, nil, r); err != nil {
		t.Fatal(err)
	}
	if r.RetryState != "paused" {
		t.Fatal("paused source retried")
	}
}

func TestDataSourceSyncLockHonorsCancellation(t *testing.T) {
	release, err := acquireDataSourceSync(context.Background(), t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = acquireDataSourceSync(ctx, t.Name()); !errors.Is(err, context.Canceled) {
		t.Fatal("lock ignored cancellation")
	}
}

func TestFileCompensationRecoversEnqueueFailureWithoutOrphanRunningLog(t *testing.T) {
	repo := &fileRetryLogs{logs: map[string]*types.SyncLog{}}
	q := &fileRetryQueue{err: errors.New("redis unavailable")}
	s := &DataSourceService{syncLogRepo: repo, taskEnqueuer: q}
	ds := &types.DataSource{ID: "ds", TenantID: 1, Status: types.DataSourceStatusActive}
	parent := &types.SyncLog{ID: "parent"}
	next := time.Now().Add(time.Minute)
	fc := fileRetryConnector{next: &next}
	payload := types.DataSourceSyncPayload{DataSourceID: "ds", TenantID: 1}
	if err := s.scheduleFileRetry(context.Background(), fc, ds, parent, payload, nil, &types.SyncResult{}); err == nil {
		t.Fatal("enqueue failure hidden")
	}
	for _, log := range repo.logs {
		if log.Status == types.SyncLogStatusRunning {
			t.Fatal("orphan running log blocks scheduler")
		}
	}
	q.err = nil
	if err := s.scheduleFileRetry(context.Background(), fc, ds, parent, payload, nil, &types.SyncResult{}); err != nil {
		t.Fatal(err)
	}
	if repo.creates != 1 {
		t.Fatal("retry created duplicate log")
	}
}

func TestFileCompensationRealAsynqScheduleDeduplicates(t *testing.T) {
	r := miniredis.RunT(t)
	opts := asynq.RedisClientOpt{Addr: r.Addr()}
	queue := asynq.NewClient(opts)
	defer queue.Close()
	inspector := asynq.NewInspector(opts)
	defer inspector.Close()
	repo := &fileRetryLogs{logs: map[string]*types.SyncLog{}}
	s := &DataSourceService{syncLogRepo: repo, taskEnqueuer: queue}
	ds := &types.DataSource{ID: "ds-real", TenantID: 1, Status: types.DataSourceStatusActive}
	next := time.Now().Add(2 * time.Minute)
	fc := fileRetryConnector{next: &next}
	payload := types.DataSourceSyncPayload{DataSourceID: ds.ID, TenantID: 1}
	for _, id := range []string{"parent1", "parent2"} {
		if err := s.scheduleFileRetry(context.Background(), fc, ds, &types.SyncLog{ID: id}, payload, nil, &types.SyncResult{}); err != nil {
			t.Fatal(err)
		}
	}
	info, err := inspector.GetQueueInfo(types.QueueSync)
	if err != nil {
		t.Fatal(err)
	}
	if info.Scheduled != 1 || repo.creates != 1 {
		t.Fatalf("duplicate schedule: %+v, logs=%d", info, repo.creates)
	}
	for id := range repo.logs {
		task, err := inspector.GetTaskInfo(types.QueueSync, "file-retry:"+id)
		if err != nil {
			t.Fatal(err)
		}
		if task.State != asynq.TaskStateScheduled || task.Type != types.TypeDataSourceSync {
			t.Fatalf("unexpected task: %+v", task)
		}
		if task.Timeout != 2*time.Hour {
			t.Fatalf("minute retries have insufficient task budget: %v", task.Timeout)
		}
	}
}
