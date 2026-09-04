package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

// ponytail: production has one app process. Multiple replicas require a shared
// per-data-source lease before enabling concurrent consumers across replicas.
var dataSourceSyncSlots sync.Map

func acquireDataSourceSync(ctx context.Context, id string) (func(), error) {
	v, _ := dataSourceSyncSlots.LoadOrStore(id, make(chan struct{}, 1))
	ch := v.(chan struct{})
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *DataSourceService) scheduleFileRetry(ctx context.Context, fc datasource.FileRetryConnector, ds *types.DataSource, parent *types.SyncLog, payload types.DataSourceSyncPayload, cursor *types.SyncCursor, result *types.SyncResult) error {
	when, err := fc.NextFileRetry(cursor)
	if err != nil {
		return err
	}
	if when == nil {
		if result.Failed > 0 {
			result.RetryState = "needs_manual"
		} else if payload.FileRetryOnly {
			result.RetryState = "completed"
		}
		return nil
	}
	if ds.Status == types.DataSourceStatusPaused {
		result.RetryState = "paused"
		return nil
	}
	// Asynq schedules with second precision. Round upward so the pass does not
	// arrive before the cursor's due time and manufacture empty retry rounds.
	at := when.Truncate(time.Second).Add(time.Second)
	when = &at
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("file-retry:"+ds.ID+":"+when.Format(time.RFC3339Nano))).String()
	child, findErr := s.syncLogRepo.FindByID(ctx, id)
	if findErr != nil || child == nil {
		child = &types.SyncLog{ID: id, DataSourceID: ds.ID, TenantID: ds.TenantID, Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC()}
		child.Result, _ = (&types.SyncResult{RetryState: "scheduled", RetryRound: payload.FileRetryRound + 1, RetryOf: parent.ID, NextRetryAt: when}).ToJSON()
		if err = s.syncLogRepo.Create(ctx, child); err != nil {
			return err
		}
	}
	if child.DataSourceID != ds.ID || child.TenantID != ds.TenantID {
		return errors.New("file retry log identity mismatch")
	}
	if child.Status == types.SyncLogStatusFailed {
		child.Status = types.SyncLogStatusRunning
		child.FinishedAt = nil
		child.ErrorMessage = ""
		child.Result, _ = (&types.SyncResult{RetryState: "scheduled", RetryRound: payload.FileRetryRound + 1, RetryOf: parent.ID, NextRetryAt: when}).ToJSON()
		if err = s.syncLogRepo.UpdateResult(ctx, child); err != nil {
			return err
		}
	}
	payload.FileRetryOnly = true
	payload.FileRetryRound++
	payload.RetryOf = parent.ID
	payload.SyncLogID = id
	payload.ForceFull = false
	payload.Trigger = "file_compensation"
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.taskEnqueuer.Enqueue(asynq.NewTask(types.TypeDataSourceSync, body),
		asynq.Queue(types.QueueSync), asynq.TaskID("file-retry:"+id), asynq.ProcessAt(*when),
		asynq.MaxRetry(2), asynq.Timeout(30*time.Minute), asynq.Retention(24*time.Hour))
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		// An unsent retry must not leave HasRunningSync permanently blocking cron.
		child.Status = types.SyncLogStatusFailed
		child.FinishedAt = timePtr(time.Now().UTC())
		child.ErrorMessage = "file compensation enqueue failed; retry plan retained"
		child.Result, _ = (&types.SyncResult{RetryState: "enqueue_failed", RetryOf: parent.ID, NextRetryAt: when}).ToJSON()
		if saveErr := s.syncLogRepo.UpdateResult(ctx, child); saveErr != nil {
			return fmt.Errorf("enqueue and status persistence failed: %w", errors.Join(err, saveErr))
		}
		return fmt.Errorf("persisted file retry could not be enqueued: %w", err)
	}
	result.RetryState = "scheduled"
	result.NextRetryAt = when
	return nil
}
