package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.uber.org/dig"
)

// SyncTaskExecutor executes tasks synchronously (in a goroutine) without Redis.
// Used in Lite mode as a drop-in replacement for *asynq.Client.
type SyncTaskExecutor struct {
	mu       sync.RWMutex
	handlers map[string]func(context.Context, *asynq.Task) error
}

func NewSyncTaskExecutor() *SyncTaskExecutor {
	return &SyncTaskExecutor{
		handlers: make(map[string]func(context.Context, *asynq.Task) error),
	}
}

// RegisterHandler registers a handler for a given task type pattern.
func (e *SyncTaskExecutor) RegisterHandler(pattern string, handler func(context.Context, *asynq.Task) error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[pattern] = handler
}

// Enqueue satisfies interfaces.TaskEnqueuer.
// Instead of queuing to Redis, it dispatches the task to a goroutine.
// Supports ProcessIn (delay) and MaxRetry options for parity with asynq.
func (e *SyncTaskExecutor) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	e.mu.RLock()
	handler, ok := e.handlers[task.Type()]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("sync task executor: no handler registered for type %q", task.Type())
	}

	var delay time.Duration
	maxRetry := 25 // asynq default
	maxRetrySet := false
	for _, opt := range opts {
		switch opt.Type() {
		case asynq.ProcessInOpt:
			if d, ok := opt.Value().(time.Duration); ok {
				delay = d
			}
		case asynq.MaxRetryOpt:
			if n, ok := opt.Value().(int); ok {
				maxRetry = n
				maxRetrySet = true
			}
		}
	}
	// Callers that explicitly pass MaxRetry(0) want no retries.
	// Without the flag we can't distinguish "not set" from "set to 0".
	if maxRetrySet && maxRetry < 0 {
		maxRetry = 0
	}

	taskID := uuid.New().String()
	info := &asynq.TaskInfo{
		ID:    taskID,
		Queue: "sync",
		Type:  task.Type(),
	}

	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}

		// Tag as a background worker execution so the per-model concurrency
		// governor throttles Lite-mode ingestion/enrichment LLM calls, mirroring
		// the asynq backgroundTaskMiddleware in the Redis path.
		ctx := types.WithBackgroundTask(context.Background())
		start := time.Now()
		logger.Infof(ctx, "[SyncTask] Executing task type=%s id=%s", task.Type(), taskID)

		var lastErr error
		for attempt := 0; attempt <= maxRetry; attempt++ {
			if attempt > 0 {
				backoff := time.Duration(attempt) * 5 * time.Second
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				logger.Infof(ctx, "[SyncTask] Retrying task type=%s id=%s attempt=%d/%d backoff=%s",
					task.Type(), taskID, attempt, maxRetry, backoff)
				time.Sleep(backoff)
			}

			attemptCtx := types.WithTaskRetryMetadata(ctx, attempt, maxRetry)
			lastErr = handler(attemptCtx, task)
			if lastErr == nil {
				logger.Infof(ctx, "[SyncTask] Task completed type=%s id=%s elapsed=%v",
					task.Type(), taskID, time.Since(start))
				return
			}
		}

		logger.Errorf(ctx, "[SyncTask] Task failed (exhausted retries) type=%s id=%s elapsed=%v err=%v",
			task.Type(), taskID, time.Since(start), lastErr)
	}()

	return info, nil
}

type SyncTaskParams struct {
	dig.In

	Executor             *SyncTaskExecutor
	KnowledgeService     interfaces.KnowledgeService
	KnowledgeBaseService interfaces.KnowledgeBaseService
	TagService           interfaces.KnowledgeTagService
	DataSourceService    interfaces.DataSourceService
	ChunkExtractor       interfaces.TaskHandler `name:"chunkExtractor"`
	DataTableSummary     interfaces.TaskHandler `name:"dataTableSummary"`
	ImageMultimodal      interfaces.TaskHandler `name:"imageMultimodal"`
	KnowledgePostProcess interfaces.TaskHandler `name:"knowledgePostProcess"`
	WikiIngest           interfaces.TaskHandler `name:"wikiIngest"`
	TemporaryDocument    interfaces.TemporaryDocumentService
	Processing           *service.ProcessingService
}

// RegisterSyncHandlers registers all task handlers on the SyncTaskExecutor.
// Used in Lite mode instead of RunAsynqServer.
func RegisterSyncHandlers(params SyncTaskParams) {
	register := func(kind string, handler func(context.Context, *asynq.Task) error) {
		params.Executor.RegisterHandler(kind, params.Processing.GuardLegacyTask(asynq.HandlerFunc(handler)).ProcessTask)
	}
	register(types.TypeChunkExtract, params.ChunkExtractor.Handle)
	register(types.TypeDataTableSummary, params.DataTableSummary.Handle)
	register(types.TypeDocumentProcess, params.KnowledgeService.ProcessDocument)
	register(types.TypeTemporaryDocumentProcess, params.TemporaryDocument.Process)
	register(types.TypeManualProcess, params.KnowledgeService.ProcessManualUpdate)
	register(types.TypeFAQImport, params.KnowledgeService.ProcessFAQImport)
	register(types.TypeQuestionGeneration, params.KnowledgeService.ProcessQuestionGeneration)
	register(types.TypeSummaryGeneration, params.KnowledgeService.ProcessSummaryGeneration)
	register(types.TypeKBClone, params.KnowledgeService.ProcessKBClone)
	register(types.TypeKnowledgeMove, params.KnowledgeService.ProcessKnowledgeMove)
	register(types.TypeKnowledgeListDelete, params.KnowledgeService.ProcessKnowledgeListDelete)
	register(types.TypeKnowledgeListReparse, params.KnowledgeService.ProcessKnowledgeListReparse)
	register(types.TypeIndexDelete, params.TagService.ProcessIndexDelete)
	register(types.TypeKBDelete, params.KnowledgeBaseService.ProcessKBDelete)
	register(types.TypeImageMultimodal, params.ImageMultimodal.Handle)
	register(types.TypeKnowledgePostProcess, params.KnowledgePostProcess.Handle)
	register(types.TypeDataSourceSync, params.DataSourceService.ProcessSync)
	register(types.TypeWikiIngest, params.WikiIngest.Handle)
	register(types.TypeWikiFinalize, params.WikiIngest.Handle)
	for _, queue := range types.QueueDefinitions() {
		register(types.TypeProcessingStep+":"+queue.Name, params.Processing.Process)
	}
	go params.Processing.Run(context.Background(), nil)
	logger.Infof(context.Background(), "[SyncTask] All task handlers registered (Lite mode, no Redis)")
}
