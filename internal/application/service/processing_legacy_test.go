package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func TestProcessingLegacyCallbacksAndHousekeepingLeaveLedgerOwnedRows(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.KnowledgeProcessingSpan{}))
	ctx := context.Background()
	stale := time.Now().Add(-3 * time.Hour)
	owned := types.Knowledge{ID: "owned", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing, SummaryStatus: types.SummaryStatusProcessing, PendingSubtasksCount: 2, UpdatedAt: stale, ProcessedAt: &stale, Metadata: types.JSON(`{"processing_protocol":"2","datasource_candidate":"true","datasource_version":"v1"}`)}
	legacy := types.Knowledge{ID: "legacy", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing, SummaryStatus: types.SummaryStatusProcessing, UpdatedAt: stale}
	require.NoError(t, db.Create(&owned).Error)
	require.NoError(t, db.Create(&legacy).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "legacy-summary", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusCompleted, SummaryStatus: types.SummaryStatusProcessing, UpdatedAt: stale}).Error)
	r := repository.NewKnowledgeRepository(db)
	t.Run("callbacks", func(t *testing.T) {
		changed, err := r.SetFinalizing(ctx, owned.ID, 10)
		require.NoError(t, err)
		require.False(t, changed)
		count, promoted, err := r.FinalizeSubtask(ctx, owned.ID)
		require.NoError(t, err)
		require.False(t, promoted)
		require.Equal(t, 2, count)
		metadata := r.(interface {
			MarkDataSourceSubtaskFailed(context.Context, string, string) error
			MarkDataSourceIndexReady(context.Context, string) error
		})
		require.NoError(t, metadata.MarkDataSourceSubtaskFailed(ctx, owned.ID, "late"))
		require.NoError(t, metadata.MarkDataSourceIndexReady(ctx, owned.ID))
		got, err := r.GetKnowledgeByIDOnly(ctx, owned.ID)
		require.NoError(t, err)
		require.JSONEq(t, string(owned.Metadata), string(got.Metadata))
		finalizing := owned
		finalizing.ID, finalizing.ParseStatus, finalizing.PendingSubtasksCount = "owned-finalizing", types.ParseStatusFinalizing, 0
		require.NoError(t, db.Create(&finalizing).Error)
		_, promoted, err = r.FinalizeSubtask(ctx, finalizing.ID)
		require.NoError(t, err)
		require.False(t, promoted, "zero legacy counters cannot publish a ledger-owned row")
	})
	t.Run("housekeeping", func(t *testing.T) {
		h := NewHousekeepingService(db, nil, nil)
		h.runSweep(ctx)
		got, err := r.GetKnowledgeByIDOnly(ctx, owned.ID)
		require.NoError(t, err)
		require.Equal(t, types.ParseStatusProcessing, got.ParseStatus)
		require.Equal(t, types.SummaryStatusProcessing, got.SummaryStatus)
		old, err := r.GetKnowledgeByIDOnly(ctx, legacy.ID)
		require.NoError(t, err)
		require.Equal(t, types.ParseStatusFailed, old.ParseStatus)
		summary, err := r.GetKnowledgeByIDOnly(ctx, "legacy-summary")
		require.NoError(t, err)
		require.Equal(t, types.SummaryStatusFailed, summary.SummaryStatus)
	})
}

func TestProcessingLegacyTaskBoundaryCoversEveryWorkerPool(t *testing.T) {
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}))
	require.NoError(t, db.Create(&types.Knowledge{ID: "owned", TenantID: 1, KnowledgeBaseID: "kb", Metadata: types.JSON(`{"processing_protocol":"2"}`)}).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "legacy", TenantID: 1, KnowledgeBaseID: "kb"}).Error)
	require.NoError(t, db.Create(&types.Chunk{ID: "chunk", KnowledgeID: "owned", TenantID: 1, KnowledgeBaseID: "kb"}).Error)
	s := NewProcessingService(repository.NewProcessingRepository(db), nil, nil, nil)
	calls := 0
	handler := s.GuardLegacyTask(asynq.HandlerFunc(func(context.Context, *asynq.Task) error { calls++; return nil }))
	for _, kind := range []string{types.TypeDocumentProcess, types.TypeManualProcess, types.TypeFAQImport, types.TypeSummaryGeneration, types.TypeQuestionGeneration, types.TypeImageMultimodal, types.TypeKnowledgePostProcess, types.TypeChunkExtract, types.TypeDataTableSummary, types.TypeWikiIngest} {
		payload, err := json.Marshal(map[string]any{"tenant_id": 1, "knowledge_id": "owned"})
		require.NoError(t, err)
		require.NoError(t, handler.ProcessTask(context.Background(), asynq.NewTask(kind, payload)))
	}
	require.Zero(t, calls, "no legacy worker may perform I/O for a protocol 2 candidate")
	require.NoError(t, handler.ProcessTask(context.Background(), asynq.NewTask(types.TypeChunkExtract, []byte(`{"tenant_id":1,"chunk_id":"chunk"}`))))
	require.NoError(t, handler.ProcessTask(context.Background(), asynq.NewTask(types.TypeImageMultimodal, []byte(`{"tenant_id":2,"knowledge_id":"legacy","chunk_id":"chunk"}`))))
	require.Zero(t, calls, "old chunk-only payloads and mismatched payload identities cannot bypass the boundary")
	require.NoError(t, handler.ProcessTask(context.Background(), asynq.NewTask(types.TypeDocumentProcess, []byte(`{"tenant_id":1,"knowledge_id":"legacy"}`))))
	require.NoError(t, handler.ProcessTask(context.Background(), asynq.NewTask(types.TypeKnowledgeListDelete, []byte(`{"tenant_id":1,"knowledge_ids":["owned"]}`))))
	require.Equal(t, 2, calls, "legacy ingestion and explicit lifecycle control keep their own handlers")
	// A database outage must fail closed, not be mistaken for legacy data.
	require.NoError(t, db.Migrator().DropTable(&types.Knowledge{}))
	require.Error(t, handler.ProcessTask(context.Background(), asynq.NewTask(types.TypeSummaryGeneration, []byte(`{"tenant_id":1,"knowledge_id":"owned"}`))))
	require.Equal(t, 2, calls)
}
