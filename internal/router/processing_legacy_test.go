package router

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type processingLegacyKnowledge struct {
	interfaces.KnowledgeService
	repo interfaces.KnowledgeRepository
}

func (s processingLegacyKnowledge) GetRepository() interfaces.KnowledgeRepository { return s.repo }

type processingLegacyTracker struct {
	service.SpanTracker
	finalized bool
}

func (s *processingLegacyTracker) FinalizeAttempt(context.Context, string, int, string, types.JSONMap, string, string) {
	s.finalized = true
}

func TestProcessingLegacyDeadletterCannotOverwriteConcurrentAdoption(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}))
	k := types.Knowledge{ID: "legacy", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing, Metadata: types.JSON(`{}`)}
	require.NoError(t, db.Create(&k).Error)
	tracker := &processingLegacyTracker{}
	callback := newDeadLetterKnowledgeFailer(processingLegacyKnowledge{repo: repository.NewKnowledgeRepository(db)}, tracker, func(context.Context, *asynq.Task) (bool, error) {
		// The old guard read already allowed the task. Adoption wins immediately
		// before the detached callback attempts its actual state update.
		require.NoError(t, db.Model(&k).Updates(map[string]any{"metadata": types.JSON(`{"processing_protocol":"2","processing_job_id":"adopted"}`), "parse_status": types.ParseStatusCompleted}).Error)
		return true, nil
	})
	callback(context.Background(), asynq.NewTask(types.TypeDocumentProcess, []byte(`{"knowledge_id":"legacy","attempt":1}`)), errors.New("old task timeout"))
	var current types.Knowledge
	require.NoError(t, db.First(&current, "id = ?", k.ID).Error)
	require.Equal(t, types.ParseStatusCompleted, current.ParseStatus)
	require.Equal(t, "adopted", current.GetMetadata()["processing_job_id"])
	require.False(t, tracker.finalized)
}
