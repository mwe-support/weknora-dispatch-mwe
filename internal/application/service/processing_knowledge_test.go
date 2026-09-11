package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

type processingUnavailableStorage struct {
	interfaces.StorageBackendResolver
}

func (processingUnavailableStorage) ResolveFileService(context.Context, *types.Tenant, string, string, string) (interfaces.FileService, string, error) {
	return nil, "", errors.New("synthetic private storage detail")
}

func TestProcessingKnowledgeNormalizesParsesAndCommitsRealChunks(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
	db := processingServiceTestDatabase(t)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.Knowledge{}, &types.Chunk{}))
	require.NoError(t, db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&types.Tenant{ID: 1, Name: "synthetic"}).Error)
	r := repository.NewProcessingRepository(db)
	fs := files.NewLocalFileService(t.TempDir(), "")
	s := &knowledgeService{config: &config.Config{Conversation: &config.ConversationConfig{}},
		kbService:  &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)},
		tenantRepo: repository.NewTenantRepository(db), repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db), fileSvc: fs}
	execute, err := NewKnowledgeProcessingExecutor(s, r, repository.NewDataSourceRepository(db))
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	metadata, err := json.Marshal(ProcessingDocumentSpec{FileID: "file", Kind: "smartcanvas", Title: "synthetic", FolderPath: "acceptance/nested", Language: "zh-CN"})
	require.NoError(t, err)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "fixed", PipelineFingerprint: ProcessingPipelineFingerprint(s.config), Metadata: metadata})
	require.NoError(t, err)
	s.storageResolver = processingUnavailableStorage{}
	blocked, err := execute(ctx, types.ProcessingLease{Job: *job})
	require.NoError(t, err)
	require.Equal(t, "STORAGE_BACKEND_UNAVAILABLE", blocked.ErrorCode, "configured storage failure must not fall back to the global backend")
	require.NotContains(t, blocked.Message, "private storage detail")
	s.storageResolver = nil
	stages := []string{"native_read", "normalize", "parse", "chunk"}
	var plan []types.ProcessingStepSpec
	for i, stage := range stages {
		spec := types.ProcessingStepSpec{Stage: stage, UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: stage, RequiredForReady: true, RequiredForCompletion: true}
		if i > 0 {
			spec.DependsOn = []string{stages[i-1] + "/body"}
		}
		plan = append(plan, spec)
	}
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, plan))
	for _, stage := range stages {
		ops, err := r.PendingDeliveries(ctx, 10)
		require.NoError(t, err)
		require.Len(t, ops, 1)
		var ref types.ProcessingRef
		require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
		lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
		require.NoError(t, err)
		var outcome types.ProcessingOutcome
		if stage == "native_read" {
			page, err := json.Marshal(map[string]string{"content": `<Paragraph id="a">` + strings.Repeat("完整正文合成验收文字。", 100) + `</Paragraph><Paragraph id="tail">NATIVE-PIPELINE-END-7391</Paragraph>`})
			require.NoError(t, err)
			payload, err := json.Marshal(tencentdocs.NativeSnapshot{Kind: "smartcanvas", CoverageComplete: true, Pages: []tencentdocs.NativePage{{Tool: "smartcanvas.read", Data: page}}})
			require.NoError(t, err)
			path, digest, err := NewProcessingArtifacts(fs, nil).Save(ctx, lease.Job, lease.Step, "native_snapshot", payload)
			require.NoError(t, err)
			outcome = types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: path, OutputDigest: digest}
		} else {
			outcome, err = execute(ctx, *lease)
			require.NoError(t, err)
			require.Equal(t, types.ProcessingSucceeded, outcome.Status, "%s: %+v", stage, outcome)
		}
		require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
	}
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	knowledge, err := s.repo.GetKnowledgeByID(ctx, 1, job.KnowledgeID)
	require.NoError(t, err)
	require.Equal(t, "acceptance/nested", knowledge.FolderPath)
	chunks, err := s.chunkRepo.ListChunksByKnowledgeID(ctx, 1, job.KnowledgeID)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)
	var text strings.Builder
	for _, chunk := range chunks {
		text.WriteString(chunk.Content)
	}
	require.Contains(t, text.String(), "NATIVE-PIPELINE-END-7391")
	require.Equal(t, "ready", job.Readiness)
	require.False(t, job.IsPublished)
}
