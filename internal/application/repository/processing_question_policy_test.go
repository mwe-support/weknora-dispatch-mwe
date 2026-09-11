package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestProcessingQuestionDefaultsPreserveInflightWorkAndUserOptIn(t *testing.T) {
	checkQuestionDefaultsPreserveInflightWorkAndUserOptIn(t, processingTestStore(t))
}

func checkQuestionDefaultsPreserveInflightWorkAndUserOptIn(t *testing.T, r *ProcessingRepository) {
	ctx := context.Background()
	require.NoError(t, r.db.AutoMigrate(&types.SystemSetting{}))
	config := &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 3, CustomInstructions: "preserve user instructions"}
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("question_generation_config", config).Error)
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").UpdateColumn("description", nil).Error)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "inflight", SourceRevision: "v1", PipelineFingerprint: "pipeline"})
	require.NoError(t, err)
	specs := []types.ProcessingStepSpec{
		{Stage: "native_read", UnitKey: "body", Phase: "prepare", InputFingerprint: "native", RequiredForReady: true, RequiredForCompletion: true},
		{Stage: "publish", UnitKey: "body", Phase: "publish", InputFingerprint: "publish", DependsOn: []string{"native_read/body"}, RequiredForCompletion: true},
		{Stage: "questions", UnitKey: "body", Kind: "barrier", Phase: "projection", InputFingerprint: "questions", DependsOn: []string{"publish/body"}, RequiredForCompletion: true},
		{Stage: "question_index", UnitKey: "body", Kind: "barrier", Phase: "projection", InputFingerprint: "questions-index", DependsOn: []string{"questions/body"}, RequiredForCompletion: true},
		{Stage: "retire_previous", UnitKey: "body", Phase: "projection", InputFingerprint: "retire", DependsOn: []string{"publish/body", "questions/body", "question_index/body"}, RequiredForCompletion: true},
	}
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, specs))
	ops, err := r.PendingDeliveries(ctx, 10)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	var ref types.ProcessingRef
	require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
	lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
	require.NoError(t, err)
	before, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	report, err := r.ResetQuestionDefaults(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, report.KnowledgeBases)
	require.Equal(t, 1, report.Jobs)
	require.Equal(t, 2, report.SkippedSteps)
	after, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, before.Generation, after.Generation)
	require.Equal(t, before.SourceRevision, after.SourceRevision)
	require.Equal(t, before.PlanDigest, after.PlanDigest, "original admission remains auditable")
	require.NotEqual(t, before.ConfigurationRevision, after.ConfigurationRevision)
	require.NoError(t, r.Heartbeat(ctx, 1, *lease, time.Minute, ""), "unrelated active work keeps its lease")
	require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded, Completeness: "complete", OutputManifestRef: "verified/source", OutputDigest: strings.Repeat("a", 64)}))
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	for _, step := range steps {
		if step.Stage == "questions" || step.Stage == "question_index" {
			require.Equal(t, types.ProcessingSkipped, step.Status)
			require.False(t, step.RequiredForCompletion)
		}
		if step.Stage == "retire_previous" {
			var dependencies []string
			require.NoError(t, json.Unmarshal(step.Dependencies, &dependencies))
			require.Len(t, dependencies, 1)
		}
	}
	var receipt types.ProcessingEvent
	require.NoError(t, r.db.Where("job_id = ? AND event_type = ?", job.ID, "questions_disabled").Take(&receipt).Error)
	var policy map[string]any
	require.NoError(t, json.Unmarshal(receipt.Detail, &policy))
	require.Equal(t, before.ConfigurationRevision, policy["previous_configuration_revision"])
	// A later source scan supplies the current question-free declaration. It
	// must reuse this generation, but an unrelated changed plan must not pass.
	nextPlan := []types.ProcessingStepSpec{specs[0], specs[1], specs[4]}
	nextPlan[2].DependsOn = []string{"publish/body"}
	for i := 1; i < len(nextPlan); i++ {
		nextPlan[i].InputFingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte(job.PipelineFingerprint+"/"+after.ConfigurationRevision+"/"+nextPlan[i].Stage)))
	}
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, nextPlan))
	changedPlan := append([]types.ProcessingStepSpec(nil), nextPlan...)
	changedPlan[0].InputFingerprint = "different-source"
	require.ErrorIs(t, r.PlanSteps(ctx, 1, job.ID, changedPlan), ErrProcessingConflict)
	var kb types.KnowledgeBase
	require.NoError(t, r.db.First(&kb, "id = ?", "kb").Error)
	require.Zero(t, kb.QuestionGenerationConfig.QuestionCount)
	require.False(t, kb.QuestionGenerationConfig.Enabled)
	require.Equal(t, config.CustomInstructions, kb.QuestionGenerationConfig.CustomInstructions)
	var descriptionStillNull bool
	require.NoError(t, r.db.Raw("SELECT description IS NULL FROM knowledge_bases WHERE id = ?", "kb").Scan(&descriptionStillNull).Error)
	require.True(t, descriptionStillNull, "the bulk reset must not normalize unrelated nullable fields")
	kb.QuestionGenerationConfig.Enabled, kb.QuestionGenerationConfig.QuestionCount = true, 2
	require.NoError(t, NewKnowledgeBaseRepository(r.db).UpdateKnowledgeBase(ctx, &kb))
	report, err = r.ResetQuestionDefaults(ctx)
	require.NoError(t, err)
	require.True(t, report.AlreadyApplied)
	require.NoError(t, r.db.First(&kb, "id = ?", "kb").Error)
	require.Equal(t, 2, kb.QuestionGenerationConfig.EffectiveCount(), "a later manual opt-in is not reset")
}

func TestProcessingQuestionDisablePreservesPublishedOutputAndOtherFailures(t *testing.T) {
	checkQuestionDisablePreservesPublishedOutputAndOtherFailures(t, processingTestStore(t))
}

func checkQuestionDisablePreservesPublishedOutputAndOtherFailures(t *testing.T, r *ProcessingRepository) {
	ctx := context.Background()
	require.NoError(t, r.db.AutoMigrate(&types.SystemSetting{}))
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("question_generation_config", &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 3}).Error)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "published", SourceRevision: "v1", PipelineFingerprint: "pipeline"})
	require.NoError(t, err)
	require.NoError(t, r.db.Model(job).Updates(map[string]any{"is_published": true, "publication_epoch": 7, "plan_sealed": true, "completeness": "complete", "status": "blocked"}).Error)
	steps := []types.ProcessingStep{
		{ID: "body", JobID: job.ID, Stage: "chunk", UnitKey: "body", Phase: "prepare", Status: "succeeded", InputFingerprint: "input", RequiredForReady: true, RequiredForCompletion: true, OutputManifestRef: "keep/body", OutputDigest: strings.Repeat("a", 64)},
		{ID: "questions", JobID: job.ID, Stage: "questions", UnitKey: "body", Kind: "barrier", Phase: "projection", Status: "waiting_external", InputFingerprint: "input", PlanSealed: true, RequiredForCompletion: true},
		{ID: "question-ok", ParentStepID: "questions", JobID: job.ID, Stage: "question", UnitKey: "ok", Phase: "projection", Status: "succeeded", InputFingerprint: "input", OutputManifestRef: "keep/question", OutputDigest: strings.Repeat("b", 64)},
		{ID: "question-bad", ParentStepID: "questions", JobID: job.ID, Stage: "question", UnitKey: "bad", Phase: "projection", Status: "blocked", InputFingerprint: "input", ErrorClass: "completeness", ErrorCode: "QUESTION_OUTPUT_EMPTY"},
		{ID: "summary-bad", JobID: job.ID, Stage: "summary", UnitKey: "body", Phase: "projection", Status: "blocked", InputFingerprint: "input", RequiredForCompletion: true, ErrorClass: "internal", ErrorCode: "STAGE_EXECUTION_ERROR"},
	}
	require.NoError(t, r.db.Create(&steps).Error)
	for i, id := range []string{"question-bad", "summary-bad"} {
		require.NoError(t, r.db.Create(&types.ProcessingEvent{JobID: job.ID, TenantID: 1, JobRevision: int64(100 + i), Generation: 1, StepID: id, Type: "step_failed", ErrorCode: "EXISTING_FAILURE"}).Error)
	}
	_, err = r.ResetQuestionDefaults(ctx)
	require.NoError(t, err)
	after, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.True(t, after.IsPublished)
	require.EqualValues(t, 7, after.PublicationEpoch)
	require.Equal(t, types.ProcessingBlocked, after.Status, "summary failure must remain a failure")
	var kept, skipped, summary types.ProcessingStep
	require.NoError(t, r.db.First(&kept, "id = ?", "question-ok").Error)
	require.NoError(t, r.db.First(&skipped, "id = ?", "question-bad").Error)
	require.NoError(t, r.db.First(&summary, "id = ?", "summary-bad").Error)
	require.Equal(t, "succeeded", kept.Status)
	require.Equal(t, "keep/question", kept.OutputManifestRef)
	require.Equal(t, "skipped", skipped.Status)
	require.Equal(t, "blocked", summary.Status)
	var resolutions []types.ProcessingEvent
	require.NoError(t, r.db.Where("resolution_type = ?", "feature_disabled").Find(&resolutions).Error)
	require.Len(t, resolutions, 1)
	require.Equal(t, "question-bad", resolutions[0].StepID)
}

func TestProcessingQuestionDisableDoesNotBypassOtherConfigurationChanges(t *testing.T) {
	r := processingTestStore(t)
	ctx := context.Background()
	require.NoError(t, r.db.Model(&types.KnowledgeBase{}).Where("id = ?", "kb").Update("question_generation_config", &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 3}).Error)
	job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: "document", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "pipeline"})
	require.NoError(t, err)
	var kb types.KnowledgeBase
	require.NoError(t, r.db.First(&kb, "id = ?", "kb").Error)
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{}
	kb.ChunkingConfig.ChunkSize = 123
	require.NoError(t, NewKnowledgeBaseRepository(r.db).UpdateKnowledgeBase(ctx, &kb))
	after, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, job.ConfigurationRevision, after.ConfigurationRevision)
	current, err := r.ConfigurationRevision(ctx, 1, "kb")
	require.NoError(t, err)
	require.NotEqual(t, current, after.ConfigurationRevision)
}

func TestProcessingQuestionDefaultsPostgres(t *testing.T) {
	dsn := os.Getenv("PROCESSING_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
	}
	require.Contains(t, dsn, "host=lifecycle-pg ")
	require.Contains(t, dsn, "dbname=lifecycle_test ")
	for name, check := range map[string]func(*testing.T, *ProcessingRepository){
		"inflight-and-idempotence":   checkQuestionDefaultsPreserveInflightWorkAndUserOptIn,
		"published-and-other-errors": checkQuestionDisablePreservesPublishedOutputAndOtherFailures,
	} {
		t.Run(name, func(t *testing.T) {
			admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
			require.NoError(t, err)
			schema := "question_defaults_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
			t.Cleanup(func() {
				_ = admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error
				raw, _ := admin.DB()
				_ = raw.Close()
			})
			db, err := gorm.Open(postgres.Open(dsn+" search_path="+schema), &gorm.Config{})
			require.NoError(t, err)
			raw, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { _ = raw.Close() })
			check(t, processingTestStoreWithDB(t, db))
		})
	}
}
