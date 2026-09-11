package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Uses the real SQL migration and concurrent connections. The dedicated test
// database is empty/synthetic; no production schema or credentials are used.
func TestProcessingPostgresConcurrentGenerationClaimAndAtomicFailure(t *testing.T) {
	dsn := os.Getenv("PROCESSING_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("requires isolated PROCESSING_TEST_POSTGRES")
	}
	require.Contains(t, dsn, "host=lifecycle-pg ")
	require.Contains(t, dsn, "dbname=lifecycle_test ")
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	schema := "processing_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	raw.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.DataSource{}, &types.Tenant{}, &types.WikiPage{}))
	require.NoError(t, db.Create(&types.Tenant{ID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.Migrator().DropColumn(&types.WikiPage{}, "MutationRevision"))
	require.NoError(t, db.Exec("INSERT INTO wiki_pages(id, tenant_id, knowledge_base_id, slug, content) VALUES ('migration-page', 1, 'kb', 'entity/migration', 'preserved synthetic content')").Error)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 1, Name: "synthetic"}).Error)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}))
	require.NoError(t, db.Migrator().DropColumn(&types.Chunk{}, "FAQIndexManifest"))
	require.NoError(t, db.Exec(`CREATE TABLE task_pending_ops (
		id BIGSERIAL PRIMARY KEY, tenant_id BIGINT, task_type VARCHAR(64), scope VARCHAR(32), scope_id VARCHAR(64),
		op VARCHAR(32), dedup_key VARCHAR(128), payload JSONB DEFAULT '{}', fail_count INTEGER DEFAULT 0,
		enqueued_at TIMESTAMPTZ, claimed_at TIMESTAMPTZ)`).Error)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "versioned", "000080_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(migration)).Error)
	references, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "versioned", "000081_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(references)).Error)
	for _, name := range []string{"000069_resource_registry.up.sql", "000082_processing_lifecycle.up.sql", "000083_processing_lifecycle.up.sql", "000084_processing_lifecycle.up.sql", "000085_processing_lifecycle.up.sql", "000086_processing_lifecycle.up.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "versioned", name))
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(migration)).Error)
	}
	var page types.WikiPage
	installProcessingHistoryTestSchema(t, db)
	for view := range processingHistoryViews {
		_, err := NewProcessingRepository(db).CreateHistorySnapshot(context.Background(), 1, "user:synthetic", "kb", ProcessingHistoryFilter{View: view}, 100)
		require.NoError(t, err, view)
	}
	require.NoError(t, db.Where("id = ?", "migration-page").Take(&page).Error)
	require.Equal(t, "preserved synthetic content", page.Content)
	require.EqualValues(t, 1, page.MutationRevision)
	require.NoError(t, db.Create(&types.DataSource{ID: "source", TenantID: 1, KnowledgeBaseID: "kb", Name: "synthetic",
		Type: types.ConnectorTypeTencentDocs, Status: types.DataSourceStatusActive}).Error)
	r := NewProcessingRepository(db)
	ctx := context.Background()
	input := types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb",
		DataSourceID: "source", ExternalID: "file", SourceRevision: "v1", PipelineFingerprint: "p1"}
	const workers = 12
	jobs := make(chan *types.ProcessingJob, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); job, err := r.EnsureJob(ctx, input); jobs <- job; errorsCh <- err }()
	}
	wg.Wait()
	close(jobs)
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	var job *types.ProcessingJob
	for candidate := range jobs {
		if job == nil {
			job = candidate
		}
		require.Equal(t, job.ID, candidate.ID)
		require.EqualValues(t, 1, candidate.Generation)
	}
	// SQL rejection of the outbox insert must roll back plan, steps and events.
	require.NoError(t, db.Exec("ALTER TABLE task_pending_ops ADD CONSTRAINT injected_outbox_failure CHECK (op <> 'deliver')").Error)
	specs := []types.ProcessingStepSpec{{Stage: "fetch", UnitKey: "source", Phase: types.ProcessingPhasePrepare,
		InputFingerprint: "input", RequiredForReady: true, RequiredForCompletion: true}}
	require.Error(t, r.PlanSteps(ctx, 1, job.ID, specs))
	var count int64
	require.NoError(t, db.Model(&types.ProcessingStep{}).Count(&count).Error)
	require.Zero(t, count)
	fresh, err := r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.False(t, fresh.PlanSealed)
	require.Equal(t, job.Revision, fresh.Revision)
	require.NoError(t, db.Exec("ALTER TABLE task_pending_ops DROP CONSTRAINT injected_outbox_failure").Error)
	require.NoError(t, r.PlanSteps(ctx, 1, job.ID, specs))
	ops, err := r.PendingDeliveries(ctx, 10)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	var ref types.ProcessingRef
	require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
	leases := make(chan *types.ProcessingLease, workers)
	errorsCh = make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
			leases <- lease
			errorsCh <- err
		}()
	}
	wg.Wait()
	close(leases)
	close(errorsCh)
	accepted, rejected := 0, 0
	for err := range errorsCh {
		if err == nil {
			accepted++
		} else {
			require.True(t, errors.Is(err, ErrProcessingConflict))
			rejected++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, workers-1, rejected)
	var lease *types.ProcessingLease
	for candidate := range leases {
		if candidate != nil {
			lease = candidate
		}
	}
	input.SourceRevision = "v2"
	next, err := r.EnsureJob(ctx, input)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Generation)
	require.ErrorIs(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded,
		OutputManifestRef: "stale", OutputDigest: "stale"}), ErrProcessingConflict)
	checkProcessingHistorySnapshotAndUnresolvedCleanup(t, r)
	checkProcessingObserverViews(t, db, schema, dsn)
	checkProcessingLegacyEvidence(t, r)
	checkProcessingLegacyRetry(t, r)
	checkProcessingLegacyHistory(t, r)
	checkProcessingTenantParser(t, r)
}
