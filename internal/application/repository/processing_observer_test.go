package repository

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func checkProcessingObserverViews(t *testing.T, db *gorm.DB, appSchema, dsn string) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}, &types.KnowledgeProcessingSpan{}))
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	observerSchema, role := "observer_"+suffix, "reader_"+suffix
	require.NoError(t, db.Exec("CREATE ROLE "+role+" LOGIN").Error)
	t.Cleanup(func() {
		_ = db.Exec("DROP SCHEMA IF EXISTS " + observerSchema + " CASCADE").Error
		_ = db.Exec("DROP OWNED BY " + role).Error
		_ = db.Exec("DROP ROLE " + role).Error
	})
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "source").Update("config", types.JSON(`{"credentials":{"token":"SECRET_OBSERVER_TOKEN"}}`)).Error)
	require.NoError(t, db.Create(&types.SyncLog{ID: "observer-log", TenantID: 1, DataSourceID: "source", Status: "failed", Result: types.JSON(`{"errors":[{"title":"synthetic","file_id":"file","source_path":"Legacy/file","message":"SECRET_PROVIDER_BODY https://invalid.test/?token=SECRET_URL","actual_bytes":42,"category":"SOURCE_FETCH_FAILED"}],"secret":"SECRET_OTHER_FIELD"}`)}).Error)
	file := filepath.Join("..", "..", "..", "deploy", "mwe-observability", "grafana", "queries", "observer-access.sql")
	contents, err := os.ReadFile(file)
	require.NoError(t, err)
	statement := strings.NewReplacer(`:"app_schema"`, appSchema, `:"observer_schema"`, observerSchema, `:"observer_role"`, role).Replace(string(contents))
	require.NoError(t, db.Exec(statement).Error)
	// A fresh login proves role defaults as well as GRANTs. All credentials and
	// business rows here are synthetic in the isolated lifecycle_test database.
	reader, err := gorm.Open(postgres.Open(strings.Replace(dsn, "user=postgres", "user="+role, 1)), &gorm.Config{})
	require.NoError(t, err)
	raw, err := reader.DB()
	require.NoError(t, err)
	defer raw.Close()
	documentID, scanID := uuid.NewString(), uuid.NewString()
	for id, kind := range map[string]string{documentID: "document", scanID: "scan"} {
		require.NoError(t, db.Create(&types.ProcessingJob{ID: id, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", Kind: kind, ExternalID: id, Generation: 1, SourceRevision: "observer-context", PipelineFingerprint: "observer-context", Status: types.ProcessingRunning, Metadata: types.JSON(`{"title":"Current file","folder_path":"Source directory","file_id":"exact-file","url":"https://docs.qq.com/doc/example?mode=view&resourceId=abc","secret":"SECRET_METADATA"}`)}).Error)
	}
	defer func() {
		require.NoError(t, db.Where("job_id IN ?", []string{documentID, scanID}).Delete(&types.ProcessingEvent{}).Error)
		require.NoError(t, db.Where("job_id IN ?", []string{documentID, scanID}).Delete(&types.ProcessingStep{}).Error)
		require.NoError(t, db.Where("id IN ?", []string{documentID, scanID}).Delete(&types.ProcessingJob{}).Error)
	}()
	downloadStep := uuid.NewString()
	require.NoError(t, db.Create(&types.ProcessingStep{ID: downloadStep, JobID: documentID, Stage: "download", UnitKey: "body", Phase: types.ProcessingPhasePrepare, Status: types.ProcessingSucceeded, Result: types.JSON(`{"bytes":1536,"secret":"SECRET_RESULT"}`)}).Error)
	for revision := int64(1); revision <= 2; revision++ {
		require.NoError(t, db.Create(&types.ProcessingEvent{TenantID: 1, JobID: documentID, JobRevision: revision, Generation: 1, StepID: downloadStep, Attempt: 1, Type: "step_failed", ErrorCode: "STAGE_EXECUTION_ERROR", ToState: "failed"}).Error)
	}
	scanStep := uuid.NewString()
	require.NoError(t, db.Create(&types.ProcessingStep{ID: scanStep, JobID: scanID, Stage: "scan_document", UnitKey: "file", Phase: types.ProcessingPhasePrepare, Status: types.ProcessingBlocked, ErrorCode: "IDENTITY_UNRESOLVED", Input: types.JSON(`{"title":"Blocked file","folder_path":"Source directory","file_id":"unadmitted-file","url":"https://docs.qq.com/doc/example?token=SECRET_QUERY","secret":"SECRET_INPUT"}`)}).Error)
	var documentContext, scanContext, legacyContext map[string]any
	require.NoError(t, reader.Table("processing_job_context").Where("job_id = ?", documentID).Take(&documentContext).Error)
	require.Equal(t, "Source directory/Current file", documentContext["source_path"])
	require.EqualValues(t, 1536, documentContext["file_bytes"])
	require.Equal(t, "https://docs.qq.com/doc/example?mode=view&resourceId=abc", documentContext["source_url"])
	require.NoError(t, reader.Table("processing_step_context").Where("step_id = ?", scanStep).Take(&scanContext).Error)
	require.Equal(t, "Source directory/Blocked file", scanContext["source_path"])
	require.Nil(t, scanContext["source_url"], "signed or arbitrary query parameters cannot enter links")
	require.Nil(t, scanContext["actual_bytes"], "unknown size must not be substituted")
	require.NoError(t, reader.Table("processing_legacy_context").Where("run_id = ?", "observer-log").Take(&legacyContext).Error)
	require.Equal(t, "Legacy/file", legacyContext["source_path"])
	require.EqualValues(t, 42, legacyContext["actual_bytes"])
	contexts, err := json.Marshal([]any{documentContext, scanContext, legacyContext})
	require.NoError(t, err)
	require.NotContains(t, string(contexts), "SECRET_")
	var readOnly string
	require.NoError(t, reader.Raw("SHOW transaction_read_only").Scan(&readOnly).Error)
	require.Equal(t, "on", readOnly)
	var path string
	require.NoError(t, reader.Raw("SHOW search_path").Scan(&path).Error)
	require.Contains(t, path, observerSchema)
	for view := range processingHistoryViews {
		require.NoError(t, reader.Exec("SELECT * FROM mwe_processing_"+view+" LIMIT 1").Error, view)
	}
	for _, table := range []string{"data_sources", "knowledges", "sync_logs", "task_dead_letters", "processing_jobs", "processing_history_rows", "resources", "processing_legacy_evidence", "processing_legacy_drains", "mwe_processing_legacy_run_state", "mwe_processing_legacy_error_state", "mwe_processing_legacy_evidence_state"} {
		require.Error(t, reader.Exec("SELECT * FROM "+appSchema+"."+table+" LIMIT 1").Error, table)
	}
	for _, table := range []string{"knowledges", "data_sources", "sync_logs", "knowledge_processing_spans", "task_dead_letters"} {
		var rows []map[string]any
		require.NoError(t, reader.Table(table).Find(&rows).Error, table)
		data, err := json.Marshal(rows)
		require.NoError(t, err)
		require.NotContains(t, string(data), "SECRET_", table)
	}
	require.Error(t, reader.Exec("DELETE FROM "+observerSchema+".knowledges").Error)
	// Execute the actual six Grafana queries after substituting only their UI
	// filter macros. Empty selectors mean all workspaces, never an RBAC grant.
	files, err := filepath.Glob(filepath.Join(filepath.Dir(file), "processing_*.sql"))
	require.NoError(t, err)
	require.Len(t, files, 6)
	replace := []string{"$__timeFilter(observed_at)", "TRUE", "$__timeFilter(c.observed_at)", "TRUE", "${records:sqlstring}", "'all'"}
	for _, name := range []string{"workspace", "knowledge_base", "source", "run", "status", "stage", "search"} {
		replace = append(replace, "${"+name+":sqlstring}", "''")
	}
	for _, file := range files {
		contents, err := os.ReadFile(file)
		require.NoError(t, err)
		require.NoError(t, reader.Exec(strings.NewReplacer(replace...).Replace(string(contents))).Error, filepath.Base(file))
		if filepath.Base(file) == "processing_stage_retry_queue.sql" {
			filtered := strings.ReplaceAll(string(contents), "${search:sqlstring}", "'Source directory/Blocked file'")
			var found []map[string]any
			require.NoError(t, reader.Raw(strings.NewReplacer(replace...).Replace(filtered)).Scan(&found).Error)
			require.Len(t, found, 1, "a scan failure is searchable before a document job exists")
			require.Equal(t, "Blocked file", found[0]["文档名称"])
			require.Equal(t, "Source directory/Blocked file", found[0]["腾讯文档路径"])
		}
		if filepath.Base(file) == "processing_unresolved_incidents.sql" {
			filtered := strings.ReplaceAll(string(contents), "${search:sqlstring}", "'"+documentID+"'")
			var found []map[string]any
			require.NoError(t, reader.Raw(strings.NewReplacer(replace...).Replace(filtered)).Scan(&found).Error)
			require.Len(t, found, 1, "same document-stage-error appears once in failure review")
			require.Equal(t, "2", found[0]["异常次数"])
			var detail map[string]any
			require.NoError(t, json.Unmarshal([]byte(found[0]["详情"].(string)), &detail))
			require.Len(t, detail["同类异常事件ID"], 2, "all original event IDs remain inspectable")
		}
	}
	for _, file := range []string{"document_failures.sql", "source_failures.sql"} {
		contents, err := os.ReadFile(filepath.Join(filepath.Dir(files[0]), file))
		require.NoError(t, err)
		require.NoError(t, reader.Exec(string(contents)+" SELECT * FROM failure_rows LIMIT 1").Error, file)
	}
	// Redacting the message changes its JSON digest. The safe evidence view
	// must join the original hash first, then project the matching safe hash.
	var digest string
	require.NoError(t, db.Raw("SELECT encode(sha256(convert_to((result->'errors'->0)::text,'UTF8')),'hex') FROM sync_logs WHERE id = ?", "observer-log").Scan(&digest).Error)
	require.NoError(t, db.Create(&types.ProcessingLegacyEvidence{ID: "observer-proof", ProcessingLegacyIdentity: types.ProcessingLegacyIdentity{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", RunID: "observer-log", ErrorOrdinal: 1, ErrorDigest: digest}, Action: "manual_confirmed", SnapshotDigest: strings.Repeat("a", 64), ArtifactDigest: strings.Repeat("b", 64), EvidenceReference: "SECRET_OBSERVER_REFERENCE", EvidenceDigest: strings.Repeat("c", 64), Actor: "SECRET_OBSERVER_ACTOR", Reason: "SECRET_OBSERVER_REASON", OperationRequestID: "observer-proof", RequestDigest: strings.Repeat("d", 64)}).Error)
	query, err := os.ReadFile(filepath.Join(filepath.Dir(file), "document_failures.sql"))
	require.NoError(t, err)
	var count int64
	require.NoError(t, reader.Raw(string(query)+" SELECT count(*) FROM unresolved_file_errors WHERE sync_log_id = 'observer-log'").Scan(&count).Error)
	require.Zero(t, count)
	var proofRows []map[string]any
	require.NoError(t, reader.Table("processing_legacy_evidence").Find(&proofRows).Error)
	encoded, err := json.Marshal(proofRows)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "SECRET_")
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", "observer-log").Update("result", types.JSON(`{"errors":[{"title":"synthetic","file_id":"file","message":"SECRET_CHANGED_MESSAGE","actual_bytes":42,"category":"SOURCE_FETCH_FAILED"}]}`)).Error)
	require.NoError(t, reader.Raw(string(query)+" SELECT count(*) FROM unresolved_file_errors WHERE sync_log_id = 'observer-log'").Scan(&count).Error)
	require.EqualValues(t, 1, count, "same redacted text cannot reuse a proof of a different original error")
}
