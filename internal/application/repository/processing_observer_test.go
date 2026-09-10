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
	require.NoError(t, db.Create(&types.SyncLog{ID: "observer-log", TenantID: 1, DataSourceID: "source", Status: "failed", Result: types.JSON(`{"errors":[{"title":"synthetic","file_id":"file","message":"SECRET_PROVIDER_BODY https://invalid.test/?token=SECRET_URL","actual_bytes":42,"category":"SOURCE_FETCH_FAILED"}],"secret":"SECRET_OTHER_FIELD"}`)}).Error)
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
	var readOnly string
	require.NoError(t, reader.Raw("SHOW transaction_read_only").Scan(&readOnly).Error)
	require.Equal(t, "on", readOnly)
	var path string
	require.NoError(t, reader.Raw("SHOW search_path").Scan(&path).Error)
	require.Contains(t, path, observerSchema)
	for view := range processingHistoryViews {
		require.NoError(t, reader.Exec("SELECT * FROM mwe_processing_"+view+" LIMIT 1").Error, view)
	}
	for _, table := range []string{"data_sources", "knowledges", "sync_logs", "task_dead_letters", "processing_jobs", "processing_history_rows", "resources"} {
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
	replace := []string{"$__timeFilter(observed_at)", "TRUE"}
	for _, name := range []string{"workspace", "knowledge_base", "source", "run", "status", "stage", "search"} {
		replace = append(replace, "${"+name+":sqlstring}", "''")
	}
	for _, file := range files {
		contents, err := os.ReadFile(file)
		require.NoError(t, err)
		require.NoError(t, reader.Exec(strings.NewReplacer(replace...).Replace(string(contents))).Error, filepath.Base(file))
	}
	for _, file := range []string{"document_failures.sql", "source_failures.sql"} {
		contents, err := os.ReadFile(filepath.Join(filepath.Dir(files[0]), file))
		require.NoError(t, err)
		require.NoError(t, reader.Exec(string(contents)+" SELECT * FROM failure_rows LIMIT 1").Error, file)
	}
}
