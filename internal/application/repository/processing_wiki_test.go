package repository

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestProcessingWikiSQLiteMigrationPreservesExistingPages(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	legacy := strings.Replace(wikiPagesTestDDL, "    mutation_revision INTEGER NOT NULL DEFAULT 1,\n", "", 1)
	require.NotContains(t, legacy, "mutation_revision")
	require.NoError(t, db.Exec(legacy).Error)
	require.NoError(t, db.Exec("INSERT INTO wiki_pages(id, tenant_id, knowledge_base_id, slug, content) VALUES ('legacy', 1, 'kb', 'entity/migration', 'preserved synthetic content')").Error)
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "sqlite", "000008_processing_lifecycle.up.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(up)).Error)
	var page types.WikiPage
	require.NoError(t, db.Where("id = ?", "legacy").Take(&page).Error)
	require.Equal(t, "preserved synthetic content", page.Content)
	require.EqualValues(t, 1, page.MutationRevision)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "sqlite", "000008_processing_lifecycle.down.sql"))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(down)).Error)
	var content string
	require.NoError(t, db.Raw("SELECT content FROM wiki_pages WHERE id = 'legacy'").Scan(&content).Error)
	require.Equal(t, page.Content, content)
}
