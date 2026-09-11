package database

import (
	"crypto/sha256"
	"database/sql"
	"fmt"

	"github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const SQLiteDriverName = "weknora_sqlite3"

func init() {
	sql.Register(SQLiteDriverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		return conn.RegisterFunc("weknora_sha256", func(value string) string {
			return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
		}, true)
	}})
}

// Every pooled connection needs the same deterministic error-identity hash.
func SQLite(dsn string) gorm.Dialector {
	return &sqlite.Dialector{DriverName: SQLiteDriverName, DSN: dsn}
}
