package database

import (
	"context"
	"database/sql"
	"testing"
)

func TestProcessingSQLiteHashExistsOnEveryConnection(t *testing.T) {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	first, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, conn := range []*sql.Conn{first, second} {
		var digest string
		if err := conn.QueryRowContext(ctx, "SELECT weknora_sha256(?)", "abc").Scan(&digest); err != nil {
			t.Fatal(err)
		}
		if digest != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
			t.Fatal("unexpected digest")
		}
	}
}
