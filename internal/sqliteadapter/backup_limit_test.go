package sqliteadapter

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOnlineBackupImageLimitModernc(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; CREATE TABLE data(value BLOB); INSERT INTO data VALUES (zeroblob(131072))"); err != nil {
		t.Fatal(err)
	}
	small := filepath.Join(dir, "small ?#.sqlite")
	if err := CreateOnlineBackupWithImageLimit(ctx, source, small, 8192); err == nil || !strings.Contains(err.Error(), "database or disk is full") {
		t.Fatalf("expected SQLite full error, got %v", err)
	}
	if info, err := os.Stat(small); err == nil && info.Size() > 8192 {
		t.Fatalf("destination exceeded limit: %d", info.Size())
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	existing := filepath.Join(dir, "existing.sqlite")
	if err := os.WriteFile(existing, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CreateOnlineBackupWithImageLimit(ctx, source, existing, 8192); err == nil {
		t.Fatal("accepted nonempty destination")
	}
	if data, err := os.ReadFile(existing); err != nil || string(data) != "untouched" {
		t.Fatalf("destination changed: %q, %v", data, err)
	}
	large := filepath.Join(dir, "large ?#.sqlite")
	if err := CreateOnlineBackupWithImageLimit(ctx, source, large, 1<<20); err != nil {
		t.Fatalf("retry: %v", err)
	}
	uri := url.URL{Scheme: "file", Path: filepath.ToSlash(large)}
	backup, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var size int
	if err := backup.QueryRowContext(ctx, "SELECT length(value) FROM data").Scan(&size); err != nil || size != 131072 {
		t.Fatalf("backup content: %d, %v", size, err)
	}
}

func TestOnlineBackupImageLimitPageSizes(t *testing.T) {
	for _, pageSize := range []int{512, 1024, 4096, 8192, 65536} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			source := filepath.Join(dir, "source.sqlite")
			db, err := sql.Open("sqlite", source)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=%d; PRAGMA journal_mode=WAL; CREATE TABLE data(value BLOB); INSERT INTO data VALUES (zeroblob(262144))", pageSize)); err != nil {
				t.Fatal(err)
			}
			var actual int
			if err := db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&actual); err != nil || actual != pageSize {
				t.Fatalf("source page size %d: %v", actual, err)
			}
			dest := filepath.Join(dir, "limited.sqlite")
			limit := uint64(pageSize * 2)
			if err := CreateOnlineBackupWithImageLimit(ctx, source, dest, limit); err == nil || !strings.Contains(err.Error(), "database or disk is full") {
				t.Fatalf("expected SQLite full error: %v", err)
			}
			if info, err := os.Stat(dest); err == nil && uint64(info.Size()) > limit {
				t.Fatalf("destination size %d exceeds %d", info.Size(), limit)
			} else if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
		})
	}
}
