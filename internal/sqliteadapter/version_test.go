package sqliteadapter

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestMinimumSQLiteVersion(t *testing.T) {
	for _, test := range []struct {
		version string
		allowed bool
	}{
		{version: "3.51.2", allowed: false},
		{version: "3.51.3", allowed: true},
		{version: "3.51.10", allowed: true},
		{version: "3.52.0", allowed: true},
		{version: "4.0.0", allowed: true},
		{version: "3.9.99", allowed: false},
		{version: "3.51", allowed: false},
		{version: "3.51.3.1", allowed: false},
		{version: "3.51.x", allowed: false},
		{version: "3.-1.3", allowed: false},
		{version: "", allowed: false},
	} {
		t.Run(test.version, func(t *testing.T) {
			if got := versionAtLeast(test.version, minimumSQLiteVersion); got != test.allowed {
				t.Fatalf("versionAtLeast(%q, %q) = %t; want %t", test.version, minimumSQLiteVersion, got, test.allowed)
			}
		})
	}
}

func TestManagedCaptureRequiresWALJournalMode(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, "CREATE TABLE records (value TEXT)"); err != nil {
		t.Fatalf("initialize rollback-mode database: %v", err)
	}
	managedDB, managedConn, err := openManagedCaptureConnection(ctx, path)
	if err == nil {
		_ = managedConn.Close()
		_ = managedDB.Close()
		t.Fatal("capture connection accepted non-WAL journal mode")
	}
	if !strings.Contains(err.Error(), "journal_mode must be WAL") {
		t.Fatalf("unexpected capture connection error: %v", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}
	managedDB, managedConn, err = openManagedCaptureConnection(ctx, path)
	if err != nil {
		t.Fatalf("capture connection with WAL: %v", err)
	}
	if err := managedConn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := managedDB.Close(); err != nil {
		t.Fatal(err)
	}
}
