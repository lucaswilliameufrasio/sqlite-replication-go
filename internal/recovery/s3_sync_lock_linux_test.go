//go:build linux

package recovery

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

func TestS3SyncHoldsLocalLockDuringUpload(t *testing.T) {
	for _, cancelUpload := range []bool{false, true} {
		name := "success"
		if cancelUpload {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source.sqlite")
			repo := filepath.Join(root, "repository")
			db, err := sql.Open("sqlite", source)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE test (value TEXT)"); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := replication.Snapshot(context.Background(), source, repo); err != nil {
				t.Fatal(err)
			}

			reached := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				once.Do(func() { close(reached); <-release })
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
			client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(server.URL); o.UsePathStyle = true })
			store, err := storages3.New(client, "test-bucket")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := SyncFilesystemToS3(ctx, repo, store, "test", "remote"); result <- err }()
			select {
			case <-reached:
			case <-ctx.Done():
				close(release)
				t.Fatal("upload did not start")
			}
			lock, lockErr := repositorylock.AcquireExclusive(repo)
			if lockErr == nil {
				_ = lock.Close()
			}
			if !errors.Is(lockErr, unix.EWOULDBLOCK) {
				close(release)
				t.Fatalf("lock during upload: %v", lockErr)
			}
			if cancelUpload {
				cancel()
			}
			close(release)
			err = <-result
			if cancelUpload && err == nil {
				t.Fatal("cancelled upload succeeded")
			}
			if !cancelUpload && err != nil {
				t.Fatalf("sync: %v", err)
			}
			lock, err = repositorylock.AcquireExclusive(repo)
			if err != nil {
				t.Fatalf("lock after sync: %v", err)
			}
			if err := lock.Close(); err != nil {
				t.Fatal(err)
			}
			if !cancelUpload {
				ack, err := spool.ReadRemoteAck(repo)
				if err != nil || ack.TXID != 1 {
					t.Fatalf("ack after sync: %+v, %v", ack, err)
				}
			}
		})
	}
}
