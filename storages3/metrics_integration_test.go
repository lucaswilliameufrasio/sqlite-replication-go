package storages3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type storageObservation struct {
	operation string
	duration  time.Duration
	bytes     int64
	class     ErrorKind
}

type storageObservations struct {
	mu     sync.Mutex
	values []storageObservation
}

func (o *storageObservations) ObserveStorage(operation string, duration time.Duration, bytes int64, class ErrorKind) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.values = append(o.values, storageObservation{operation, duration, bytes, class})
}

func TestStorageMetricsAcrossLocalHTTPBoundary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Length", "5")
			_, _ = io.WriteString(w, "value")
		case http.MethodHead:
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", "")),
		Retryer:      aws.NopRetryer{},
	})
	observations := &storageObservations{}
	store, err := NewWithMetrics(client, "test-bucket", observations)
	if err != nil {
		t.Fatalf("construct store: %v", err)
	}
	output, err := store.Get(context.Background(), "sensitive-key-fixture", "")
	if err != nil {
		t.Fatalf("get local object: %v", err)
	}
	body, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := output.Body.Close(); err != nil {
		t.Fatalf("close response: %v", err)
	}
	if string(body) != "value" {
		t.Fatalf("unexpected response body: %q", body)
	}
	_, err = store.Head(context.Background(), "other-sensitive-key-fixture")
	if err == nil {
		t.Fatal("HEAD against forbidden server succeeded")
	}
	if strings.Contains(err.Error(), "other-sensitive-key-fixture") || strings.Contains(err.Error(), "test-bucket") {
		t.Fatal("storage error includes object key or bucket")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, "canceled-key-fixture", ""); err == nil {
		t.Fatal("canceled GET succeeded")
	}
	observations.mu.Lock()
	defer observations.mu.Unlock()
	if len(observations.values) != 3 {
		t.Fatalf("got %d observations, want 3", len(observations.values))
	}
	want := []storageObservation{
		{operation: "get", bytes: 5, class: ErrorNone},
		{operation: "head", bytes: 0, class: ErrorPermanent},
		{operation: "get", bytes: 0, class: ErrorCanceled},
	}
	for i, got := range observations.values {
		if got.operation != want[i].operation || got.bytes != want[i].bytes || got.class != want[i].class || got.duration < 0 {
			t.Errorf("observation %d = %+v, want operation=%s bytes=%d class=%d and nonnegative duration", i, got, want[i].operation, want[i].bytes, want[i].class)
		}
	}
}
