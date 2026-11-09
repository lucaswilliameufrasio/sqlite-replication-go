package storages3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

func TestNewRequiresClientAndBucket(t *testing.T) {
	if _, err := New(nil, "bucket"); err == nil {
		t.Fatal("New accepted a nil client")
	}
	if _, err := New(&s3.Client{}, " "); err == nil {
		t.Fatal("New accepted an empty bucket")
	}
}

func TestValidateKey(t *testing.T) {
	for _, key := range []string{"", "/absolute", "contains\x00nul"} {
		if err := validateKey(key); err == nil {
			t.Errorf("validateKey(%q) succeeded", key)
		}
	}
	if err := validateKey("prefix/valid.ltx"); err != nil {
		t.Fatalf("validateKey(valid): %v", err)
	}
}

func TestReconcileRejectsInvalidInventoryBeforeRemoteIO(t *testing.T) {
	store, err := New(&s3.Client{}, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	validHash := fmt.Sprintf("%x", sha256.Sum256([]byte("x")))
	tests := []struct {
		name    string
		objects []ObjectDescriptor
		prefix  string
	}{
		{name: "invalid checksum", prefix: "test/", objects: []ObjectDescriptor{{Key: "test/object", Size: 1, SHA256: "not-hex"}}},
		{name: "outside prefix", prefix: "test/", objects: []ObjectDescriptor{{Key: "other/object", Size: 1, SHA256: validHash}}},
		{name: "duplicate key", prefix: "test/", objects: []ObjectDescriptor{
			{Key: "test/object", Size: 1, SHA256: validHash},
			{Key: "test/object", Size: 1, SHA256: validHash},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.Reconcile(context.Background(), test.prefix, test.objects); err == nil {
				t.Fatal("Reconcile accepted an invalid local inventory")
			}
		})
	}
}

func TestMetricsUseLowCardinalityOperations(t *testing.T) {
	observer := &recordingMetrics{}
	store, err := NewWithMetrics(&s3.Client{}, "bucket", observer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(context.Background(), "/private-object-key", "")
	if err == nil {
		t.Fatal("invalid key unexpectedly succeeded")
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.operation != "get" || observer.class != ErrorPermanent || observer.duration < 0 {
		t.Fatalf("unexpected metric: %+v", observer)
	}
}

func TestPutImmutableHonorsCanceledContextDuringStaging(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	consumed := false
	reader := readerFunc(func([]byte) (int, error) {
		consumed = true
		return 0, io.EOF
	})
	store, err := New(&s3.Client{}, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PutImmutable(ctx, "key", reader)
	var storageErr *StorageError
	if !errors.As(err, &storageErr) || storageErr.Kind != ErrorCanceled {
		t.Fatalf("PutImmutable error = %v, want ErrorCanceled", err)
	}
	if consumed {
		t.Fatal("PutImmutable consumed input after context cancellation")
	}
}

func TestClassifyHTTPFailureClasses(t *testing.T) {
	tests := []struct {
		status int
		want   ErrorKind
	}{
		{status: http.StatusForbidden, want: ErrorPermanent},
		{status: http.StatusNotFound, want: ErrorNotFound},
		{status: http.StatusConflict, want: ErrorConflict},
		{status: http.StatusPreconditionFailed, want: ErrorConflict},
		{status: http.StatusTooManyRequests, want: ErrorRetryable},
		{status: http.StatusServiceUnavailable, want: ErrorRetryable},
	}
	for _, test := range tests {
		response := &smithyhttp.Response{Response: &http.Response{StatusCode: test.status}}
		failure := &smithyhttp.ResponseError{Response: response, Err: errors.New("synthetic transport response")}
		var storageErr *StorageError
		if !errors.As(classifyError("test", failure), &storageErr) || storageErr.Kind != test.want {
			t.Errorf("status %d mapped to %v, want %v", test.status, storageErr, test.want)
		}
	}
}

func TestClassifyConditionalWriteConflicts(t *testing.T) {
	for _, code := range []string{"PreconditionFailed", "ConditionalRequestConflict"} {
		response := &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusPreconditionFailed}}
		failure := &smithyhttp.ResponseError{
			Response: response,
			Err:      &smithy.GenericAPIError{Code: code, Message: "conditional write rejected"},
		}
		var storageErr *StorageError
		if !errors.As(classifyError("put", failure), &storageErr) || storageErr.Kind != ErrorConflict {
			t.Errorf("API code %q did not map to ErrorConflict: %v", code, storageErr)
		}
	}
}

func TestSDKRetriesTransientHeadFailure(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodHead {
			t.Errorf("request method = %s, want HEAD", request.Method)
		}
		if attempts.Add(1) == 1 {
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(writer, `<Error><Code>ServiceUnavailable</Code><Message>retry</Message></Error>`)
			return
		}
		writer.Header().Set("Content-Length", "17")
		writer.Header().Set("ETag", `"test-etag"`)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 3
				options.BaseDelay = time.Millisecond
				options.MaxBackoff = 5 * time.Millisecond
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	store, err := New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(ctx, "key"); err != nil {
		t.Fatalf("Head after transient 503: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("SDK attempts = %d, want one retry after transient 503", got)
	}
}

func TestSDKRetryAttemptsAreBounded(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writer.Header().Set("Content-Type", "application/xml")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(writer, `<Error><Code>ServiceUnavailable</Code></Error>`)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 3
				options.BaseDelay = time.Millisecond
				options.MaxBackoff = 5 * time.Millisecond
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	store, err := New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(ctx, "persistently-unavailable"); err == nil {
		t.Fatal("Head succeeded despite every request returning 503")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("SDK attempts = %d, want the configured maximum of 3", got)
	}
}

func TestHeadPropagatesRequestCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(started)
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	store, err := New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, requestErr := store.Head(ctx, "slow-key")
		result <- requestErr
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("S3 request did not reach the local test server")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Head succeeded after context cancellation")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Head did not return after context cancellation")
	}
}

func TestHeadHonorsOperationDeadline(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(started)
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)

	configCtx, cancelConfig := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelConfig()
	awsConfig, err := config.LoadDefaultConfig(configCtx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 1
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	store, err := New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, requestErr := store.Head(ctx, "deadline-key")
		result <- requestErr
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("S3 request did not reach the local test server")
	}
	select {
	case err := <-result:
		var storageErr *StorageError
		if !errors.As(err, &storageErr) || storageErr.Kind != ErrorRetryable {
			t.Fatalf("deadline error = %v, want retryable timeout", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Head did not return after its deadline")
	}
}

func TestPutImmutableReconcilesLostResponse(t *testing.T) {
	var mu sync.Mutex
	var stored []byte
	var putAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPut:
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, "could not read request body", http.StatusBadRequest)
				return
			}
			if putAttempts.Add(1) == 1 {
				mu.Lock()
				stored = body
				mu.Unlock()
				connection, buffer, err := writer.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				_ = buffer.Flush()
				_ = connection.Close()
				return
			}
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(writer, `<Error><Code>PreconditionFailed</Code></Error>`)
		case http.MethodGet:
			mu.Lock()
			body := append([]byte(nil), stored...)
			mu.Unlock()
			writer.Header().Set("Content-Length", fmt.Sprint(len(body)))
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write(body)
		default:
			http.Error(writer, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	store, err := New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("committed before reply was lost")
	receipt, err := store.PutImmutable(ctx, "lost-reply.ltx", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutImmutable should verify the committed object after lost response: %v", err)
	}
	if receipt.Size != int64(len(content)) || receipt.SHA256 != fmt.Sprintf("%x", sha256.Sum256(content)) {
		t.Fatalf("receipt after lost response = %+v", receipt)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(stored, content) {
		t.Fatalf("server stored %q, want %q", stored, content)
	}
}

type readerFunc func([]byte) (int, error)

func (read readerFunc) Read(buffer []byte) (int, error) { return read(buffer) }

type recordingMetrics struct {
	mu        sync.Mutex
	operation string
	duration  time.Duration
	bytes     int64
	class     ErrorKind
}

func (metrics *recordingMetrics) ObserveStorage(operation string, duration time.Duration, bytes int64, class ErrorKind) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.operation = operation
	metrics.duration = duration
	metrics.bytes = bytes
	metrics.class = class
}

func TestFlociAdapterConformance(t *testing.T) {
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SQLITE_REPLICATION_FLOCI_ENDPOINT to run S3 integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load test SDK config: %v", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	bucket := fmt.Sprintf("sqlite-replication-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create isolated test bucket: %v", err)
	}
	defer func() {
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()
	store, err := New(client, bucket)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("immutable Floci object\n")
	metadata := map[string]string{"source": "conformance"}
	receipt, err := store.PutImmutableWithMetadata(ctx, "test/small.ltx", bytes.NewReader(content), metadata)
	if err != nil {
		t.Fatalf("PutImmutable small: %v", err)
	}
	wantHash := fmt.Sprintf("%x", sha256.Sum256(content))
	if receipt.Size != int64(len(content)) || receipt.SHA256 != wantHash {
		t.Fatalf("receipt = %+v, want size %d sha256 %s", receipt, len(content), wantHash)
	}
	if _, err := store.PutImmutableWithMetadata(ctx, "test/small.ltx", bytes.NewReader(content), metadata); err != nil {
		t.Fatalf("identical immutable retry: %v", err)
	}
	if _, err := store.PutImmutableWithMetadata(ctx, "test/small.ltx", bytes.NewReader(content), map[string]string{"source": "different"}); err == nil {
		t.Fatal("retry with different metadata succeeded")
	}
	if _, err := store.PutImmutable(ctx, "test/small.ltx", strings.NewReader("different")); err == nil {
		t.Fatal("conflicting immutable retry succeeded")
	} else {
		var storageErr *StorageError
		if !errors.As(err, &storageErr) || storageErr.Kind != ErrorConflict {
			t.Fatalf("conflicting retry error = %v, want ErrorConflict", err)
		}
	}

	head, err := store.Head(ctx, "test/small.ltx")
	if err != nil || aws.ToInt64(head.ContentLength) != int64(len(content)) {
		t.Fatalf("Head = (%v, %v)", head, err)
	}
	if head.Metadata["source"] != "conformance" {
		t.Fatalf("Head metadata = %v, want caller metadata", head.Metadata)
	}
	emptyReceipt, err := store.PutImmutable(ctx, "test/empty.ltx", bytes.NewReader(nil))
	if err != nil || emptyReceipt.Size != 0 || emptyReceipt.SHA256 != fmt.Sprintf("%x", sha256.Sum256(nil)) {
		t.Fatalf("empty object receipt = (%+v, %v)", emptyReceipt, err)
	}
	got, err := store.Get(ctx, "test/small.ltx", "bytes=0-8")
	if err != nil {
		t.Fatalf("Get range: %v", err)
	}
	ranged, err := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if err != nil || !bytes.Equal(ranged, content[:9]) {
		t.Fatalf("Get range = %q, %v", ranged, err)
	}

	large := bytes.Repeat([]byte("multipart-data-"), (multipartThreshold/(len("multipart-data-")))+128)
	largeReceipt, err := store.PutImmutable(ctx, "test/large.ltx", bytes.NewReader(large))
	if err != nil {
		t.Fatalf("PutImmutable multipart: %v", err)
	}
	if largeReceipt.Size != int64(len(large)) {
		t.Fatalf("multipart receipt size = %d, want %d", largeReceipt.Size, len(large))
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("test/divergent.ltx"),
		Body: strings.NewReader("evil"), ContentLength: aws.Int64(4),
	}); err != nil {
		t.Fatalf("create divergent reconciliation fixture: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("test/unexpected.ltx"),
		Body: strings.NewReader("untracked"), ContentLength: aws.Int64(9),
	}); err != nil {
		t.Fatalf("create unexpected reconciliation fixture: %v", err)
	}
	missingHash := fmt.Sprintf("%x", sha256.Sum256([]byte("x")))
	divergentHash := fmt.Sprintf("%x", sha256.Sum256([]byte("safe")))
	report, err := store.Reconcile(ctx, "test/", []ObjectDescriptor{
		{Key: "test/small.ltx", Size: receipt.Size, SHA256: receipt.SHA256},
		{Key: "test/large.ltx", Size: largeReceipt.Size, SHA256: largeReceipt.SHA256},
		{Key: "test/empty.ltx", Size: emptyReceipt.Size, SHA256: emptyReceipt.SHA256},
		{Key: "test/missing.ltx", Size: 1, SHA256: missingHash},
		{Key: "test/divergent.ltx", Size: 4, SHA256: divergentHash},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !reflect.DeepEqual(report.Missing, []string{"test/missing.ltx"}) ||
		!reflect.DeepEqual(report.Divergent, []string{"test/divergent.ltx"}) ||
		!reflect.DeepEqual(report.Unexpected, []string{"test/unexpected.ltx"}) {
		t.Fatalf("Reconcile report = %+v", report)
	}

	page, err := store.ListPage(ctx, "test/", nil, 1)
	if err != nil || len(page.Contents) != 1 || page.NextContinuationToken == nil {
		t.Fatalf("first list page = (%v, %v), want one item and continuation", page, err)
	}
	secondPage, err := store.ListPage(ctx, "test/", page.NextContinuationToken, 1)
	if err != nil || len(secondPage.Contents) != 1 {
		t.Fatalf("second list page = (%v, %v), want one item", secondPage, err)
	}
	if err := store.DeleteBatch(ctx, []string{"test/small.ltx", "test/large.ltx", "test/empty.ltx", "test/divergent.ltx", "test/unexpected.ltx"}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
}

func TestFlociRestartMarker(t *testing.T) {
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	phase := os.Getenv("SQLITE_REPLICATION_FLOCI_RESTART_PHASE")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	fixturePath := os.Getenv("SQLITE_REPLICATION_FLOCI_FIXTURE")
	if endpoint == "" || phase == "" || runID == "" || fixturePath == "" {
		t.Skip("requires the Floci restart conformance environment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load test SDK config: %v", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	bucket := "sqlite-replication-adapter-" + runID
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read LTX fixture: %v", err)
	}
	store, err := New(client, bucket)
	if err != nil {
		t.Fatal(err)
	}
	switch phase {
	case "write":
		if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("create adapter restart bucket: %v", err)
		}
		if _, err := store.PutImmutable(ctx, "go-marker.ltx", bytes.NewReader(fixture)); err != nil {
			t.Fatalf("write adapter restart marker: %v", err)
		}
	case "verify":
		for _, key := range []string{"go-marker.ltx", "rust-marker.ltx"} {
			object, err := store.Get(ctx, key, "")
			if err != nil {
				t.Fatalf("read cross-language marker %s: %v", key, err)
			}
			got, readErr := io.ReadAll(object.Body)
			_ = object.Body.Close()
			if readErr != nil || !bytes.Equal(got, fixture) {
				t.Fatalf("cross-language marker %s differs from the LTX fixture: %v", key, readErr)
			}
			if key == "rust-marker.ltx" {
				restoreFixtureWithGo(t, ctx, got)
			}
		}
		if err := store.DeleteBatch(ctx, []string{"go-marker.ltx", "rust-marker.ltx"}); err != nil {
			t.Fatalf("delete restart markers: %v", err)
		}
		if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("delete restart bucket: %v", err)
		}
	default:
		t.Fatalf("unknown Floci restart phase %q", phase)
	}
}

func restoreFixtureWithGo(t *testing.T, ctx context.Context, ltxBytes []byte) {
	t.Helper()
	root := t.TempDir()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatalf("initialize Go restore repository: %v", err)
	}
	ltxPath := storagefs.LTXPath(root, 1, 1)
	if err := os.MkdirAll(filepath.Dir(ltxPath), 0o750); err != nil {
		t.Fatalf("create Go LTX level directory: %v", err)
	}
	if err := os.WriteFile(ltxPath, ltxBytes, 0o600); err != nil {
		t.Fatalf("stage Rust-published LTX for Go restore: %v", err)
	}
	destination := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := replication.RestoreSnapshot(ctx, root, destination); err != nil {
		t.Fatalf("restore Rust-published S3 LTX with Go: %v", err)
	}
}
