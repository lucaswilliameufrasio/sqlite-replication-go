package telemetry

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	_ "modernc.org/sqlite"
)

func TestTracedFlushPropagatesContextAndRedactsErrors(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "source.sqlite")
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE data (value TEXT); INSERT INTO data VALUES ('example')"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(context.Background(), database, repository); err != nil {
		t.Fatal(err)
	}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := provider.Tracer("sqlite-replication-example")
	parentCtx, parent := tracer.Start(context.Background(), "synthetic-parent")
	parentID := parent.SpanContext()
	available := true
	var observed trace.SpanContext
	config := replicator.CoordinatorConfig{
		RepositoryRoot: repository,
		DatabasePath:   database,
		ObserveTXID: func(ctx context.Context) (uint64, bool, error) {
			observed = trace.SpanContextFromContext(ctx)
			if !available {
				return 0, false, errors.New("sensitive-error-fixture")
			}
			return 1, true, nil
		},
		Capture: func(context.Context) (replicator.CaptureResult, error) {
			return replicator.CaptureResult{TXID: 1}, nil
		},
		SetAdmission: func(context.Context, bool, replicator.Limits) error { return nil },
		DrainWrites:  func(context.Context) error { return nil },
		Limits: replicator.Limits{MaxSpoolBytes: 1 << 30, MaxWALBytes: 1 << 30,
			MaxUnprotectedAge: time.Hour, MaxUploadsInFlight: 1},
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := TracedFlush(parentCtx, tracer, coordinator); err != nil {
		t.Fatalf("successful flush: %v", err)
	}
	firstObserved := observed
	available = false
	if err := TracedFlush(parentCtx, tracer, coordinator); err == nil {
		t.Fatal("expected adapter failure")
	}
	canceled, cancel := context.WithCancel(parentCtx)
	cancel()
	if err := TracedFlush(canceled, tracer, coordinator); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("got %d ended spans, want 3", len(spans))
	}
	for i, want := range []struct {
		outcome string
		status  codes.Code
		message string
	}{{"success", codes.Unset, ""}, {"failure", codes.Error, "flush failed"}, {"canceled", codes.Error, "flush canceled"}} {
		span := spans[i]
		if span.Name() != "sqlite.replication.flush" || span.Parent().SpanID() != parentID.SpanID() || span.SpanContext().TraceID() != parentID.TraceID() {
			t.Errorf("span %d has unexpected name or parent", i)
		}
		if span.Status().Code != want.status || span.Status().Description != want.message {
			t.Errorf("span %d has unexpected status", i)
		}
		attrs := span.Attributes()
		if len(attrs) != 1 || string(attrs[0].Key) != "outcome" || attrs[0].Value.AsString() != want.outcome || len(span.Events()) != 0 {
			t.Errorf("span %d has unexpected attributes or events", i)
		}
	}
	if firstObserved.SpanID() != spans[0].SpanContext().SpanID() {
		t.Fatal("adapter did not receive flush span context")
	}
	parent.End()
}
