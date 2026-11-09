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
	"github.com/prometheus/client_golang/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	_ "modernc.org/sqlite"
)

func TestCoordinatorExportsInMemoryMetrics(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	observer, err := NewFlushMetrics(registry, provider.Meter("sqlite-replication-example"))
	if err != nil {
		t.Fatalf("create metrics: %v", err)
	}
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	db, err := sql.Open("sqlite", databasePath)
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
	if _, err := replication.Snapshot(ctx, databasePath, repository); err != nil {
		t.Fatal(err)
	}
	available := true
	config := replicator.CoordinatorConfig{
		RepositoryRoot: repository,
		DatabasePath:   databasePath,
		ObserveTXID: func(context.Context) (uint64, bool, error) {
			return 1, available, nil
		},
		Capture: func(context.Context) (replicator.CaptureResult, error) {
			if !available {
				return replicator.CaptureResult{}, errors.New("injected capture failure")
			}
			return replicator.CaptureResult{TXID: 1}, nil
		},
		SetAdmission: func(context.Context, bool, replicator.Limits) error { return nil },
		DrainWrites:  func(context.Context) error { return nil },
		Limits: replicator.Limits{MaxSpoolBytes: 1 << 30, MaxWALBytes: 1 << 30,
			MaxUnprotectedAge: time.Hour, MaxUploadsInFlight: 1},
		Metrics: observer,
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("successful flush: %v", err)
	}
	available = false
	if err := coordinator.Flush(ctx); err == nil {
		t.Fatal("failing capture succeeded")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := coordinator.Flush(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled flush: %v", err)
	}
	families, err := registry.Gather()
	if err != nil || len(families) != 1 {
		t.Fatalf("gather Prometheus: families=%d error=%v", len(families), err)
	}
	promCounts := make(map[string]uint64)
	for _, sample := range families[0].Metric {
		if len(sample.Label) != 1 || sample.Label[0].GetName() != "outcome" {
			t.Fatal("unexpected Prometheus labels")
		}
		promCounts[sample.Label[0].GetValue()] = sample.Histogram.GetSampleCount()
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatal(err)
	}
	otelCounts := make(map[string]uint64)
	for _, scope := range collected.ScopeMetrics {
		for _, measure := range scope.Metrics {
			if measure.Name != "sqlite.replication.flush.duration" {
				t.Fatalf("unexpected metric %s", measure.Name)
			}
			histogram, ok := measure.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatal("unexpected OpenTelemetry aggregation")
			}
			for _, point := range histogram.DataPoints {
				label, ok := point.Attributes.Value("outcome")
				if !ok || point.Attributes.Len() != 1 {
					t.Fatal("unexpected OpenTelemetry attributes")
				}
				otelCounts[label.AsString()] = point.Count
			}
		}
	}
	if len(promCounts) != 3 || len(otelCounts) != 3 {
		t.Fatalf("unexpected outcome series: Prometheus=%d OpenTelemetry=%d", len(promCounts), len(otelCounts))
	}
	for _, outcome := range []string{"success", "failure", "canceled"} {
		if promCounts[outcome] != 1 || otelCounts[outcome] != 1 {
			t.Errorf("outcome %s: Prometheus=%d OpenTelemetry=%d", outcome, promCounts[outcome], otelCounts[outcome])
		}
	}
}
