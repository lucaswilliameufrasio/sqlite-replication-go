// Package telemetry demonstrates optional in-process metrics without an HTTP server.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// FlushMetrics demonstrates two independent backends. Applications would usually
// select one, rather than export both and double-count the same operations.
type FlushMetrics struct {
	prometheus *prometheus.HistogramVec
	otel       metric.Float64Histogram
}

func NewFlushMetrics(registerer prometheus.Registerer, meter metric.Meter) (*FlushMetrics, error) {
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "sqlite_replication_flush_duration_seconds",
		Help: "Duration of coordinator flush calls, including time waiting for the lock.",
	}, []string{"outcome"})
	otelHistogram, err := meter.Float64Histogram("sqlite.replication.flush.duration", metric.WithUnit("s"), metric.WithDescription("Duration of coordinator flush calls, including time waiting for the lock."))
	if err != nil {
		return nil, fmt.Errorf("create OpenTelemetry histogram: %w", err)
	}
	if err := registerer.Register(histogram); err != nil {
		return nil, fmt.Errorf("register Prometheus histogram: %w", err)
	}
	return &FlushMetrics{prometheus: histogram, otel: otelHistogram}, nil
}

func (m *FlushMetrics) ObserveFlush(duration time.Duration, outcome replicator.FlushOutcome) {
	// Limit label values even if callers invoke this observer themselves.
	label := "other"
	switch outcome {
	case replicator.FlushSucceeded, replicator.FlushFailed, replicator.FlushCanceled:
		label = string(outcome)
	}
	seconds := duration.Seconds()
	m.prometheus.WithLabelValues(label).Observe(seconds)
	// The callback has no request context: this records metrics, not trace spans.
	m.otel.Record(context.Background(), seconds, metric.WithAttributes(attribute.String("outcome", label)))
}
