package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// FlushOperation is deliberately narrow: the example does not instrument Run.
type FlushOperation interface {
	Flush(context.Context) error
}

// TracedFlush propagates the caller's context to Flush and emits a bounded
// result attribute. It does not record error messages or request identifiers.
func TracedFlush(ctx context.Context, tracer trace.Tracer, operation FlushOperation) error {
	ctx, span := tracer.Start(ctx, "sqlite.replication.flush")
	defer span.End()
	err := operation.Flush(ctx)
	result := "success"
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		result = "canceled"
		span.SetStatus(codes.Error, "flush canceled")
	} else if err != nil {
		result = "failure"
		span.SetStatus(codes.Error, "flush failed")
	}
	span.SetAttributes(attribute.String("outcome", result))
	return err
}
