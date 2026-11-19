package redisotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// TelemetryApprovalReuseDemo demonstrates source-level telemetry review.
// This PR-only example is not called by the client and must not be merged.
func TelemetryApprovalReuseDemo(meter metric.Meter) error {
	retries, err := meter.Int64Counter("redis.client.demo.approval_retries", metric.WithUnit("{retry}"))
	if err != nil {
		return err
	}
	retries.Add(context.Background(), 1, metric.WithAttributes(attribute.String("demo.reason", "retry")))
	return nil
}
