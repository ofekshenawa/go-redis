package redisotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// TelemetryApprovalReuseDemo demonstrates source-level telemetry review.
// This PR-only example is not called by the client and must not be merged.
// Updating this comment leaves the approved metric and its attributes unchanged.
func TelemetryApprovalReuseDemo(meter metric.Meter) error {
	retries, err := meter.Int64Counter("redis.client.demo.approval_retries", metric.WithUnit("{retry}"))
	if err != nil {
		return err
	}
	retries.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("demo.reason", "retry"),
		attribute.String("demo.phase", "backoff"),
		attribute.Bool("demo.retryable", true),
	))
	return nil
}
