package redisotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// This deliberately uncalled function demonstrates source-only detection.
// No runtime test needs to execute it for telemetry CI to notice the new metric.
func telemetryDemo(meter metric.Meter) {
	counter, _ := meter.Int64Counter("redis.client.demo.retries", metric.WithUnit("{retry}"))
	counter.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("demo.reason", "presentation")))
}
