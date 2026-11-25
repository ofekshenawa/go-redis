package redisotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DemoRetries intentionally has no callers. The review covers source declarations.
func DemoRetries(meter metric.Meter) {
	counter, _ := meter.Int64Counter("redis.client.demo.retries", metric.WithUnit("{retry}"))
	counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("demo.reason", "retry")))
}
