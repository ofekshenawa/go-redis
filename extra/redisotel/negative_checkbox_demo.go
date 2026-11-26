package redisotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// demoRetries is deliberately uncalled so the demo exercises static collection.
func demoRetries(meter metric.Meter) {
	retries, _ := meter.Int64Counter("redis.client.demo.retries", metric.WithUnit("{retry}"))
	retries.Add(context.Background(), 1, metric.WithAttributes(attribute.String("demo.reason", "retry")))
}
