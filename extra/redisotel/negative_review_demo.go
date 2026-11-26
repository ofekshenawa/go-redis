package redisotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DemoRetries demonstrates a counter with a string-valued reason attribute.
// It intentionally has no callers: the scanner finds the metric declaration
// and connects its attributes to the counter by inspecting the Add call.
func DemoRetries(meter metric.Meter) {
	counter, _ := meter.Int64Counter("redis.client.demo.retries", metric.WithUnit("{retry}"))
	counter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("demo.reason", "retry")))
}
