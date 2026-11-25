package redisotel

import "go.opentelemetry.io/otel/metric"

// NegativeTestExample illustrates a registration that cannot be inventoried statically.
func NegativeTestExample(m metric.Meter, name string) error {
	_, err := m.Int64Counter(name)
	return err
}
