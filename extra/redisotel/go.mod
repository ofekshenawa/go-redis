module github.com/redis/go-redis/extra/redisotel/v9

go 1.15

replace github.com/redis/go-redis/v9 => ../..

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/redis/go-redis/extra/rediscmd/v9 v9.0.0
	github.com/redis/go-redis/v9 v9.0.0
	go.opentelemetry.io/otel v1.11.2
	go.opentelemetry.io/otel/metric v0.34.0
	go.opentelemetry.io/otel/sdk v1.9.0
	go.opentelemetry.io/otel/trace v1.11.2
)
