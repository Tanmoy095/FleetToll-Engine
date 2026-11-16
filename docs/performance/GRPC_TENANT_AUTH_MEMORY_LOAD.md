# Authenticated gRPC probe — in-memory mode

**Run date:** 2026-11-17
**Environment:** local macOS development machine; aggregator and store in volatile in-memory mode
**Command:** `AGGREGATOR_GRPC_TOKEN=<local secret> go run ./scripts/grpc_aggregate_load -target 127.0.0.1:13001 -users 1000`

| Requests | Concurrency | Elapsed | Throughput | p50 | p95 | p99 | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,000 | 35.8 ms | 27,898 req/s | 26.3 ms | 34.4 ms | 35.0 ms | 0 |

This is a one-shot local measurement of protobuf/gRPC, service-token authentication and in-memory aggregation. It is not comparable to the earlier PostgreSQL-backed run and is not a production capacity claim. It excludes Kafka, WebSocket connections, database work, realistic payload diversity and sustained load. Rerun against PostgreSQL and repeat multiple times on a recorded machine before drawing performance conclusions.
