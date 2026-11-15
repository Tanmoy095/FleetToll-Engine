# Local gRPC aggregator load probe

**Date:** 2026-11-16

**Command:** `go run ./scripts/grpc_aggregate_load -users 1000`

**Backend:** local PostgreSQL 15 Compose service; aggregator bound to loopback; one unary request per concurrent goroutine.

## Result

```text
requests=1000 concurrency=1000 elapsed=339.179958ms throughput=2948.3_req_s p50=179.30125ms p95=319.934541ms p99=331.634541ms errors=0
```

All 1,000 calls returned successfully. This is the final rerun after widening OBU IDs to `int64`. The run was performed on macOS 26.5.1, arm64. CPU-core count was not available in the restricted shell. The data uses a local PostgreSQL container and loopback network.

## Interpretation and limits

This is a short, one-shot local probe, not sustained load, a production SLO, a capacity certification or a comparison proving gRPC faster than HTTP. It does not include Kafka ingestion, WebSockets, multiple service replicas, TLS, authentication, warm-up, long-running connections, mixed invoice reads, CPU/memory profiling or failure injection. Percentiles are per-RPC client latency and the test submits unique IDs to one OBU. Run multiple times and profile before drawing performance conclusions.

Reproduce it by starting `docker compose up -d postgres`, starting the aggregator with the PostgreSQL DSN on `DATABASE_URL`, and executing `make load-grpc`. The probe is safe only against a local development database because it writes 1,000 event rows.
