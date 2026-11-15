# FleetToll implementation roadmap

Updated: 2026-11-16. This is an evidence-based progress record, not a production-readiness claim.

## Implemented and verified slices

- [x] WebSocket telemetry intake with per-connection lifecycle, bounded 4 KiB frames, strict JSON and coordinate validation, same-origin browser checks, and concurrent connection unit/integration coverage.
- [x] Keyed Kafka producer with OBU partition key, idempotent producer configuration, `acks=all`, delivery result handling and real-broker producer integration.
- [x] Kafka consumer with auto-commit disabled, explicit commit after successful HTTP aggregation, cancellation/close behavior and real-broker consumer integration.
- [x] Haversine kilometer calculation with coordinate/OBU validation and per-OBU state in local volatile mode.
- [x] Defensive HTTP contracts for aggregation and invoice queries, including bounded strict JSON and typed invoice-not-found behavior.
- [x] PostgreSQL persistence mode for the trip inbox/baseline and aggregate event ledger. The calculator transaction stores `processed_telemetry` and `trip_state` together. Replays reuse the saved distance; aggregator unique-key insertion prevents duplicate total contribution.
- [x] Stable event identity built from Kafka topic, partition and offset; conflicts that reuse one event ID for different OBU/distance data are rejected.
- [x] Versioned protobuf contract and generated gRPC client/server for internal aggregation; the HTTP API remains available for demo and inspection.
- [x] Local 1,000-concurrent-call gRPC probe against PostgreSQL: 1,000 successes, zero errors, p95 319.9 ms. This is a one-shot development measurement only.
- [x] Real PostgreSQL 15 integration checks for calculator replay across a new service instance, baseline continuity, duplicate aggregate writes, event-ID conflict and SQL-shaped input.
- [x] Local 1,000-request concurrency probe for the PostgreSQL-backed HTTP aggregator. The initial implementation produced 53 client timeouts; after reducing the normal insert path from explicit `BEGIN`/insert/`COMMIT` to one parameterized autocommit insert, later runs completed 1,000/1,000 with zero errors. The latest run measured 1,911.2 req/s and p95 456 ms, with substantial variance between runs; results and limits are recorded in `docs/performance/AGGREGATE_API_LOCAL_LOAD.md`.

## Current feature status

The durable distance-processing slice previously passed unit tests, race detection, `go vet`, real PostgreSQL tests, and real Kafka tests. This gRPC slice has focused loopback transport tests and updates the Kafka integration path to use the generated gRPC client; run the commands recorded in the README before relying on the latest result. PostgreSQL is optional: both services fall back to memory when `DATABASE_URL` is unset, and that mode is not restart-safe. The PostgreSQL tables are not tenant-scoped, OBU IDs are client-supplied, and HTTP/gRPC endpoints are unauthenticated. There is no DLQ, bounded retry policy, audit trail, PostGIS, financial-grade amount ledger, distributed tracing, production topology or external penetration test. The latest previous local HTTP API run measured p95 456 ms, but run-to-run variance was significant. No 1,000-WebSocket or sustained combined-workload test has been run.

## Next implementation sequence

1. Add tenant, fleet, vehicle and server-authenticated device identity; scope every read and write by tenant.
2. Add bounded Kafka retry, poison-event handling, DLQ, alerting and operator-controlled replay.
3. Separate schema/service ownership and define retention, backup/restore, pool sizing and migration operations.
4. Add PostGIS toll zones, boundary semantics, effective-dated tariffs and toll assessment.
5. Replace prototype floating-point amounts with an integer-minor-unit ledger, immutable invoices and reconciliation.
6. Instrument trace propagation across WebSocket/Kafka/HTTP/PostgreSQL, bounded-cardinality metrics, dashboards and SLOs.
7. Extend threat review, failure injection and performance runs to 1,000 WebSocket connections and combined workloads where machine resources allow; profile before tuning.
8. Improve the simulator and provide a deterministic end-to-end demo.

Each feature needs a demonstrated RED/GREEN/REFACTOR loop, meaningful unit tests, real integration tests where infrastructure is relevant, a local adversarial review, honest measured performance evidence and a learning guide.
