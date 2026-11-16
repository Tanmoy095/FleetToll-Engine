# FleetToll implementation roadmap

Updated: 2026-11-17. This is an evidence-based progress record, not a production-readiness claim.

## Implemented and verified slices

- [x] WebSocket telemetry intake with per-connection lifecycle, bounded 4 KiB frames, strict JSON and coordinate validation, same-origin browser checks, and concurrent connection unit/integration coverage.
- [x] Keyed Kafka producer with tenant/OBU partition key, idempotent producer configuration, `acks=all`, and delivery result handling. Real-broker integration passed on the prior revision; rerun after the tenant-key change.
- [x] Kafka consumer with auto-commit disabled, explicit commit after successful authenticated gRPC aggregation, and cancellation/close behavior. Real-broker integration passed on the prior revision; rerun after the tenant-auth change.
- [x] Haversine kilometer calculation with coordinate/OBU validation and per-OBU state in local volatile mode.
- [x] Defensive HTTP contracts for aggregation and invoice queries, including bounded strict JSON and typed invoice-not-found behavior.
- [x] PostgreSQL persistence mode for the trip inbox/baseline and aggregate event ledger. The calculator transaction stores `processed_telemetry` and `trip_state` together. Replays reuse the saved distance; aggregator unique-key insertion prevents duplicate total contribution.
- [x] Stable event identity built from Kafka topic, partition and offset; conflicts that reuse one event ID for different OBU/distance data are rejected.
- [x] Versioned protobuf contract and generated gRPC client/server for internal aggregation; tenant ID is part of the typed message, gRPC and aggregate HTTP require a shared service token, invoice HTTP requires a tenant operator credential.
- [x] Earlier local 1,000-concurrent-call gRPC probe against PostgreSQL (2026-11-16): 1,000 successes, zero errors, p95 319.9 ms. This is a one-shot result for the earlier revision, not a tenant-isolation performance result.
- [x] Real PostgreSQL 15 integration checks for calculator replay across a new service instance, baseline continuity, duplicate aggregate writes, event-ID conflict and SQL-shaped input.
- [x] Local 1,000-request concurrency probe for the PostgreSQL-backed HTTP aggregator. The initial implementation produced 53 client timeouts; after reducing the normal insert path from explicit `BEGIN`/insert/`COMMIT` to one parameterized autocommit insert, later runs completed 1,000/1,000 with zero errors. The latest run measured 1,911.2 req/s and p95 456 ms, with substantial variance between runs; results and limits are recorded in `docs/performance/AGGREGATE_API_LOCAL_LOAD.md`.

## Current feature status

Tenant isolation is implemented across credential-derived device identity, OBU allowlists, tenant-scoped memory/PostgreSQL access, tenant-qualified Kafka keys, migration 002, tenant-aware protobuf fields, and service-token protection for internal aggregation. The exact checks and limits for this revision are:

- Full unit suite, race detector, and `go vet` passed.
- Tagged PostgreSQL integration suite passed twice against an isolated local PostgreSQL cluster. It covers migration/backfill, configured-tenant validation, replay, and same OBU/event IDs across tenants.
- Kafka integration was not run because no broker was listening. Earlier real-broker results apply to the prior revision; rerun after the tenant key/auth change.
- Authenticated 1,000-call gRPC probe passed with zero errors against the in-memory store. This is not a PostgreSQL load result.
- Security review was adversarial self-review, not an independent audit.

Credential configuration is a prototype shared-secret scheme. Tenant provisioning beyond seeded tenant 1 is manual. Rotation, revocation, TLS/mTLS, audit events, rate limits and managed identity remain open. Legacy messages without tenant ID map to tenant 1 for compatibility; remove that fallback before accepting untrusted producers. DLQ/retry, PostGIS, financial-grade billing, tracing, production topology and an external penetration test are not implemented. No 1,000-WebSocket or sustained combined-workload test has run.

## Next implementation sequence

1. Re-run all five engineering gates after this feature: tests, real integration, RED/GREEN/REFACTOR evidence, adversarial security review and measured performance. Fix failed gates before adding scope.
2. Replace prototype shared bearer secrets with managed identity, rotation/revocation, TLS/mTLS, audit events and rate limiting; remove tenant-1 compatibility fallbacks.
3. Add bounded Kafka retry, poison-event handling, DLQ, alerting and operator-controlled replay.
4. Define retention, backup/restore, pool sizing and low-downtime migration operations.
5. Add PostGIS toll zones, boundary semantics, effective-dated tariffs and toll assessment.
6. Replace prototype floating-point amounts with an integer-minor-unit ledger, immutable invoices and reconciliation.
7. Add trace propagation, bounded-cardinality metrics, dashboards, SLOs, 1,000-WebSocket and combined-load runs; profile before tuning.
8. Improve the simulator and provide a deterministic end-to-end demo.

Each feature needs a demonstrated RED/GREEN/REFACTOR loop, meaningful unit tests, real integration tests where infrastructure is relevant, a local adversarial review, honest measured performance evidence and a learning guide.
