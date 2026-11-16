# ADR-021: Persist telemetry inbox, trip state and idempotent distance ledger

- **Status:** Accepted; implemented for local PostgreSQL mode
- **Date:** 2026-11-15

## Context

The original Kafka consumer committed only after HTTP success; the current consumer uses authenticated gRPC and commits only after RPC success. HTTP and Kafka do not share a transaction. If the calculator advances its in-memory point and the HTTP call fails, replay can calculate a different distance. If the aggregator accepted the write but its response was lost, replay can add the same distance a second time. Process restarts also discard both calculator state and invoice totals.

## Requirements and constraints

- Derive an idempotency identity from Kafka topic, partition and offset, scoped by tenant.
- Commit the calculated distance and next tenant/OBU coordinate atomically.
- Return the original distance for a replay, including after a calculator restart and independently for each tenant.
- Make invoice aggregation safe to retry after ambiguous gRPC outcomes.
- Keep current services and simple local development workflow.
- Do not claim a cross-service exactly-once transaction.

## Considered alternatives

1. Keep volatile maps and rely on Kafka producer idempotence. Rejected: producer idempotence does not cover consumer replay or process state.
2. Commit Kafka offsets before the HTTP call. Rejected: a downstream outage can lose a distance permanently.
3. Add PostgreSQL inbox/state and a unique aggregation ledger. Selected: PostgreSQL transactions protect related state and stable event identities make retries safe.
4. Adopt a transactional outbox with a single database-owned event publisher. Deferred: useful when downstream event publication becomes part of the database transaction; current boundary is synchronous HTTP and an offset commit.

## Decision

The consumer sends `topic:partition:offset` as `eventID`, alongside tenant ID from authenticated ingestion. Embedded, version-named SQL files are tracked in `schema_migrations` and applied transactionally under a database advisory lock, so starting both local services concurrently does not race schema changes. `PostgresCalculator.CalculateEvent` opens a read-committed transaction, takes a transaction-scoped advisory lock for the tenant/OBU pair, returns an existing `processed_telemetry` result if found, otherwise reads `trip_state`, calculates Haversine distance, writes the new point and inbox row, and commits. Migration 002 keys `aggregated_distance_events` by `(tenant_id,event_id)`. Repeating the same tenant/OBU/distance/timestamp is a no-op; conflicting reuse inside a tenant is an error, while separate tenants may reuse the same event ID.

The consumer acknowledges Kafka only after its authenticated gRPC aggregation call succeeds. `DATABASE_URL` enables PostgreSQL in both calculator and aggregator; unset configuration retains an explicitly volatile in-memory development mode.

## Consequences and trade-offs

- Replays after a committed calculator transaction return the original result, so a new process can safely resume.
- A database outage stops processing without committing Kafka; the event remains available for retry. A poison event can still block its partition because DLQ handling is not implemented.
- PostgreSQL serializes processing per tenant/OBU identity. This provides simple correctness but may constrain a single very hot vehicle key; profile before changing.
- Each event creates inbox and aggregate rows. Retention, partitioning, archival and deletion policy are future operational work.
- `eventID` is a Kafka identity, not proof of caller identity. The tenant-auth increment now protects transport boundaries and scopes all store identities; shared-secret and legacy tenant-1 compatibility limits remain.
- The database transaction and Kafka commit are not atomic together. The design is at-least-once delivery with idempotent database effects, not end-to-end exactly-once processing.
- A composite primary key on `(tenant_id,event_id)` makes scoped deduplication enforceable. Tenant-first indexes support invoice filtering at the cost of write amplification and storage.
- PostgreSQL is shared by the current local services for demonstration. A larger deployment should clarify service ownership and access boundaries.

## Security and operations

Queries use bound parameters. The Compose password is development-only; deploy with restricted roles, managed credentials, network policy, TLS and backups. Monitor database pool wait, transaction latency, lock contention, uncommitted Kafka lag and duplicate/conflict counts before claiming an operational SLO.

## Testing implications

Unit tests exercise duplicate handling, conflicting identities and concurrent memory-store writes. Tagged integration tests use real PostgreSQL to verify replay across a new calculator instance, baseline continuity, duplicate aggregate writes, SQL-shaped input and unique-key conflict behavior. The separate real Kafka tests verified producer and consumer boundaries on the previous revision; rerun after the tenant-key change (broker unavailable on 2026-11-17).

## Reconsider when

- Event throughput or write retention warrants partitioning or archival.
- A service-owned outbox becomes necessary for asynchronous downstream events.
- Tenant ownership requires a new composite key or state key.
- The hot-key lock materially limits throughput in a measured workload.
