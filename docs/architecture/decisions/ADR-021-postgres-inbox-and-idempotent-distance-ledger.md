# ADR-021: Persist telemetry inbox, trip state and idempotent distance ledger

- **Status:** Accepted; implemented for local PostgreSQL mode
- **Date:** 2026-11-15

## Context

The Kafka consumer commits only after an HTTP success. HTTP and Kafka do not share a transaction. If the calculator advances its in-memory point and the HTTP call fails, replay can calculate a different distance. If the aggregator accepted the write but its response was lost, replay can add the same distance a second time. Process restarts also discard both calculator state and invoice totals.

## Requirements and constraints

- Derive an idempotency identity from Kafka topic, partition and offset.
- Commit the calculated distance and next OBU coordinate atomically.
- Return the original distance for a replay, including after a calculator restart.
- Make invoice aggregation safe to retry after ambiguous HTTP outcomes.
- Keep current services and simple local development workflow.
- Do not claim a cross-service exactly-once transaction.

## Considered alternatives

1. Keep volatile maps and rely on Kafka producer idempotence. Rejected: producer idempotence does not cover consumer replay or process state.
2. Commit Kafka offsets before the HTTP call. Rejected: a downstream outage can lose a distance permanently.
3. Add PostgreSQL inbox/state and a unique aggregation ledger. Selected: PostgreSQL transactions protect related state and stable event identities make retries safe.
4. Adopt a transactional outbox with a single database-owned event publisher. Deferred: useful when downstream event publication becomes part of the database transaction; current boundary is synchronous HTTP and an offset commit.

## Decision

The consumer sends `topic:partition:offset` as `eventID`. Embedded, version-named SQL files are tracked in `schema_migrations` and applied transactionally under a database advisory lock, so starting both local services concurrently does not race schema changes. `PostgresCalculator.CalculateEvent` opens a read-committed transaction, takes a transaction-scoped advisory lock for the OBU, returns an existing `processed_telemetry` result if found, otherwise reads `trip_state`, calculates Haversine distance, writes the new point and inbox row, and commits. The aggregator stores each distance in `aggregated_distance_events` under a unique `event_id`. Repeating the same OBU and distance is a no-op; reusing an event ID with different OBU or distance is an error.

The consumer acknowledges Kafka only after its aggregator HTTP call succeeds. `DATABASE_URL` enables PostgreSQL in both calculator and aggregator; unset configuration retains an explicitly volatile in-memory development mode.

## Consequences and trade-offs

- Replays after a committed calculator transaction return the original result, so a new process can safely resume.
- A database outage stops processing without committing Kafka; the event remains available for retry. A poison event can still block its partition because DLQ handling is not implemented.
- PostgreSQL serializes processing per numeric OBU key. This provides simple correctness but may constrain a single very hot vehicle key; profile before changing.
- Each event creates inbox and aggregate rows. Retention, partitioning, archival and deletion policy are future operational work.
- `eventID` is a Kafka identity, not proof of caller identity. The HTTP endpoints remain unauthenticated; tenant isolation is absent.
- The database transaction and Kafka commit are not atomic together. The design is at-least-once delivery with idempotent database effects, not end-to-end exactly-once processing.
- A unique index on `event_id` makes deduplication constant-time. The `(obu_id)` index supports invoice filtering at the cost of write amplification and storage.
- PostgreSQL is shared by the current local services for demonstration. A larger deployment should clarify service ownership and access boundaries.

## Security and operations

Queries use bound parameters. The Compose password is development-only; deploy with restricted roles, managed credentials, network policy, TLS and backups. Monitor database pool wait, transaction latency, lock contention, uncommitted Kafka lag and duplicate/conflict counts before claiming an operational SLO.

## Testing implications

Unit tests exercise duplicate handling, conflicting identities and concurrent memory-store writes. Tagged integration tests use real PostgreSQL to verify replay across a new calculator instance, baseline continuity, duplicate aggregate writes, SQL-shaped input and unique-key conflict behavior. The separate real Kafka tests continue to verify producer and consumer boundaries.

## Reconsider when

- Event throughput or write retention warrants partitioning or archival.
- A service-owned outbox becomes necessary for asynchronous downstream events.
- Tenant ownership changes the idempotency key or state key.
- The hot-key lock materially limits throughput in a measured workload.
