# Feature 07: PostgreSQL inbox and replay-safe distance aggregation

## Business problem

Kafka can redeliver an event when an RPC response is lost, the process exits before committing the offset, or a consumer group rebalances. If the calculator updates a volatile previous point before the RPC, a replay may produce zero distance instead of the original movement. If the aggregator accepted the request but the response disappeared, adding the same distance again overstates the invoice total.

## Implemented path

The consumer constructs `eventID = topic:partition:offset` in `distance_calculator/consumer.go`. A Kafka offset is stable for the lifetime of its retained record, so it identifies a delivery independently of process restarts. `types.Distance` carries tenant ID and event identity through the protobuf/gRPC client.

At startup `internal/postgres.Open` applies versioned embedded SQL migrations in transactions and stores the applied filenames in `schema_migrations`. A PostgreSQL advisory lock serializes migration startup across the two local services. Migration 001 creates the original tables; migration 002 creates/seeds the tenant registry, backfills existing rows to tenant 1, and changes primary keys/indexes to tenant-aware keys. When configured with `DATABASE_URL`, `PostgresCalculator.CalculateEvent` in `distance_calculator/postgres_service.go`:

1. Validates tenant ID, OBU ID and coordinates.
2. Starts a PostgreSQL read-committed transaction.
3. Acquires a tenant-qualified `pg_advisory_xact_lock(hashtextextended(tenantID:OBUID))` so concurrent workers for one tenant/OBU serialize without unnecessarily locking another tenant’s same-numbered OBU.
4. Checks `processed_telemetry` for this `(tenant_id,event_id)`. If present, it returns the saved distance and commits without changing the current baseline.
5. Otherwise reads `trip_state`, calculates Haversine distance, upserts the coordinate, inserts the processed event and commits both changes together.

The aggregator inserts the result into `aggregated_distance_events`, keyed by `(tenant_id,event_id)`. The unique constraint makes a replay a no-op. If the same tenant/event ID is reused for a different OBU, distance or timestamp, the store returns `ErrEventIDConflict` instead of silently suppressing potentially valid data. Invoice totals are `SUM(distance_km)` over the accepted ledger rows.

## Why the transaction matters

The state row and inbox result describe one business operation. If only the coordinate commits, a later replay could calculate a different distance. If only the inbox row commits, a later event may use an old coordinate. Committing both in one PostgreSQL transaction makes the event result and next state move together.

The advisory lock covers the read/modify/write sequence for one tenant/OBU, even when its `trip_state` row does not yet exist. Different tenant/OBU keys can process concurrently. This is a deliberate simple coordination mechanism; a highly hot OBU key is still serialized.

## Failure cases

| Failure point | Result |
|---|---|
| PostgreSQL unavailable before transaction | Consumer returns an error and does not commit Kafka; the event can be retried |
| Transaction rolls back before commit | Neither inbox result nor next point persists; retry recalculates from the previous committed point |
| PostgreSQL commits, then gRPC fails | Replay reads the stored distance and retries the aggregate request |
| Aggregator commits, gRPC response is lost | Replay sends the same event ID; unique ledger row prevents double addition |
| Aggregator rejects conflicting event-ID reuse | Consumer does not commit; operator intervention is needed because DLQ is not implemented |
| `DATABASE_URL` unset | In-memory fallback is used; restart/replay guarantees do not apply |

Kafka offset commit and the PostgreSQL/gRPC operations still do not share a distributed transaction. The guarantee is at-least-once delivery with idempotent database effects.

## How to verify

```bash
docker compose up -d postgres
GOCACHE=/tmp/fleettoll-go-cache make integration-postgres
GOCACHE=/tmp/fleettoll-go-cache make integration-kafka
```

The PostgreSQL suite creates real rows and checks replay through a second calculator instance, state continuity on the following event, duplicate ledger insertion, conflict handling and SQL-shaped event IDs. The Kafka suite tests real broker publication and consumption.

## Security notes

SQL uses placeholders rather than concatenating values. Event IDs are bounded to 512 bytes at the domain boundary. Transport boundaries now use prototype bearer credentials and tenant-scoped storage. The shared gRPC token has broad cross-tenant authority, credentials lack rotation/revocation, TLS and rate limiting, and managed identity remain open. See `docs/security/TENANT_ISOLATION_REVIEW.md`.

## Interview explanation

“Kafka gives us at-least-once delivery because the offset is committed after the downstream RPC. I use topic, partition and offset as a stable inbox key, scoped by tenant. One PostgreSQL transaction serializes a tenant/vehicle state update and stores the resulting distance. The aggregator uses a unique tenant/event ledger, so an ambiguous RPC replay cannot add the same distance twice. These are idempotent effects across separate systems, not a distributed exactly-once transaction.”

## Practice task for the learner

Draw a timeline for “database commit succeeds, gRPC response is lost, process restarts.” For each step identify the durable row, the next retry behavior, and whether Kafka's committed offset moves. Then add a PostgreSQL test proving two tenants can reuse an OBU and event ID while retaining independent baselines and totals. Add a second test for missing tenant provisioning and explain the foreign-key failure. Do not implement the change until you can explain how the existing transaction behaves.
