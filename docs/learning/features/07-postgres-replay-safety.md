# Feature 07: PostgreSQL inbox and replay-safe distance aggregation

## Business problem

Kafka can redeliver an event when an HTTP response is lost, the process exits before committing the offset, or a consumer group rebalances. If the calculator updates a volatile previous point before the HTTP call, a replay may produce zero distance instead of the original movement. If the aggregator accepted the request but the response disappeared, adding the same distance again overstates the invoice total.

## Implemented path

The consumer constructs `eventID = topic:partition:offset` in `distance_calculator/consumer.go`. A Kafka offset is stable for the lifetime of its retained record, so it identifies a delivery independently of process restarts. `types.Distance` carries that value through the JSON client.

At startup `internal/postgres.Open` applies versioned embedded SQL migrations in transactions and stores the applied filenames in `schema_migrations`. A PostgreSQL advisory lock serializes migration startup across the two local services. When configured with `DATABASE_URL`, `PostgresCalculator.CalculateEvent` in `distance_calculator/postgres_service.go`:

1. Validates the OBU and coordinates.
2. Starts a PostgreSQL read-committed transaction.
3. Acquires `pg_advisory_xact_lock(OBUID)` so two workers cannot advance one OBU state concurrently.
4. Checks `processed_telemetry` for this event ID. If present, it returns the saved distance and commits without changing the current baseline.
5. Otherwise reads `trip_state`, calculates Haversine distance, upserts the coordinate, inserts the processed event and commits both changes together.

The aggregator inserts the result into `aggregated_distance_events`, keyed by event ID. The unique constraint makes a replay a no-op. If the same event ID is reused for a different OBU or distance, the store returns `ErrEventIDConflict` instead of silently suppressing potentially valid data. Invoice totals are `SUM(distance_km)` over the accepted ledger rows.

## Why the transaction matters

The state row and inbox result describe one business operation. If only the coordinate commits, a later replay could calculate a different distance. If only the inbox row commits, a later event may use an old coordinate. Committing both in one PostgreSQL transaction makes the event result and next state move together.

The advisory lock covers the read/modify/write sequence for one OBU, even when its `trip_state` row does not yet exist. Different OBU keys can process concurrently. This is a deliberate simple coordination mechanism; a highly hot OBU key is still serialized.

## Failure cases

| Failure point | Result |
|---|---|
| PostgreSQL unavailable before transaction | Consumer returns an error and does not commit Kafka; the event can be retried |
| Transaction rolls back before commit | Neither inbox result nor next point persists; retry recalculates from the previous committed point |
| PostgreSQL commits, then HTTP fails | Replay reads the stored distance and retries the aggregate request |
| Aggregator commits, HTTP response is lost | Replay sends the same event ID; unique ledger row prevents double addition |
| Aggregator rejects conflicting event-ID reuse | Consumer does not commit; operator intervention is needed because DLQ is not implemented |
| `DATABASE_URL` unset | In-memory fallback is used; restart/replay guarantees do not apply |

Kafka offset commit and the PostgreSQL/HTTP operations still do not share a distributed transaction. The guarantee is at-least-once delivery with idempotent database effects.

## How to verify

```bash
docker compose up -d postgres
GOCACHE=/tmp/fleettoll-go-cache make integration-postgres
GOCACHE=/tmp/fleettoll-go-cache make integration-kafka
```

The PostgreSQL suite creates real rows and checks replay through a second calculator instance, state continuity on the following event, duplicate ledger insertion, conflict handling and SQL-shaped event IDs. The Kafka suite tests real broker publication and consumption.

## Security notes

SQL uses placeholders rather than concatenating values. Event IDs are bounded to 512 bytes at the HTTP/domain boundary. This does not authenticate the HTTP caller: a reachable caller can still submit distance records, request invoices by OBU ID, or attempt to preempt an event key. Authentication, server-owned vehicle identity, rate limiting and tenant-scoped authorization remain open work.

## Interview explanation

“Kafka gives us at-least-once delivery because the offset is committed after the downstream request. I use the record's topic, partition and offset as a stable inbox key. In one PostgreSQL transaction, I serialize that vehicle's state update, return the original result for duplicates, and atomically persist a new result with the next coordinate. The aggregator has a unique event ledger, so an ambiguous HTTP retry cannot add the same distance twice. This is not a distributed exactly-once transaction; it is replay-safe idempotent effects around an at-least-once source.”

## Practice task for the learner

Draw a timeline for “database commit succeeds, HTTP response is lost, process restarts.” For each step identify the durable row, the next retry behavior, and whether Kafka's committed offset moves. Then change the PostgreSQL test to inject a cancellation immediately before commit and prove that both the inbox row and trip baseline roll back. Do not implement the change until you can explain how the existing transaction behaves.
