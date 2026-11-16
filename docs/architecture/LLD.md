# FleetToll Engine low-level design

**Status:** documents the current Go implementation as of 2026-11-17, including prototype bearer authentication and tenant-scoped PostgreSQL persistence when `DATABASE_URL` is set.

## Package map

| Package/path | Important types | Role |
|---|---|---|
| `types` | `OBUData`, `Distance`, `Invoice` | JSON contracts shared by current services |
| `data_receiver` | `DataReceiver`, `DataProducer`, `KafkaProducer` | Decode and validate WebSocket frames; publish telemetry |
| `distance_calculator` | `CalculatorServicer`, `CalculatorService`, `PostgresCalculator`, `kafkaConsumer`, `kafkaMessageReader` | Calculate distance, persist trip baseline and inbox when configured, poll/commit Kafka |
| `distance_calculator/client` | `GRPCClient` | Send tenant-qualified distance through authenticated protobuf/gRPC |
| `aggregator` | `Aggregator`, `InvoiceAggregator`, `MemoryStore`, `PostgresStore` | Validate business input, idempotently persist totals, serve invoice queries |

The module path is `github.com/Tanmoy095/FleetToll-Engine`. The application uses composition and small interfaces at the receiver producer and Kafka consumer seams so unit tests can verify behavior without replacing real infrastructure integration tests.

## Current message contracts

Telemetry event:

```json
{"obuID":12345,"lat":23.7806,"long":90.4074}
```

Internal distance event carries `tenantID`, `eventID`, `value`, `obuID` and `unix`; gRPC is the calculator path. `/aggregate` accepts the same strict JSON shape behind the shared service bearer token. Telemetry frames omit tenant identity: the receiver sets it from the device credential.

Invoice query: `GET /invoice?obu=12345` returns `obuID`, `totalDistance`, and `totalAmount`. Distance events include Kafka identity for replay safety. Tenant IDs are assigned from configured bearer credentials; a managed device identity service and telemetry schema version are still absent. Invoices lack currency and financial-ledger semantics. These are prototype contracts, not finalized public APIs.

## Receiver implementation

`newWebSocketHandler` wires `/ws` to `DataReceiver.handleWS`. Each HTTP request owns one WebSocket connection and reads text frames sequentially. `SetReadLimit` caps the payload at 4 KiB. `decodeOBUData` uses pointer fields to distinguish missing values from valid zeroes, `DisallowUnknownFields`, and a second decode requiring EOF. `validateOBUData` rejects nonpositive IDs and invalid/non-finite coordinates before producer use.

`KafkaProducer.ProduceData` JSON-encodes the tenant-qualified point, sets `Key` to `tenantID:OBUID`, enqueues one Kafka message, and waits on a delivery channel with a 15-second timer. Producer idempotence and `acks=all` strengthen delivery behavior; they do not provide event-level deduplication.

## Distance consumer implementation

Without `DATABASE_URL`, `CalculatorService` stores `map[int]coordinate` behind one `sync.Mutex`; this is explicitly volatile local mode. In PostgreSQL mode the consumer detects `EventCalculator` and calls `PostgresCalculator.CalculateEvent(ctx, eventID, data)`. Its transaction acquires `pg_advisory_xact_lock(hashtextextended(tenantID:OBUID))`, checks `processed_telemetry`, reads `trip_state`, calculates using mean Earth radius `6371.0088` km, and writes the event result and next coordinate together. A duplicate event ID returns the saved distance.

`kafkaMessageReader` abstracts only `ReadMessage`, `CommitMessage`, `SubscribeTopics`, and `Close`. `processMessage` derives `eventID` from Kafka topic, partition and offset so a redelivery has a stable key. Production passes Confluent's Kafka consumer. `NewKafkaConsumer` reads brokers and consumer group from environment, turns auto-commit off, subscribes, and uses earliest reset for a new group. `Start(ctx)` owns cleanup. `run(ctx)` polls for up to 250 ms, calls `processMessage`, then commits only after the aggregator reports success. It stops on observed processing or commit failures.

This serial order is intentional for the current small consumer: later records are not processed after an earlier failure. Parallel processing would need per-partition sequencing and careful offset tracking so a later completed record cannot advance a partition past an earlier unfinished one.

## Aggregator HTTP and storage

`newHTTPHandler` mounts `POST /aggregate` and `GET /invoice`. The POST boundary checks method, JSON media type, a 1 MiB maximum body, one strict JSON value, and positive OBU ID / nonnegative finite distance / positive timestamp. The invoice route requires a tenant bearer credential, exactly one positive `obu` parameter and maps `ErrInvoiceNotFound` to 404. `/aggregate` requires the internal service bearer token. The receiver derives tenant identity from the device token and overwrites no client-provided tenant field (unknown fields are rejected).

`InvoiceAggregator` validates event ID (nonblank, at most 512 bytes), positive OBU ID, finite nonnegative distance and positive timestamp. `MemoryStore` is the volatile fallback and keys totals/events by tenant plus OBU/event. `PostgresStore` inserts into `aggregated_distance_events` with `ON CONFLICT (tenant_id, event_id) DO NOTHING`, then checks an existing identity has the same OBU, distance and timestamp; a mismatch returns `ErrEventIDConflict`. Invoice distance is `SUM(distance_km)` filtered by tenant and OBU. Composite tenant-first keys and indexes support scoped lookups. Amount calculation uses `float64` prototype pricing. Migration 002 backfills old rows to tenant 1 and replaces event/OBU primary keys with tenant-composite keys.

## Internal protobuf/gRPC boundary

`api/fleettoll/v1/aggregator.proto` defines `AggregatorService.AggregateDistance` and `GetInvoice`. The Kafka consumer sends `tenant_id`, `event_id`, `obu_id` (`int64`, matching PostgreSQL `BIGINT` capacity), `distance_km` and `event_unix_nano`; the gRPC adapter checks that the OBU ID fits the current Go `int` and maps fields into the existing `types.Distance`. It delegates to the same `Aggregator` interface used by HTTP and does not duplicate business or persistence logic.

The adapter rejects invalid identifiers, non-positive OBU IDs, negative/non-finite distance and non-positive timestamps with `InvalidArgument`. Event identity conflicts map to `AlreadyExists`, absent invoice to `NotFound`, and storage failures to sanitized `Internal`. The gRPC server caps inbound message size at 1 MiB and requires a shared service bearer token. It binds to `127.0.0.1:3001` by default, while `-grpc-listenaddr` can override the address. Transport is still plaintext; keep it on loopback until TLS/mTLS is implemented.

Generated bindings are committed and reproducible using the pinned plugin versions in `Makefile`. Field numbers are stable wire identifiers: never renumber or reuse them; reserve deleted fields and names. Adding fields is usually wire-compatible, but changing application assumptions still requires compatibility tests. The RPC handler receives a context, but the current domain/store interfaces do not accept one, so cancellation cannot interrupt an in-flight database call yet. The shared internal service token authorizes cross-tenant aggregation; tenant IDs must be generated only by this trusted processor, not accepted directly from untrusted callers.

## Error and cancellation flow

The internal gRPC client attaches a service bearer token and sends tenant identity as a typed protobuf field. HTTP invoice reads use tenant credentials; database lookups remain tenant-scoped. A non-200 response becomes an error. The consumer wraps errors with topic/partition/offset context and exits. SIGINT/SIGTERM cancellation is converted into context cancellation by `signal.NotifyContext`; the consumer's bounded poll observes it and closes the Kafka client.

Current edge behavior:

- Broker poll timeout is expected and retries the loop.
- Other broker read errors stop the process.
- Malformed JSON and invalid telemetry stop without offset commit; there is no DLQ yet.
- HTTP failure stops without offset commit. PostgreSQL mode replays the saved distance; the aggregate ledger handles an ambiguous HTTP outcome idempotently.
- HTTP success followed by a commit error can cause replay; event identity lets both durable stores detect the repeat.
- Cancellation can cancel an in-flight HTTP request; the event remains uncommitted, and the request outcome may be ambiguous.

## Implemented relational schema and future model boundary

`internal/postgres.Open` applies embedded migration files in one transaction each, serializes concurrent startup with a transaction-scoped advisory lock, and records applied filenames in `schema_migrations`. `internal/postgres/migrations/001_distance_events.sql` creates:

- `trip_state` keyed by tenant ID and OBU ID, with validated latitude and longitude.
- `processed_telemetry` keyed by tenant ID and event ID, storing its calculated distance.
- `aggregated_distance_events` keyed by tenant ID and event ID, storing the distance ledger row.
- A B-tree index on `aggregated_distance_events(obu_id)` for invoice aggregation.

The two services share this schema in the local demo. Migration 002 adds a tenant registry, backfills existing rows into tenant 1, changes primary keys to `(tenant_id, obu_id)` or `(tenant_id, event_id)`, and adds tenant-first indexes. It has no retention policy, partitions, financial amount columns or immutable invoice records. Before expanding the relational model, tenant ownership and authenticated device identity need to be designed.

- `tenants` and tenant-scoped `fleets`/`vehicles`, with unique constraints matching business identity.
- Durable `telemetry_events`/inbox identity so replay can be detected.
- A `trip_state` row keyed to the server-identified vehicle, updated transactionally with accepted event sequence and location/time policy.
- Toll-zone geometry in PostGIS, with SRID and polygon validity rules, only when geofence behavior is implemented.
- Immutable ledger/invoice rows using integer minor units and currency, with reversal/adjustment semantics.

These are design requirements for future ADRs and migrations; they are not current tables or guarantees.
