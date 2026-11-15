# FleetToll Engine low-level design

**Status:** documents the present Go implementation. Future database and auth contracts are not specified here as if they already existed.

## Package map

| Package/path | Important types | Role |
|---|---|---|
| `types` | `OBUData`, `Distance`, `Invoice` | JSON contracts shared by current services |
| `data_receiver` | `DataReceiver`, `DataProducer`, `KafkaProducer` | Decode and validate WebSocket frames; publish telemetry |
| `distance_calculator` | `CalculatorServicer`, `CalculatorService`, `kafkaConsumer`, `kafkaMessageReader` | Track process-local OBU points, calculate distances, poll/commit Kafka |
| `distance_calculator/client` | `Client` | Send distance JSON to the aggregator with context and timeout |
| `aggregator` | `Aggregator`, `InvoiceAggregator`, `MemoryStore` | Validate business input, accumulate totals, serve invoice queries |

The module path is `github.com/Tanmoy095/FleetToll-Engine`. The application uses composition and small interfaces at the receiver producer and Kafka consumer seams so unit tests can verify behavior without replacing real infrastructure integration tests.

## Current message contracts

Telemetry event:

```json
{"obuID":12345,"lat":23.7806,"long":90.4074}
```

Distance event sent to `POST /aggregate`:

```json
{"value":1.25,"obuID":12345,"unix":1794772800000000000}
```

Invoice query: `GET /invoice?obu=12345` returns `obuID`, `totalDistance`, and `totalAmount`. These contracts lack immutable event IDs, tenant IDs, currency, schema versioning and authenticated identity; they are prototype contracts, not finalized public APIs.

## Receiver implementation

`newWebSocketHandler` wires `/ws` to `DataReceiver.handleWS`. Each HTTP request owns one WebSocket connection and reads text frames sequentially. `SetReadLimit` caps the payload at 4 KiB. `decodeOBUData` uses pointer fields to distinguish missing values from valid zeroes, `DisallowUnknownFields`, and a second decode requiring EOF. `validateOBUData` rejects nonpositive IDs and invalid/non-finite coordinates before producer use.

`KafkaProducer.ProduceData` JSON-encodes the point, sets `Key` to decimal OBU ID, enqueues one Kafka message, and waits on a delivery channel with a 15-second timer. Producer idempotence and `acks=all` strengthen delivery behavior; they do not provide event-level deduplication.

## Distance consumer implementation

`CalculatorService` stores `map[int]coordinate` behind one `sync.Mutex`. A single lock covers lookup and update so concurrent calculation calls cannot interleave a check/update pair. It validates before locking and applies the Haversine formula using mean Earth radius `6371.0088` km.

`kafkaMessageReader` abstracts only `ReadMessage`, `CommitMessage`, `SubscribeTopics`, and `Close`. Production passes Confluent's Kafka consumer. `NewKafkaConsumer` reads brokers and consumer group from environment, turns auto-commit off, subscribes, and uses earliest reset for a new group. `Start(ctx)` owns cleanup. `run(ctx)` polls for up to 250 ms, calls `processMessage`, then commits only after the aggregator reports success. It stops on observed processing or commit failures.

This serial order is intentional for the current small consumer: later records are not processed after an earlier failure. Parallel processing would need per-partition sequencing and careful offset tracking so a later completed record cannot advance a partition past an earlier unfinished one.

## Aggregator HTTP and storage

`newHTTPHandler` mounts `POST /aggregate` and `GET /invoice`. The POST boundary checks method, JSON media type, a 1 MiB maximum body, one strict JSON value, and positive OBU ID / nonnegative finite distance / positive timestamp. The invoice route requires exactly one positive `obu` parameter and maps `ErrInvoiceNotFound` to 404.

`InvoiceAggregator` is the service boundary and `MemoryStore` holds totals protected by an `RWMutex`. The write path is process-local and not durable, transactional, tenant-scoped, or idempotent. Amount calculation uses `float64` and prototype pricing.

## Error and cancellation flow

The distance client uses `http.NewRequestWithContext`, a five-second default `http.Client` timeout, and closes every response body. A non-200 response becomes an error. The consumer wraps errors with topic/partition/offset context and exits. SIGINT/SIGTERM cancellation is converted into context cancellation by `signal.NotifyContext`; the consumer's bounded poll observes it and closes the Kafka client.

Current edge behavior:

- Broker poll timeout is expected and retries the loop.
- Other broker read errors stop the process.
- Malformed JSON and invalid telemetry stop without offset commit; there is no DLQ yet.
- HTTP failure stops without offset commit, but in-memory calculation state has already advanced.
- HTTP success followed by a commit error can cause replay; the aggregator cannot detect a duplicate.
- Cancellation can cancel an in-flight HTTP request; the event remains uncommitted, and the request outcome may be ambiguous.

## Future relational model boundary

No SQL schema is implemented. Before adding tables, the design must settle tenant key ownership and device identity. A durable model will need at minimum:

- `tenants` and tenant-scoped `fleets`/`vehicles`, with unique constraints matching business identity.
- Durable `telemetry_events`/inbox identity so replay can be detected.
- A `trip_state` row keyed to the server-identified vehicle, updated transactionally with accepted event sequence and location/time policy.
- Toll-zone geometry in PostGIS, with SRID and polygon validity rules, only when geofence behavior is implemented.
- Immutable ledger/invoice rows using integer minor units and currency, with reversal/adjustment semantics.

These are design requirements for future ADRs and migrations; they are not current tables or guarantees.
