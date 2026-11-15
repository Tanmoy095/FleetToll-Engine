# FleetToll Engine

FleetToll Engine is a Go backend portfolio project for reliable fleet telemetry processing. It accepts vehicle GPS points, validates them, buffers them through Kafka, calculates per-vehicle distance, and exposes an aggregated invoice view.

I am developing FleetToll to explore a practical logistics and fleet-finance problem: operators need a consistent, auditable way to turn noisy vehicle telemetry into distance and eventually toll charges. Duplicate events, service restarts, and network failures can otherwise lose or overstate business records. The current system demonstrates a durable replay-safe distance slice; it is not yet a production tolling or billing platform.

## Current status

| Capability | Status | Evidence / limits |
|---|---|---|
| WebSocket GPS ingestion | Implemented | Bounded frames, strict JSON, coordinate validation, isolated connections; devices are not authenticated |
| Kafka telemetry publishing | Implemented | OBU-keyed records, idempotent producer configuration; real-broker integration test |
| Distance calculation | Implemented | Haversine kilometers and per-OBU baseline; volatile fallback remains available when the database is unset |
| Durable replay-safe processing | Implemented in PostgreSQL mode | Inbox result and trip baseline commit in one transaction; real PostgreSQL tests cover replay after a new service instance |
| Distance aggregation | Implemented | PostgreSQL event ledger has a unique event ID and sums records for the prototype invoice; duplicate events do not add twice |
| Toll zones and pricing | Planned | PostGIS geometry, tariff versioning and toll assessment are not implemented |
| Identity and tenant authorization | Planned | OBU ID is currently client-supplied; APIs do not authenticate callers |
| DLQ and operator replay | Planned | A poison Kafka record can still block its partition |
| Tracing, metrics and dashboards | Planned | Not implemented |
| Large-scale workload | In progress | A reproducible 1,000-request local API probe is included; it is not a 1,000-WebSocket test or production capacity claim |

## Architecture

```mermaid
flowchart LR
  Device[Vehicle device / simulator] -->|WebSocket JSON| Receiver[Receiver]
  Receiver -->|Kafka record keyed by OBU| Kafka[(Kafka)]
  Kafka -->|topic:partition:offset| Calculator[Distance calculator]
  Calculator -->|inbox + trip state transaction| DB[(PostgreSQL)]
  Calculator -->|HTTP distance + event ID| Aggregator[Aggregator]
  Aggregator -->|unique distance ledger| DB
  Aggregator --> Invoice[GET /invoice]
  Calculator -->|commit after HTTP success| Kafka
```

### One telemetry event, end to end

1. A device sends a JSON GPS point to `ws://localhost:30000/ws`.
2. The receiver checks the OBU identifier and coordinate ranges, caps each frame at 4 KiB, and publishes to Kafka with the OBU ID as key. Kafka keeps a key's records in one partition; it does not provide global ordering.
3. The consumer forms a stable event identity from the Kafka topic, partition and offset.
4. With `DATABASE_URL` configured, the calculator starts a PostgreSQL transaction, serializes updates for that OBU using a transaction-scoped advisory lock, checks the processed-event inbox, calculates Haversine distance from the prior point, and commits the result and next point together.
5. A replay returns the distance saved for that event and does not advance the vehicle baseline again.
6. The calculator sends the event and distance to `POST /aggregate`. The aggregator inserts into a ledger with a unique event ID. Same-ID/same-data delivery is a no-op; same-ID/different-data delivery is rejected.
7. The consumer commits the Kafka offset only after the aggregator returns success. If a response is lost, redelivery reuses the inbox result and the aggregate ledger deduplicates it.

This is at-least-once delivery with idempotent database effects. Kafka, PostgreSQL and HTTP do not share one distributed transaction, so the project does **not** claim end-to-end exactly-once processing.

## Business and technical scope

The business direction is a multi-tenant fleet tolling and revenue-assurance platform. The first engineering slice is intentionally smaller: establish a reliable telemetry-to-distance path before adding financial rules. The current invoice endpoint is a prototype total (`10 × distance`) and uses floating point; it is not a real tariff, currency calculation or immutable financial invoice.

HTTP is used for the current request/response boundary because the contract is small and easy to inspect and test. Kafka buffers telemetry and allows replay across service failures. PostgreSQL is the system of record for trip baselines, processed-event identity and aggregate distance when durable mode is enabled. gRPC may make sense for future typed internal APIs or streaming, but it is not currently implemented. PostGIS is deferred until toll-zone geometry and spatial queries exist.

## Repository layout

```text
aggregator/                 HTTP API, invoice service, memory and PostgreSQL stores
data_receiver/              WebSocket endpoint and Kafka producer
distance_calculator/        Kafka consumer, Haversine calculation, PostgreSQL inbox/state
internal/postgres/          Connection pool setup and embedded schema migrations
scripts/                    Reproducible local API load probe
types/                      Telemetry, distance and invoice contracts
docs/architecture/          HLD, LLD and accepted architecture decisions
docs/learning/              Implementation guides and hands-on learning path
docs/security/              Local security reviews and remaining risks
```

## Run locally

Requirements: Go 1.21+, Docker Compose and a local network that permits loopback tests. Compose starts Kafka/Zookeeper and PostgreSQL; the Go services run in separate terminals.

```bash
docker compose up -d broker postgres
```

PostgreSQL is bound to `127.0.0.1:15432` so it does not collide with a local server on port 5432. The Compose credentials are development-only.

For durable mode, export the same DSN in the calculator and aggregator terminals:

```bash
export DATABASE_URL='postgres://fleettoll:fleetdev@localhost:15432/fleettoll?sslmode=disable'
```

Then start each service:

```bash
go run ./aggregator -listenaddr :3000
```

```bash
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=obudata go run ./distance_calculator
```

```bash
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=obudata go run ./data_receiver
```

The services read environment variables directly and do not load `.env` files. If `DATABASE_URL` is unset, the calculator and aggregator explicitly use volatile in-memory state; restart-safe behavior then does not apply.

Example telemetry frame:

```json
{"obuID":12345,"lat":23.7806,"long":90.4074}
```

After two valid points for the same OBU, query the prototype invoice:

```bash
curl "http://127.0.0.1:3000/invoice?obu=12345"
```

The first point initializes a baseline and contributes zero kilometers. Later points contribute great-circle distance from the preceding accepted point.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Receiver and calculator broker |
| `KAFKA_TOPIC` | `obudata` | Telemetry topic |
| `KAFKA_CONSUMER_GROUP` | `fleettoll-distance-calculator` | Calculator consumer group |
| `DATABASE_URL` | unset | Enables durable calculator and aggregator PostgreSQL mode |
| `KAFKA_TEST_BROKERS` | `localhost:9092` | Real Kafka integration tests |
| `DATABASE_TEST_URL` | Compose PostgreSQL DSN | Real PostgreSQL integration tests |

## Test and verify

```bash
GOCACHE=/tmp/fleettoll-go-cache go test ./...
GOCACHE=/tmp/fleettoll-go-cache go test -race ./...
GOCACHE=/tmp/fleettoll-go-cache go vet ./...
GOCACHE=/tmp/fleettoll-go-cache make integration-kafka
GOCACHE=/tmp/fleettoll-go-cache make integration-postgres
```

The integration targets require the local Compose dependencies. The PostgreSQL tests exercise actual SQL and transaction behavior, not an in-memory substitute.

### Latest verification

On 2026-11-15, all of these completed successfully on the development machine:

- `GOCACHE=/tmp/fleettoll-go-cache go test ./...` — all Go packages passed.
- `GOCACHE=/tmp/fleettoll-go-cache go test -race ./...` — all packages passed; macOS emitted non-fatal linker `LC_DYSYMTAB` warnings for test binaries.
- `GOCACHE=/tmp/fleettoll-go-cache go vet ./...` — passed.
- `make integration-postgres` — replay, persisted baseline, parallel duplicate writes, event-ID conflict and SQL-shaped input passed against PostgreSQL 15.
- `KAFKA_TEST_BROKERS=localhost:9092 make integration-kafka` — real Kafka producer and consumer integration passed.
- Local 1,000-request API probe — final run completed with zero errors; latency and throughput are reported below and in the performance note.

These checks verify the feature in a local development environment; they are not external penetration-test evidence, a 1,000-WebSocket run, or a production availability/capacity claim.

### Local concurrent API probe

With the aggregator running in PostgreSQL mode:

```bash
GOCACHE=/tmp/fleettoll-go-cache go run ./scripts/aggregate_load.go -users 1000
```

The probe sends one request from each of 1,000 concurrent goroutines and reports throughput, p50/p95/p99 latency and errors. Record the machine, database mode and command when comparing results. This is a local development measurement, not a service-level objective or a WebSocket capacity result.

On 2026-11-15, the final PostgreSQL-backed local run accepted 1,000/1,000 requests at 1,911.2 requests/second with p50 278 ms, p95 456 ms, p99 518 ms, and zero errors. An initial version timed out 53 requests; removing explicit transaction round trips from the one-row insert path let later runs complete without errors. Results varied significantly between runs, so this is a local one-shot observation, not a capacity guarantee. Full results are in [`docs/performance/AGGREGATE_API_LOCAL_LOAD.md`](docs/performance/AGGREGATE_API_LOCAL_LOAD.md).

## Engineering decisions and learning

The current design and trade-offs are documented in [`docs/architecture/HLD.md`](docs/architecture/HLD.md), [`docs/architecture/LLD.md`](docs/architecture/LLD.md), and the ADRs. The inbox/transaction walkthrough is in [`docs/learning/features/07-postgres-replay-safety.md`](docs/learning/features/07-postgres-replay-safety.md). Test evidence and open findings live under [`docs/security/`](docs/security/) and [`docs/IMPLEMENTATION_ROADMAP.md`](docs/IMPLEMENTATION_ROADMAP.md).

Key concepts to be ready to explain:

- Kafka producer idempotence handles producer retries; it does not by itself deduplicate a business event after consumer replay.
- The inbox row and per-OBU baseline must commit in one PostgreSQL transaction or replay can lose or double-count distance.
- A unique event key makes the aggregate write safe to repeat after an ambiguous HTTP response.
- An advisory lock serializes updates for one OBU while allowing different OBUs to proceed concurrently.
- The database transaction and Kafka offset commit are separate, so the honest guarantee is at-least-once delivery plus idempotent effects.

## Roadmap

1. Add authenticated device identity and tenant-scoped fleet/vehicle ownership.
2. Add bounded retry, poison-event handling, a DLQ and operator-controlled replay.
3. Add PostGIS toll zones, geofence semantics and effective-dated pricing.
4. Replace prototype amounts with an integer-minor-unit ledger and immutable invoices; add statement reconciliation.
5. Add OpenTelemetry traces, bounded-cardinality metrics, dashboards, SLOs and alerts.
6. Run failure-injection, 1,000 concurrent API users, 1,000 WebSocket devices and combined load where the development environment permits; profile measured bottlenecks.

The roadmap is a plan, not a completion claim. Each item needs implementation evidence, real integration checks and a security review before its status changes.
