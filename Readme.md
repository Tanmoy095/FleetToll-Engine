# FleetToll Engine

This repository is a production-oriented proof-of-concept for a truck toll calculation pipeline implemented in Go. It's written as a microservice suite demonstrating event-driven streaming (WebSocket -> Kafka -> consumer -> aggregator) with clear separation of concerns and straightforward control flow — suitable for a live demo and a technical interview walkthrough.

## Project overview (diagram)

![Project Overview](https://private-user-images.githubusercontent.com/96974600/298181962-18fa76f9-5a29-4b81-a333-5f0c8ee360f7.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NzAzNjc5NTIsIm5iZiI6MTc3MDM2NzY1MiwicGF0aCI6Ii85Njk3NDYwMC8yOTgxODE5NjItMThmYTc2ZjktNWEyOS00YjgxLWEzMzMtNWYwYzhlZTM2MGY3LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNjAyMDYlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjYwMjA2VDA4NDczMlomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTQ4N2YyMThiMjJjNjZmNTVjMGMxNjJhMzAxYWM1YjQ4ZjE0YWM0YmEwODExODBhODgxMTU1NGUwNDk0MTYwMzQmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0._Yu6rELU93bQ29u8RSk-ZqQdx8wVRIduFd8Yg_MKd6Q)

> The diagram shows OBUs connecting via WebSocket to `data_receiver`, messages flowing into Kafka, processed by `distance_calculator`, and aggregated by `aggregator` (with an isolated `invoice_calculator` service and DB).

---

## Table of contents

- Overview
- Architecture & Data Flow
- Components (what each service does)
- API specification & example payloads
- Environment variables (recommended)
- Local development & run steps
- Example `docker-compose` (local Kafka + Zookeeper)
- Testing & validation
- Design trade-offs, limitations & next steps
- Files to review (quick map)

---

## Overview

Core idea: OBUs (on-board units) send periodic GPS points via WebSocket. Points are published to Kafka. A distance-calculator consumer reads the stream, computes incremental distances, and forwards distances to an aggregator service which accumulates and produces invoices.

This demo prioritizes correctness and understandability. Production hardening (persistence, metrics, auth, and stronger delivery semantics) is described below as immediate next steps.

---

## Architecture & Data Flow

1. OBU (WebSocket client) -> connects to `data_receiver` WebSocket (`/ws`) and sends JSON `OBUData`.
2. `data_receiver` publishes received messages to Kafka topic `obudata`.
3. `distance_calculator` consumes `obudata`, computes the Euclidean distance between consecutive points for the same `obuID`, and forwards `Distance` payloads to the `aggregator` by HTTP POST to `/aggregate`.
4. `aggregator` stores the distances (current implementation: in-memory) and exposes `/invoice?obu=<id>` which returns an `Invoice` (simple pricing = basePrice \* totalDistance).

Notes:

- Kafka provides reliable buffering and replay for reprocessing.
- Replace Euclidean with Haversine for accurate geodesic distances when using real lat/long data.

---

## Components

- data_receiver: WebSocket server that accepts `OBUData` and produces to Kafka.
- distance_calculator: Kafka consumer that calculates distances and forwards them to aggregator.
- aggregator: HTTP API that aggregates distances and exposes invoices.
- obu: simulator that connects to the `data_receiver` WebSocket and streams synthetic points for demo/testing.
- types: shared type definitions used across services (`OBUData`, `Distance`, `Invoice`).

---

## Detailed flow (main.go simulation & service responsibilities)

In `obu/main.go` (and referenced `main.go` across services) we simulate an OBU: a small client that sits in a truck and sends GPS coordinates at a fixed interval via WebSocket. The primary flow implemented in code is:

- OBU (simulator) sends JSON GPS messages to `data_receiver` over WebSocket.
- `data_receiver` receives messages and produces them to Kafka (`obudata` topic).
- `distance_calculator` consumes OBU messages from Kafka, computes incremental distances (distance between the last point and the current point for each `obuID`) and POSTs `Distance` objects to `aggregator` (`/aggregate`).
- `aggregator` aggregates distances per `obuID` and exposes an API (`/invoice?obu=<id>`) that returns an `Invoice` object.

Invoice calculator service: the `invoice_calculator` is intentionally isolated as a standalone microservice (not tightly coupled to `aggregator`). This allows the calculator to be used directly for "what-if" queries (e.g., "How much would it cost to travel from A to B?") without invoking the full pipeline. The `aggregator` calls `invoice_calculator` to compute amounts and persists invoices in the DB.

Note: distance calculation in `distance_calculator/service.go` is Euclidean for demo purposes — replace with Haversine for accurate geodesic distances.

---

## Project dependencies & install commands

Install required Go packages and tools used in the project:

- WebSocket (Gorilla):

```bash
go get github.com/gorilla/websocket
```

- Kafka Go client (Confluent):

```bash
go get github.com/confluentinc/confluent-kafka-go/v2/kafka
```

- Logger (logrus):

```bash
go get github.com/sirupsen/logrus
```

- Prometheus Go client:

```bash
go get github.com/prometheus/client_golang/prometheus
```

gRPC & Protobuf (optional / future):

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get google.golang.org/protobuf
go get google.golang.org/grpc
go get google.golang.org/genproto
```

Install `protoc` (protocol buffer compiler):

```bash
# Linux (example)
sudo apt install -y protobuf-compiler

# macOS
brew install protobuf
```

Kafka (local) using Docker Compose:

```bash
docker-compose up -d
```

Prometheus docker commands (examples):

```bash
docker run -d --name prometheus -p 127.0.0.1:9090:9090 -v /path/to/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus

# alternative (example path from development)
docker run -d --name prometheus -p 127.0.0.1:9090:9090 -v /home/you/project/.config/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
```

Access Prometheus at: http://localhost:9090

---

## API specification & example payloads

1. WebSocket (data_receiver)

- Endpoint: `ws://127.0.0.1:30000/ws`
- Payload (JSON) — OBU -> server

```json
{
  "obuID": 12345,
  "lat": 51.218,
  "long": 4.421
}
```

2. Aggregator HTTP POST (from distance_calculator)

- Endpoint: `POST http://127.0.0.1:3000/aggregate`
- Payload (JSON) — Distance

```json
{
  "value": 12.34,
  "obuID": 12345,
  "unix": 1670000000
}
```

3. Aggregator HTTP GET (client)

- Endpoint: `GET http://127.0.0.1:3000/invoice?obu=<id>`
- Response (JSON) — Invoice

```json
{
  "obuID": 12345,
  "totalDistance": 123.45,
  "totalAmount": 1234.5
}
```

Behavioral notes:

- `distance_calculator` currently uses Euclidean distance (demo). For real-world lat/long use Haversine.
- Aggregator multiplies `basePrice * totalDistance` to compute `totalAmount` (see `aggregator/service.go`).

---

## Environment variables (recommended)

Replace hard-coded constants with these env vars for runtime configuration. Create a `.env.example` at repo root documenting values.

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `KAFKA_TOPIC` (default: `obudata`)
- `WS_PORT` (default: `30000`) — data_receiver
- `AGGREGATOR_ADDR` (default: `:3000`)
- `AGGREGATOR_ENDPOINT` (full URL used by `distance_calculator` to POST distances)
- `BASE_PRICE` (default: `10.0`)

Suggested `.env.example`:

```
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=obudata
WS_PORT=30000
AGGREGATOR_ADDR=:3000
AGGREGATOR_ENDPOINT=http://127.0.0.1:3000/aggregate
BASE_PRICE=10.0
```

---

## Local development & run steps

1. Ensure Go and modules:

```bash
go version # use 1.21+
go mod tidy
```

2. Start Kafka & Zookeeper (use docker-compose snippet below or your own).

3. Create Kafka topic (if required):

```bash
kafka-topics.sh --create --topic obudata --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

4. Run services in separate terminals:

```bash
# aggregator
cd aggregator && go run main.go

# distance_calculator
cd ../distance_calculator && go run main.go

# data_receiver
cd ../data_receiver && go run main.go

# optionally run the OBU simulator
cd ../obu && go run main.go
```

5. Verify by querying the invoice endpoint:

```bash
curl "http://127.0.0.1:3000/invoice?obu=<some-obu-id>"
```

---

## How to run (Make targets & examples)

Run Kafka/Zookeeper first (via `docker-compose up -d`), then you can run services using the provided `Makefile` targets or by running `go run` directly.

Available `make` targets (see `Makefile`):

```bash
make receiver   # builds and runs data_receiver
make obu        # builds and runs the OBU simulator
make calculator # builds and runs distance_calculator
```

Note: there is no `make agg` target in the Makefile by default. To run the aggregator use:

```bash
cd aggregator && go run main.go
```

Example endpoints you can test with Thunder Client / curl:

- GET invoice (calculates invoice for an OBU ID):

```bash
GET http://localhost:3000/invoice?obu=6428921451518044973
```

Sample response:

```json
{
  "obuID": 6428921451518044973,
  "totalDistance": 35.86001233283358,
  "totalAmount": 112.95903884842576
}
```

- POST aggregate (used by distance_calculator to send distances; can be used manually):

```bash
POST http://localhost:3000/aggregate

Request body:
{
	"value": 20.12,
	"obuID": 1838,
	"unix": 73378
}
```

Server log example on `/aggregate`:

```
HTTP Transport running at port :3000...
INFO[0003] aggregating distance         distance=20.12 obuid=1838 unix=73378
INFO[0003] AggregateDistance            err="<nil>" took="47.03µs"
```

---

## Project status

Note: This project is currently in development. Work was paused for a short period (personal scheduling). Key items remaining: persistence for aggregator, env-based configuration, metrics, and Dockerfiles for each service.

## Example docker-compose (local Kafka + Zookeeper)

Add this minimal compose for demos (single-node Kafka; not production):

```yaml
version: '3.8'
services:
	zookeeper:
		image: wurstmeister/zookeeper:3.4.6
		ports:
			- 2181:2181
	kafka:
		image: wurstmeister/kafka:2.12-2.2.1
		ports:
			- 9092:9092
		environment:
			KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
			KAFKA_ADVERTISED_HOST_NAME: kafka
			KAFKA_CREATE_TOPICS: "obudata:1:1"
		depends_on:
			- zookeeper
```

Start with:

```bash
docker-compose up -d
```

---

## Testing & validation

- Unit tests: add `*_test.go` files and run `go test ./...`.
- Integration test plan (recommended):
  1.  Spin up Kafka and services via docker-compose.
  2.  Run `obu` simulator for a short duration.
  3.  Assert that `/invoice` returns increased `totalDistance` for known OBU IDs.
- Edge cases: duplicate messages, out-of-order timestamps, consumer restarts and replay.

---

## Design trade-offs, limitations & next steps

Points to discuss in interviews:

- Accuracy vs simplicity: Euclidean distance vs Haversine for geodesic accuracy.
- Durability: aggregator uses in-memory storage; swap to Postgres/Timescale for production.
- Delivery semantics: current at-least-once Kafka consumer; adopt idempotency keys or transactional producers for exactly-once behavior.
- Observability: add Prometheus metrics, structured logs, and distributed tracing.
- Security: secure WebSocket and Kafka traffic with TLS and add auth to HTTP endpoints.

Roadmap (prioritized):

1. Persist aggregator data to Postgres/Timescale and add migrations.
2. Add env-based configuration and `.env.example` (recommended next task).
3. Add health/readiness endpoints, retries, and backoff strategies.
4. Add metrics and Grafana dashboards.
5. Dockerize services and provide CI integration.

---

## Files to review (quick map)

- Aggregator: [aggregator/main.go](aggregator/main.go), [aggregator/service.go](aggregator/service.go), [aggregator/store.go](aggregator/store.go)
- Data receiver: [data_receiver/main.go](data_receiver/main.go), [data_receiver/producer.go](data_receiver/producer.go)
- Distance calculator: [distance_calculator/main.go](distance_calculator/main.go), [distance_calculator/service.go](distance_calculator/service.go), [distance_calculator/consumer.go](distance_calculator/consumer.go)
- OBU simulator: [obu/main.go](obu/main.go)
- Types: [types/types.go](types/types.go)
- Project manifest: [go.mod](go.mod)

---

## How to present this in an interview

- Start with the high-level flow and why Kafka introduces useful guarantees (replay, decoupling).
- Point to code that shows practical skills: producer/consumer wiring, simple service boundaries, and portability of `types` package.
- Explain trade-offs and the concrete steps you'd make to productionize the system.
- Offer a 2–3 minute live demo using the `obu` simulator and the aggregator `/invoice` endpoint.
