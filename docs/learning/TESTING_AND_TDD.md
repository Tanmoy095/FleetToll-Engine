# Testing and TDD

## Haversine distance slice

### RED

Command: `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./distance_calculator -run 'TestCalculatorService' -count=1`

Expected: failures demonstrate incorrect degree-as-distance behavior, cross-OBU state, and acceptance of invalid coordinates.

Actual: failed as expected. One degree along the equator returned `1` instead of about `111.1949266` km; interleaved OBU state returned `9` instead of `111.1949266` km; all invalid coordinate cases were accepted. `-vet=off` isolated the intended test failures from an unrelated pre-existing vet failure in the consumer logger.

A separate adversarial regression test was then added for OBU IDs `0` and `-1`.

Command: `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./distance_calculator -run 'TestCalculatorServiceRejectsInvalidOBUIdentity' -count=1`

Actual: failed as expected because both identities were accepted before the fix.

### GREEN

Implemented validated coordinates, per-OBU previous-point state protected by a mutex, and Haversine kilometers.

Command: `GOCACHE=/tmp/fleettoll-go-cache go test ./distance_calculator -run 'TestCalculatorService' -count=1`

Actual: passed.

### REFACTOR and broader checks

Corrected `logrus.Errorf` formatting in `distance_calculator/consumer.go`, ran gofmt, and added concurrent OBU initialization coverage.

Commands and actual results:

- `GOCACHE=/tmp/fleettoll-go-cache go test -race ./...` — passed for all packages. The linker emitted a macOS `LC_DYSYMTAB` warning, but command exit status was 0.
- `GOCACHE=/tmp/fleettoll-go-cache go vet ./...` — passed.
- `GOCACHE=/tmp/fleettoll-go-cache make tdd PKG=./distance_calculator TEST=TestCalculatorServiceCalculatesHaversineKilometers` — passed; confirms the required Make target works for focused tests.

The new `make tdd PKG=... TEST=...` target runs a focused named Go test; it is a convenience command, not proof of RED/GREEN by itself.

## Remaining verification

Historical checkpoint status: the original pure calculation slice lacked infrastructure integration. Later PostgreSQL replay and gRPC/Kafka integration work is documented in the implementation roadmap. On the 2026-11-17 tenant increment, PostgreSQL tests passed using an isolated temporary cluster; Kafka was unavailable. This is still not an external penetration test or a 1,000-device workload.

## Adversarial review notes

Executed unit probes for latitude/longitude bounds, NaN, and invalid OBU IDs; all are rejected before state mutation after the fix. The in-memory map is protected by a mutex and was exercised under the race detector. The tenant-auth increment now checks device credential OBU scope at the WebSocket boundary and uses tenant/OBU state keys. A publisher with direct Kafka access can still spoof messages because Kafka ACLs are absent; managed identity, bounded state lifecycle and external review remain open. No external penetration test was performed.

## Aggregator HTTP boundary

### RED

Command: `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./aggregator -run 'TestAggregatorHTTP' -count=1`

The first sandbox run could not bind a loopback port; it was an environment restriction, not a test failure. Reran with approved elevated permissions. The tests then failed for the intended behavior: wrong methods returned 200/500 instead of 405; unknown/trailing JSON, missing content type and oversized body were accepted; negative distance was stored; invalid/repeated IDs were not rejected; absent invoices returned 500; response content type was missing; and storage failure panicked the server, producing EOF.

### GREEN and REFACTOR

The production mux and handlers now apply the request contract, service validation, typed not-found classification and synchronized store. The tests use `httptest.NewServer` to send real loopback HTTP requests through production route wiring into the actual service and MemoryStore.

- `GOCACHE=/tmp/fleettoll-go-cache go test ./aggregator -run 'TestAggregatorHTTP' -count=1` — passed after implementation.
- `GOCACHE=/tmp/fleettoll-go-cache go test -race ./...` — passed, including 40 concurrent aggregate writes; non-fatal macOS linker warning for the distance test binary.
- `GOCACHE=/tmp/fleettoll-go-cache go vet ./...` — passed.

At the time this checkpoint was written, the API lacked auth. The tenant-auth increment now protects aggregate/invoice routes and scopes invoice reads; see `docs/security/TENANT_ISOLATION_REVIEW.md` for current controls and remaining risks. No external penetration test is claimed.

## WebSocket telemetry ingestion

### RED

- `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./data_receiver -run '^TestWebSocketRejectsInvalidCoordinates$' -count=1` — failed against the old receiver because latitude 91 was produced; disconnect then triggered `panic: repeated read on failed websocket connection` from the detached loop.
- `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./data_receiver -run '^TestWebSocketRejectsMissingCoordinateFields$' -count=1` — failed because absent longitude was decoded as zero and sent to the producer.

### GREEN and REFACTOR

- `GOCACHE=/tmp/fleettoll-go-cache go test ./data_receiver -count=1` — passed; real WebSocket requests cover invalid and missing fields, oversized/unknown frames, cross-origin upgrade, simultaneous connections, and log redaction.
- `GOCACHE=/tmp/fleettoll-go-cache make tdd PKG=./data_receiver TEST=TestWebSocketRejectsInvalidCoordinates` — passed.
- `GOCACHE=/tmp/fleettoll-go-cache go test -race ./...` — passed across all packages. macOS linker warnings on test binaries were non-fatal.
- `GOCACHE=/tmp/fleettoll-go-cache go vet ./...` — passed.

The fake producer verifies that valid and invalid WebSocket messages reach or do not reach the producer interface. Kafka broker delivery, auth, TLS deployment, rate limits and 1,000 concurrent clients remain unverified.

## Kafka telemetry producer

The first real Kafka integration test demonstrated that the old producer's record had an empty key (`"", want OBU ID 8142`). After the producer used OBU ID as its key and waited for the delivery report, the tests passed.

- `GOCACHE=/tmp/fleettoll-go-cache go test ./data_receiver -run '^TestKafkaProducer' -count=1` — passed environment/default, empty-topic and invalid-record checks.
- `KAFKA_TEST_BROKERS=localhost:9092 GOCACHE=/tmp/fleettoll-go-cache go test -tags=integration ./data_receiver -run '^TestKafkaProducerDeliversKeyedTelemetry$' -count=1 -timeout=60s` — passed against local Compose Kafka.
- `GOCACHE=/tmp/fleettoll-go-cache make integration-kafka` — passed against a real broker and consumer.

The integration test creates a temporary two-partition topic, publishes one OBU record and reads it through a consumer to verify key and payload. It does not validate sustained throughput or consumer-to-invoice behavior.

## Distance-calculator HTTP client

RED: `GOCACHE=/tmp/fleettoll-go-cache go test ./distance_calculator/client -run '^TestAggregateInvoice' -count=1` failed because the client omitted `Content-Type: application/json` (the real HTTP test server returned 415) and left response bodies open.

GREEN: the client now sets JSON `Content-Type` and `Accept`, applies a five-second default timeout, supports request context, closes every response body and reports non-200 results. The same command passed tests for request encoding, response cleanup and timeout behavior.

This is an HTTP integration boundary. Kafka consumer offset/retry behavior and full broker-to-invoice guarantees remain separate and are not implied by these tests.

## Kafka consumer lifecycle and offset boundary

### RED

Added tests for downstream-success-before-commit, no commit on aggregator failure, no commit on malformed data, commit failure termination, and lifecycle close on cancellation. The initial test run failed to compile because the consumer had no injectable reader or testable run-loop seam. This exposed that offset/lifecycle behavior was not an isolated unit-testable boundary yet.

### GREEN and integration

- `GOCACHE=/tmp/fleettoll-go-cache go test ./distance_calculator -run '^TestKafkaConsumer' -count=1` — passed after implementation with loopback access. The fake records commit attempts; HTTP fixtures exercise real client calls.
- `GOCACHE=/tmp/fleettoll-go-cache make integration-kafka` — after resolving a cancellation race in the integration fixture, passed against the local Compose broker. It includes producer-to-broker read-back and two real consumer records delivered through the real calculation service to a loopback aggregator.

The consumer disables auto-commit and commits only after a successful gRPC response. A downstream failure or invalid record stops the consumer without committing. This avoids silently acknowledging an observed failure but a malformed record can block the partition. A response lost after the aggregator accepted a write can cause a repeated RPC, which the durable event ledger deduplicates. The Kafka offset and PostgreSQL work still do not share one transaction; durable state, bounded retry and an audited DLQ remain necessary for robust operation.

## Internal protobuf/gRPC aggregation

The wire contract and full learning walkthrough are in `features/08-protobuf-grpc-aggregator.md`.

### RED / GREEN evidence

- `TestDialGRPCDoesNotReportUnavailableTargetAsConnected` failed against the lazy `grpc.NewClient` constructor because it returned before making a connection. A context-aware blocking dial made it pass and made the startup deadline meaningful.
- `TestLoggingMiddlewareDoesNotEmitStorageErrorDetails` failed and showed raw wrapped store errors in the log output. Replacing raw error fields with a boolean failure marker made the regression pass.
- Adapter tests verify valid replay, large `int64` OBU IDs, validation, conflict/not-found status mapping, oversized message rejection and sanitized storage errors.

### Green and integration evidence

- `go test -race ./...` — passed on 2026-11-16; non-fatal macOS linker warnings were emitted for two test binaries.
- `go vet ./...` — passed.
- `make integration-postgres` — passed against PostgreSQL 15, including a generated gRPC client writing and replaying one event through the actual SQL store.
- `make integration-kafka` — passed against the local broker; the real consumer path uses the gRPC client.
- `make generate-proto` — completed with no generated-file diff after the final schema change.
- One-shot 1,000-call PostgreSQL-backed gRPC probe — 1,000 successes, zero errors, p95 319.9 ms. See the performance note for hardware and limitations.

The first test attempts were blocked by the restricted module cache and loopback policy; they were rerun after approved dependency and local-network access. The current open security findings include plaintext transport if exposed beyond loopback, shared-secret lifecycle limits, missing context propagation into SQL interfaces, and no rate limits. This is not an external penetration test or production readiness claim.
