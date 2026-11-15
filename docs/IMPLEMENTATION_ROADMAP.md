# FleetToll implementation roadmap

Updated: 2026-11-15. This is a progress record, not a completion claim. The project is being built in verified increments; the current prototype is not production-ready.

## Completed and verified slices

- [x] Established a Go service baseline and an incremental implementation/verification process.
- [x] Distance calculation now uses validated per-OBU state and Haversine kilometers; unit tests, race detection and `go vet` passed. Kafka state persistence, identity binding and realistic device load remain open.
- [x] Aggregator HTTP handlers now enforce methods, JSON media type, a 1 MiB request cap, strict single-object decoding and domain input validation; invoice lookup validates IDs and returns a typed 404. Storage failures are returned rather than panicked.
- [x] MemoryStore protects concurrent reads/writes with an RWMutex.
- [x] HTTP request/response integration tests run over `httptest.NewServer` into the production handler, service and store. Concurrent aggregation was tested under `-race`.
- [x] WebSocket receiver now isolates each socket in its handler, bounds frames, strictly decodes and validates payloads, terminates on disconnect/error, checks browser Origin, and omits device/location values from logs. Protocol tests cover simultaneous connections.
- [x] Kafka producer reads broker/topic settings from the process environment, keys records by OBU ID, enables idempotent publishing with `acks=all`, reports broker delivery errors, and closes its client cleanly. A tagged integration test verifies a real Kafka write/read.
- [x] Kafka consumer disables auto-commit, explicitly commits only after successful HTTP aggregation, stops on processing/commit errors, propagates shutdown context, and closes the consumer. Unit tests cover commit/error ordering; a tagged real-broker test covers two telemetry events through the consumer into a loopback aggregator.
- [x] Distance-calculator HTTP client sends JSON with a bounded default timeout, supports context cancellation and closes response bodies; loopback contract tests pass.
- [x] The initial Makefile `tdd` target and feature learning notes exist.

## Current feature status

The distance, aggregator HTTP, WebSocket and Kafka producer/consumer slices are **implemented but not fully verified** against all project gates. Real single-broker tests now cover producer write/read and consumer-to-aggregator flow. Consumer delivery remains at-least-once with ambiguous HTTP timeout outcomes; its per-OBU state and aggregator totals are volatile, there is no inbox/deduplication, and a poison record blocks its partition. There is no PostgreSQL integration environment, auth, tenant isolation, dynamic performance workload, or external penetration test yet. The `:3000` aggregator endpoint has no authentication; anyone who can reach it can submit distances and read an invoice by OBU ID. The WebSocket endpoint trusts client-supplied OBU IDs and has no connection/rate quotas or enforced TLS. The Kafka local broker uses plaintext without ACLs. Track these as open findings until identity/device authorization and operational controls are implemented and retested.

## Next implementation sequence

1. Add PostgreSQL/PostGIS infrastructure and migrations; verify repository behavior against real PostgreSQL. Design durable trip state, inbox deduplication and idempotent aggregation together because current Kafka consumer state is volatile.
2. Model tenant, fleet, vehicle and device identity with authenticated device/user access, RBAC and tenant-scoped reads/writes; close the open authorization findings.
3. Add service rate limits, TLS deployment controls and device revocation/rotation.
4. Add bounded Kafka retries, DLQ and operator replay after durable/idempotent processing is available; then implement outbox/inbox and failure recovery.
5. Add toll-zone geospatial pricing, billing ledger, immutable invoices and statement reconciliation.
6. Instrument traces, metrics and structured correlated logs; add operational dashboards and SLOs.
7. Complete security, E2E and measured load workloads, including 1,000 API users and 1,000 vehicle connections where local resources permit.
8. Finish service/API/operations/learning docs and deterministic demo.

Order may change when dependency constraints require it. Each feature needs documented RED/GREEN/REFACTOR, unit tests, appropriate real integration tests, adversarial review, honest performance evidence and a learning guide.
