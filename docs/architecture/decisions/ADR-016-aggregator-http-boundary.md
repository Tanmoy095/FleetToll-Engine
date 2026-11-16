# ADR-016: Validate and bound the aggregator HTTP boundary

- Status: Accepted
- Date: 2026-11-15

## Context

The aggregator exposes `POST /aggregate` and `GET /invoice`. Before this change, handlers accepted any method, decoded permissive JSON without a size limit, could accept trailing values, omitted a successful aggregate response, and set JSON content type after writing the status. Storage errors could panic the server. Concurrent HTTP calls could race on the in-memory map.

## Problem and requirements

Keep the existing paths and HTTP 200 aggregate success contract for the current distance-calculator client. Reject malformed or resource-exhausting requests before storage mutation, distinguish invalid input from missing invoices, avoid leaking internal storage errors, and remain race-safe while the demo store is in use.

## Alternatives considered

1. Add validation only in the HTTP handler. Rejected because other service callers would still be able to pass invalid data.
2. Decode arbitrary JSON and rely on Go zero values. Rejected because unknown fields, trailing values and oversized bodies are ambiguous and can hide client mistakes.
3. Return HTTP 200 for every request and encode errors in the body. Rejected because clients and monitors need standard HTTP status semantics.
4. Replace the store with PostgreSQL in this slice. Deferred because schema, migrations and transaction behavior form a separate persistence feature requiring real PostgreSQL integration tests.

## Decision

The handler restricts methods, requires `application/json`, caps aggregate bodies at 1 MiB, rejects unknown fields and trailing JSON, validates positive OBU ID, finite non-negative distance and positive timestamp, and emits JSON before the status. Invoice reads require exactly one positive OBU ID, return 404 for the typed not-found error, and set `Cache-Control: no-store`. The service validates its domain input and wraps storage errors rather than panicking. MemoryStore uses an RWMutex. Production routes are constructed by `newHTTPHandler` so tests exercise the same mux and handlers.

## Benefits and trade-offs

Requests fail predictably, malformed payloads cannot mutate totals, body allocation is bounded, and concurrent operations are safe for this in-memory demo. The 1 MiB cap is intentionally generous for one small distance record; it should be configuration only if a future batch API needs a different contract. The store remains volatile and is not a production system of record.

## Security implications

Strict decoding, size limits and validation reduce parser ambiguity and resource abuse. `no-store` prevents intermediary caching of invoice responses. At the time of this ADR, authentication and tenant ownership were absent. The later tenant-auth increment requires a service token for `/aggregate`, a tenant-role credential for `/invoice`, and uses tenant-scoped storage; shared-secret lifecycle and TLS risks remain (see ADR-023).

## Operational and testing implications

Tests use `httptest.NewServer` with the production mux and in-process service/store. Current tests cover authentication and tenant isolation; the 2026-11-17 unit/race/vet/PostgreSQL checks passed. The latest Kafka broker integration remained unavailable.

## Reconsider this decision when

The API gains batch ingestion, managed workload identity, TLS/mTLS, PostgreSQL context propagation, or a generated OpenAPI contract. Revisit request-size limits and status/error schemas together with client compatibility.
