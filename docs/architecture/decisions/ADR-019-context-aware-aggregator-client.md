# ADR-019: Bound and close the legacy distance-to-aggregator HTTP client

- Status: Accepted
- Date: 2026-11-15

## Current status

This ADR documents the HTTP client retained for compatibility and tests. The active Kafka processing path now uses the authenticated protobuf/gRPC client described in ADR-022 and ADR-023.

## Context

The distance calculator calls the aggregator for each computed increment. Its client used `http.DefaultClient` without a timeout, omitted JSON content type, and did not close the response body. The aggregator now requires JSON content type and applies strict input validation.

## Problem and requirements

Keep the existing endpoint and JSON payload, make the request acceptable to the current handler, bound how long a call can block, allow caller context cancellation, and release response resources on both success and non-success status.

## Alternatives considered

1. Continue using `http.DefaultClient` with no timeout. Rejected because a stalled service can hold the Kafka consumer forever and leak response connections.
2. Set a global default-client timeout. Rejected because package-level global mutation affects unrelated HTTP clients/tests.
3. Give `Client` an injectable `*http.Client` with a five-second default and a context method. Selected for local ownership, testability and cancellation.
4. Retry automatically in the client. Deferred because safe retries need an idempotency key and explicit consumer offset policy; a timeout is an ambiguous result.

## Decision

`NewClient` initializes a five-second HTTP client. `AggregateInvoiceContext` encodes the payload, sets JSON `Content-Type` and `Accept`, creates a context-bound POST, closes the response body, and returns a clear error for non-200 responses. The existing `AggregateInvoice` method remains as a compatibility wrapper using a background context.

## Benefits and trade-offs

Calls no longer wait indefinitely and response connections can be reused. The five-second timeout is a local default that may need configuration after latency measurements. The method still reports timeout as failure without an idempotency key; upstream must not blindly retry as if the first POST definitely failed.

## Security implications

This legacy utility is not the production Kafka path. The retained `/aggregate` HTTP route now requires the shared internal service token, but the local HTTP transport remains plaintext. TLS and a managed service identity are still required before deployment on an untrusted network.

## Testing implications

Tests use a real loopback HTTP server for request contract and timeout behavior plus a response-body close tracker. No distributed end-to-end or load test is claimed.

## Reconsider this decision when

Adding service credentials, retry/idempotency semantics, configurable timeout policy or a generated API client.
