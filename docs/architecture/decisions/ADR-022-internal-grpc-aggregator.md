# ADR-022: Use protobuf/gRPC for calculator-to-aggregator calls

- Status: Accepted
- Date: 2026-11-16

## Context

The Kafka consumer previously sent each calculated distance through an untyped HTTP JSON request. The project needs an explicit service-to-service contract, generated Go client/server code, stable error semantics and a concrete place to practice protocol evolution. Existing HTTP endpoints remain useful for manual inspection and demo clients.

## Decision

Define `AggregatorService` in `api/fleettoll/v1/aggregator.proto` with `AggregateDistance` and `GetInvoice` RPCs. Use `int64` for OBU IDs to align with the PostgreSQL `BIGINT` model, then validate host-`int` range before conversion. Generate and check in Go bindings using protoc-gen-go v1.36.6 and protoc-gen-go-grpc v1.5.1. The calculator uses a generated gRPC client for the Kafka processing path. The aggregator registers a gRPC adapter and retains the HTTP endpoints. Both adapters call the same domain service and stores.

Map malformed input to `InvalidArgument`, conflicting event identity to `AlreadyExists`, missing invoice to `NotFound`, and storage failures to a generic `Internal` message. Limit inbound gRPC messages to 1 MiB. Bind gRPC to loopback by default and shut both servers down on process termination.

## Alternatives considered

1. Keep HTTP only: lower setup cost, but this misses the typed interface and generated service contract the system needs to demonstrate.
2. Replace HTTP completely: rejected because the HTTP invoice endpoint is useful for people, curl and the existing local demo.
3. Add reflection and expose gRPC broadly: rejected because there is no auth, tenant authorization or TLS yet; reflection also reveals service schema to any reachable caller.
4. Add streaming or a gateway: deferred; current interaction is one distance event per request and no measured need justifies the extra complexity.

## Consequences

The internal call has typed messages, generated clients and language-neutral schema. The system now owns protobuf compatibility, code-generation tool versions and another listener. This change does not claim lower latency or higher throughput than HTTP. gRPC's listener is local-only by default; an override can expose unauthenticated plaintext RPC and must be guarded at deployment.

The server adapter delegates to the existing domain service, so idempotency stays in the event ledger. The current service/store interfaces lack context parameters; a cancelled RPC may not stop an already-running PostgreSQL operation. Add context propagation before relying on deadlines for database resource control.

## Verification

The focused transport tests use a generated client over loopback TCP and verify replay-safe same-event calls, conflict and validation status codes, not-found mapping, and storage-error redaction. The real-Kafka integration path is updated to call the gRPC client. Load results and the limits of the local one-shot probe belong in `docs/performance/GRPC_AGGREGATOR_LOCAL_LOAD.md`.

## Revisit when

Add authentication/TLS before remote deployment. Revisit request shape and compatibility policy before adding tenant identity, batch requests, currency amounts or long-lived streaming.
