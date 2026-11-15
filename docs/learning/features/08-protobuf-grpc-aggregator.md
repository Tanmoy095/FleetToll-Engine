# Feature 08: Typed internal aggregation with protobuf and gRPC

## Business reason

After each Kafka event, the distance calculator must submit one calculated increment to the aggregator. A JSON HTTP call is easy to inspect and remains useful for the demo API, but it does not provide a generated, language-neutral service contract. This feature adds a small protobuf schema and generated Go gRPC client/server for that internal call. It does not add streaming or claim a performance win.

## Trace one request

1. Kafka delivers telemetry; the calculator derives a stable event ID from topic, partition and offset.
2. `distance_calculator/consumer.go` builds a `types.Distance` and calls its narrow aggregation-client interface.
3. The production client in `distance_calculator/client/grpc.go` maps it to `AggregateDistanceRequest` and passes the caller context to the generated client.
4. The aggregator adapter in `aggregator/grpc_transport.go` validates the fields, checks that the `int64` OBU ID fits the host `int`, maps the request to `types.Distance`, and delegates to the same domain service as HTTP. `int64` aligns with PostgreSQL `BIGINT` and avoids a smaller `int32` wire limit.
5. The existing memory or PostgreSQL store deduplicates by event ID. The adapter maps domain outcomes to gRPC status codes.
6. Only after successful aggregation does the consumer commit the Kafka offset.

```mermaid
sequenceDiagram
  participant K as Kafka consumer
  participant C as gRPC client
  participant S as Aggregator gRPC adapter
  participant D as Aggregator domain service
  participant P as PostgreSQL ledger
  K->>C: Distance + topic:partition:offset ID
  C->>S: AggregateDistance protobuf request
  S->>S: Validate message and bounds
  S->>D: AggregateDistance(types.Distance)
  D->>P: Insert unique event (when DATABASE_URL is set)
  P-->>D: inserted or same-payload replay
  D-->>S: accepted
  S-->>C: AggregateDistanceResponse
  C-->>K: success; commit offset
```

## Why both HTTP and gRPC exist

The generated service is used for service-to-service communication and supplies typed request/response messages. HTTP `/aggregate` and `/invoice` remain handy for curl, local demos and manual inspection. This is intentional protocol diversity at the edge, not duplicated business logic. Both transports delegate to one `Aggregator` service.

Costs include code generation, protocol compatibility policy, a second listener and gRPC tooling knowledge. gRPC uses HTTP/2 and protobuf but that alone does not prove it is faster or simpler for every call. The current one-event RPC is unary; no streaming requirement or benchmark justifies streaming yet.

## Schema and generated code

Source: `api/fleettoll/v1/aggregator.proto`. Generated files are committed so normal Go builds do not need `protoc`.

```bash
make install-proto-tools
make generate-proto
git diff -- api/fleettoll/v1
```

The plugin versions are pinned in the Makefile. A field number is part of the binary wire representation. Never change or reuse a published field number. When removing a field, reserve both its number and name. Additive fields are commonly wire-safe, but application behavior and generated language APIs still need compatibility tests. Read [ADR-022](../../architecture/decisions/ADR-022-internal-grpc-aggregator.md) before changing this contract.

## Error mapping

| Domain/transport outcome | gRPC code | Why |
|---|---|---|
| Invalid ID, OBU, distance or timestamp | `InvalidArgument` | Caller can correct the request |
| Same event ID with different data | `AlreadyExists` | Event identity was already bound to another payload |
| Invoice has no events | `NotFound` | Requested aggregate does not exist |
| Store failure | `Internal` with generic text | Do not return SQL, host or credential details |
| Inbound message exceeds 1 MiB | `ResourceExhausted` from gRPC | Bound parser and allocation work |

Do not map every error to `Internal`: retry policies depend on distinguishing invalid data, identity conflicts and transient infrastructure failures. This implementation intentionally has no automatic RPC retries. A response timeout is ambiguous, but event-ID idempotency makes a later explicit replay safe when storage is durable.

## Security and reliability limits

- The gRPC listener defaults to `127.0.0.1:3001`; changing the address can expose it to other machines.
- Credentials are plaintext and the RPC has no caller authentication or tenant authorization. Do not expose it outside local development.
- The gRPC handler receives `context.Context`, but the current domain/store interfaces do not take context. A cancelled RPC might not stop a running SQL statement.
- `DialGRPC` blocks until a connection is ready or its caller deadline expires. This catches a missing local service at startup; it does not add retry policy to individual RPCs.
- PostgreSQL ledger uniqueness handles duplicate attempts, but the Kafka offset and SQL commit are separate. The claim is at-least-once delivery with idempotent database effects, not exactly-once processing.
- A poison event/conflict can still stop its Kafka partition. DLQ and operator replay are later work.
- Middleware now records a failure boolean instead of raw error details. See the security review for findings and evidence.

## Verify locally

```bash
GOMODCACHE=/tmp/fleettoll-gomodcache GOCACHE=/tmp/fleettoll-go-cache go test ./aggregator ./distance_calculator/client -run '^(TestGRPCAggregator|TestGRPCClient)' -count=1
make integration-grpc
make integration-kafka
make integration-postgres
```

The focused tests use loopback TCP and the generated Go client; the Kafka integration starts a real broker test and crosses gRPC. The PostgreSQL integration calls the real gRPC adapter backed by the real PostgreSQL ledger. Network sandbox restrictions may require approved local test execution. Record actual outcomes; do not infer that an integration ran because the test exists.

### TDD evidence from this slice

- `TestDialGRPCDoesNotReportUnavailableTargetAsConnected` was run before the dial change and failed because `grpc.NewClient` returned a connection object without establishing connectivity. Replacing it with context-aware blocking dial made the test pass.
- `TestLoggingMiddlewareDoesNotEmitStorageErrorDetails` failed and printed raw wrapped storage errors for both aggregation and invoice paths. Changing those log fields to a boolean failure flag made the regression pass.
- RPC tests cover the adapter contract and were run as the implementation was completed. The first execution attempts were blocked by dependency-cache and loopback sandbox limits; those environment failures were separated from code failures and rerun after approved local access.

## Explain it in an interview

Be able to explain: why the domain service is shared; why the contract lives in a versioned proto package; which field numbers are stable; how the adapter maps errors; why HTTP still exists; why no gRPC retry policy is enabled; what happens when PostgreSQL commits but the RPC response is lost; why the offset is committed after the RPC; and why this is not end-to-end exactly-once.

### Questions to answer without AI first

1. If a new server adds an optional protobuf field, what can an old client do with it? What schema changes are unsafe?
2. When should an RPC return `InvalidArgument`, `AlreadyExists`, `NotFound`, `Unavailable` or `Internal`?
3. If the client deadline expires after the database commit, what does the caller know? How does the event ID make the next attempt safe?
4. Why is an interceptor not a substitute for domain validation?
5. Why does the listener default to loopback? What changes before deployment to another host?
6. What work is still not cancelled when `context.Context` is not passed into the service and pgx call?
7. Which metric labels would be dangerous/high-cardinality for this RPC and why?
8. Why should we benchmark before concluding gRPC is faster than HTTP here?

## Practice task

Implement a context-aware domain method as a separate exercise: add `AggregateDistanceContext(ctx, event)` and pass context through the service and PostgreSQL store. Preserve the HTTP and gRPC semantics, add a test proving cancellation reaches a blocked store, then compare the lock/cancellation behavior under a concurrent workload. Do not change the public protobuf fields for this task.
