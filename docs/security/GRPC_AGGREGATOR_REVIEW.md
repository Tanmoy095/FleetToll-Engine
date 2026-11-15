# Internal gRPC aggregator security review

**Date:** 2026-11-16

**Scope:** generated aggregator RPC, client wiring, message limits, status mapping, logs and local transport.

**Method:** source review plus automated loopback tests and real PostgreSQL/Kafka integration. No external penetration test was performed.

## Findings

| ID | Severity | Status | Attack or evidence | Fix / remaining risk |
|---|---|---|---|---|
| FT-GRPC-01 | High if network-reachable; Medium in the default local setup | Open | `aggregator/main.go` binds `127.0.0.1:3001` by default, but the listen flag can bind another interface. The server accepts RPCs without authentication/TLS. A reachable caller can submit fabricated distances or query another OBU invoice. | Keep the listener loopback-only for local demos. Before remote deployment add authenticated workload identity, tenant authorization, TLS/mTLS, network policy and audit. No production exposure is approved by this review. |
| FT-GRPC-02 | Medium | Open | Message receive size is capped at 1 MiB, but a caller can still open many concurrent RPCs or connections. A single-message cap is not a total memory, connection or rate limit. | Add bounded connection/concurrency quotas and rate controls after establishing expected traffic. The 1,000-call probe is one short request per client, not a sustained abuse test. |
| FT-GRPC-03 | Medium | Open | RPC handlers receive a context, but `Aggregator` and `PostgresStore` methods do not. A client deadline/cancel cannot stop a database query already in progress. | Introduce context-aware domain/store methods and verify cancellation against a deliberately blocked PostgreSQL operation. |
| FT-GRPC-04 | Medium | Fixed | Before the patch, the existing middleware logged raw wrapped store errors (`storage unavailable`), which could include SQL or connection details. `TestLoggingMiddlewareDoesNotEmitStorageErrorDetails` failed and showed both aggregation and invoice errors in logs. | Middleware logs a boolean failure flag instead of the raw error. The regression test passes; gRPC also returns stable generic internal messages. |
| FT-GRPC-05 | Medium | Mitigated by existing idempotency | Replay after an ambiguous response can repeat an RPC. A unique event ID and same-payload check make identical PostgreSQL deliveries a no-op; different payload reuse is rejected. | The real PostgreSQL gRPC integration verifies duplicate calls contribute distance only once. This relies on durable PostgreSQL mode; the in-memory store is not restart-safe. |
| FT-GRPC-06 | Low, verified | Fixed at boundary | Malformed values, negative/non-finite distance, non-positive OBU/timestamp, conflicting event IDs, absent invoices and oversized requests were sent through the generated client. | Tests assert `InvalidArgument`, `AlreadyExists`, `NotFound`, `ResourceExhausted`, and generic `Internal` without returning store error text. |
| FT-GRPC-07 | Low, fixed | `grpc.NewClient` is lazy: the first version of `DialGRPC` returned success even when no service was listening. This delayed a configuration/connectivity failure until a Kafka event arrived. | `TestDialGRPCDoesNotReportUnavailableTargetAsConnected` failed against the lazy connection constructor. `DialGRPC` now uses a context-aware blocking dial; the regression passes and startup respects its deadline. |

## Checks executed

- Focused generated-client loopback tests: pass.
- Full unit suite with `-race`: pass. macOS emitted non-fatal linker `LC_DYSYMTAB` warnings for test binaries.
- Real PostgreSQL 15 integration: pass, including gRPC aggregate/replay/invoice round trip.
- Real Kafka broker integration: pass through the calculator's gRPC client path.
- 1,000 concurrent one-shot gRPC calls against PostgreSQL: 1,000/1,000 succeeded; see `docs/performance/GRPC_AGGREGATOR_LOCAL_LOAD.md`.
- Static analysis: `go vet ./...` pass.

## Acceptance boundary

This feature is verified for local development, not for a network-exposed service. FT-GRPC-01 and FT-GRPC-03 must be addressed before making a remote production-readiness claim. No external penetration test is claimed.
