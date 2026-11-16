# Internal gRPC aggregator security review

**Original review date:** 2026-11-16
**Status rechecked:** 2026-11-17

**Scope:** generated aggregator RPC, client wiring, message limits, status mapping, logs and local transport.

**Method:** source review plus automated loopback tests and real PostgreSQL/Kafka integration. No external penetration test was performed.

## Findings

| ID | Severity | Status | Attack or evidence | Fix / remaining risk |
|---|---|---|---|---|
| FT-GRPC-01 | High if network-reachable; Medium in the default local setup | Partially mitigated | `aggregator/main.go` binds `127.0.0.1:3001` by default, but the listen flag can bind another interface. At the time of this review, the server accepted RPCs without auth/TLS. The tenant increment now requires a shared service token and adds tenant IDs to RPC messages; plaintext transport and broad shared-token authority remain risks. | Keep loopback-only. Before remote deployment replace the shared token with workload identity, add TLS/mTLS, network policy and audit. No production exposure is approved by this review. |
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
- Real Kafka broker integration: not available on the 2026-11-17 feature run (local broker port was closed); the earlier 2026-11-16 result applied to the previous revision.
- 1,000 concurrent one-shot authenticated gRPC calls against the in-memory store: 1,000/1,000 succeeded; see `docs/performance/GRPC_TENANT_AUTH_MEMORY_LOAD.md`. The earlier PostgreSQL run is historical.
- Static analysis: `go vet ./...` pass.

## Acceptance boundary

This feature is verified for local development, not for a network-exposed service. Plaintext transport/shared token scope and missing SQL context propagation must be addressed before any remote production-readiness claim. Current tenant controls and open findings are in `TENANT_ISOLATION_REVIEW.md`. No external penetration test is claimed.
