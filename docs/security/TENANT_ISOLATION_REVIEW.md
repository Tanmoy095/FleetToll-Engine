# Tenant isolation and authentication review

**Date:** 2026-11-17
**Scope:** tenant-aware telemetry ingestion, Kafka identity, calculator persistence, aggregator HTTP/gRPC boundaries and invoice reads. This is an engineering review, not an external penetration test.

## Controls reviewed

- WebSocket route rejects absent/unknown bearer credentials. Only device-role credentials can send telemetry; the payload OBU ID must be in the credential allowlist. The receiver assigns the tenant ID; strict JSON rejects `tenantID` from a device frame.
- Invoice route requires a credential. Only tenant-role credentials can read; lookup uses authenticated tenant ID and requested OBU ID.
- Aggregate HTTP and internal gRPC enforce the service bearer token. Token length is checked at startup and comparisons use SHA-256 digests with constant-time compare.
- Postgres access paths, composite keys and the Kafka key include tenant identity. Startup validates configured tenant IDs in PostgreSQL.
- Regression tests prove same OBU and event ID can coexist in two tenants and invoice totals remain isolated; missing/wrong/device-role invoice credentials are rejected.

## Findings and residual risks

| ID | Severity | Status | Finding |
|---|---|---|---|
| TEN-01 | High before remote exposure | Open | Shared bearer credentials are configured as plaintext environment values. Rotation, revocation, secret-manager integration, TLS/mTLS and audit are absent. Keep the gRPC listener loopback-only and terminate TLS before any remote HTTP exposure. |
| TEN-02 | Medium | Open | The internal service token grants cross-tenant aggregate authority. Only the trusted calculator should possess it; no caller-level tenant allowlist exists at this boundary. |
| TEN-03 | Medium | Open | Old Kafka/protobuf messages without tenant ID map to tenant 1. This aids local compatibility but can misclassify untrusted legacy traffic. Remove after data migration and producer upgrade. |
| TEN-04 | Medium | Open | Tenant provisioning is currently manual SQL and credential changes require coordinated environment updates/restarts. There is no lifecycle/audit API. |
| TEN-05 | Medium | Open | No explicit rate limit or maximum WebSocket session duration is added in this increment; credential brute force and resource exhaustion require deployment controls. |
| TEN-06 | Low | Open | Credentials are hashed in memory but raw tokens remain in process environment/config memory and are not individually revocable without restart. |

## Verification status

Focused auth, receiver, aggregator HTTP/gRPC and calculator tests passed. This environment had no available Docker daemon and no PostgreSQL listener, so the new migration and real SQL cross-tenant tests have not been verified here. Do not mark that real database gate passed until `make integration-postgres` completes locally. Kafka integration and full race/vet/load checks also need to be rerun on the feature revision before a production claim.
