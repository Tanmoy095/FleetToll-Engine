# Feature 09 — Tenant isolation and credentials

## Business problem

Two fleet operators can both have OBU 42 and Kafka event IDs are not business-global identifiers. A global key or an invoice query keyed only by OBU can mix data and expose another operator’s activity. Identity must be attached before telemetry becomes a durable record, then carried through every key and query.

## Implemented request path

1. The device presents a bearer token on the WebSocket handshake.
2. The receiver resolves a device principal containing tenant ID and allowed OBU IDs.
3. The frame is strict JSON and contains OBU/coordinates only. Server-side code assigns tenant ID and checks the OBU allowlist.
4. Kafka key is `tenantID:OBUID`, preserving order per tenant/vehicle key.
5. Calculator memory state and PostgreSQL trip/inbox rows use the tenant/OBU identity. A tenant-qualified advisory lock prevents tenants with the same OBU from blocking each other.
6. The trusted calculator passes tenant ID in the protobuf gRPC request with the internal service token.
7. The aggregator deduplicates `(tenant_id,event_id)` and stores `(tenant_id,obu_id)` totals. Invoice reads authenticate a tenant operator and query only that tenant.

## Why composite keys

A tenant ID in application code alone can be forgotten in one query. Composite primary keys make the namespace explicit in the database too. They allow tenants to reuse OBU/event IDs while preventing cross-tenant collisions. The trade-off is that every relevant foreign key, join, cache key, event and API must carry tenant identity. A tenant-first index helps scoped access but adds storage and write work.

## Credential roles

- `device`: may connect to telemetry ingestion and can submit only listed OBU IDs.
- `tenant`: may read invoices in its tenant; it cannot authenticate telemetry ingestion.
- internal service token: authorizes the trusted calculator to submit distance events. It has broad cross-tenant authority and must stay private to that service.

The JSON config key is `FLEETTOLL_TENANT_API_KEYS`; the service token is `AGGREGATOR_GRPC_TOKEN`. Use unique random values of at least 32 bytes. Do not put real values in code, README or shell history. The local example is shared-secret training code, not a substitute for OAuth, workload identity, secret rotation, TLS/mTLS or a managed identity service.

## PostgreSQL migration reasoning

Migration 002 creates a tenant registry, seeds `legacy-local`, adds tenant 1 to existing rows, changes primary keys to tenant-composite keys and drops migration-only defaults. Existing records remain associated to tenant 1. New inserts must supply tenant ID. Before running a similar migration on a large production table, plan lock duration, a staged nullable/backfill/validation rollout, rollback strategy and dual-read/write compatibility.

## TDD and security checklist

For this increment, credential parser tests were written first and failed to compile while the authentication API was absent; implementation made them pass. Receiver integration tests then showed the tenant assignment/OBU-scope ordering bug before the handler was corrected. The final cross-tenant invoice regression passes. This records the observed red/green evidence; the new PostgreSQL tests should still be treated as a regression suite and rerun on a clean cluster.

- [ ] RED: same OBU/event ID under tenant 1 and tenant 2 conflicts or leaks before tenant-aware keys exist.
- [ ] GREEN: add tenant identity through model, Kafka, protobuf, SQL and auth boundaries.
- [ ] REFACTOR: extract credential parsing and tenant key types; preserve useful error boundaries.
- [ ] Missing, invalid, wrong-role and OBU-out-of-scope credentials fail closed.
- [ ] Client cannot spoof tenant ID in strict JSON.
- [ ] Same tenant/event replay is idempotent; changed payload is a conflict.
- [ ] Real PostgreSQL migration/backfill and duplicate ID isolation are tested.
- [ ] Remove legacy tenant-1 fallback before any untrusted old producer is allowed.

The current five gates are: (1) unit/race/static checks, (2) real PostgreSQL and Kafka integration, (3) a recorded RED/GREEN/REFACTOR cycle, (4) adversarial security review, and (5) a measured 1,000-call workload. This revision passed gates 1, 3, and PostgreSQL portion of 2; Kafka integration is pending because the broker was unavailable. Gate 4 was a local adversarial self-review, not an independent audit. Gate 5 passed for in-memory gRPC only; PostgreSQL capacity remains unmeasured.

See [ADR-023](../../architecture/decisions/ADR-023-tenant-isolation-and-auth.md) and the [security review](../../security/TENANT_ISOLATION_REVIEW.md).
