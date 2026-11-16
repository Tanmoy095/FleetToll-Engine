# Aggregator HTTP security review

Original review date: 2026-11-15. Status rechecked: 2026-11-17. Scope: local aggregator HTTP handlers and the in-memory service/store. This is an adversarial code/test review, not an external penetration test or security certification.

## Finding FT-API-001 — unauthenticated distance mutation and invoice lookup

- Severity: High
- Affected routes: `POST /aggregate`, `GET /invoice?obu=<id>`
- Attack: any client with network reachability can submit a fabricated distance for a chosen positive OBU ID and query accumulated totals for guessed IDs.
- Evidence: handlers contain no authentication or authorization check; server default binds `:3000` (all interfaces). No identity is bound to the supplied OBU ID.
- Reproduction: start aggregator and send a valid JSON aggregate request with an arbitrary positive `obuID`, then query `/invoice?obu=<same id>`.
- Root cause: the demo pipeline treats OBU ID as a self-asserted identity and has no tenant/user model.
- Fix: implemented for the local prototype in the tenant-auth increment. `/aggregate` requires the internal service token; `/invoice` requires a tenant-role credential and resolves tenant ID from the authenticated principal.
- Regression test: `TestAggregatorHTTPAuthorizationAndTenantIsolation` exercises missing, unknown, device-role credentials and same OBU/event IDs across tenants.
- Retest: focused aggregator tests pass.
- Remaining risk: shared secret lifecycle and plaintext transport remain deployment risks. See `TENANT_ISOLATION_REVIEW.md`; external exposure is not approved.

## Input-handling results

Tested over local HTTP: wrong methods; missing and unsupported content type; malformed JSON; unknown field; multiple JSON values; body exceeding 1 MiB; negative distance; zero or repeated OBU IDs; unknown invoice; and storage adapter failure. Invalid requests are rejected before storage update, storage failures return a generic 500 without process panic, and unknown invoices return 404.

The service/store tests do not exercise SQL because this feature has no database. SQL injection, credential handling, tenant isolation, TLS, rate limiting, WebSocket security and Kafka ACLs remain unassessed here. No vulnerability scanner or external network test was run.


This document preserves the original finding and evidence. Current implementation status and residual issues are tracked in [TENANT_ISOLATION_REVIEW.md](TENANT_ISOLATION_REVIEW.md).
