# Aggregator HTTP security review

Date: 2026-11-15. Scope: local aggregator HTTP handlers and the in-memory service/store. This is an adversarial code/test review, not an external penetration test or security certification.

## Finding FT-API-001 — unauthenticated distance mutation and invoice lookup

- Severity: High
- Affected routes: `POST /aggregate`, `GET /invoice?obu=<id>`
- Attack: any client with network reachability can submit a fabricated distance for a chosen positive OBU ID and query accumulated totals for guessed IDs.
- Evidence: handlers contain no authentication or authorization check; server default binds `:3000` (all interfaces). No identity is bound to the supplied OBU ID.
- Reproduction: start aggregator and send a valid JSON aggregate request with an arbitrary positive `obuID`, then query `/invoice?obu=<same id>`.
- Root cause: the demo pipeline treats OBU ID as a self-asserted identity and has no tenant/user model.
- Fix: not yet implemented. Add verified device/service authentication for ingestion and user JWT/RBAC authorization with tenant-scoped resource lookup before making the service externally reachable.
- Regression test: pending with the identity/tenant feature.
- Retest: pending. Finding remains OPEN.
- Remaining risk: arbitrary invoice disclosure and financial-total manipulation while the service is network reachable.

## Input-handling results

Tested over local HTTP: wrong methods; missing and unsupported content type; malformed JSON; unknown field; multiple JSON values; body exceeding 1 MiB; negative distance; zero or repeated OBU IDs; unknown invoice; and storage adapter failure. Invalid requests are rejected before storage update, storage failures return a generic 500 without process panic, and unknown invoices return 404.

The service/store tests do not exercise SQL because this feature has no database. SQL injection, credential handling, tenant isolation, TLS, rate limiting, WebSocket security and Kafka ACLs remain unassessed here. No vulnerability scanner or external network test was run.
