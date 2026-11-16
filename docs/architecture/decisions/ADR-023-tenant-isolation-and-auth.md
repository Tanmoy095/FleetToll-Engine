# ADR-023: Tenant isolation and prototype authentication

**Status:** Accepted for the portfolio prototype (2026-11-17)

## Context

OBU IDs are only unique inside an operator’s fleet. Using OBU ID or event ID as a global database key lets one tenant collide with another and makes invoice authorization impossible. The previous service boundaries also trusted client-supplied identity and exposed internal aggregation operations without credentials.

## Decision

- Device credentials contain a tenant ID and an explicit OBU allowlist. The WebSocket receiver derives tenant identity from the authenticated principal; the strict frame decoder rejects a client-supplied `tenantID`.
- Tenant operator credentials can query invoices only within their tenant. Device credentials cannot use the invoice endpoint.
- The internal calculator-to-aggregator gRPC path and HTTP aggregate route require one configured shared service bearer token. This caller is trusted to submit the tenant ID carried in the message.
- Tenant ID is part of telemetry/distance domain values, Kafka partition keys, the protobuf contract, in-memory store keys, PostgreSQL composite primary keys, and invoice queries.
- Migration 002 seeds tenant 1, backfills all existing data into it, then removes the migration-time defaults so new writes must specify tenant identity.
- Credentials are loaded from environment JSON and never stored in source control. When `DATABASE_URL` is configured, startup checks that each configured tenant exists.
- Missing tenant IDs in old Kafka/protobuf records map to tenant 1 for local compatibility during migration.

## Consequences and trade-offs

Composite keys enforce namespace isolation at the storage boundary and the tenant-first indexes support scoped reads. A shared service token is small and easy to operate locally, but has broad authority and requires synchronized secret distribution. Manual credential and tenant provisioning makes this a learning prototype, not a production identity system. Legacy fallback can silently classify an old record as tenant 1, so it must be removed before untrusted producers or multiple production tenants are enabled. Plaintext gRPC is acceptable only on the local loopback demo path.

## Validation expectations

Test equal OBU/event IDs under separate tenants, cross-tenant invoice reads, missing/invalid credentials, device OBU scope violations, spoofed tenant fields, migration backfill, and replay after a process restart against real PostgreSQL. Require unit, real integration, TDD, security and performance evidence before deployment.
