# PostgreSQL replay-safety security review

**Scope:** event identity validation, PostgreSQL query construction, duplicate writes and transaction ordering for the durable distance slice. **Environment:** local PostgreSQL 15 Compose service; automated integration tests only. This is not an external penetration test or a review of the entire application.

## Findings and evidence

| ID | Severity | Finding | Attack / evidence | Remediation and remaining risk |
|---|---|---|---|---|
| FT-DB-01 | High, fixed for this slice | A repeated aggregate could double-count a distance | The pre-fix unit regression submitted one 4.25 km event twice and observed 8.5 km | Event ID unique ledger; real PostgreSQL replay test verifies one 4.25 km contribution |
| FT-DB-02 | High, fixed for this slice | Replay after calculator state advancement could return a different distance | Durable integration test replays the same event through a newly created calculator instance and confirms it returns the first saved distance; following point still measures about 111.195 km | Inbox result and trip baseline commit in one transaction |
| FT-DB-03 | Medium, mitigated | SQL injection-shaped event ID could escape into SQL | Real PostgreSQL integration test inserts `event'); DROP TABLE aggregated_distance_events; --` and verifies the table remains queryable | All values use pgx bound parameters; values are still bounded to 512 bytes |
| FT-DB-04 | Medium, fixed | Reusing an event ID with different vehicle or distance could silently hide data | Unit and PostgreSQL tests reuse an ID with a changed distance | Store rejects mismatch as `ErrEventIDConflict`; poison-event handling still requires operator intervention |
| FT-DB-05 | High, open | Aggregator and invoice routes have no authentication or tenant authorization | Code inspection: `/aggregate` accepts any reachable caller; `/invoice?obu=` accepts caller-selected IDs | Add authenticated server-owned tenant/device identity and tenant-scoped authorization before external deployment |
| FT-DB-06 | High, open | Kafka local listener and PostgreSQL Compose credentials are development-only | Compose exposes plaintext Kafka locally and uses a static local-only PostgreSQL password | Restrict to local development; deployment requires TLS/SASL/ACL, secrets, network policy and database roles |
| FT-DB-07 | Medium, open | Poison telemetry/conflicting identity can stop the consumer partition | Consumer intentionally exits without offset commit on processing errors; no DLQ or bounded retry is present | Add retry/DLQ and audited replay after persistence and authorization policy are established |

## Checks actually run

- Unit regression: `TestAggregatorDeduplicatesKafkaEventIdentity` failed before the memory-store fix because total distance was 8.5 instead of 4.25 km; passed after the fix.
- Unit adversarial checks: same-key concurrent delivery and conflicting identity reuse.
- Real PostgreSQL: replay after a new calculator instance, baseline continuity, duplicate ledger insertion, conflicting reuse and SQL-shaped input.
- Real Kafka integration remains separately covered by `make integration-kafka`.
- Race detection, static analysis and a measured 1,000-request API probe are recorded in the repository README after execution.

No credentials, tenant IDs or GPS coordinates are emitted in the new persistence error messages. The existing APIs are still unauthenticated and must not be exposed publicly as a production service.
