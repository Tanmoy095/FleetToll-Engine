# Kafka telemetry producer security review

Original review date: 2026-11-15. Status rechecked: 2026-11-17. Scope: receiver producer configuration, keyed record format and local Compose broker integration. This is not a complete Kafka penetration test.

## Tested

- Invalid OBU IDs and coordinates are rejected before producer use.
- Real broker integration sent one keyed record to a temporary topic and verified its key and JSON value with a real Kafka consumer.
- Logging middleware excludes OBU IDs and GPS coordinates.

## Findings

### KAFKA-SEC-001 — broker has no authentication or encryption

- Severity: High outside an isolated local development network
- Affected component: Kafka transport and Compose broker.
- Attack: any client with network access to plaintext broker ports can publish spoofed telemetry or read GPS events if topic access is open.
- Evidence: Compose listener uses `PLAINTEXT`; producer config has no SASL/TLS credentials or ACL configuration.
- Reproduction: inspect `docker-compose.yml`; broker exposes ports 9092 and 29092 without auth.
- Root cause: local learning environment is configured for convenience only.
- Fix: pending production profile with TLS/SASL, least-privilege ACLs, secret injection and private network policy.
- Regression test/retest: pending deployment-level security test.
- Remaining risk: complete topic spoofing/observation while this configuration is used beyond local development.

### KAFKA-SEC-002 — broker payload identity remains trusted

- Severity: Medium; high if the broker is reachable by untrusted producers
- Affected component: WebSocket receiver and producer record key.
- Attack: a client claims another positive OBU ID; the forged ID determines the Kafka partition key and downstream vehicle association.
- Evidence: the WebSocket ingress now authenticates device credentials, checks its OBU allowlist and derives tenant ID. Kafka itself has no ACL and will accept records from any network-reachable producer.
- Reproduction: publish directly to the unauthenticated local Kafka topic with a forged tenant/OBU payload.
- Fix: prototype WebSocket authentication and OBU scope checks are implemented. Add Kafka TLS/SASL/ACLs so only the receiver and calculator can access authorized topics.
- Regression test/retest: receiver auth tests pass; direct-broker ACL enforcement remains untested and unimplemented.
- Remaining risk: a direct broker writer can spoof tenant identity or poison a partition.

The producer idempotence setting is not a broker authentication or business deduplication control. Device credentials protect the WebSocket ingress only; they do not protect Kafka from direct broker clients. No external scanner, broker ACL attack test, or producer load test was run. The one-message integration test is functional evidence only.

## Consumer processing assessment

Scope: `distance_calculator/consumer.go`, focused unit tests, and the tagged real-broker-to-loopback-aggregator test. This is a local adversarial review, not an external penetration test.

### KAFKA-SEC-003 — poison event can block its Kafka partition

- Severity: Medium (availability)
- Affected component: distance-calculator consumer.
- Attack scenario: a writer with Kafka topic access publishes malformed JSON or an invalid coordinate. The consumer refuses to commit and exits; when restarted, it reads the same event and exits again, preventing later records in that partition from progressing.
- Evidence: `processMessage` returns decode/calculation errors and `run` exits before `CommitMessage`; there is no DLQ or retry classification.
- Reproduction: publish a record whose value is `{` to the subscribed topic and start the calculator; it exits with a decode error and leaves that offset uncommitted.
- Root cause: fail-stop semantics were selected to avoid silently dropping a potentially billable event, before a durable DLQ/replay workflow exists.
- Fix: pending a bounded retry and audited DLQ/replay design with durable event identity and operator alerts.
- Regression test: `TestKafkaConsumerDoesNotCommitMalformedTelemetry` verifies no offset commit. Real poison-event broker recovery is pending.
- Retest: unit test is included in the focused consumer suite; do not interpret as a DLQ or recovery test.
- Remaining risk: partition-wide availability loss until operator intervention; a restart alone does not heal it.

### KAFKA-SEC-004 — ambiguous gRPC outcome and replay safety

- Severity: Medium in PostgreSQL mode; High if financial events are processed in volatile mode
- Affected component: Kafka consumer to authenticated aggregator gRPC boundary.
- Attack/failure scenario: aggregator persists a distance, but the response is lost or the consumer is canceled before observing it. Kafka offset remains uncommitted and replay submits the same distance again.
- Evidence: gRPC result and Kafka offset commit are separate operations. PostgreSQL mode persists tenant-scoped inbox/trip state and a `(tenant_id,event_id)` aggregate ledger; memory mode remains volatile.
- Reproduction: no deterministic fault-injection test yet; the RPC/offset commit window follows from separate systems. A durable tenant/event key makes repeated database writes idempotent.
- Root cause: there is no distributed transaction spanning Kafka and PostgreSQL/gRPC.
- Mitigation: durable tenant-scoped inbox and event ledger make database effects idempotent; replay still remains at-least-once.
- Regression evidence: PostgreSQL integration verifies replay and conflicting event IDs.
- Remaining risk: volatile mode is not restart-safe, and no injected lost-response Kafka integration ran on the 2026-11-17 revision. End-to-end exactly-once is not claimed.
