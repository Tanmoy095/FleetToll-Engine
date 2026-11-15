# Kafka telemetry producer security review

Date: 2026-11-15. Scope: receiver producer configuration, keyed record format and local Compose broker integration. This is not a complete Kafka penetration test.

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

### KAFKA-SEC-002 — key identity is client-supplied

- Severity: High until device authentication is implemented
- Affected component: WebSocket receiver and producer record key.
- Attack: a client claims another positive OBU ID; the forged ID determines the Kafka partition key and downstream vehicle association.
- Evidence: current WebSocket JSON includes `obuID`; no device credential or registry maps the connection to an authorized vehicle.
- Reproduction: connect without credentials and send a valid payload using an arbitrary positive ID; the receiver produces the record.
- Fix: pending authenticated device identity; derive record key from verified server-side device registration, not payload.
- Regression test/retest: pending identity feature.
- Remaining risk: cross-vehicle data contamination and billing manipulation.

The producer idempotence setting is not an authentication or business deduplication control. No external scanner, broker ACL attack test, or producer load test was run. The one-message integration test is functional evidence only.

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

### KAFKA-SEC-004 — ambiguous HTTP outcome permits duplicate aggregation

- Severity: High for financial correctness until idempotency exists
- Affected component: Kafka consumer to aggregator HTTP boundary.
- Attack/failure scenario: aggregator persists a distance, but the response is lost or the consumer is canceled before observing it. Kafka offset remains uncommitted and replay submits the same distance again.
- Evidence: HTTP request and Kafka offset commit are separate operations; aggregator has no event-id deduplication. The Haversine state and aggregator totals are in-memory only.
- Reproduction: no deterministic fault-injection test yet; the duplicate window follows directly from commit-after-HTTP ordering and the lack of an idempotency key.
- Root cause: no durable event identity/inbox and no idempotent aggregation API.
- Fix: pending durable trip/invoice persistence and an event ID that is transactionally deduplicated at the write boundary.
- Regression test/retest: pending; current tests prove commit is withheld on observed HTTP failure, not that an accepted-but-unacknowledged request is safe.
- Remaining risk: distance totals can duplicate or be lost across process restart. End-to-end exactly-once is not claimed.
