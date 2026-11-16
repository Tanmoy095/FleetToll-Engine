# ADR-020: Commit telemetry offsets only after downstream aggregation

- Status: Accepted for the current increment; durable processing remains incomplete
- Date: 2026-11-15

## Context

The distance calculator consumes GPS telemetry, updates per-OBU distance state and sends a distance to the aggregator over HTTP. The original consumer enabled Kafka's default automatic offset commits, continued after processing errors and could acknowledge records whose downstream operation had failed. It also used a hard-coded broker and had no cancellation path.

## Problem and requirements

An event must not be acknowledged until the aggregator returns success. Consumer shutdown must stop polling promptly and close the Kafka client. Broker, decode, calculation, aggregator and commit failures must be visible and must stop this process rather than silently advance past work. The behavior must be testable without pretending a fake is a real Kafka integration test.

## Alternatives considered

1. Keep auto-commit. Rejected because the broker can commit an offset before processing finishes.
2. Commit before the aggregator call. Rejected because an HTTP failure would lose the event.
3. Continue after a failed event and commit later offsets. Rejected because a later offset commit can acknowledge the failed earlier event in the same partition.
4. Retry HTTP in an unbounded loop. Rejected for this increment because it can hold a partition indefinitely and has no retry budget, DLQ or idempotency key.
5. Stop without committing on processing failure. Selected as a fail-stop boundary: after process restart/group reassignment, Kafka can redeliver from the last committed offset. A poison event can block its partition until operator action; a DLQ policy is a follow-up.

## Decision

`newKafkaConsumer` uses `enable.auto.commit=false`, a configurable broker address and group ID, and earliest offset reset for a new group. The consumer polls with a bounded timeout, processes one record, sends the distance using the run context, and calls `CommitMessage` only after a successful aggregator response. Any decode, calculation, downstream or commit error terminates the loop with topic/partition/offset context. `Start(ctx)` owns and closes the consumer. The executable derives cancellation from SIGINT/SIGTERM.

Unit tests inject only the small `kafkaMessageReader` surface to verify commit ordering and failure behavior. A separate integration-tagged test uses a real Kafka topic and a loopback HTTP aggregator to verify two telemetry points produce the expected initial and Haversine distances.

## Consistency limits

This is not end-to-end exactly-once processing. The aggregator can accept a request while the HTTP response is lost, causing Kafka redelivery and duplicate distance aggregation. The calculator's prior-point map is volatile and is updated before HTTP succeeds; a failed request followed by retry can calculate from an already-advanced in-memory point, and a process restart loses all vehicle state. Those are correctness gaps. Durable trip state, event identity/inbox deduplication, and an idempotent downstream write must be designed together with PostgreSQL before claiming crash-safe processing.

## Benefits and trade-offs

Successful downstream writes are followed by an explicit group offset commit. Failures are observable and do not cause the consumer to march past unprocessed records. The fail-stop policy reduces throughput during an outage, and one poison record can stop progress on its partition. A response timeout leaves the write outcome ambiguous.

## Security implications

Kafka payload identity is still trusted and broker transport is still plaintext without authentication or ACLs in the local Compose environment. A producer with topic access can spoof OBU IDs. This decision originally added no tenant or device authentication; prototype device auth and tenant scoping were added later in ADR-023. Kafka broker authentication/ACLs remain open.

## Operational and testing implications

The process should be restarted only after downstream recovery, with the current in-memory state limitation understood. `KAFKA_BROKERS`, `KAFKA_TOPIC`, and `KAFKA_CONSUMER_GROUP` configure local runtime values. The real broker integration uses a unique group and temporary topic. No consumer throughput, lag, retry, DLQ, failover or load measurement is claimed.

## Consequences and reconsideration

The consumer now makes offset advancement explicit and has a bounded cancellation delay of one poll timeout. Revisit this ADR when durable state and event idempotency are available, and add bounded retries plus a DLQ/inbox policy that can distinguish poison data from transient dependency failures without losing financial events.
