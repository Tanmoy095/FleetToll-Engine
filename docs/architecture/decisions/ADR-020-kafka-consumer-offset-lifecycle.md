# ADR-020: Commit telemetry offsets only after downstream aggregation

- Status: Accepted; historical snapshot updated by ADR-021 and ADR-023
- Date: 2026-11-15

## Context

At the time of this decision, the distance calculator consumed GPS telemetry, updated per-OBU distance state and sent distance to the aggregator over HTTP. The current path uses tenant-scoped PostgreSQL state and authenticated gRPC. The original consumer enabled Kafka's default automatic offset commits, continued after processing errors and could acknowledge records whose downstream operation had failed. It also used a hard-coded broker and had no cancellation path.

## Problem and requirements

An event must not be acknowledged until authenticated gRPC aggregation returns success. Consumer shutdown must stop polling promptly and close the Kafka client. Broker, decode, calculation, aggregator and commit failures must be visible and must stop this process rather than silently advance past work. The behavior must be testable without pretending a fake is a real Kafka integration test.

## Alternatives considered

1. Keep auto-commit. Rejected because the broker can commit an offset before processing finishes.
2. Commit before the aggregator call. Rejected because an HTTP failure would lose the event.
3. Continue after a failed event and commit later offsets. Rejected because a later offset commit can acknowledge the failed earlier event in the same partition.
4. Retry gRPC in an unbounded loop. Rejected for this increment because it can hold a partition indefinitely and has no retry budget, DLQ or idempotency key.
5. Stop without committing on processing failure. Selected as a fail-stop boundary: after process restart/group reassignment, Kafka can redeliver from the last committed offset. A poison event can block its partition until operator action; a DLQ policy is a follow-up.

## Decision

`newKafkaConsumer` uses `enable.auto.commit=false`, a configurable broker address and group ID, and earliest offset reset for a new group. The consumer polls with a bounded timeout, processes one record, sends the tenant-qualified distance through authenticated gRPC using the run context, and calls `CommitMessage` only after a successful aggregator response. Any decode, calculation, downstream or commit error terminates the loop with topic/partition/offset context. `Start(ctx)` owns and closes the consumer. The executable derives cancellation from SIGINT/SIGTERM.

Unit tests inject only the small `kafkaMessageReader` surface to verify commit ordering and failure behavior. A separate integration-tagged test uses a real Kafka topic and the gRPC client path to verify two telemetry points produce the expected initial and Haversine distances. The latest 2026-11-17 Kafka integration was not run because no broker was listening.

## Consistency limits

This is not end-to-end exactly-once processing. The aggregator can accept a gRPC request while its response is lost, causing Kafka redelivery. In PostgreSQL mode the tenant-scoped inbox/trip-state transaction returns the saved distance, and the `(tenant_id,event_id)` aggregate ledger prevents a duplicate total. Memory mode remains volatile and is not restart-safe. Kafka offset, calculator transaction and aggregator write are separate commit boundaries.

## Benefits and trade-offs

Successful downstream writes are followed by an explicit group offset commit. Failures are observable and do not cause the consumer to march past unprocessed records. The fail-stop policy reduces throughput during an outage, and one poison record can stop progress on its partition. A response timeout leaves the write outcome ambiguous.

## Security implications

Direct Kafka payload identity remains trusted and broker transport is plaintext without authentication or ACLs in the local Compose environment. A producer with topic access can spoof tenant/OBU IDs. This decision originally added no tenant or device authentication; prototype device auth and tenant scoping were added later in ADR-023. Kafka broker authentication/ACLs remain open.

## Operational and testing implications

The process should be restarted only after downstream recovery, with the current in-memory state limitation understood. `KAFKA_BROKERS`, `KAFKA_TOPIC`, and `KAFKA_CONSUMER_GROUP` configure local runtime values. The real broker integration uses a unique group and temporary topic. No consumer throughput, lag, retry, DLQ, failover or load measurement is claimed.

## Consequences and reconsideration

The consumer now makes offset advancement explicit and has a bounded cancellation delay of one poll timeout. Revisit this ADR when durable state and event idempotency are available, and add bounded retries plus a DLQ/inbox policy that can distinguish poison data from transient dependency failures without losing financial events.
