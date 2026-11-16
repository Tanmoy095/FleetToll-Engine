# ADR-018: Key telemetry by tenant/OBU and wait for Kafka delivery

- Status: Accepted
- Date: 2026-11-15

## Context

The data receiver's Kafka producer used a hard-coded `localhost`, sent no message key, discarded delivery reports, and returned as soon as a record entered the local producer queue. Per-vehicle processing needs messages for one tenant/OBU pair to map to the same Kafka partition so their order can be preserved within that partition. This ADR originally used OBU ID alone; ADR-023 extended the key to `tenantID:OBUID` because OBU IDs are tenant-local.

## Problem and requirements

Make the broker and topic configurable, preserve per-tenant/OBU partition affinity, configure Kafka's idempotent producer mode, expose broker delivery failures to the caller, bound how long the WebSocket handler waits, and provide a real broker integration test.

## Alternatives considered

1. Keep fire-and-forget `Produce` and rely on buffering. Rejected because local queue acceptance is not broker delivery and errors could be silently lost.
2. Key by a random event ID. Rejected for this stream because it would scatter one vehicle's events across partitions.
3. Key by tenant ID plus OBU ID. Selected because OBU IDs are tenant-local; this preserves per-vehicle ordering within a tenant and parallelism across tenant/vehicle pairs.
4. Enable auto-topic creation. Rejected; topics should be provisioned explicitly by infrastructure.
5. Claim end-to-end exactly-once. Rejected: producer idempotence covers producer retries within a producer session, not replayed business events, consumer processing, or downstream HTTP/database writes.

## Decision

The producer reads `KAFKA_BROKERS` (default `localhost:9092`) and the receiver reads `KAFKA_TOPIC` (default `obudata`). The original implementation used OBU ID as the key. The tenant-auth revision now uses `tenantID:OBUID`, `enable.idempotence=true`, `acks=all`, and a 15-second message/delivery timeout. `ProduceData` waits for its per-record delivery event and returns an error on enqueue, broker failure or timeout. A background event reader services producer events; `Close` flushes and closes the client. The real-broker integration test creates a temporary topic and verifies the record key and JSON value with a Kafka consumer.

## Benefits and trade-offs

Per-tenant/OBU ordering is possible within the selected partition, and callers observe actual broker delivery outcomes. Waiting synchronously provides natural per-connection backpressure but reduces throughput per socket and can block for up to 15 seconds. A timeout can be ambiguous: the broker may have accepted a record even if the acknowledgment was delayed. Device retries can still produce business duplicates.

## Security implications

The local Compose broker uses plaintext and has no ACLs or authentication. The producer sends GPS telemetry. Production requires authenticated encrypted Kafka transport, network restrictions, broker ACLs and secret management. At the time of this ADR the record key was derived from client-supplied OBU ID. Device authentication now binds the connection to a tenant and checks the OBU allowlist; direct Kafka writes remain untrusted until broker ACLs are configured.

## Operational and testing implications

`make integration-kafka` uses the `integration` build tag and `KAFKA_TEST_BROKERS`; it creates/deletes a unique topic, publishes a test point and reads it back. The local Compose test passed on the original revision. Kafka integration was unavailable on 2026-11-17, so this gate must be rerun after the tenant-key change; producer-to-broker delivery does not verify consumer offset policy or end-to-end processing.

## Reconsider this decision when

Changing partition count/strategy, moving to a managed Kafka service, introducing a schema registry, or replacing synchronous delivery wait with a bounded asynchronous queue and explicit device acknowledgment protocol.
