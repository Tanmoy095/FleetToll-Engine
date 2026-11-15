# ADR-018: Key telemetry by OBU and wait for Kafka delivery

- Status: Accepted
- Date: 2026-11-15

## Context

The data receiver's Kafka producer used a hard-coded `localhost`, sent no message key, discarded delivery reports, and returned as soon as a record entered the local producer queue. Per-vehicle processing needs messages for one OBU to map to the same Kafka partition so their order can be preserved within that partition.

## Problem and requirements

Make the broker and topic configurable, preserve per-OBU partition affinity, configure Kafka's idempotent producer mode, expose broker delivery failures to the caller, bound how long the WebSocket handler waits, and provide a real broker integration test.

## Alternatives considered

1. Keep fire-and-forget `Produce` and rely on buffering. Rejected because local queue acceptance is not broker delivery and errors could be silently lost.
2. Key by a random event ID. Rejected for this stream because it would scatter one vehicle's events across partitions.
3. Key by OBU ID. Selected because it gives partition affinity and ordering per device while retaining parallelism across devices.
4. Enable auto-topic creation. Rejected; topics should be provisioned explicitly by infrastructure.
5. Claim end-to-end exactly-once. Rejected: producer idempotence covers producer retries within a producer session, not replayed business events, consumer processing, or downstream HTTP/database writes.

## Decision

The producer reads `KAFKA_BROKERS` (default `localhost:9092`) and the receiver reads `KAFKA_TOPIC` (default `obudata`). It uses OBU ID as the Kafka record key, `enable.idempotence=true`, `acks=all`, and a 15-second message/delivery timeout. `ProduceData` waits for its per-record delivery event and returns an error on enqueue, broker failure or timeout. A background event reader services producer events; `Close` flushes and closes the client. The real-broker integration test creates a temporary topic and verifies the record key and JSON value with a Kafka consumer.

## Benefits and trade-offs

Per-OBU ordering is possible within the selected partition, and callers observe actual broker delivery outcomes. Waiting synchronously provides natural per-connection backpressure but reduces throughput per socket and can block for up to 15 seconds. A timeout can be ambiguous: the broker may have accepted a record even if the acknowledgment was delayed. Device retries can still produce business duplicates.

## Security implications

The local Compose broker uses plaintext and has no ACLs or authentication. The producer sends GPS telemetry. Production requires authenticated encrypted Kafka transport, network restrictions, broker ACLs and secret management. The record key is derived from client-supplied OBU ID until authenticated device identity is implemented.

## Operational and testing implications

`make integration-kafka` uses the `integration` build tag and `KAFKA_TEST_BROKERS`; it creates/deletes a unique topic, publishes a test point and reads it back. The local Compose test passed. This verifies only producer-to-broker delivery; consumer offset policy and end-to-end processing are separate work.

## Reconsider this decision when

Changing partition count/strategy, moving to a managed Kafka service, introducing a schema registry, or replacing synchronous delivery wait with a bounded asynchronous queue and explicit device acknowledgment protocol.
