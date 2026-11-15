# Data receiver

The receiver accepts vehicle telemetry over WebSocket and publishes validated events to Kafka. It listens on port `30000` at `/ws`.

## Telemetry contract

Send one text frame per point:

```json
{"obuID":12345,"lat":23.7806,"long":90.4074}
```

`obuID` must be positive; latitude must be finite and within `[-90, 90]`; longitude must be finite and within `[-180, 180]`. The receiver rejects unknown/missing JSON fields, non-text frames, invalid coordinates, and frames larger than 4 KiB. Browser connections must provide a same-origin `Origin`; clients without an Origin header are allowed for native device compatibility.

## Kafka behavior

`KAFKA_BROKERS` selects the broker list (default `localhost:9092`) and `KAFKA_TOPIC` selects the topic (default `obudata`). Events are keyed by the decimal OBU ID so records for a device map to one Kafka partition. The producer uses idempotent producer mode and `acks=all`, then waits up to 15 seconds for the broker delivery result before accepting the next message on that socket.

Producer idempotence does not authenticate devices or deduplicate repeated business events. The local Compose broker is plaintext and has no ACLs; do not expose it outside an isolated development environment.

## Run and verify

Start the local broker with `docker compose up -d`, then run:

```bash
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=obudata go run ./data_receiver
```

Run unit and loopback WebSocket tests:

```bash
GOCACHE=/tmp/fleettoll-go-cache go test ./data_receiver -count=1
```

The real broker delivery test is included in `make integration-kafka`. Tests cover accepted/rejected payloads, frame limits, Origin policy, concurrent connections, log redaction, keyed publication and broker read-back.

## Current security and lifecycle limits

The endpoint has no device authentication, revocation, tenant binding, connection quotas, or enforced TLS. OBU identity is supplied by the client. The receiver process does not yet coordinate graceful shutdown across active WebSockets and Kafka producer flush/close. These items are tracked in `docs/security/WEBSOCKET_REVIEW.md`, `docs/security/KAFKA_REVIEW.md`, and the implementation roadmap.
