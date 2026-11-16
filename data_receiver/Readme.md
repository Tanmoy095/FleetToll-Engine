# Data receiver

The receiver accepts vehicle telemetry over WebSocket and publishes validated events to Kafka. It listens on port `30000` at `/ws`.

## Telemetry contract

Send one text frame per point:

```json
{"obuID":12345,"lat":23.7806,"long":90.4074}
```

`obuID` must be positive; latitude must be finite and within `[-90, 90]`; longitude must be finite and within `[-180, 180]`. The receiver rejects unknown/missing JSON fields, non-text frames, invalid coordinates, and frames larger than 4 KiB. Browser connections must provide a same-origin `Origin`; clients without an Origin header are allowed for native device compatibility.

## Kafka behavior

`KAFKA_BROKERS` selects the broker list (default `localhost:9092`) and `KAFKA_TOPIC` selects the topic (default `obudata`). Authenticated events carry server-assigned tenant identity and use `tenantID:OBUID` as the Kafka key, so each tenant/vehicle pair maps consistently to a partition. The producer uses idempotent producer mode and `acks=all`, then waits up to 15 seconds for the broker delivery result before accepting the next message on that socket.

Producer idempotence does not authenticate devices or deduplicate repeated business events. The local Compose broker is plaintext and has no ACLs; do not expose it outside an isolated development environment.

## Run and verify

Start the local broker with `docker compose up -d`, then run:

```bash
FLEETTOLL_TENANT_API_KEYS="$FLEETTOLL_TENANT_API_KEYS" DATABASE_URL="$DATABASE_URL" KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=obudata go run ./data_receiver
```

Run unit and loopback WebSocket tests:

```bash
GOCACHE=/tmp/fleettoll-go-cache go test ./data_receiver -count=1
```

The real broker delivery test is included in `make integration-kafka`. Tests cover accepted/rejected payloads, frame limits, Origin policy, concurrent connections, log redaction, keyed publication and broker read-back.

## Current security and lifecycle limits

The endpoint now requires device bearer authentication and derives tenant identity from the credential, with an OBU allowlist. Managed revocation/rotation, connection quotas, enforced TLS and graceful shutdown across WebSockets/Kafka remain open. See `docs/security/TENANT_ISOLATION_REVIEW.md` and `docs/security/KAFKA_REVIEW.md`.
