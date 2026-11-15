# ADR-017: Keep WebSocket state local to each connection

- Status: Accepted
- Date: 2026-11-15

## Context

The receiver stored a single `*websocket.Conn` on `DataReceiver` and launched a detached read goroutine. A second client could overwrite that connection, and read failures caused a tight loop that eventually panicked with Gorilla WebSocket's repeated-read guard. Frames were not bounded and JSON values were not validated before Kafka publication.

## Problem and requirements

Accept multiple OBU device connections without cross-routing data; validate each message before publishing; bound memory/resource use per frame; close connections on invalid payloads or producer failure; preserve `/ws` and the existing JSON fields; avoid logging GPS coordinates or device IDs.

## Alternatives considered

1. Retain the shared connection and lock it. Rejected because a lock does not provide per-connection ownership and would serialize unrelated devices.
2. Keep one detached goroutine per connection. Rejected because lifecycle ownership becomes unclear and the old loop leaked after disconnect.
3. Handle reads in the HTTP handler goroutine, with local connection state. Selected: the net/http server owns handler lifecycle and the handler returns when the client disconnects.
4. Decode directly into `OBUData`. Rejected because absent numeric JSON fields become zero, which is a valid latitude/longitude; pointer fields preserve missing-versus-zero distinction.

## Decision

`DataReceiver` stores only the shared producer. Each `handleWS` invocation upgrades one request, sets a 4 KiB read limit, then synchronously reads, decodes and validates messages until the socket ends. JSON decoding rejects unknown/trailing values and requires `obuID`, `lat`, and `long`. Coordinate and ID bounds are checked before calling the producer. Bad frames are closed with a policy/invalid-payload code; producer failure closes with 1011. Origin must match request host and transport scheme; clients without an Origin header (native OBU clients) are allowed. Logs omit device ID and coordinates.

## Benefits and trade-offs

Connection ownership is easy to reason about, concurrent devices no longer overwrite one another, and the HTTP handler ends with the socket. The 4 KiB cap bounds one frame, not total connection count, message rate, or aggregate memory. There is no application acknowledgment proving Kafka broker delivery; current producer interface only returns enqueue errors.

## Security implications

Origin checking mitigates browser cross-site WebSocket use but is not authentication. Any native client can omit Origin and claim any positive OBU ID. Authentication, device revocation, rate limiting, TLS, and tenant binding remain open. GPS values and device IDs are removed from logs.

## Operational and testing implications

Tests use a real loopback WebSocket handshake/frame exchange via `httptest.Server`, with an in-memory recording producer. This verifies protocol and validation boundaries but not Kafka delivery, TLS termination, multi-process behavior, or sustained connection capacity. Race tests exercise simultaneous connections.

## Reconsider this decision when

Introducing device authentication, a bounded connection manager, broker-delivery acknowledgments, or a different WebSocket library. Revisit frame size and close/error policy with a versioned client protocol.
