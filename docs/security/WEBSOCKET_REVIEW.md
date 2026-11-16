# WebSocket ingestion security review

Original review date: 2026-11-15. Status rechecked: 2026-11-17. Scope: `/ws` handshake and telemetry validation in the local receiver. This is a code review with local protocol tests, not an external penetration test.

## Tested

- Browser Origin from another host: rejected with HTTP 403.
- Invalid/out-of-range coordinates and missing fields: connection closed before producer invocation.
- Unknown JSON fields: rejected.
- Frame larger than 4 KiB: rejected by the WebSocket read limit.
- Simultaneous connections: each connection's valid message reaches the producer independently.
- Sensitive logging: tests confirm OBU ID and latitude/longitude are not logged by producer middleware.

The tests use a recording producer, not a Kafka broker. They prove receiver protocol behavior only.

## Open findings

### WS-AUTH-001 — unauthenticated device identity (fixed in prototype)

- Severity: High
- Affected component: `/ws`
- Attack: connect without an Origin header and send a payload with any positive `obuID`; the receiver forwards it as telemetry for that device.
- Evidence: valid-message integration test completes a WebSocket handshake without credentials; `handleWS` has no identity check.
- Reproduction: connect to `ws://<receiver>/ws` without Authorization, then send `{"obuID":7,"lat":51.2,"long":4.4}`.
- Root cause: device registration/authentication is not implemented.
- Fix: prototype device credentials now bind to a tenant and OBU allowlist. Receiver derives tenant ID from principal and rejects client tenant fields.
- Regression test and retest: `TestWebSocketAuthenticatesTenantAndScopesOBU` verifies missing auth, two tenants sharing an OBU ID, and out-of-scope identity; receiver tests pass.
- Remaining risk: rotation/revocation, TLS, quotas and managed enrollment remain absent; see `TENANT_ISOLATION_REVIEW.md`.

### WS-RESOURCE-002 — no connection or message-rate quotas

- Severity: Medium
- Affected component: `/ws`
- Attack: open many valid connections or send valid small frames at an unbounded rate to consume goroutines, sockets, CPU and producer queue capacity.
- Evidence: source has a per-frame size cap but no concurrent connection limit or rate limiter.
- Reproduction: pending measured resource test; identified by source inspection, not claimed as dynamically executed.
- Fix: pending bounded connection semaphore, per-device/global token bucket, producer backpressure policy and metrics.
- Retest: pending.
- Remaining risk: distributed clients may exhaust the receiver before limits are applied.

### WS-TRANSPORT-003 — plaintext local listener

- Severity: Medium
- Affected component: receiver listener `:30000`
- Attack: on an untrusted network path, observe or modify GPS telemetry because the listener uses HTTP upgrade without TLS.
- Evidence: `http.Server.ListenAndServe` is used, not TLS; deployment TLS termination is not configured/documented as enforced.
- Fix: terminate TLS at a trusted ingress or configure TLS and authenticated service discovery; document secure deployment.
- Retest: pending.
- Remaining risk: GPS confidentiality and integrity on shared networks.

Origin validation is a browser cross-site defense only. Missing Origin is allowed for native clients, so it cannot substitute for authentication. No SQL layer exists in this feature. No external network scan or vulnerability scanner was run.


The unauthenticated finding above is historical and fixed in the prototype. Review [TENANT_ISOLATION_REVIEW.md](TENANT_ISOLATION_REVIEW.md) for current controls and open risks.
