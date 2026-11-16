# FleetToll learning checkpoint

This file contains foundational retrospective exercises from earlier implementation stages. PostgreSQL, protobuf/gRPC, and tenant isolation are now implemented. Start with the current tenant isolation exercise at the end, then use the earlier prompts to test whether you can explain the full delivery path and its failure windows.

## Exercise 1 — Trace one telemetry record

Read these files:

- `data_receiver/main.go`
- `data_receiver/producer.go`
- `distance_calculator/consumer.go`
- `distance_calculator/service.go`
- `distance_calculator/client/grpc.go`
- `aggregator/service.go`
- `aggregator/store.go`

Write a short sequence diagram or numbered trace from a WebSocket frame through `CommitMessage`. Then answer in your own words:

1. Which exact tenant/OBU value becomes the Kafka key, and what ordering does that preserve?
2. Why is the first point for an OBU assigned zero distance?
3. At what point does calculator state commit relative to the gRPC call and Kafka offset?
4. What can happen if the aggregator stores a distance but its gRPC response is lost?
5. What is lost if the distance-calculator process restarts after a committed record?
6. Why does turning off Kafka auto-commit not create a transaction spanning Kafka and the aggregator?
7. What would a malformed event do to its partition today, and what is missing to recover safely?
8. Name the tests that cover commit ordering and the real Kafka integration scenario.

Deliverable: a one-page explanation plus one failure timeline for “aggregator accepted the gRPC write, client timed out before receiving the response.” Be precise about what the current code proves and what it does not prove.

## Exercise 2 — Sketch durable state before coding it

Retrospective: sketch the PostgreSQL transaction now used for one telemetry event and compare your design against `distance_calculator/postgres_service.go`. Identify:

- a stable event identity used by an inbox/unique constraint;
- the tenant and vehicle identity obtained from trusted server-side records;
- the row that stores the previous accepted point;
- the business rule for out-of-order timestamps;
- how the event inbox and trip-state update commit atomically;
- how an RPC retry can be made idempotent;
- what response or event is produced when the event is a duplicate.

Do not write implementation code yet. The schema and transaction boundary must be reviewed before migrations are added. The future implementation will then use RED → GREEN → REFACTOR and a real PostgreSQL/PostGIS integration test.

## Mentor review checklist

Before moving on, be able to explain why “commit Kafka after gRPC succeeds” is useful but insufficient, and why durable state plus event deduplication must be designed together. The private Session 5 assignment focuses on tenant-scoped migration and authorization rather than implementing PostgreSQL from scratch.

## Exercise 3 — Tenant isolation and authorization

The implementation now includes tenant identity across the receiver, Kafka, calculator state, protobuf/gRPC and aggregator storage. Before changing it, complete the private **Session 5 — Tenant isolation, authorization and migration review** in your personal practice file. Do not commit your answers. Then independently add a real PostgreSQL test for same OBU/event IDs in two tenants, an invoice authorization test, and a focused Go map/slice grouping exercise. Your explanation must cover the shared token trust boundary, the tenant-1 legacy fallback, composite keys, migration locking, and a production identity/rotation design. See [Feature 09](features/09-tenant-isolation.md).
