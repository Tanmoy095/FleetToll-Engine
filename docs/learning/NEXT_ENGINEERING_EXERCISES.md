# FleetToll learning checkpoint

Complete the first exercise before starting the PostgreSQL feature. The goal is to make the current delivery boundary understandable enough that the next persistence design solves the actual failure, rather than adding a database without a consistency model.

## Exercise 1 — Trace one telemetry record

Read these files:

- `data_receiver/main.go`
- `data_receiver/producer.go`
- `distance_calculator/consumer.go`
- `distance_calculator/service.go`
- `distance_calculator/client/client.go`
- `aggregator/service.go`
- `aggregator/store.go`

Write a short sequence diagram or numbered trace from a WebSocket frame through `CommitMessage`. Then answer in your own words:

1. Which exact value becomes the Kafka key, and what ordering does that preserve?
2. Why is the first point for an OBU assigned zero distance?
3. At what line does calculator state change relative to the HTTP request?
4. What can happen if the aggregator stores a distance but its HTTP response is lost?
5. What is lost if the distance-calculator process restarts after a committed record?
6. Why does turning off Kafka auto-commit not create a transaction spanning Kafka and the aggregator?
7. What would a malformed event do to its partition today, and what is missing to recover safely?
8. Name the tests that cover commit ordering and the real Kafka integration scenario.

Deliverable: a one-page explanation plus one failure timeline for “aggregator accepted the HTTP write, client timed out before receiving the response.” Be precise about what the current code proves and what it does not prove.

## Exercise 2 — Sketch durable state before coding it

After the trace is reviewed, sketch a PostgreSQL transaction for one telemetry event. Identify:

- a stable event identity used by an inbox/unique constraint;
- the tenant and vehicle identity obtained from trusted server-side records;
- the row that stores the previous accepted point;
- the business rule for out-of-order timestamps;
- how the event inbox and trip-state update commit atomically;
- how an HTTP retry can be made idempotent;
- what response or event is produced when the event is a duplicate.

Do not write implementation code yet. The schema and transaction boundary must be reviewed before migrations are added. The future implementation will then use RED → GREEN → REFACTOR and a real PostgreSQL/PostGIS integration test.

## Mentor review checklist

Before moving on, be able to explain why “commit Kafka after HTTP succeeds” is useful but insufficient, and why durable state plus event deduplication must be designed together. The project roadmap puts PostgreSQL next, but the implementation begins after this design checkpoint is understood.
