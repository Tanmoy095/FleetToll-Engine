# Documentation guide

This index describes what each documentation area covers. Current implementation status is maintained in the root README and `IMPLEMENTATION_ROADMAP.md`. Older ADRs and feature notes preserve the reasoning/evidence from the date they were written; they now link to the later decisions that supersede them.

## Start here

- [Project overview, setup, configuration, verification, status and roadmap](../README.md)
- [Implementation roadmap and latest verification gates](IMPLEMENTATION_ROADMAP.md)
- [High-level architecture](architecture/HLD.md)
- [Low-level design and current contracts](architecture/LLD.md)
- [Tenant isolation and authentication decision](architecture/decisions/ADR-023-tenant-isolation-and-auth.md)

## Architecture decisions

- [HTTP validation boundary](architecture/decisions/ADR-016-aggregator-http-boundary.md)
- [WebSocket connection lifecycle](architecture/decisions/ADR-017-websocket-connection-isolation.md)
- [Tenant/OBU Kafka key](architecture/decisions/ADR-018-keyed-kafka-telemetry-publishing.md)
- [Legacy HTTP client](architecture/decisions/ADR-019-context-aware-aggregator-client.md)
- [Kafka offset lifecycle](architecture/decisions/ADR-020-kafka-consumer-offset-lifecycle.md)
- [PostgreSQL replay safety](architecture/decisions/ADR-021-postgres-inbox-and-idempotent-distance-ledger.md)
- [Protobuf/gRPC contract](architecture/decisions/ADR-022-internal-grpc-aggregator.md)
- [Tenant isolation/auth](architecture/decisions/ADR-023-tenant-isolation-and-auth.md)

## Learning and assignments

- [Feature learning guide](learning/FEATURE_LEARNING_GUIDE.md)
- [Testing and TDD evidence](learning/TESTING_AND_TDD.md)
- [Foundational and tenant-isolation exercises](learning/NEXT_ENGINEERING_EXERCISES.md)
- [Feature 02: aggregator HTTP boundary](learning/features/02-aggregator-http-boundary.md)
- [Feature 03: WebSocket ingestion](learning/features/03-websocket-ingestion.md)
- [Feature 04: Kafka producer](learning/features/04-kafka-producer.md)
- [Feature 05: legacy HTTP client](learning/features/05-distance-aggregator-http-client.md)
- [Feature 06: Kafka consumer lifecycle](learning/features/06-kafka-consumer-lifecycle.md)
- [Feature 07: PostgreSQL replay safety](learning/features/07-postgres-replay-safety.md)
- [Feature 08: protobuf/gRPC](learning/features/08-protobuf-grpc-aggregator.md)
- [Feature 09: tenant isolation](learning/features/09-tenant-isolation.md)

The detailed interview assignment is private and intentionally stored outside this repository. Do not copy your answer notes into public project documentation.

## Security and performance evidence

- [HTTP API review](security/HTTP_API_REVIEW.md)
- [WebSocket review](security/WEBSOCKET_REVIEW.md)
- [Kafka review](security/KAFKA_REVIEW.md)
- [PostgreSQL replay review](security/POSTGRES_REPLAY_REVIEW.md)
- [gRPC review](security/GRPC_AGGREGATOR_REVIEW.md)
- [Current tenant-isolation review](security/TENANT_ISOLATION_REVIEW.md)
- [Historical PostgreSQL-backed HTTP probe](performance/AGGREGATE_API_LOCAL_LOAD.md)
- [Historical PostgreSQL-backed gRPC probe](performance/GRPC_AGGREGATOR_LOCAL_LOAD.md)
- [Latest authenticated in-memory gRPC probe](performance/GRPC_TENANT_AUTH_MEMORY_LOAD.md)

Security reviews are code-level engineering reviews, not external audits. Performance reports state the store mode and workload; do not compare unlike runs as if they represent the same capacity.
