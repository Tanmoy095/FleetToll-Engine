# Aggregate API local concurrency measurement

**Date:** 2026-11-15  
**Environment:** local macOS development machine, Go service on loopback, PostgreSQL 15 in Docker Desktop on loopback port 15432. This is a development measurement only.

## Workload and command

`scripts/aggregate_load.go` starts 1,000 goroutines behind one release barrier. Each sends one unique aggregate event ID to the PostgreSQL-backed `POST /aggregate`, then the probe records response latency, throughput and errors. It does not model sustained traffic, Kafka ingestion, WebSocket connections, warm-up, multiple tenants or a production network.

```bash
DATABASE_URL='postgres://fleettoll:fleetdev@localhost:15432/fleettoll?sslmode=disable' \
  go run ./aggregator -listenaddr 127.0.0.1:13000
GOCACHE=/tmp/fleettoll-go-cache \
  go run ./scripts/aggregate_load.go -url http://127.0.0.1:13000/aggregate -users 1000
```

## Results

| Run | Requests / concurrency | Throughput | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Initial implementation, explicit `BEGIN` + insert + `COMMIT` for each new event | 1,000 / 1,000 | 99.1 req/s | 6.530 s | 10.001 s | 10.006 s | 53 timeouts |
| After reducing the normal insert path to one parameterized autocommit statement (optimization check) | 1,000 / 1,000 | 447.9 req/s | 1.410 s | 2.110 s | 2.178 s | 0 |
| Final code confirmation before versioned migration runner | 1,000 / 1,000 | 398.0 req/s | 1.184 s | 2.366 s | 2.486 s | 0 |
| Final code with versioned migration runner | 1,000 / 1,000 | 1,911.2 req/s | 278 ms | 456 ms | 518 ms | 0 |

The initial result failed the client’s 10-second timeout for 53 requests. Code inspection showed every normal unique insert incurred an explicit transaction begin, insert, and commit round trip. The aggregate row is a single independent insert protected by its unique primary key, so the explicit transaction was unnecessary on the common path. The revised store uses one parameterized `INSERT ... ON CONFLICT DO NOTHING`; it only queries the stored row on a duplicate to detect mismatched reuse. Later 1,000-request runs completed without request errors, but latency varied significantly between runs. The latest run used the final versioned-migration build and is recorded separately; treat every figure as a local observation, not a benchmark claim.

## Interpretation and next engineering work

The one-shot local result is evidence that this version accepted 1,000 simultaneous request attempts, not a capacity guarantee. p95 at about 2.1 seconds is high for a production API. The Docker Desktop network/database and local machine are in the measured path, and this probe does not isolate their costs. Next, capture PostgreSQL pool wait/active counts, database CPU, query latency and Go CPU/memory profiles; add sustained arrivals and a realistic mix of OBU keys. Tune the pool only after those measurements. Do not extrapolate this result to 1,000 WebSocket connections.

No query-plan optimization is claimed here. The invoice query filters by `obu_id` and sums matching rows; the migration adds a B-tree index for that filter. Run `EXPLAIN (ANALYZE, BUFFERS)` on a representative dataset before claiming the index improves a production query plan.
