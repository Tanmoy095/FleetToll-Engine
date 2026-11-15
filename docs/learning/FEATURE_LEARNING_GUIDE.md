# Feature learning guide

## Distance calculation: Haversine and per-vehicle state

### Business behavior

Each valid GPS point is compared only with the previous accepted point for that same OBU. The OBU ID must be positive. The first point initializes state and contributes zero kilometers. Subsequent points add great-circle distance in kilometers, using a mean Earth radius of 6,371.0088 km. A latitude outside `[-90, 90]`, longitude outside `[-180, 180]`, NaN, or infinity is rejected before state changes.

### Why Haversine

Latitude and longitude are angles, not planar distances. A one-degree longitude change is about 111 km at the equator, but progressively shorter toward the poles. Haversine converts the angular difference to an arc along a spherical Earth; it is suitable for this platform's distance accumulation and avoids treating degrees as kilometers. It remains a spherical approximation, not ellipsoidal survey-grade geodesy.

### Go concepts in this implementation

- `map[int]coordinate` associates each OBU ID with its most recent accepted point. A single slice shared by all vehicles caused cross-vehicle contamination.
- `sync.Mutex` protects both lookup and update as one critical section. Without it concurrent Kafka processing could race or overwrite state inconsistently.
- Validation occurs before taking the lock and before state mutation, so rejected input cannot poison the next distance calculation.
- `math.Atan2` computes the central angle robustly. The Haversine intermediate is clamped to `[0, 1]` to account for floating-point rounding near antipodal points.
- The algorithm is O(1) per GPS point and retains O(number of active OBUs) in-memory state. State is currently lost on restart; durable trip state is a separate unfinished phase.

### Test evidence

`distance_calculator/service_test.go` covers first-point zero distance, same-position zero distance, a known one-degree equatorial arc, interleaved vehicles, invalid coordinate boundaries, non-finite input, and concurrent independent vehicle initialization. Focused RED/GREEN and race-test results are recorded in `TESTING_AND_TDD.md`.

### Current limitation

The calculation service has no trip expiry, late-event policy, event-time ordering, persistence, or tenant-aware identity. It assumes each incoming point is the next point for that OBU. Kafka replay can therefore repeat state transitions unless the consumer and state persistence gain idempotency. Basic malicious-input probes and race detection passed, but Kafka authentication, device identity binding, tenant authorization, state bounds, infrastructure integration, and external penetration testing remain outstanding.

# Testing and TDD

## Haversine distance slice

### RED

Command: `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./distance_calculator -run 'TestCalculatorService' -count=1`

Expected: failures demonstrate incorrect degree-as-distance behavior, cross-OBU state, and acceptance of invalid coordinates.

Actual: failed as expected. One degree along the equator returned `1` instead of about `111.1949266` km; interleaved OBU state returned `9` instead of `111.1949266` km; all invalid coordinate cases were accepted. `-vet=off` isolated the intended test failures from an unrelated pre-existing vet failure in the consumer logger.

A separate adversarial regression test was then added for OBU IDs `0` and `-1`.

Command: `GOCACHE=/tmp/fleettoll-go-cache go test -vet=off ./distance_calculator -run 'TestCalculatorServiceRejectsInvalidOBUIdentity' -count=1`

Actual: failed as expected because both identities were accepted before the fix.

### GREEN

Implemented validated coordinates, per-OBU previous-point state protected by a mutex, and Haversine kilometers.

Command: `GOCACHE=/tmp/fleettoll-go-cache go test ./distance_calculator -run 'TestCalculatorService' -count=1`

Actual: passed.

### REFACTOR and broader checks

Corrected `logrus.Errorf` formatting in `distance_calculator/consumer.go`, ran gofmt, and added concurrent OBU initialization coverage.

Commands and actual results:

- `GOCACHE=/tmp/fleettoll-go-cache go test -race ./...` — passed for all packages. The linker emitted a macOS `LC_DYSYMTAB` warning, but command exit status was 0.
- `GOCACHE=/tmp/fleettoll-go-cache go vet ./...` — passed.
- `GOCACHE=/tmp/fleettoll-go-cache make tdd PKG=./distance_calculator TEST=TestCalculatorServiceCalculatesHaversineKilometers` — passed; confirms the required Make target works for focused tests.

The new `make tdd PKG=... TEST=...` target runs a focused named Go test; it is a convenience command, not proof of RED/GREEN by itself.

## Remaining verification

No real Kafka or end-to-end test was run for this feature. The calculation path is a pure in-process service, and its state behavior has unit and race coverage; the consumer-to-aggregator delivery boundary still lacks infrastructure-backed integration tests. No penetration test, load test, or 1,000-device workload was run. This feature is therefore **IMPLEMENTED BUT NOT FULLY VERIFIED** under the master prompt's completion criteria.

## Adversarial review notes

Executed unit probes for latitude/longitude bounds, NaN, and invalid OBU IDs; all are rejected before state mutation after the fix. The in-memory map is protected by a mutex and was exercised under the race detector. This does not establish authentication or tenant ownership: the Kafka message's OBU ID is still trusted, so a publisher that can write to the topic can spoof another OBU or grow state with many IDs. Kafka ACLs, device identity binding, bounded state lifecycle, and tenant checks remain open security work. No external penetration test was performed.
