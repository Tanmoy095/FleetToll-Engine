obu:
	go build -o bin/obu obu/main.go
	./bin/obu

receiver:
	go build -o bin/receiver ./data_receiver
	./bin/receiver

calculator:
	go build -o bin/calculator ./distance_calculator
	./bin/calculator

PROTOC_GEN_GO_VERSION ?= v1.36.6
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

.PHONY: obu receiver calculator tdd integration-kafka integration-postgres generate-proto install-proto-tools integration-grpc load-grpc

tdd:
	@test -n "$(PKG)" || (echo "usage: make tdd PKG=./path TEST=TestName" >&2; exit 2)
	@test -n "$(TEST)" || (echo "usage: make tdd PKG=./path TEST=TestName" >&2; exit 2)
	go test -run '^$(TEST)$$' $(PKG)

KAFKA_TEST_BROKERS ?= localhost:9092

integration-kafka:
	KAFKA_TEST_BROKERS="$(KAFKA_TEST_BROKERS)" go test -tags=integration ./data_receiver ./distance_calculator -run '^(TestKafkaProducerDeliversKeyedTelemetry|TestKafkaConsumerProcessesRealTelemetryIntoAggregator)$$' -count=1 -timeout=60s

DATABASE_TEST_URL ?= postgres://fleettoll:fleetdev@localhost:15432/fleettoll?sslmode=disable

integration-postgres:
	DATABASE_TEST_URL="$(DATABASE_TEST_URL)" go test -tags=integration ./aggregator ./distance_calculator -run '^TestPostgres' -count=1 -timeout=60s

install-proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

generate-proto:
	protoc --go_out=. --go_opt=module=github.com/Tanmoy095/FleetToll-Engine --go-grpc_out=. --go-grpc_opt=module=github.com/Tanmoy095/FleetToll-Engine api/fleettoll/v1/aggregator.proto

integration-grpc:
	go test ./aggregator ./distance_calculator/client -run '^(TestGRPCAggregator|TestGRPCClient)' -count=1 -timeout=60s

load-grpc:
	go run ./scripts/grpc_aggregate_load -users 1000
