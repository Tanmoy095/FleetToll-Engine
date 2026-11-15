obu:
	go build -o bin/obu obu/main.go
	./bin/obu

receiver:
	go build -o bin/receiver ./data_receiver
	./bin/receiver

calculator:
	go build -o bin/calculator ./distance_calculator
	./bin/calculator

.PHONY: obu receiver calculator tdd integration-kafka

tdd:
	@test -n "$(PKG)" || (echo "usage: make tdd PKG=./path TEST=TestName" >&2; exit 2)
	@test -n "$(TEST)" || (echo "usage: make tdd PKG=./path TEST=TestName" >&2; exit 2)
	go test -run '^$(TEST)$$' $(PKG)

KAFKA_TEST_BROKERS ?= localhost:9092

integration-kafka:
	KAFKA_TEST_BROKERS="$(KAFKA_TEST_BROKERS)" go test -tags=integration ./data_receiver ./distance_calculator -run '^(TestKafkaProducerDeliversKeyedTelemetry|TestKafkaConsumerProcessesRealTelemetryIntoAggregator)$$' -count=1 -timeout=60s
