package main

import (
	"testing"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

func TestKafkaProducerUsesEnvironmentConfiguration(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "  kafka.example:9093  ")
	t.Setenv("KAFKA_TOPIC", "telemetry-v2")
	if got := kafkaBrokersFromEnv(); got != "kafka.example:9093" {
		t.Errorf("brokers = %q, want trimmed environment value", got)
	}
	if got := kafkaTopicFromEnv(); got != "telemetry-v2" {
		t.Errorf("topic = %q, want telemetry-v2", got)
	}
}

func TestKafkaProducerUsesDefaultsWhenEnvironmentIsUnset(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	if got := kafkaBrokersFromEnv(); got != defaultKafkaBrokers {
		t.Errorf("brokers = %q, want %q", got, defaultKafkaBrokers)
	}
	if got := kafkaTopicFromEnv(); got != defaultKafkaTopic {
		t.Errorf("topic = %q, want %q", got, defaultKafkaTopic)
	}
}

func TestKafkaProducerRejectsInvalidTelemetryBeforeKafkaAccess(t *testing.T) {
	producer := &KafkaProducer{topic: "telemetry"}
	err := producer.ProduceData(types.OBUData{OBUID: 0, Latitude: 0, Longitude: 0})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestKafkaProducerRejectsEmptyTopic(t *testing.T) {
	if _, err := NewKafkaProducer(" "); err == nil {
		t.Fatal("expected empty topic error")
	}
}
