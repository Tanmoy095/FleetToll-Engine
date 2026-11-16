//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestKafkaProducerDeliversKeyedTelemetry(t *testing.T) {
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("set KAFKA_TEST_BROKERS to run against a real Kafka broker")
	}
	t.Setenv("KAFKA_BROKERS", brokers)

	topic := fmt.Sprintf("fleettoll-integration-%d", time.Now().UnixNano())
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatalf("create Kafka admin client: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}})
	if err != nil {
		t.Fatalf("create test topic: %v", err)
	}
	if len(results) != 1 || results[0].Error.Code() != kafka.ErrNoError {
		t.Fatalf("create test topic result: %+v", results)
	}
	defer func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.DeleteTopics(deleteCtx, []string{topic})
	}()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          topic,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("create Kafka consumer: %v", err)
	}
	defer consumer.Close()
	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Fatalf("subscribe test consumer: %v", err)
	}

	producer, err := NewKafkaProducer(topic)
	if err != nil {
		t.Fatalf("create Kafka producer: %v", err)
	}
	defer producer.Close()

	want := types.OBUData{TenantID: types.LegacyTenantID, OBUID: 8142, Latitude: 23.7806, Longitude: 90.4074}
	if err := producer.ProduceData(want); err != nil {
		t.Fatalf("produce telemetry: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	var got *kafka.Message
	for time.Now().Before(deadline) {
		msg, err := consumer.ReadMessage(time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			t.Fatalf("read test telemetry: %v", err)
		}
		if msg.TopicPartition.Topic != nil && *msg.TopicPartition.Topic == topic {
			got = msg
			break
		}
	}
	if got == nil {
		t.Fatal("timed out waiting for telemetry from Kafka")
	}
	if key := string(got.Key); key != fmt.Sprintf("%d:%d", want.TenantID, want.OBUID) {
		t.Errorf("record key = %q, want tenant/OBU key for %d/%d", key, want.TenantID, want.OBUID)
	}
	var actual types.OBUData
	if err := json.Unmarshal(got.Value, &actual); err != nil {
		t.Fatalf("decode Kafka value: %v", err)
	}
	if actual != want {
		t.Errorf("Kafka payload = %+v, want %+v", actual, want)
	}
}
