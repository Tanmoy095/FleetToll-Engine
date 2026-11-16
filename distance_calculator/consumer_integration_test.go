//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"github.com/Tanmoy095/FleetToll-Engine/distance_calculator/client"
	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/grpc"
)

func TestKafkaConsumerProcessesRealTelemetryIntoAggregator(t *testing.T) {
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("set KAFKA_TEST_BROKERS to run against a real Kafka broker")
	}
	t.Setenv("KAFKA_BROKERS", brokers)
	topic := fmt.Sprintf("fleettoll-consumer-integration-%d", time.Now().UnixNano())
	t.Setenv("KAFKA_CONSUMER_GROUP", topic)

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatalf("create Kafka admin client: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}})
	if err != nil {
		t.Fatalf("create test topic: %v", err)
	}
	if len(results) != 1 || results[0].Error.Code() != kafka.ErrNoError {
		t.Fatalf("create test topic result: %+v", results)
	}
	defer func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer deleteCancel()
		_, _ = admin.DeleteTopics(deleteCtx, []string{topic})
	}()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test aggregator gRPC: %v", err)
	}
	aggregated := make(chan types.Distance, 2)
	rpcServer := grpc.NewServer()
	fleettollv1.RegisterAggregatorServiceServer(rpcServer, &recordingIntegrationAggregator{UnimplementedAggregatorServiceServer: fleettollv1.UnimplementedAggregatorServiceServer{}, aggregated: aggregated})
	go func() { _ = rpcServer.Serve(listener) }()
	defer rpcServer.Stop()
	defer listener.Close()
	aggregatorClient, err := client.DialGRPC(ctx, listener.Addr().String(), "service-test-token-0123456789abcdef")
	if err != nil {
		t.Fatalf("create aggregator gRPC client: %v", err)
	}
	defer aggregatorClient.Close()

	consumer, err := NewKafkaConsumer(topic, NewCalculatorService(), aggregatorClient)
	if err != nil {
		t.Fatalf("create distance consumer: %v", err)
	}
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatalf("create test producer: %v", err)
	}
	defer producer.Close()

	first := types.OBUData{TenantID: types.LegacyTenantID, OBUID: 71, Latitude: 0, Longitude: 0}
	second := types.OBUData{TenantID: types.LegacyTenantID, OBUID: 71, Latitude: 0, Longitude: 1}
	produceTelemetry := func(data types.OBUData) error {
		payload, err := json.Marshal(data)
		if err != nil {
			return err
		}
		delivery := make(chan kafka.Event, 1)
		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(strconv.Itoa(data.OBUID)),
			Value:          payload,
		}, delivery); err != nil {
			return err
		}
		select {
		case event := <-delivery:
			if message, ok := event.(*kafka.Message); ok {
				return message.TopicPartition.Error
			}
			return fmt.Errorf("unexpected Kafka delivery event %T", event)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := produceTelemetry(first); err != nil {
		t.Fatalf("produce first telemetry point: %v", err)
	}
	if err := produceTelemetry(second); err != nil {
		t.Fatalf("produce second telemetry point: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.Start(runCtx) }()
	got := make([]types.Distance, 0, 2)
	for len(got) < 2 {
		select {
		case distance := <-aggregated:
			got = append(got, distance)
		case <-ctx.Done():
			stop()
			t.Fatalf("timed out waiting for aggregator requests: received %d", len(got))
		}
	}
	// The handler signals before the HTTP response is fully consumed by the
	// calculator. Allow the consumer to complete the response and commit both
	// offsets before cancellation exercises its graceful poll exit.
	time.Sleep(300 * time.Millisecond)
	stop()
	if err := <-runDone; err != nil {
		t.Fatalf("consumer stopped with error: %v", err)
	}
	if got[0].OBUID != first.OBUID || got[0].Value != 0 {
		t.Errorf("first aggregate = %+v, want OBU %d with zero initial distance", got[0], first.OBUID)
	}
	if got[1].OBUID != second.OBUID || got[1].Value < 111.19 || got[1].Value > 111.20 {
		t.Errorf("second aggregate = %+v, want about 111.195 km for OBU %d", got[1], second.OBUID)
	}
}

type recordingIntegrationAggregator struct {
	fleettollv1.UnimplementedAggregatorServiceServer
	aggregated chan<- types.Distance
}

func (s *recordingIntegrationAggregator) AggregateDistance(_ context.Context, request *fleettollv1.AggregateDistanceRequest) (*fleettollv1.AggregateDistanceResponse, error) {
	s.aggregated <- types.Distance{TenantID: types.LegacyTenantID, EventID: request.GetEventId(), OBUID: int(request.GetObuId()), Value: request.GetDistanceKm(), Unix: request.GetEventUnixNano()}
	return &fleettollv1.AggregateDistanceResponse{Accepted: true}, nil
}
