package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/distance_calculator/client"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type fakeKafkaConsumer struct {
	mu          sync.Mutex
	message     *kafka.Message
	commits     int
	commitErr   error
	messageRead chan struct{}
	closed      bool
}

func (f *fakeKafkaConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.message != nil {
		msg := f.message
		f.message = nil
		close(f.messageRead)
		return msg, nil
	}
	return nil, kafka.NewError(kafka.ErrTimedOut, "poll timeout", false)
}

func (f *fakeKafkaConsumer) CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	return nil, f.commitErr
}

func (f *fakeKafkaConsumer) SubscribeTopics([]string, kafka.RebalanceCb) error { return nil }
func (f *fakeKafkaConsumer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func TestKafkaConsumerCommitsOnlyAfterAggregationSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reader := &fakeKafkaConsumer{
		message:     &kafka.Message{Value: []byte(`{"obuID":7,"lat":23.7,"long":90.4}`)},
		messageRead: make(chan struct{}),
	}
	c := newKafkaConsumerWithReader(reader, NewCalculatorService(), client.NewClient(server.URL))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.run(ctx) }()

	select {
	case <-reader.messageRead:
	case <-time.After(time.Second):
		t.Fatal("consumer did not read the Kafka message")
	}
	deadline := time.Now().Add(time.Second)
	for {
		reader.mu.Lock()
		commits := reader.commits
		reader.mu.Unlock()
		if commits == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("message was not committed after successful aggregation")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run returned error after cancellation: %v", err)
	}
}

func TestKafkaConsumerDoesNotCommitWhenAggregationFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	reader := &fakeKafkaConsumer{
		message:     &kafka.Message{Value: []byte(`{"obuID":7,"lat":23.7,"long":90.4}`)},
		messageRead: make(chan struct{}),
	}
	c := newKafkaConsumerWithReader(reader, NewCalculatorService(), client.NewClient(server.URL))
	if err := c.run(context.Background()); err == nil {
		t.Fatal("expected downstream aggregation error")
	}
	reader.mu.Lock()
	commits := reader.commits
	reader.mu.Unlock()
	if commits != 0 {
		t.Fatalf("commits = %d, want 0 after aggregation failure", commits)
	}
}

func TestKafkaConsumerDoesNotCommitMalformedTelemetry(t *testing.T) {
	reader := &fakeKafkaConsumer{
		message:     &kafka.Message{Value: []byte(`{"obuID":`)},
		messageRead: make(chan struct{}),
	}
	c := newKafkaConsumerWithReader(reader, NewCalculatorService(), client.NewClient("http://127.0.0.1:1"))
	if err := c.run(context.Background()); err == nil {
		t.Fatal("expected malformed telemetry error")
	}
	reader.mu.Lock()
	commits := reader.commits
	reader.mu.Unlock()
	if commits != 0 {
		t.Fatalf("commits = %d, want 0 for malformed telemetry", commits)
	}
}

func TestKafkaConsumerStopsWhenOffsetCommitFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reader := &fakeKafkaConsumer{
		message:     &kafka.Message{Value: []byte(`{"obuID":7,"lat":23.7,"long":90.4}`)},
		messageRead: make(chan struct{}),
		commitErr:   errors.New("coordinator unavailable"),
	}
	c := newKafkaConsumerWithReader(reader, NewCalculatorService(), client.NewClient(server.URL))
	if err := c.run(context.Background()); err == nil {
		t.Fatal("expected offset commit error")
	}
	reader.mu.Lock()
	commits := reader.commits
	reader.mu.Unlock()
	if commits != 1 {
		t.Fatalf("commit attempts = %d, want 1", commits)
	}
}

func TestKafkaConsumerStartClosesReaderAfterCancellation(t *testing.T) {
	reader := &fakeKafkaConsumer{messageRead: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := newKafkaConsumerWithReader(reader, NewCalculatorService(), client.NewClient("http://127.0.0.1:1"))
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start returned error after cancellation: %v", err)
	}
	reader.mu.Lock()
	closed := reader.closed
	reader.mu.Unlock()
	if !closed {
		t.Fatal("consumer reader was not closed by Start")
	}
}
