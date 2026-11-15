package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const consumerPollTimeout = 250 * time.Millisecond

// kafkaMessageReader is the part of librdkafka used by the processing loop.
// Keeping it narrow makes offset behavior unit-testable without replacing the
// real-broker integration test.
type kafkaMessageReader interface {
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Close() error
}

type kafkaConsumer struct {
	consumer    kafkaMessageReader
	calcService CalculatorServicer
	aggClient   distanceAggregatorClient
}

type distanceAggregatorClient interface {
	AggregateInvoiceContext(context.Context, types.Distance) error
}

func NewKafkaConsumer(topic string, svc CalculatorServicer, aggClient distanceAggregatorClient) (*kafkaConsumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	groupID := os.Getenv("KAFKA_CONSUMER_GROUP")
	if groupID == "" {
		groupID = "fleettoll-distance-calculator"
	}
	return newKafkaConsumer(topic, brokers, groupID, svc, aggClient)
}

func newKafkaConsumer(topic, brokers, groupID string, svc CalculatorServicer, aggClient distanceAggregatorClient) (*kafkaConsumer, error) {
	if topic == "" {
		return nil, errors.New("Kafka topic must not be empty")
	}
	if brokers == "" {
		return nil, errors.New("Kafka brokers must not be empty")
	}
	if groupID == "" {
		return nil, errors.New("Kafka consumer group must not be empty")
	}
	if svc == nil || aggClient == nil {
		return nil, errors.New("calculator service and aggregator client are required")
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("subscribe to Kafka topic %q: %w", topic, err)
	}
	return newKafkaConsumerWithReader(c, svc, aggClient), nil
}

func newKafkaConsumerWithReader(reader kafkaMessageReader, svc CalculatorServicer, aggClient distanceAggregatorClient) *kafkaConsumer {
	return &kafkaConsumer{consumer: reader, calcService: svc, aggClient: aggClient}
}

// Start owns the Kafka client lifecycle. Cancellation stops polling and closes
// the client, allowing the group to rebalance and redeliver uncommitted work.
func (c *kafkaConsumer) Start(ctx context.Context) error {
	defer func() {
		if err := c.consumer.Close(); err != nil {
			logrus.WithError(err).Error("close Kafka consumer")
		}
	}()
	logrus.Info("kafka transport started")
	return c.run(ctx)
}

func (c *kafkaConsumer) run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		msg, err := c.consumer.ReadMessage(consumerPollTimeout)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			return fmt.Errorf("read Kafka message: %w", err)
		}
		if err := c.processMessage(ctx, msg); err != nil {
			return err
		}
		committed, err := c.consumer.CommitMessage(msg)
		if err != nil {
			return fmt.Errorf("commit Kafka offset: %w", err)
		}
		for _, partition := range committed {
			if partition.Error != nil {
				return fmt.Errorf("commit Kafka offset for %s[%d]@%v: %w", topicName(partition.Topic), partition.Partition, partition.Offset, partition.Error)
			}
		}
	}
}

func (c *kafkaConsumer) processMessage(ctx context.Context, msg *kafka.Message) error {
	if msg == nil {
		return errors.New("Kafka returned an empty message")
	}
	var data types.OBUData
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		return fmt.Errorf("decode telemetry at %s[%d]@%v: %w", topicName(msg.TopicPartition.Topic), msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
	}
	partition := msg.TopicPartition.Partition
	eventID := fmt.Sprintf("%s:%d:%d", topicName(msg.TopicPartition.Topic), partition, msg.TopicPartition.Offset)
	distance := 0.0
	var err error
	if durable, ok := c.calcService.(EventCalculator); ok {
		distance, err = durable.CalculateEvent(ctx, eventID, data)
	} else {
		distance, err = c.calcService.CalculateDistance(data)
	}
	if err != nil {
		return fmt.Errorf("calculate distance at %s[%d]@%v: %w", topicName(msg.TopicPartition.Topic), msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
	}
	req := types.Distance{EventID: eventID, Value: distance, Unix: time.Now().UnixNano(), OBUID: data.OBUID}
	if err := c.aggClient.AggregateInvoiceContext(ctx, req); err != nil {
		return fmt.Errorf("aggregate distance for OBU %d at %s[%d]@%v: %w", data.OBUID, topicName(msg.TopicPartition.Topic), msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
	}
	return nil
}

func topicName(topic *string) string {
	if topic == nil || strings.TrimSpace(*topic) == "" {
		return "<unknown-topic>"
	}
	return *topic
}
