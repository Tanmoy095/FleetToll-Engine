package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	defaultKafkaBrokers  = "localhost:9092"
	defaultKafkaTopic    = "obudata"
	kafkaDeliveryTimeout = 15 * time.Second
)

type DataProducer interface {
	ProduceData(types.OBUData) error
}

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
}

func NewKafkaProducer(topic string) (*KafkaProducer, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("Kafka topic must not be empty")
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaBrokersFromEnv(),
		"enable.idempotence": true,
		"acks":               "all",
		"message.timeout.ms": int(kafkaDeliveryTimeout.Milliseconds()),
		"client.id":          "fleettoll-data-receiver",
	})
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	producer := &KafkaProducer{producer: p, topic: topic, stop: make(chan struct{}), done: make(chan struct{})}
	go producer.readEvents()
	return producer, nil
}

func (p *KafkaProducer) readEvents() {
	defer close(p.done)
	for {
		select {
		case <-p.stop:
			return
		case event, ok := <-p.producer.Events():
			if !ok {
				return
			}
			if message, ok := event.(*kafka.Message); ok && message.TopicPartition.Error != nil {
				logrus.WithError(message.TopicPartition.Error).Error("Kafka message delivery failed")
			}
		}
	}
}

func kafkaBrokersFromEnv() string {
	if brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); brokers != "" {
		return brokers
	}
	return defaultKafkaBrokers
}

func kafkaTopicFromEnv() string {
	if topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC")); topic != "" {
		return topic
	}
	return defaultKafkaTopic
}

func (p *KafkaProducer) ProduceData(data types.OBUData) error {
	if err := validateOBUData(data); err != nil {
		return fmt.Errorf("invalid telemetry: %w", err)
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode telemetry: %w", err)
	}
	key := []byte(fmt.Sprintf("%d:%d", data.TenantID, data.OBUID))
	delivery := make(chan kafka.Event, 1)
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          payload,
	}
	if err := p.producer.Produce(message, delivery); err != nil {
		return fmt.Errorf("enqueue telemetry: %w", err)
	}

	timer := time.NewTimer(kafkaDeliveryTimeout)
	defer timer.Stop()
	select {
	case event := <-delivery:
		message, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected Kafka delivery event %T", event)
		}
		if message.TopicPartition.Error != nil {
			return fmt.Errorf("Kafka delivery failed: %w", message.TopicPartition.Error)
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("Kafka delivery timed out after %s", kafkaDeliveryTimeout)
	}
}

func (p *KafkaProducer) Close() error {
	remaining := p.producer.Flush(int(kafkaDeliveryTimeout.Milliseconds()))
	p.close.Do(func() {
		close(p.stop)
		<-p.done
		p.producer.Close()
	})
	if remaining > 0 {
		return fmt.Errorf("Kafka producer closed with %d undelivered messages", remaining)
	}
	return nil
}
