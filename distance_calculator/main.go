package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/distance_calculator/client"
	"github.com/Tanmoy095/FleetToll-Engine/internal/postgres"
)

const (
	aggregatorEndpoint = "http://127.0.0.1:3000/aggregate"
)

func main() {
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "obudata"
	}
	var (
		err error
		svc CalculatorServicer
	)
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		initCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool, openErr := postgres.Open(initCtx, databaseURL)
		cancel()
		if openErr != nil {
			log.Fatal(openErr)
		}
		defer pool.Close()
		svc = NewPostgresCalculator(pool)
	} else {
		fmt.Println("DATABASE_URL is unset; using volatile in-memory trip state")
		svc = NewCalculatorService()
	}
	svc = NewLogMiddleware(svc)

	// Creating a client to send data to Aggregator
	aggregatorClient := client.NewClient(aggregatorEndpoint)

	kafkaConsumer, err := NewKafkaConsumer(topic, svc, aggregatorClient)

	if err != nil {
		log.Fatal(err)

	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := kafkaConsumer.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
