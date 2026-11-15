//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"github.com/Tanmoy095/FleetToll-Engine/internal/postgres"
	"github.com/Tanmoy095/FleetToll-Engine/types"
)

func TestPostgresStoreDeduplicatesReplayedDistance(t *testing.T) {
	dsn := os.Getenv("DATABASE_TEST_URL")
	if dsn == "" {
		t.Skip("set DATABASE_TEST_URL to run against PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := NewPostgresStore(pool)
	obuID := int(time.Now().UnixNano() % 1_000_000_000)
	event := types.Distance{EventID: fmt.Sprintf("pg-test:%d", time.Now().UnixNano()), OBUID: obuID, Value: 4.25, Unix: time.Now().UnixNano()}
	const duplicateDeliveries = 32
	var wg sync.WaitGroup
	errCh := make(chan error, duplicateDeliveries)
	for i := 0; i < duplicateDeliveries; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- store.Insert(event) }()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Error(err)
		}
	}
	conflict := event
	conflict.Value++
	if err := store.Insert(conflict); err != ErrEventIDConflict {
		t.Fatalf("conflicting reuse error = %v, want %v", err, ErrEventIDConflict)
	}
	if err := store.Insert(event); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(event.OBUID)
	if err != nil {
		t.Fatal(err)
	}
	if got != event.Value {
		t.Fatalf("sum = %v, want %v after replay", got, event.Value)
	}
}

func TestPostgresStoreTreatsEventIDAsData(t *testing.T) {
	dsn := os.Getenv("DATABASE_TEST_URL")
	if dsn == "" {
		t.Skip("set DATABASE_TEST_URL to run against PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	nonce := time.Now().UnixNano()
	event := types.Distance{EventID: fmt.Sprintf("event'); DROP TABLE aggregated_distance_events; --:%d", nonce), OBUID: int(nonce % 1_000_000_000), Value: 1, Unix: nonce}
	if err := NewPostgresStore(pool).Insert(event); err != nil {
		t.Fatalf("parameterized insert rejected event ID: %v", err)
	}
	if _, err := pool.Exec(ctx, `SELECT 1 FROM aggregated_distance_events LIMIT 1`); err != nil {
		t.Fatalf("table missing after injection-shaped input: %v", err)
	}
}

func TestPostgresAggregatorGRPCRoundTrip(t *testing.T) {
	dsn := os.Getenv("DATABASE_TEST_URL")
	if dsn == "" {
		t.Skip("set DATABASE_TEST_URL to run against PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, server, conn := startGRPCTestServer(t, NewInvoiceAggregator(NewPostgresStore(pool)))
	defer server.Stop()
	defer conn.Close()
	rpc := fleettollv1.NewAggregatorServiceClient(conn)
	obuID := time.Now().UnixNano() % 1_000_000_000
	request := &fleettollv1.AggregateDistanceRequest{EventId: fmt.Sprintf("grpc-pg:%d", time.Now().UnixNano()), ObuId: obuID, DistanceKm: 3.75, EventUnixNano: time.Now().UnixNano()}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := rpc.AggregateDistance(ctx, request); err != nil {
			t.Fatalf("AggregateDistance() error = %v", err)
		}
	}
	invoice, err := rpc.GetInvoice(ctx, &fleettollv1.GetInvoiceRequest{ObuId: obuID})
	if err != nil {
		t.Fatalf("GetInvoice() error = %v", err)
	}
	if invoice.GetTotalDistanceKm() != 3.75 {
		t.Fatalf("invoice distance = %v, want 3.75 after replay", invoice.GetTotalDistanceKm())
	}
}
