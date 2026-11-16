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
	"github.com/Tanmoy095/FleetToll-Engine/internal/tenantauth"
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
	event := types.Distance{TenantID: types.LegacyTenantID, EventID: fmt.Sprintf("pg-test:%d", time.Now().UnixNano()), OBUID: obuID, Value: 4.25, Unix: time.Now().UnixNano()}
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
	got, err := store.Get(event.TenantID, event.OBUID)
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
	event := types.Distance{TenantID: types.LegacyTenantID, EventID: fmt.Sprintf("event'); DROP TABLE aggregated_distance_events; --:%d", nonce), OBUID: int(nonce % 1_000_000_000), Value: 1, Unix: nonce}
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
	ctx = tenantauth.WithServiceToken(ctx, testServiceToken)
	request := &fleettollv1.AggregateDistanceRequest{TenantId: types.LegacyTenantID, EventId: fmt.Sprintf("grpc-pg:%d", time.Now().UnixNano()), ObuId: obuID, DistanceKm: 3.75, EventUnixNano: time.Now().UnixNano()}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := rpc.AggregateDistance(ctx, request); err != nil {
			t.Fatalf("AggregateDistance() error = %v", err)
		}
	}
	invoice, err := rpc.GetInvoice(ctx, &fleettollv1.GetInvoiceRequest{TenantId: types.LegacyTenantID, ObuId: obuID})
	if err != nil {
		t.Fatalf("GetInvoice() error = %v", err)
	}
	if invoice.GetTotalDistanceKm() != 3.75 {
		t.Fatalf("invoice distance = %v, want 3.75 after replay", invoice.GetTotalDistanceKm())
	}
}

func TestPostgresStoreIsolatesSameOBUAndEventAcrossTenants(t *testing.T) {
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
	tenantID := time.Now().UnixNano()
	if _, err := pool.Exec(ctx, `INSERT INTO tenants (tenant_id, tenant_name) VALUES ($1, $2)`, tenantID, fmt.Sprintf("integration-%d", tenantID)); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresStore(pool)
	obuID := int(tenantID % 1_000_000_000)
	eventID := fmt.Sprintf("shared-event:%d", tenantID)
	first := types.Distance{TenantID: types.LegacyTenantID, EventID: eventID, OBUID: obuID, Value: 2, Unix: tenantID}
	second := types.Distance{TenantID: tenantID, EventID: eventID, OBUID: obuID, Value: 8, Unix: tenantID}
	if err := store.Insert(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(second); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tenant int64
		want   float64
	}{{first.TenantID, 2}, {second.TenantID, 8}} {
		got, err := store.Get(tc.tenant, obuID)
		if err != nil || got != tc.want {
			t.Fatalf("tenant %d total = %v, err %v; want %v", tc.tenant, got, err, tc.want)
		}
	}
}

func TestPostgresConfiguredTenantValidation(t *testing.T) {
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
	if err := postgres.ValidateTenantIDs(ctx, pool, []int64{types.LegacyTenantID}); err != nil {
		t.Fatalf("validate seeded tenant: %v", err)
	}
	if err := postgres.ValidateTenantIDs(ctx, pool, []int64{time.Now().UnixNano()}); err == nil {
		t.Fatal("expected unprovisioned tenant to fail validation")
	}
}
