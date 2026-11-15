//go:build integration

package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/internal/postgres"
	"github.com/Tanmoy095/FleetToll-Engine/types"
)

func TestPostgresCalculatorReplayKeepsDistanceAndBaseline(t *testing.T) {
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
	service := NewPostgresCalculator(pool)
	obuID := int(time.Now().UnixNano()%1_000_000_000) + 1
	first := types.OBUData{OBUID: obuID, Latitude: 0, Longitude: 0}
	second := types.OBUData{OBUID: obuID, Latitude: 0, Longitude: 1}
	third := types.OBUData{OBUID: obuID, Latitude: 0, Longitude: 2}
	if got, err := service.CalculateEvent(ctx, fmt.Sprintf("pg-trip:%d:1", obuID), first); err != nil || got != 0 {
		t.Fatalf("baseline event distance = %v, err = %v", got, err)
	}
	want := 111.1949266
	got, err := service.CalculateEvent(ctx, fmt.Sprintf("pg-trip:%d:2", obuID), second)
	if err != nil || math.Abs(got-want) > 0.001 {
		t.Fatalf("second event distance = %.8f, err = %v; want %.8f", got, err, want)
	}
	// Simulate a process restart: a new service instance shares only PostgreSQL state.
	restarted := NewPostgresCalculator(pool)
	replay, err := restarted.CalculateEvent(ctx, fmt.Sprintf("pg-trip:%d:2", obuID), second)
	if err != nil || replay != got {
		t.Fatalf("replayed distance = %.8f, err = %v; want original %.8f", replay, err, got)
	}
	next, err := restarted.CalculateEvent(ctx, fmt.Sprintf("pg-trip:%d:3", obuID), third)
	if err != nil || math.Abs(next-want) > 0.001 {
		t.Fatalf("next distance after replay = %.8f, err = %v; want %.8f", next, err, want)
	}
}
