package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventCalculator interface {
	CalculateEvent(context.Context, string, types.OBUData) (float64, error)
}

type PostgresCalculator struct{ pool *pgxpool.Pool }

func NewPostgresCalculator(pool *pgxpool.Pool) *PostgresCalculator {
	return &PostgresCalculator{pool: pool}
}

func (s *PostgresCalculator) CalculateDistance(types.OBUData) (float64, error) {
	return 0, fmt.Errorf("PostgreSQL distance calculation requires a Kafka event identity")
}

// CalculateEvent commits event identity, distance and the next vehicle baseline atomically.
func (s *PostgresCalculator) CalculateEvent(ctx context.Context, eventID string, data types.OBUData) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if eventID == "" {
		return 0, fmt.Errorf("event identity is required")
	}
	if err := validateOBUData(data); err != nil {
		return 0, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin telemetry transaction: %w", err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(data.OBUID)); err != nil {
		return 0, fmt.Errorf("lock vehicle state: %w", err)
	}
	var priorDistance float64
	err = tx.QueryRow(ctx, `SELECT distance_km FROM processed_telemetry WHERE event_id = $1`, eventID).Scan(&priorDistance)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit replay lookup: %w", err)
		}
		return priorDistance, nil
	}
	if err != pgx.ErrNoRows {
		return 0, fmt.Errorf("lookup processed event: %w", err)
	}
	var previous coordinate
	err = tx.QueryRow(ctx, `SELECT latitude, longitude FROM trip_state WHERE obu_id = $1 FOR UPDATE`, data.OBUID).Scan(&previous.latitude, &previous.longitude)
	distance := 0.0
	if err == nil {
		distance = haversineDistance(previous, coordinate{latitude: data.Latitude, longitude: data.Longitude})
	} else if err != pgx.ErrNoRows {
		return 0, fmt.Errorf("load vehicle baseline: %w", err)
	}
	if math.IsNaN(distance) || math.IsInf(distance, 0) {
		return 0, fmt.Errorf("computed distance is not finite")
	}
	_, err = tx.Exec(ctx, `INSERT INTO trip_state (obu_id, latitude, longitude) VALUES ($1,$2,$3)
		ON CONFLICT (obu_id) DO UPDATE SET latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude, updated_at=now()`, data.OBUID, data.Latitude, data.Longitude)
	if err != nil {
		return 0, fmt.Errorf("update vehicle baseline: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO processed_telemetry (event_id, obu_id, distance_km) VALUES ($1,$2,$3)`, eventID, data.OBUID, distance)
	if err != nil {
		return 0, fmt.Errorf("record processed event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit telemetry processing: %w", err)
	}
	return distance, nil
}

func validateOBUData(data types.OBUData) error {
	if data.OBUID <= 0 {
		return fmt.Errorf("OBU ID must be positive")
	}
	if math.IsNaN(data.Latitude) || math.IsInf(data.Latitude, 0) || data.Latitude < -90 || data.Latitude > 90 {
		return fmt.Errorf("invalid latitude")
	}
	if math.IsNaN(data.Longitude) || math.IsInf(data.Longitude, 0) || data.Longitude < -180 || data.Longitude > 180 {
		return fmt.Errorf("invalid longitude")
	}
	return nil
}

var _ EventCalculator = (*PostgresCalculator)(nil)
