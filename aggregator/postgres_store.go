package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct{ pool *pgxpool.Pool }

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

func (s *PostgresStore) Insert(distance types.Distance) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command, err := s.pool.Exec(ctx, `
		INSERT INTO aggregated_distance_events (tenant_id, event_id, obu_id, distance_km, event_unix)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, event_id) DO NOTHING`,
		distance.TenantID, distance.EventID, distance.OBUID, distance.Value, distance.Unix)
	if err != nil {
		return fmt.Errorf("insert distance event: %w", err)
	}
	if command.RowsAffected() == 0 {
		var obuID int64
		var value float64
		var eventUnix int64
		if err := s.pool.QueryRow(ctx, `SELECT obu_id, distance_km, event_unix FROM aggregated_distance_events WHERE tenant_id=$1 AND event_id=$2`, distance.TenantID, distance.EventID).Scan(&obuID, &value, &eventUnix); err != nil {
			return fmt.Errorf("read existing event identity: %w", err)
		}
		if obuID != int64(distance.OBUID) || value != distance.Value || eventUnix != distance.Unix {
			return ErrEventIDConflict
		}
	}
	return nil
}

func (s *PostgresStore) Get(tenantID int64, obuID int) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var distance float64
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(distance_km), 0), COUNT(*)
		FROM aggregated_distance_events WHERE tenant_id=$1 AND obu_id=$2`, tenantID, obuID).Scan(&distance, &count)
	if err != nil {
		return 0, fmt.Errorf("query invoice distance: %w", err)
	}
	if count == 0 {
		return 0, fmt.Errorf("OBU ID %d: %w", obuID, ErrInvoiceNotFound)
	}
	return distance, nil
}
