package postgres

import (
	"context"
	"embed"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("configure PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("read database migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		migration, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("begin migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(879512421)`); err != nil {
			_ = tx.Rollback(context.Background())
			pool.Close()
			return nil, fmt.Errorf("lock migrations: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
			_ = tx.Rollback(context.Background())
			pool.Close()
			return nil, fmt.Errorf("create migration history: %w", err)
		}
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, entry.Name()).Scan(&applied); err != nil {
			_ = tx.Rollback(context.Background())
			pool.Close()
			return nil, fmt.Errorf("check migration %s: %w", entry.Name(), err)
		}
		if applied {
			if err := tx.Commit(ctx); err != nil {
				pool.Close()
				return nil, fmt.Errorf("finish migration check %s: %w", entry.Name(), err)
			}
			continue
		}
		if _, err := tx.Exec(ctx, string(migration)); err != nil {
			_ = tx.Rollback(context.Background())
			pool.Close()
			return nil, fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, entry.Name()); err != nil {
			_ = tx.Rollback(context.Background())
			pool.Close()
			return nil, fmt.Errorf("record migration %s: %w", entry.Name(), err)
		}
		if err := tx.Commit(ctx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("commit migration %s: %w", entry.Name(), err)
		}
	}
	return pool, nil
}

func ValidateTenantIDs(ctx context.Context, pool *pgxpool.Pool, tenantIDs []int64) error {
	if len(tenantIDs) == 0 {
		return fmt.Errorf("at least one tenant ID is required")
	}
	rows, err := pool.Query(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id = ANY($1)`, tenantIDs)
	if err != nil {
		return fmt.Errorf("validate configured tenants: %w", err)
	}
	defer rows.Close()
	found := make(map[int64]struct{}, len(tenantIDs))
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			return fmt.Errorf("read configured tenant: %w", err)
		}
		found[tenantID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read configured tenants: %w", err)
	}
	for _, tenantID := range tenantIDs {
		if _, exists := found[tenantID]; !exists {
			return fmt.Errorf("configured tenant ID %d is not provisioned", tenantID)
		}
	}
	return nil
}
