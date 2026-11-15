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
