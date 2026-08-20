// Package repository persists domain entities in PostgreSQL — the source of
// truth for job state. Transitions use guarded UPDATEs so concurrent actors
// (e.g. an API cancel racing a worker claim) can't corrupt the state machine.
package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pa15asti/portrait-engine/internal/config"
)

var (
	ErrNotFound = errors.New("not found")
	// ErrConflict: a guarded update matched no row because state changed
	// concurrently (optimistic-concurrency conflict).
	ErrConflict            = errors.New("state conflict")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request")
)

// NewPool creates and verifies a pgx connection pool from configuration.
// The caller owns the returned pool and must Close it.
func NewPool(ctx context.Context, cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
