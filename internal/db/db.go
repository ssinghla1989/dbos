package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ssinghl/dbos/internal/config"
)

func Init(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	if cfg.DatabaseURL == "" {
		return nil, nil
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}

// Connect provides a minimal helper for callers that only have a database URL at hand.
// TODO: consolidate this helper with Init once a single configuration path is established.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}

// Close safely shuts down a pgx pool instance.
// TODO: replace with central connection lifecycle management.
func Close(pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	pool.Close()
}

var (
	// ErrNilPool indicates callers attempted to operate on a nil connection pool.
	ErrNilPool = errors.New("db: connection pool is nil")
	// ErrNilExecutor indicates callers attempted to operate on a nil transaction or pool.
	ErrNilExecutor = errors.New("db: exec/query target is nil")
)

// BeginTx safely begins a transaction using the provided pool.
func BeginTx(ctx context.Context, pool *pgxpool.Pool, opts pgx.TxOptions) (pgx.Tx, error) {
	if pool == nil {
		return nil, ErrNilPool
	}
	return pool.BeginTx(ctx, opts)
}

// Exec delegates to Exec on the provided target (pool or transaction) with nil checks.
// TODO: replace this helper with generated query builders as the data layer stabilizes.
func Exec(ctx context.Context, target ExecTarget, sql string, args ...any) (pgconn.CommandTag, error) {
	if target == nil {
		return pgconn.CommandTag{}, ErrNilExecutor
	}
	return target.Exec(ctx, sql, args...)
}

// QueryRow delegates to QueryRow on the provided target (pool or transaction) with nil checks.
// TODO: replace this helper with generated query builders as the data layer stabilizes.
func QueryRow(ctx context.Context, target QueryRowTarget, sql string, args ...any) (pgx.Row, error) {
	if target == nil {
		return nil, ErrNilExecutor
	}
	return target.QueryRow(ctx, sql, args...), nil
}

// Query delegates to Query on the provided target (pool or transaction) with nil checks.
// TODO: replace this helper with generated query builders as the data layer stabilizes.
func Query(ctx context.Context, target QueryTarget, sql string, args ...any) (pgx.Rows, error) {
	if target == nil {
		return nil, ErrNilExecutor
	}
	return target.Query(ctx, sql, args...)
}

// ExecTarget captures the minimal Exec capability shared by pools and transactions.
type ExecTarget interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// QueryRowTarget captures the minimal QueryRow capability shared by pools and transactions.
type QueryRowTarget interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// QueryTarget captures the minimal Query capability shared by pools and transactions.
type QueryTarget interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}
