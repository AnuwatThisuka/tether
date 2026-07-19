package mutate

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler applies a mutation inside an open transaction.
type Handler func(ctx context.Context, tx pgx.Tx, op, key string, claims any, args map[string]any) error

// Result is the outcome of Apply.
type Result struct {
	Duplicate bool
	Rejected  bool
	Reason    string
}

// EnsureSchema creates tether.mutation_keys for idempotency (Invariant 3).
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("mutate: pool is nil")
	}
	_, err := pool.Exec(ctx, `
CREATE SCHEMA IF NOT EXISTS tether;

CREATE TABLE IF NOT EXISTS tether.mutation_keys (
	idempotency_key TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`)
	if err != nil {
		return fmt.Errorf("mutate: ensure schema: %w", err)
	}
	return nil
}

// IsReject is supplied by the caller to detect business rejects that should roll back.
type IsReject func(error) bool

// RejectReason extracts a reason string from a reject error.
type RejectReason func(error) string

// Apply runs handler inside a transaction that also records key.
// Duplicate keys return Result{Duplicate:true} without invoking handler.
// Reject errors roll back (including the key insert).
func Apply(
	ctx context.Context,
	pool *pgxpool.Pool,
	key string,
	op string,
	claims any,
	args map[string]any,
	handler Handler,
	isReject IsReject,
	rejectReason RejectReason,
) (Result, error) {
	if pool == nil {
		return Result{}, fmt.Errorf("mutate: pool is nil")
	}
	if handler == nil {
		return Result{}, fmt.Errorf("mutate: handler is nil")
	}
	if key == "" {
		return Result{}, fmt.Errorf("mutate: idempotency key is required")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, fmt.Errorf("mutate: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx,
		`INSERT INTO tether.mutation_keys (idempotency_key) VALUES ($1)`, key)
	if err != nil {
		if isUniqueViolation(err) {
			// Key already applied — same txn has no effect; roll back empty work.
			_ = tx.Rollback(ctx)
			return Result{Duplicate: true}, nil
		}
		return Result{}, fmt.Errorf("mutate: insert key: %w", err)
	}

	if err := handler(ctx, tx, op, key, claims, args); err != nil {
		_ = tx.Rollback(ctx)
		if isReject != nil && isReject(err) {
			reason := ""
			if rejectReason != nil {
				reason = rejectReason(err)
			}
			return Result{Rejected: true, Reason: reason}, nil
		}
		return Result{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("mutate: commit: %w", err)
	}
	return Result{}, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	return false
}
