package wal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureSchema creates the internal tether schema used for the Phase 1
// durable change log and LSN checkpoint (Invariant 1).
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("wal: pool is nil")
	}
	_, err := pool.Exec(ctx, `
CREATE SCHEMA IF NOT EXISTS tether;

CREATE TABLE IF NOT EXISTS tether.checkpoint (
	consumer_id TEXT PRIMARY KEY,
	confirmed_lsn TEXT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tether.change_log (
	id BIGSERIAL PRIMARY KEY,
	consumer_id TEXT NOT NULL,
	lsn TEXT NOT NULL,
	xid BIGINT,
	seq INT NOT NULL,
	schema_name TEXT NOT NULL,
	table_name TEXT NOT NULL,
	op TEXT NOT NULL,
	relation_fingerprint TEXT NOT NULL,
	old_row JSONB,
	new_row JSONB,
	UNIQUE (consumer_id, lsn, seq)
);
`)
	if err != nil {
		return fmt.Errorf("wal: ensure schema: %w", err)
	}
	return nil
}
