package wal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SlotLagBytes returns how far behind restart_lsn the slot is, in bytes.
// A missing slot reports 0 (nothing is pinning WAL).
func SlotLagBytes(ctx context.Context, pool *pgxpool.Pool, slotName string) (int64, error) {
	if pool == nil {
		return 0, fmt.Errorf("wal: nil pool")
	}
	if slotName == "" {
		return 0, fmt.Errorf("wal: empty slot name")
	}
	var lag *int64
	err := pool.QueryRow(ctx, `
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
FROM pg_replication_slots
WHERE slot_name = $1`, slotName).Scan(&lag)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("wal: slot lag: %w", err)
	}
	if lag == nil {
		return 0, nil
	}
	if *lag < 0 {
		return 0, nil
	}
	return *lag, nil
}

// SlotExists reports whether a replication slot is present.
func SlotExists(ctx context.Context, pool *pgxpool.Pool, slotName string) (bool, error) {
	var n int
	err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("wal: slot exists: %w", err)
	}
	return n > 0, nil
}

// TerminateSlotBackend kills the walsender holding the slot, if any.
// Required before DropSlot while a consumer is connected.
func TerminateSlotBackend(ctx context.Context, pool *pgxpool.Pool, slotName string) error {
	_, err := pool.Exec(ctx, `
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE slot_name = $1 AND active AND active_pid IS NOT NULL`, slotName)
	if err != nil {
		return fmt.Errorf("wal: terminate slot backend: %w", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var active bool
		err := pool.QueryRow(ctx, `
SELECT COALESCE(active, false)
FROM pg_replication_slots
WHERE slot_name = $1`, slotName).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("wal: wait slot inactive: %w", err)
		}
		if !active {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil
}
