package wal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TableRef names a Postgres table included in the publication.
type TableRef struct {
	Schema string
	Name   string
}

func (t TableRef) Qualified() string {
	schema := t.Schema
	if schema == "" {
		schema = "public"
	}
	return quoteIdent(schema) + "." + quoteIdent(t.Name)
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// Config configures a WAL consumer.
type Config struct {
	ConsumerID  string
	SlotName    string
	Publication string
	Tables      []TableRef
}

func (c Config) validate() error {
	if c.ConsumerID == "" || c.SlotName == "" || c.Publication == "" {
		return fmt.Errorf("%w: ConsumerID, SlotName, and Publication are required", ErrNotConfigured)
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("%w: at least one table is required", ErrNotConfigured)
	}
	for _, t := range c.Tables {
		if t.Name == "" {
			return fmt.Errorf("%w: table name is required", ErrNotConfigured)
		}
	}
	return nil
}

// EnsurePublication creates the publication if missing and adds any listed
// tables that are not yet members. Idempotent.
func EnsurePublication(ctx context.Context, pool *pgxpool.Pool, name string, tables []TableRef) error {
	if pool == nil {
		return fmt.Errorf("wal: pool is nil")
	}
	if name == "" {
		return fmt.Errorf("wal: publication name is required")
	}
	if len(tables) == 0 {
		return fmt.Errorf("wal: publication requires at least one table")
	}

	var exists bool
	err := pool.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)`, name,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("wal: check publication: %w", err)
	}

	if !exists {
		parts := make([]string, len(tables))
		for i, t := range tables {
			parts[i] = t.Qualified()
		}
		sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quoteIdent(name), strings.Join(parts, ", "))
		if _, err := pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("wal: create publication: %w", err)
		}
		return nil
	}

	for _, t := range tables {
		schema := t.Schema
		if schema == "" {
			schema = "public"
		}
		var member bool
		err := pool.QueryRow(ctx, `
SELECT EXISTS(
	SELECT 1
	FROM pg_publication_tables
	WHERE pubname = $1 AND schemaname = $2 AND tablename = $3
)`, name, schema, t.Name).Scan(&member)
		if err != nil {
			return fmt.Errorf("wal: check publication member: %w", err)
		}
		if member {
			continue
		}
		sql := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", quoteIdent(name), t.Qualified())
		if _, err := pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("wal: add table to publication: %w", err)
		}
	}
	return nil
}

// EnsureSlot creates a logical pgoutput replication slot if it does not exist.
// Must be called on a replication connection.
func EnsureSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) error {
	if conn == nil {
		return fmt.Errorf("wal: replication conn is nil")
	}
	if slotName == "" {
		return fmt.Errorf("wal: slot name is required")
	}

	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" {
		return nil
	}
	if strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return fmt.Errorf("wal: create slot: %w", err)
}

// DropSlot drops a replication slot if present. Used by tests for cleanup.
func DropSlot(ctx context.Context, pool *pgxpool.Pool, slotName string) error {
	_, err := pool.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slotName)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42704" {
			return nil
		}
		if strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		return fmt.Errorf("wal: drop slot: %w", err)
	}
	return nil
}

// DropPublication drops a publication if present. Used by tests for cleanup.
func DropPublication(ctx context.Context, pool *pgxpool.Pool, name string) error {
	_, err := pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", quoteIdent(name)))
	if err != nil {
		return fmt.Errorf("wal: drop publication: %w", err)
	}
	return nil
}

func loadCheckpoint(ctx context.Context, pool *pgxpool.Pool, consumerID string) (string, error) {
	var lsn string
	err := pool.QueryRow(
		ctx,
		`SELECT confirmed_lsn FROM tether.checkpoint WHERE consumer_id = $1`,
		consumerID,
	).Scan(&lsn)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("wal: load checkpoint: %w", err)
	}
	return lsn, nil
}
