package snapshot

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether/internal/shape"
)

// Request describes a filtered table snapshot on a normal connection pool.
type Request struct {
	Schema  string
	Table   string
	Columns []string // empty means SELECT *
	Filter  shape.Filter
}

// Result is a consistent snapshot at LSN plus matching rows.
type Result struct {
	LSN  pglogrepl.LSN
	Rows []map[string]any
}

// Take reads rows under REPEATABLE READ and returns the snapshot LSN _N_
// recorded at transaction start. Callers must apply WAL changes strictly
// after _N_ (AGENTS.md Invariant 4).
//
// Uses a normal pool connection — never the replication connection.
func Take(ctx context.Context, pool *pgxpool.Pool, req Request) (Result, error) {
	if pool == nil {
		return Result{}, fmt.Errorf("snapshot: pool is nil")
	}
	schema := req.Schema
	if schema == "" {
		schema = "public"
	}
	if req.Table == "" {
		return Result{}, fmt.Errorf("snapshot: table is required")
	}
	if err := validateIdent(schema); err != nil {
		return Result{}, err
	}
	if err := validateIdent(req.Table); err != nil {
		return Result{}, err
	}

	where, args, err := req.Filter.SQLClause()
	if err != nil {
		return Result{}, fmt.Errorf("snapshot: filter: %w", err)
	}

	cols := "*"
	if len(req.Columns) > 0 {
		quoted := make([]string, len(req.Columns))
		for i, c := range req.Columns {
			if err := validateIdent(c); err != nil {
				return Result{}, err
			}
			quoted[i] = quoteIdent(c)
		}
		cols = strings.Join(quoted, ", ")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return Result{}, fmt.Errorf("snapshot: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var lsnText string
	if err := tx.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsnText); err != nil {
		return Result{}, fmt.Errorf("snapshot: read lsn: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return Result{}, fmt.Errorf("snapshot: parse lsn %q: %w", lsnText, err)
	}

	sql := fmt.Sprintf(`SELECT %s FROM %s.%s WHERE %s`,
		cols, quoteIdent(schema), quoteIdent(req.Table), where)
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return Result{}, fmt.Errorf("snapshot: query: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	var out []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return Result{}, fmt.Errorf("snapshot: values: %w", err)
		}
		row := make(map[string]any, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[string(fd.Name)] = vals[i]
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("snapshot: rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("snapshot: commit: %w", err)
	}
	return Result{LSN: lsn, Rows: out}, nil
}

func validateIdent(name string) error {
	if name == "" {
		return fmt.Errorf("snapshot: empty identifier")
	}
	for i, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' ||
			(i > 0 && r >= '0' && r <= '9')
		if !ok {
			return fmt.Errorf("snapshot: invalid identifier %q", name)
		}
	}
	return nil
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
