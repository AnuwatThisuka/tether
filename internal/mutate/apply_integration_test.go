//go:build integration

package mutate_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether/internal/mutate"
)

type rejectErr struct{ reason string }

func (e *rejectErr) Error() string { return e.reason }

func TestApply_IdempotentByKey(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := mutate.EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}

	calls := 0
	handler := func(ctx context.Context, tx pgx.Tx, op, key string, claims any, args map[string]any) error {
		calls++
		_, err := tx.Exec(ctx, `SELECT 1`)
		return err
	}

	res, err := mutate.Apply(ctx, pool, "k1-"+t.Name(), "op", nil, nil, handler, nil, nil)
	if err != nil || res.Duplicate || res.Rejected {
		t.Fatalf("first apply: res=%+v err=%v", res, err)
	}
	res, err = mutate.Apply(ctx, pool, "k1-"+t.Name(), "op", nil, nil, handler, nil, nil)
	if err != nil || !res.Duplicate {
		t.Fatalf("second apply: res=%+v err=%v", res, err)
	}
	if calls != 1 {
		t.Fatalf("handler calls=%d, want 1", calls)
	}
}

func TestApply_RejectRollsBackKey(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := mutate.EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}

	key := "k-reject-" + t.Name()
	handler := func(ctx context.Context, tx pgx.Tx, op, key string, claims any, args map[string]any) error {
		return &rejectErr{reason: "nope"}
	}
	isReject := func(err error) bool {
		var r *rejectErr
		return errors.As(err, &r)
	}
	reasonFn := func(err error) string {
		var r *rejectErr
		if errors.As(err, &r) {
			return r.reason
		}
		return ""
	}

	res, err := mutate.Apply(ctx, pool, key, "op", nil, nil, handler, isReject, reasonFn)
	if err != nil || !res.Rejected || res.Reason != "nope" {
		t.Fatalf("reject: res=%+v err=%v", res, err)
	}

	calls := 0
	okHandler := func(ctx context.Context, tx pgx.Tx, op, key string, claims any, args map[string]any) error {
		calls++
		return nil
	}
	res, err = mutate.Apply(ctx, pool, key, "op", nil, nil, okHandler, isReject, reasonFn)
	if err != nil || res.Duplicate || res.Rejected || calls != 1 {
		t.Fatalf("retry after reject: res=%+v calls=%d err=%v", res, calls, err)
	}
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	pool, err := pgxpool.New(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
