//go:build integration

package wal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether/internal/wal"
)

var harnessSeq atomic.Uint64

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	return dsn
}

func normalDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

type harness struct {
	t     *testing.T
	pool  *pgxpool.Pool
	cfg   wal.Config
	table string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()
	replDSN := testDSN(t)
	norm, err := normalDSN(replDSN)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, norm)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	n := harnessSeq.Add(1)
	suffix := fmt.Sprintf("%d_%d", time.Now().UnixNano(), n)
	// Slot/publication names must be valid unquoted identifiers.
	id := fmt.Sprintf("t%d%d", time.Now().Unix()%100000, n)
	table := "wal_p1_" + id
	slot := "tether_slot_" + id
	pub := "tether_pub_" + id
	consumer := "consumer_" + id
	_ = suffix

	h := &harness{
		t:     t,
		pool:  pool,
		table: table,
		cfg: wal.Config{
			ConsumerID:  consumer,
			SlotName:    slot,
			Publication: pub,
			Tables:      []wal.TableRef{{Schema: "public", Name: table}},
		},
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE public.%s (
	id INT PRIMARY KEY,
	name TEXT NOT NULL,
	note TEXT NOT NULL DEFAULT ''
)`, table)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := wal.EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := wal.EnsurePublication(ctx, pool, pub, h.cfg.Tables); err != nil {
		t.Fatal(err)
	}

	repl, err := pgconn.Connect(ctx, replDSN)
	if err != nil {
		t.Fatalf("repl connect: %v", err)
	}
	if err := wal.EnsureSlot(ctx, repl, slot); err != nil {
		_ = repl.Close(ctx)
		t.Fatalf("ensure slot: %v", err)
	}
	_ = repl.Close(ctx)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = wal.DropSlot(ctx, pool, slot)
		_ = wal.DropPublication(ctx, pool, pub)
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS public.%s", table))
		_, _ = pool.Exec(ctx, `DELETE FROM tether.change_log WHERE consumer_id = $1`, consumer)
		_, _ = pool.Exec(ctx, `DELETE FROM tether.checkpoint WHERE consumer_id = $1`, consumer)
	})

	return h
}

func (h *harness) openRepl(t *testing.T) *pgconn.PgConn {
	t.Helper()
	conn, err := pgconn.Connect(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("repl connect: %v", err)
	}
	return conn
}

func (h *harness) runUntil(t *testing.T, pred func() bool, hook func(pglogrepl.LSN) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	repl := h.openRepl(t)
	defer func() { _ = repl.Close(context.Background()) }()

	c, err := wal.NewConsumer(h.pool, repl, h.cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.AfterDurableCommit = hook

	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(ctx) }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			cancel()
			select {
			case err := <-errCh:
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					if hook != nil && strings.Contains(err.Error(), "after durable commit hook") {
						return
					}
					t.Fatalf("consumer: %v", err)
				}
			case <-time.After(3 * time.Second):
			}
			return
		}
		select {
		case err := <-errCh:
			if hook != nil && err != nil && strings.Contains(err.Error(), "after durable commit hook") {
				return
			}
			t.Fatalf("consumer stopped early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	<-errCh
	t.Fatal("timeout waiting for consumer predicate")
}

func (h *harness) changeCount(ops ...string) int {
	h.t.Helper()
	ctx := context.Background()
	q := `SELECT count(*) FROM tether.change_log WHERE consumer_id = $1 AND table_name = $2`
	args := []any{h.cfg.ConsumerID, h.table}
	if len(ops) == 1 {
		q += ` AND op = $3`
		args = append(args, ops[0])
	}
	var n int
	if err := h.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		h.t.Fatalf("count: %v", err)
	}
	return n
}

func TestIngestInsertPersisted(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name) VALUES (1, 'alpha')`, h.table,
	)); err != nil {
		t.Fatal(err)
	}

	h.runUntil(t, func() bool { return h.changeCount("insert") >= 1 }, nil)

	var newRow []byte
	err := h.pool.QueryRow(ctx, `
SELECT new_row FROM tether.change_log
WHERE consumer_id = $1 AND table_name = $2 AND op = 'insert'
ORDER BY id LIMIT 1`, h.cfg.ConsumerID, h.table).Scan(&newRow)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(newRow, &m); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(m["name"]) != "alpha" {
		t.Fatalf("new_row=%v, want name=alpha", m)
	}
}

func TestRestartResumesWithoutGap(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name) VALUES (1, 'one')`, h.table,
	)); err != nil {
		t.Fatal(err)
	}
	h.runUntil(t, func() bool { return h.changeCount("insert") >= 1 }, nil)

	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name) VALUES (2, 'two')`, h.table,
	)); err != nil {
		t.Fatal(err)
	}
	h.runUntil(t, func() bool { return h.changeCount("insert") >= 2 }, nil)

	if n := h.changeCount("insert"); n != 2 {
		t.Fatalf("insert count = %d, want 2", n)
	}
}

func TestPersistBeforeAck_CrashReplay(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name) VALUES (1, 'crash-me')`, h.table,
	)); err != nil {
		t.Fatal(err)
	}

	h.runUntil(t, func() bool { return h.changeCount("insert") >= 1 }, func(pglogrepl.LSN) error {
		return errors.New("simulated crash before ack")
	})

	if h.changeCount("insert") < 1 {
		t.Fatal("change missing after crash-before-ack")
	}

	h.runUntil(t, func() bool {
		var lsn string
		_ = h.pool.QueryRow(ctx, `
SELECT confirmed_lsn FROM tether.checkpoint WHERE consumer_id = $1`, h.cfg.ConsumerID).Scan(&lsn)
		return lsn != "" && h.changeCount("insert") >= 1
	}, nil)

	if n := h.changeCount("insert"); n != 1 {
		t.Fatalf("insert count = %d, want 1 (no duplicate after replay)", n)
	}
}

func TestSchemaDriftStopsConsumer(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	repl := h.openRepl(t)
	defer func() { _ = repl.Close(context.Background()) }()
	c, err := wal.NewConsumer(h.pool, repl, h.cfg)
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(runCtx) }()

	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, name) VALUES (1, 'before')`, h.table,
	)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for h.changeCount("insert") < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for initial insert")
		}
		select {
		case err := <-errCh:
			t.Fatalf("consumer stopped early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE public.%s ADD COLUMN extra TEXT`, h.table,
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE public.%s SET name = 'after' WHERE id = 1`, h.table,
	)); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected schema drift error, got nil")
		}
		if !errors.Is(err, wal.ErrSchemaDrift) {
			t.Fatalf("err = %v, want ErrSchemaDrift", err)
		}
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("timeout waiting for schema drift error")
	}
}
