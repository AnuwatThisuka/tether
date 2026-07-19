//go:build integration

package tether_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/wal"
)

// Close after cancel must not deadlock with an in-flight fan-out cycle.
// Mirrors Fatalf cleanup order (defer cancel, then defer eng.Close) when
// fan-out still holds a change_log query and dispatchChange needs e.mu —
// holding that mutex across pool.Close hung CI for the full 10m test timeout.
func TestClose_WhileFanOut(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	n := p6seq.Add(1)
	id := fmt.Sprintf("cls%d%d", time.Now().Unix()%100000, n)
	table := "cls_" + id
	slot := "cls_slot_" + id
	pub := "cls_pub_" + id

	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) {
			return int64(1), nil
		}),
		tether.WithClaimsKey(func(c tether.Claims) string { return fmt.Sprint(c) }),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
		tether.MaxSlotLag(0),
		tether.MaxClientIdle(0),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := eng.Shape(table, func(c tether.Claims) tether.Filter {
		return tether.Where("org_id = ?", c.(int64))
	}, tether.Table("public", table)); err != nil {
		t.Fatal(err)
	}

	poolDSN, err := stripRepl(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, poolDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE public.%s (
	id INT PRIMARY KEY,
	org_id INT NOT NULL,
	title TEXT NOT NULL
)`, table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS public.%s", table))
		_ = wal.DropSlot(context.Background(), pool, slot)
		_ = wal.DropPublication(context.Background(), pool, pub)
	})

	runCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)

	for i := 1; i <= 40; i++ {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO public.%s (id, org_id, title) VALUES ($1, 1, $2)`, table),
			i, fmt.Sprintf("r%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(80 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		cancel()
		eng.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close deadlocked with fan-out (held mutex across pool.Close)")
	}
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
}
