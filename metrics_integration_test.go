//go:build integration

package tether_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/wal"
)

func TestMetrics_LagAndClientsSampled(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	id := fmt.Sprintf("met%d", time.Now().UnixNano()%1_000_000_000)
	table := "met_" + id
	slot := "met_slot_" + id
	pub := "met_pub_" + id

	rec := &recordingMetrics{}
	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) { return int64(1), nil }),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
		tether.MaxSlotLag(0),
		tether.MaxClientIdle(0),
		tether.WithMetrics(rec),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

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

	var forced atomic.Bool
	eng.SetSlotLagOverrideForTest(func(context.Context) (int64, error) {
		forced.Store(true)
		return 42_000, nil
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = eng.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()
	conn, _ := dialSubscribe(t, ctx, srv.URL, table, nil)
	defer conn.Close(websocket.StatusNormalClosure, "")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		sawLag := false
		for _, n := range rec.lag {
			if n == 42_000 {
				sawLag = true
				break
			}
		}
		sawClient := false
		for _, n := range rec.clients {
			if n >= 1 {
				sawClient = true
				break
			}
		}
		sawOffset := len(rec.offsets) > 0
		rec.mu.Unlock()
		if sawLag && sawClient && sawOffset {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	t.Fatalf("metrics incomplete forced=%v lag=%v clients=%v offsets=%d",
		forced.Load(), rec.lag, rec.clients, len(rec.offsets))
}
