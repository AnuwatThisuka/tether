//go:build integration

package tether_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/wal"
)

func TestMetrics_SlowClientDisconnected(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	id := fmt.Sprintf("bp%d", time.Now().UnixNano()%1_000_000_000)
	table := "bp_" + id
	slot := "bp_slot_" + id
	pub := "bp_pub_" + id

	rec := &recordingMetrics{}
	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) { return int64(1), nil }),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
		tether.WithClientBuffer(1), // tiny buffer → easy slow_client
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

	runCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	defer func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
		}
	}()
	time.Sleep(300 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	// Connect and subscribe, then stop reading so the outbound buffer fills.
	// Do not Read again in the wait loop — draining the socket relieves
	// backpressure and races past slow_client on fast CI hosts.
	wsURL := "ws" + srv.URL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	writeJSON(t, ctx, conn, proto.Hello{Type: "hello", Protocol: proto.ProtocolVersion})
	writeJSON(t, ctx, conn, proto.Subscribe{Type: "subscribe", Shapes: []string{table}})
	if typ := readTyped(t, ctx, conn, &proto.Change{}); typ != "snapshot" {
		t.Fatalf("want snapshot after subscribe, got %q", typ)
	}

	// Large rows fill the peer TCP window so WritePump blocks and the
	// 1-slot enqueue buffer trips Invariant 7 (small JSON often never does).
	pad := strings.Repeat("x", 256*1024)
	for i := 0; i < 32; i++ {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO public.%s (id, org_id, title) VALUES ($1, 1, $2)`, table,
		), i+1, pad); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		for _, r := range rec.disc {
			if r == proto.ReasonSlowClient {
				rec.mu.Unlock()
				return
			}
		}
		rec.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	t.Fatalf("expected ClientDisconnected(%q), got %v", proto.ReasonSlowClient, rec.disc)
}
