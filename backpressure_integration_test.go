//go:build integration

package tether_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

	// Connect and subscribe, then stop reading so the buffer fills.
	wsURL := "ws" + srv.URL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	writeJSON(t, ctx, conn, proto.Hello{Type: "hello", Protocol: proto.ProtocolVersion})
	writeJSON(t, ctx, conn, proto.Subscribe{Type: "subscribe", Shapes: []string{table}})
	// Drain snapshot once so membership is live, then stop consuming.
	_ = readTyped(t, ctx, conn, &proto.Change{})

	for i := 0; i < 40; i++ {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO public.%s (id, org_id, title) VALUES ($1, 1, $2)`, table,
		), i+1, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		for _, r := range rec.disc {
			if r == proto.ReasonSlowClient {
				rec.mu.Unlock()
				return
			}
		}
		rec.mu.Unlock()
		// Best-effort read so we notice socket close; ignore body.
		rctx, rcancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, data, err := conn.Read(rctx)
		rcancel()
		if err == nil {
			var env struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			}
			_ = json.Unmarshal(data, &env)
			if env.Type == "bye" && env.Reason == proto.ReasonSlowClient {
				// Wait briefly for metric emit (same call path as bye).
				time.Sleep(50 * time.Millisecond)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	t.Fatalf("expected ClientDisconnected(%q), got %v", proto.ReasonSlowClient, rec.disc)
}
