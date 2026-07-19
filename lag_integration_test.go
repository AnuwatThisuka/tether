//go:build integration

package tether_test

import (
	"context"
	"encoding/json"
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
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/wal"
)

var p6seq atomic.Uint64

func TestSlotLag_ForcesResnapshot(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	n := p6seq.Add(1)
	id := fmt.Sprintf("p6%d%d", time.Now().Unix()%100000, n)
	table := "lagws_" + id
	slot := "lagws_slot_" + id
	pub := "lagws_pub_" + id

	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) {
			return int64(1), nil
		}),
		tether.WithClaimsKey(func(c tether.Claims) string { return fmt.Sprint(c) }),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
		// High enough that setup WAL cannot trip the guard; override forces one trip.
		tether.MaxSlotLag(1<<62),
		tether.MaxClientIdle(0),
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
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	conn, _ := dialSubscribe(t, ctx, srv.URL, table, nil)
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Artificial lag once: override reports over MaxSlotLag so the guard trips.
	var tripped atomic.Bool
	eng.SetSlotLagOverrideForTest(func(context.Context) (int64, error) {
		if tripped.CompareAndSwap(false, true) {
			return 1<<62 + 1, nil
		}
		return 0, nil
	})

	deadline := time.Now().Add(10 * time.Second)
	var sawResnapshot bool
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		_, data, err := conn.Read(rctx)
		rcancel()
		if err != nil {
			continue
		}
		var env struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatal(err)
		}
		if env.Type == "error" && env.Code == proto.CodeMustResnapshot {
			sawResnapshot = true
			break
		}
	}
	if !sawResnapshot {
		t.Fatal("expected must_resnapshot after slot lag exceeded")
	}

	// Slot must be recreated so Postgres is not left without a consumer target forever.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		exists, err := wal.SlotExists(ctx, pool, slot)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			cancel()
			select {
			case <-errCh:
			case <-time.After(3 * time.Second):
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("slot was not recreated after lag drop")
}

func TestMaxClientIdle_Disconnects(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	n := p6seq.Add(1)
	id := fmt.Sprintf("idle%d%d", time.Now().Unix()%100000, n)
	table := "idle_" + id
	slot := "idle_slot_" + id
	pub := "idle_pub_" + id

	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) {
			return int64(1), nil
		}),
		tether.WithClaimsKey(func(c tether.Claims) string { return fmt.Sprint(c) }),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
		tether.MaxSlotLag(0),
		tether.MaxClientIdle(200*time.Millisecond),
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
	defer cancel()
	go func() { _ = eng.Run(runCtx) }()
	time.Sleep(200 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	wsURL := "ws" + srv.URL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	hello, _ := json.Marshal(proto.Hello{Type: "hello", Protocol: proto.ProtocolVersion})
	if err := conn.Write(ctx, websocket.MessageText, hello); err != nil {
		t.Fatal(err)
	}
	sub, _ := json.Marshal(proto.Subscribe{Type: "subscribe", Shapes: []string{table}})
	if err := conn.Write(ctx, websocket.MessageText, sub); err != nil {
		t.Fatal(err)
	}

	// Drain snapshot then stay silent until idle bye.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(ctx, time.Second)
		_, data, err := conn.Read(rctx)
		rcancel()
		if err != nil {
			// Idle disconnect closes the socket.
			return
		}
		var env struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatal(err)
		}
		if env.Type == "bye" && env.Reason == proto.ReasonIdleClient {
			return
		}
	}
	t.Fatal("expected idle_client bye")
}
