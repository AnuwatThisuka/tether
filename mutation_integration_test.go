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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/wal"
)

var p5seq atomic.Uint64

func TestMutationIdempotentAndVisible(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	n := p5seq.Add(1)
	id := fmt.Sprintf("p5%d%d", time.Now().Unix()%100000, n)
	table := "mut_" + id
	slot := "mut_slot_" + id
	pub := "mut_pub_" + id

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

	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) { return int64(1), nil }),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
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

	eng.OnMutation(func(ctx context.Context, tx pgx.Tx, m tether.Mutation) error {
		switch m.Op {
		case "item.upsert":
			idArg := m.Arg("id")
			title := m.Arg("title")
			_, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO public.%s (id, org_id, title) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title`, table),
				idArg, m.Claims.(int64), title)
			return err
		default:
			return tether.Reject("unknown op")
		}
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	// Subscriber sees mutation effects via WAL.
	sub, _ := dialSubscribe(t, ctx, srv.URL, table, nil)
	defer sub.Close(websocket.StatusNormalClosure, "")

	writer, _, err := websocket.Dial(ctx, "ws"+srv.URL[len("http"):], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(websocket.StatusNormalClosure, "")
	writeJSON(t, ctx, writer, proto.Hello{Type: "hello", Protocol: 1})

	key := "idem-" + id
	writeJSON(t, ctx, writer, proto.Mutation{
		Type: "mutation", ID: "req-1", Op: "item.upsert", Key: key,
		Args: map[string]any{"id": 1, "title": "hello"},
	})
	assertMutationOK(t, ctx, writer, false)

	// Replay same key — still ok, no second effect.
	writeJSON(t, ctx, writer, proto.Mutation{
		Type: "mutation", ID: "req-2", Op: "item.upsert", Key: key,
		Args: map[string]any{"id": 1, "title": "should-not-apply"},
	})
	assertMutationOK(t, ctx, writer, true)

	var title string
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT title FROM public.%s WHERE id=1`, table)).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "hello" {
		t.Fatalf("title=%q after duplicate mutate, want hello", title)
	}

	// Subscriber should see the insert via shape stream.
	_ = waitChange(t, ctx, sub, "hello")

	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
}

func assertMutationOK(t *testing.T, ctx context.Context, conn *websocket.Conn, wantDup bool) {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		t.Fatal(err)
	}
	var msg proto.MutationOK
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	if msg.Type != "mutation_ok" {
		t.Fatalf("type=%q body=%s", msg.Type, data)
	}
	if msg.Duplicate != wantDup {
		t.Fatalf("duplicate=%v want %v", msg.Duplicate, wantDup)
	}
}
