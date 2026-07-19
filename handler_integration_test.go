//go:build integration

package tether_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

var p4seq atomic.Uint64

func TestWebSocketResume(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	n := p4seq.Add(1)
	id := fmt.Sprintf("p4%d%d", time.Now().Unix()%100000, n)
	table := "ws_" + id
	slot := "ws_slot_" + id
	pub := "ws_pub_" + id

	eng, err := tether.New(
		dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) {
			return int64(1), nil
		}),
		tether.WithClaimsKey(func(c tether.Claims) string { return fmt.Sprint(c) }),
		tether.WithClientBuffer(32),
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

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, org_id, title) VALUES (1, 1, 'seed')`, table,
	)); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	connA, snapOff := dialSubscribe(t, ctx, srv.URL, table, nil)
	defer connA.Close(websocket.StatusNormalClosure, "")

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, org_id, title) VALUES (2, 1, 'live')`, table,
	)); err != nil {
		t.Fatal(err)
	}
	ch1 := waitChange(t, ctx, connA, "live")
	lastOff := ch1.Offset
	if lastOff <= snapOff {
		t.Fatalf("change offset %d should be > snapshot offset %d", lastOff, snapOff)
	}

	_ = connA.Close(websocket.StatusNormalClosure, "reconnect")

	connB, _ := dialSubscribe(t, ctx, srv.URL, table, map[string]int64{table: lastOff})
	defer connB.Close(websocket.StatusNormalClosure, "")

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, org_id, title) VALUES (3, 1, 'resumed')`, table,
	)); err != nil {
		t.Fatal(err)
	}
	ch2 := waitChange(t, ctx, connB, "resumed")
	if ch2.Offset <= lastOff {
		t.Fatalf("resumed offset %d want > %d", ch2.Offset, lastOff)
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
}

func dialSubscribe(t *testing.T, ctx context.Context, baseURL, shape string, resume map[string]int64) (*websocket.Conn, int64) {
	t.Helper()
	wsURL := "ws" + baseURL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeJSON(t, ctx, conn, proto.Hello{Type: "hello", Protocol: 1, Resume: resume})
	writeJSON(t, ctx, conn, proto.Subscribe{Type: "subscribe", Shapes: []string{shape}})

	if resume != nil && resume[shape] > 0 {
		return conn, resume[shape]
	}
	var env map[string]any
	readJSON(t, ctx, conn, &env)
	if env["type"] != "snapshot" {
		t.Fatalf("want snapshot, got %#v", env)
	}
	off, _ := env["offset"].(float64)
	return conn, int64(off)
}

func waitChange(t *testing.T, ctx context.Context, conn *websocket.Conn, title string) proto.Change {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var msg proto.Change
		typ := readTyped(t, ctx, conn, &msg)
		if typ != "change" {
			continue
		}
		if fmt.Sprint(msg.Row["title"]) == title {
			return msg
		}
	}
	t.Fatalf("timeout waiting for change title=%s", title)
	return proto.Change{}
}

func writeJSON(t *testing.T, ctx context.Context, conn *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, ctx context.Context, conn *websocket.Conn, dest any) {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, dest); err != nil {
		t.Fatal(err)
	}
}

func readTyped(t *testing.T, ctx context.Context, conn *websocket.Conn, change *proto.Change) string {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	if env.Type == "change" {
		if err := json.Unmarshal(data, change); err != nil {
			t.Fatal(err)
		}
	}
	return env.Type
}

func stripRepl(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String(), nil
}
