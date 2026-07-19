//go:build integration

package e2e_test

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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/wal"
)

type queuedMut struct {
	key   string
	rowID int
	title string
}

// TestConvergence is the v0.1 headline correctness claim from README / AGENTS:
// two clients edit overlapping rows while offline; one is killed mid-write and
// cold-restarted; both reconnect, replay, and end in identical state with no
// lost write and no double-apply.
func TestConvergence(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	ctx := context.Background()
	id := fmt.Sprintf("cv%d", time.Now().UnixNano()%1_000_000_000)
	table := "conv_" + id
	slot := "conv_slot_" + id
	pub := "conv_pub_" + id

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

	var applyCount atomic.Int64
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
	defer eng.Close()

	if err := eng.Shape(table, func(c tether.Claims) tether.Filter {
		return tether.Where("org_id = ?", c.(int64))
	}, tether.Table("public", table)); err != nil {
		t.Fatal(err)
	}

	eng.OnMutation(func(ctx context.Context, tx pgx.Tx, m tether.Mutation) error {
		if m.Op != "item.upsert" {
			return tether.Reject("unknown op")
		}
		applyCount.Add(1)
		_, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO public.%s (id, org_id, title) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title`, table),
			m.Arg("id"), m.Claims.(int64), m.Arg("title"))
		return err
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	time.Sleep(300 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	queueA := []queuedMut{
		{key: "a-" + id + "-1", rowID: 1, title: "a-row1"},
		{key: "a-" + id + "-2", rowID: 2, title: "a-row2"},
		{key: "a-" + id + "-3", rowID: 1, title: "a-row1-v2"},
	}
	queueB := []queuedMut{
		{key: "b-" + id + "-1", rowID: 1, title: "b-row1"},
		{key: "b-" + id + "-2", rowID: 3, title: "b-row3"},
		{key: "b-" + id + "-3", rowID: 2, title: "b-row2"},
	}
	allKeys := append(mutKeys(queueA), mutKeys(queueB)...)

	// 1. Both clients online briefly, empty snapshots, go offline.
	connA := dialHello(t, ctx, srv.URL)
	_ = snapSubscribe(t, ctx, connA, table)
	_ = connA.Close(websocket.StatusNormalClosure, "offline")

	connB := dialHello(t, ctx, srv.URL)
	_ = snapSubscribe(t, ctx, connB, table)
	_ = connB.Close(websocket.StatusNormalClosure, "offline")

	// 2. Simulated offline window (README "ten minutes" — not wall-clock).
	time.Sleep(50 * time.Millisecond)

	// 3. Client A reconnects; killed mid-write after first ok, during second send.
	connA = dialHello(t, ctx, srv.URL)
	writeMut(t, ctx, connA, queueA[0], "a-req-1")
	assertMutOK(t, ctx, connA)
	writeMut(t, ctx, connA, queueA[1], "a-req-2")
	_ = connA.Close(websocket.StatusGoingAway, "killed")

	// Cold restart: drop local shape state; retain and replay full queue.
	connA = dialHello(t, ctx, srv.URL)
	for i, m := range queueA {
		writeMut(t, ctx, connA, m, fmt.Sprintf("a-replay-%d", i))
		assertMutOK(t, ctx, connA)
	}
	_ = connA.Close(websocket.StatusNormalClosure, "done-a")

	// 4. Client B reconnects and replays full queue.
	connB = dialHello(t, ctx, srv.URL)
	for i, m := range queueB {
		writeMut(t, ctx, connB, m, fmt.Sprintf("b-replay-%d", i))
		assertMutOK(t, ctx, connB)
	}
	_ = connB.Close(websocket.StatusNormalClosure, "done-b")

	waitKeys(t, ctx, pool, allKeys)
	wantApplies := int64(len(allKeys))
	if got := applyCount.Load(); got != wantApplies {
		t.Fatalf("handler apply count=%d want %d (double-apply or lost apply)", got, wantApplies)
	}

	// 5. Fresh snapshots — clients must match each other and the table.
	snapA := freshSnapshot(t, ctx, srv.URL, table)
	snapB := freshSnapshot(t, ctx, srv.URL, table)
	dbRows := loadDB(t, ctx, pool, table)

	if !sameRows(snapA, snapB) {
		t.Fatalf("clients diverged:\n  A=%v\n  B=%v", snapA, snapB)
	}
	if !sameRows(snapA, dbRows) {
		t.Fatalf("clients != db:\n  clients=%v\n  db=%v", snapA, dbRows)
	}
	for _, idWant := range []int{1, 2, 3} {
		if _, ok := snapA[idWant]; !ok {
			t.Fatalf("lost write: missing row id=%d in final state %v", idWant, snapA)
		}
	}

	// 6. Full re-replay — every key duplicate; apply count unchanged.
	connA = dialHello(t, ctx, srv.URL)
	for i, m := range queueA {
		writeMut(t, ctx, connA, m, fmt.Sprintf("a-dup-%d", i))
		assertMutOKDup(t, ctx, connA, true)
	}
	_ = connA.Close(websocket.StatusNormalClosure, "")
	connB = dialHello(t, ctx, srv.URL)
	for i, m := range queueB {
		writeMut(t, ctx, connB, m, fmt.Sprintf("b-dup-%d", i))
		assertMutOKDup(t, ctx, connB, true)
	}
	_ = connB.Close(websocket.StatusNormalClosure, "")

	if got := applyCount.Load(); got != wantApplies {
		t.Fatalf("double-apply after re-replay: apply count=%d want %d", got, wantApplies)
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
}

func mutKeys(ms []queuedMut) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.key
	}
	return out
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

func dialHello(t *testing.T, ctx context.Context, baseURL string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + baseURL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeJSON(t, ctx, conn, proto.Hello{Type: "hello", Protocol: proto.ProtocolVersion})
	return conn
}

func snapSubscribe(t *testing.T, ctx context.Context, conn *websocket.Conn, shape string) map[int]string {
	t.Helper()
	writeJSON(t, ctx, conn, proto.Subscribe{Type: "subscribe", Shapes: []string{shape}})
	return readSnapshot(t, ctx, conn)
}

func freshSnapshot(t *testing.T, ctx context.Context, baseURL, shape string) map[int]string {
	t.Helper()
	conn := dialHello(t, ctx, baseURL)
	defer conn.Close(websocket.StatusNormalClosure, "")
	return snapSubscribe(t, ctx, conn, shape)
}

func readSnapshot(t *testing.T, ctx context.Context, conn *websocket.Conn) map[int]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, data, err := conn.Read(rctx)
		cancel()
		if err != nil {
			continue
		}
		var env struct {
			Type string           `json:"type"`
			Rows []map[string]any `json:"rows"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatal(err)
		}
		if env.Type != "snapshot" {
			continue
		}
		out := make(map[int]string, len(env.Rows))
		for _, row := range env.Rows {
			out[intFrom(row["id"])] = fmt.Sprint(row["title"])
		}
		return out
	}
	t.Fatal("timeout waiting for snapshot")
	return nil
}

func writeMut(t *testing.T, ctx context.Context, conn *websocket.Conn, m queuedMut, reqID string) {
	t.Helper()
	writeJSON(t, ctx, conn, proto.Mutation{
		Type: "mutation",
		ID:   reqID,
		Op:   "item.upsert",
		Key:  m.key,
		Args: map[string]any{"id": m.rowID, "title": m.title},
	})
}

func assertMutOK(t *testing.T, ctx context.Context, conn *websocket.Conn) {
	t.Helper()
	assertMutOKDup(t, ctx, conn, false)
}

func assertMutOKDup(t *testing.T, ctx context.Context, conn *websocket.Conn, requireDup bool) {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		t.Fatal(err)
	}
	var msg proto.MutationOK
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("body=%s: %v", data, err)
	}
	if msg.Type != "mutation_ok" {
		t.Fatalf("type=%q body=%s", msg.Type, data)
	}
	if requireDup && !msg.Duplicate {
		t.Fatalf("expected duplicate mutation_ok, body=%s", data)
	}
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

func waitKeys(t *testing.T, ctx context.Context, pool *pgxpool.Pool, keys []string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM tether.mutation_keys WHERE idempotency_key = ANY($1)`, keys).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n == len(keys) {
			time.Sleep(200 * time.Millisecond)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d mutation keys", len(keys))
}

func loadDB(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) map[int]string {
	t.Helper()
	rows, err := pool.Query(ctx, fmt.Sprintf(`SELECT id, title FROM public.%s ORDER BY id`, table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int]string{}
	for rows.Next() {
		var rowID int
		var title string
		if err := rows.Scan(&rowID, &title); err != nil {
			t.Fatal(err)
		}
		out[rowID] = title
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sameRows(a, b map[int]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	default:
		var n int
		_, _ = fmt.Sscan(fmt.Sprint(v), &n)
		return n
	}
}
