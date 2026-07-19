// Command bench measures tether end-to-end insert → WebSocket shape delivery.
//
// Phase B: commit-aligned lag (wall clock after Commit/CopyFrom returns),
// p50/p95/p99, and optional batched inserts (-batch).
//
//	make db-up
//	export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
//	make bench-lag
//
// See docs/benchmark.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/wal"
)

func main() {
	dsn := flag.String("dsn", envOr("TETHER_TEST_DSN", ""), "Postgres DSN (replication=database ok)")
	rowsN := flag.Int("rows", 5_000, "number of inserts after warmup")
	clientsN := flag.Int("clients", 1, "concurrent WebSocket subscribers")
	warmup := flag.Int("warmup", 100, "warmup inserts discarded from lag stats")
	buffer := flag.Int("buffer", 256, "per-client outbound buffer")
	batch := flag.Int("batch", 1, "rows per commit (1=single INSERT; >1 uses COPY)")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "bench: -dsn or TETHER_TEST_DSN required (make db-up first)")
		os.Exit(2)
	}
	if *rowsN <= 0 || *clientsN <= 0 || *batch <= 0 {
		fmt.Fprintln(os.Stderr, "bench: -rows, -clients, -batch must be > 0")
		os.Exit(2)
	}

	ctx := context.Background()
	id := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000_000)
	table := "bench_" + id
	slot := "bench_slot_" + id
	pub := "bench_pub_" + id

	poolDSN, err := stripRepl(*dsn)
	if err != nil {
		fail(err)
	}
	pool, err := pgxpool.New(ctx, poolDSN)
	if err != nil {
		fail(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE public.%s (
	id INT PRIMARY KEY,
	org_id INT NOT NULL,
	pad TEXT NOT NULL DEFAULT ''
)`, table)); err != nil {
		fail(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS public.%s", table))
		_ = wal.DropSlot(context.Background(), pool, slot)
		_ = wal.DropPublication(context.Background(), pool, pub)
	}()

	eng, err := tether.New(
		*dsn,
		tether.WithAuth(func(*http.Request) (tether.Claims, error) { return int64(1), nil }),
		tether.WithClaimsKey(func(c tether.Claims) string { return fmt.Sprint(c) }),
		tether.WithClientBuffer(*buffer),
		tether.WithSlotName(slot),
		tether.WithPublication(pub),
		tether.MaxSlotLag(0),
		tether.MaxClientIdle(0),
		tether.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	)
	if err != nil {
		fail(err)
	}
	defer eng.Close()

	if err := eng.Shape(table, func(c tether.Claims) tether.Filter {
		return tether.Where("org_id = ?", c.(int64))
	}, tether.Table("public", table)); err != nil {
		fail(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- eng.Run(runCtx) }()
	time.Sleep(400 * time.Millisecond)

	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	totalRows := *warmup + *rowsN
	totalNeed := int64(totalRows * *clientsN)
	var received atomic.Int64
	var commitAt sync.Map // id(int) -> commit unix nano (after Commit returns)
	var lagMu sync.Mutex
	var samples []time.Duration

	var wg sync.WaitGroup
	for i := 0; i < *clientsN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runClient(ctx, srv.URL, table, *warmup, &commitAt, &lagMu, &samples, &received); err != nil {
				fmt.Fprintln(os.Stderr, "bench client:", err)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)

	fmt.Printf("bench: table=%s clients=%d warmup=%d rows=%d batch=%d\n",
		table, *clientsN, *warmup, *rowsN, *batch)

	start := time.Now()
	if err := insertRows(ctx, pool, table, totalRows, *batch, &commitAt); err != nil {
		fail(err)
	}
	insertDone := time.Since(start)

	deadline := time.Now().Add(3 * time.Minute)
	for received.Load() < totalNeed && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	wall := time.Since(start)
	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}

	lagMu.Lock()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	out := append([]time.Duration(nil), samples...)
	lagMu.Unlock()

	got := received.Load()
	fmt.Printf("received: %d / %d (clients×rows including warmup)\n", got, totalNeed)
	fmt.Printf("insert_wall: %s (%.0f rows/s commit throughput)\n",
		insertDone.Round(time.Millisecond), float64(totalRows)/insertDone.Seconds())
	fmt.Printf("e2e_wall:    %s (%.0f rows/s insert→all clients)\n",
		wall.Round(time.Millisecond), float64(totalRows)/wall.Seconds())
	if len(out) == 0 {
		fmt.Println("commit_to_client_lag: no samples (clients may have failed)")
		os.Exit(1)
	}
	fmt.Printf(
		"commit_to_client_lag: n=%d p50=%s p95=%s p99=%s max=%s\n",
		len(out),
		percentile(out, 50).Round(time.Microsecond),
		percentile(out, 95).Round(time.Microsecond),
		percentile(out, 99).Round(time.Microsecond),
		out[len(out)-1].Round(time.Microsecond),
	)
	fmt.Println("note: lag = WS recv − wall clock when INSERT/COPY commit returned (durable from client POV)")
	if got < totalNeed {
		os.Exit(1)
	}
}

func insertRows(ctx context.Context, pool *pgxpool.Pool, table string, total, batch int, commitAt *sync.Map) error {
	if batch == 1 {
		q := fmt.Sprintf(`INSERT INTO public.%s (id, org_id, pad) VALUES ($1, 1, '')`, table)
		for id := 1; id <= total; id++ {
			if _, err := pool.Exec(ctx, q, id); err != nil {
				return err
			}
			commitAt.Store(id, time.Now().UnixNano())
		}
		return nil
	}

	for id := 1; id <= total; {
		end := id + batch - 1
		if end > total {
			end = total
		}
		rows := make([][]any, 0, end-id+1)
		for i := id; i <= end; i++ {
			rows = append(rows, []any{i, 1, ""})
		}
		_, err := pool.CopyFrom(ctx, pgx.Identifier{"public", table}, []string{"id", "org_id", "pad"}, pgx.CopyFromRows(rows))
		if err != nil {
			return fmt.Errorf("copy %d-%d: %w", id, end, err)
		}
		ns := time.Now().UnixNano()
		for i := id; i <= end; i++ {
			commitAt.Store(i, ns)
		}
		id = end + 1
	}
	return nil
}

func runClient(
	ctx context.Context,
	baseURL, shape string,
	warmup int,
	commitAt *sync.Map,
	lagMu *sync.Mutex,
	samples *[]time.Duration,
	received *atomic.Int64,
) error {
	wsURL := "ws" + baseURL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

	if err := writeJSON(ctx, conn, proto.Hello{Type: "hello", Protocol: proto.ProtocolVersion}); err != nil {
		return err
	}
	if err := writeJSON(ctx, conn, proto.Subscribe{Type: "subscribe", Shapes: []string{shape}}); err != nil {
		return err
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		switch env.Type {
		case "snapshot":
			continue
		case "change":
			var ch proto.Change
			if err := json.Unmarshal(data, &ch); err != nil {
				continue
			}
			recv := time.Now()
			rowID := intFrom(ch.Row["id"])
			if rowID > warmup {
				if v, ok := commitAt.Load(rowID); ok {
					commitNs := v.(int64)
					lagMu.Lock()
					*samples = append(*samples, recv.Sub(time.Unix(0, commitNs)))
					lagMu.Unlock()
				}
			}
			received.Add(1)
		case "bye", "error":
			return fmt.Errorf("server %s: %s", env.Type, data)
		}
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	// Nearest-rank: ceil(p/100 * n) - 1
	idx := (p*len(sorted) + 99) / 100
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
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

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int64:
		return int(x)
	case int32:
		return int(x)
	case int:
		return x
	default:
		var n int
		_, _ = fmt.Sscan(fmt.Sprint(v), &n)
		return n
	}
}
