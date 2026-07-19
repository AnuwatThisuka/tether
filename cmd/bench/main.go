// Command bench measures tether end-to-end insert → WebSocket shape delivery.
//
//	make db-up
//	export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
//	make bench
//
// This is a tether microbench, not a fair bake-off against Electric/PowerSync/Zero.
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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/wal"
)

func main() {
	dsn := flag.String("dsn", envOr("TETHER_TEST_DSN", ""), "Postgres DSN (replication=database ok)")
	rowsN := flag.Int("rows", 5_000, "number of inserts after warmup")
	clientsN := flag.Int("clients", 1, "concurrent WebSocket subscribers")
	warmup := flag.Int("warmup", 100, "warmup inserts discarded from stats")
	buffer := flag.Int("buffer", 256, "per-client outbound buffer")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "bench: -dsn or TETHER_TEST_DSN required (make db-up first)")
		os.Exit(2)
	}
	if *rowsN <= 0 || *clientsN <= 0 {
		fmt.Fprintln(os.Stderr, "bench: -rows and -clients must be > 0")
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
	sent_ns BIGINT NOT NULL
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

	totalNeed := int64((*warmup + *rowsN) * *clientsN)
	var received atomic.Int64
	var lagMu sync.Mutex
	var samples []time.Duration

	var wg sync.WaitGroup
	for i := 0; i < *clientsN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runClient(ctx, srv.URL, table, *warmup, &lagMu, &samples, &received); err != nil {
				fmt.Fprintln(os.Stderr, "bench client:", err)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)

	fmt.Printf("bench: table=%s clients=%d warmup=%d rows=%d\n", table, *clientsN, *warmup, *rowsN)

	start := time.Now()
	for i := 1; i <= *warmup+*rowsN; i++ {
		sent := time.Now().UnixNano()
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO public.%s (id, org_id, sent_ns) VALUES ($1, 1, $2)`, table,
		), i, sent); err != nil {
			fail(err)
		}
	}
	insertDone := time.Since(start)

	deadline := time.Now().Add(2 * time.Minute)
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
	fmt.Printf("insert_wall: %s (%.0f rows/s insert-only)\n",
		insertDone.Round(time.Millisecond), float64(*warmup+*rowsN)/insertDone.Seconds())
	fmt.Printf("e2e_wall:    %s (%.0f rows/s insert→all clients)\n",
		wall.Round(time.Millisecond), float64(*warmup+*rowsN)/wall.Seconds())
	if len(out) == 0 {
		fmt.Println("lag: no measured samples (clients may have failed)")
		os.Exit(1)
	}
	fmt.Printf(
		"shape_lag:   n=%d p50=%s p99=%s max=%s\n",
		len(out),
		percentile(out, 50).Round(time.Microsecond),
		percentile(out, 99).Round(time.Microsecond),
		out[len(out)-1].Round(time.Microsecond),
	)
	if got < totalNeed {
		os.Exit(1)
	}
}

func runClient(ctx context.Context, baseURL, shape string, warmup int, lagMu *sync.Mutex, samples *[]time.Duration, received *atomic.Int64) error {
	wsURL := "ws" + baseURL[len("http"):]
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

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
			id := intFrom(ch.Row["id"])
			sentNs := int64From(ch.Row["sent_ns"])
			if id > warmup && sentNs > 0 {
				lagMu.Lock()
				*samples = append(*samples, recv.Sub(time.Unix(0, sentNs)))
				lagMu.Unlock()
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
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
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

func int64From(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	default:
		var n int64
		_, _ = fmt.Sscan(fmt.Sprint(v), &n)
		return n
	}
}
