//go:build integration

package snapshot_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether/internal/shape"
	"github.com/anuwatthisuka/tether/internal/snapshot"
	"github.com/anuwatthisuka/tether/internal/wal"
)

var handoffSeq atomic.Uint64

func TestSnapshotStreamHandoff(t *testing.T) {
	dsn := os.Getenv("TETHER_TEST_DSN")
	if dsn == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	norm, err := stripReplication(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, norm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	n := handoffSeq.Add(1)
	id := fmt.Sprintf("h%d%d", time.Now().Unix()%100000, n)
	table := "snap_p3_" + id
	slot := "snap_slot_" + id
	pub := "snap_pub_" + id
	consumerID := "snap_consumer_" + id

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
	})

	if err := wal.EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	tables := []wal.TableRef{{Schema: "public", Name: table}}
	if err := wal.EnsurePublication(ctx, pool, pub, tables); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.DropPublication(context.Background(), pool, pub) })

	replSetup, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.EnsureSlot(ctx, replSetup, slot); err != nil {
		_ = replSetup.Close(ctx)
		t.Fatal(err)
	}
	_ = replSetup.Close(ctx)
	t.Cleanup(func() { _ = wal.DropSlot(context.Background(), pool, slot) })

	// Seed baseline rows for org 1 and org 2.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
INSERT INTO public.%s (id, org_id, title) VALUES
	(1, 1, 'a'), (2, 1, 'b'), (3, 2, 'x')`, table)); err != nil {
		t.Fatal(err)
	}

	cfg := wal.Config{
		ConsumerID: consumerID, SlotName: slot, Publication: pub, Tables: tables,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	repl, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repl.Close(ctx)
	consumer, err := wal.NewConsumer(pool, repl, cfg)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Run(runCtx) }()

	waitChangeCount(t, pool, consumerID, table, 3)

	// Concurrent writers while we snapshot.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	nextID := atomic.Int64{}
	nextID.Store(100)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := nextID.Add(1)
				org := int64(1)
				if id%5 == 0 {
					org = 2
				}
				_, _ = pool.Exec(
					context.Background(), fmt.Sprintf(
						`INSERT INTO public.%s (id, org_id, title) VALUES ($1, $2, $3)
					 ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title`, table,
					),
					id, org, fmt.Sprintf("t-%d", id),
				)
				if id%7 == 0 {
					_, _ = pool.Exec(
						context.Background(), fmt.Sprintf(
							`UPDATE public.%s SET title = title || '-u' WHERE id = $1 AND org_id = 1`, table,
						),
						id-3,
					)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond) // let writers produce some load

	filt, err := shape.ParseWhere("org_id = ?", int64(1))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := snapshot.Take(ctx, pool, snapshot.Request{
		Schema: "public",
		Table:  table,
		Filter: filt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snap.LSN == 0 {
		t.Fatal("snapshot lsn is zero")
	}

	close(stop)
	wg.Wait()

	// A few more commits after snapshot.
	for i := 0; i < 10; i++ {
		id := nextID.Add(1)
		if _, err := pool.Exec(
			ctx, fmt.Sprintf(
				`INSERT INTO public.%s (id, org_id, title) VALUES ($1, 1, $2)`, table,
			),
			id, fmt.Sprintf("post-%d", id),
		); err != nil {
			t.Fatal(err)
		}
	}

	waitConsumerCaughtUp(t, pool, consumerID, table)

	def := shape.Definition{
		Name: table, Schema: "public", Table: table, PrimaryKey: []string{"id"},
		Bind: func(any) (shape.Filter, error) { return filt, nil },
	}
	inst, err := shape.NewInstance(def, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.LoadSnapshot(snap.LSN, snap.Rows); err != nil {
		t.Fatal(err)
	}

	applyChangeLogAfter(t, pool, consumerID, table, snap.LSN, inst)

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}

	want := queryOrgRows(t, pool, table, 1)
	got := inst.Materialized()
	if !rowSetsEqual(want, got) {
		t.Fatalf("row-set mismatch\nwant (%d): %s\ngot  (%d): %s\nsnapshot_lsn=%s snap_rows=%d",
			len(want), summarize(want), len(got), summarize(got), snap.LSN, len(snap.Rows))
	}
}

func waitChangeCount(t *testing.T, pool *pgxpool.Pool, consumerID, table string, n int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var c int
		err := pool.QueryRow(
			context.Background(),
			`SELECT count(*) FROM tether.change_log WHERE consumer_id=$1 AND table_name=$2`,
			consumerID, table,
		).Scan(&c)
		if err == nil && c >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d change_log rows", n)
}

func waitConsumerCaughtUp(t *testing.T, pool *pgxpool.Pool, consumerID, table string) {
	t.Helper()
	// Do not compare against pg_current_wal_lsn(): it advances for unrelated
	// WAL while our checkpoint only moves on decoded publication commits.
	deadline := time.Now().Add(20 * time.Second)
	lastCount := -1
	stable := 0
	for time.Now().Before(deadline) {
		var count int
		var maxLSN, conf string
		err := pool.QueryRow(context.Background(), `
SELECT count(*), COALESCE(max(lsn), '')
FROM tether.change_log
WHERE consumer_id = $1 AND table_name = $2`, consumerID, table).Scan(&count, &maxLSN)
		if err != nil {
			t.Fatal(err)
		}
		err = pool.QueryRow(
			context.Background(),
			`SELECT confirmed_lsn FROM tether.checkpoint WHERE consumer_id=$1`, consumerID,
		).Scan(&conf)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if count == lastCount {
			stable++
		} else {
			stable = 0
			lastCount = count
		}
		if maxLSN != "" && conf != "" && stable >= 4 {
			maxParsed, err1 := pglogrepl.ParseLSN(maxLSN)
			confParsed, err2 := pglogrepl.ParseLSN(conf)
			if err1 == nil && err2 == nil && confParsed >= maxParsed {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timeout waiting for consumer catch-up")
}

func applyChangeLogAfter(t *testing.T, pool *pgxpool.Pool, consumerID, table string, after pglogrepl.LSN, inst *shape.Instance) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
SELECT lsn, op, schema_name, table_name, relation_fingerprint, old_row, new_row
FROM tether.change_log
WHERE consumer_id = $1 AND table_name = $2
ORDER BY id`, consumerID, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			lsnText, op, schema, tbl, fp string
			oldJSON, newJSON             []byte
		)
		if err := rows.Scan(&lsnText, &op, &schema, &tbl, &fp, &oldJSON, &newJSON); err != nil {
			t.Fatal(err)
		}
		lsn, err := pglogrepl.ParseLSN(lsnText)
		if err != nil {
			t.Fatal(err)
		}
		ch := wal.Change{
			Schema:              schema,
			Table:               tbl,
			Op:                  wal.Op(op),
			RelationFingerprint: fp,
			CommitLSN:           lsn,
			Old:                 jsonMap(t, oldJSON),
			New:                 jsonMap(t, newJSON),
		}
		if _, err := inst.Apply(ch); err != nil {
			t.Fatal(err)
		}
		_ = after // gating happens inside Apply
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func queryOrgRows(t *testing.T, pool *pgxpool.Pool, table string, org int64) []map[string]any {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		fmt.Sprintf(`SELECT id, org_id, title FROM public.%s WHERE org_id = $1 ORDER BY id`, table), org)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, orgID int32
		var title string
		if err := rows.Scan(&id, &orgID, &title); err != nil {
			t.Fatal(err)
		}
		out = append(out, map[string]any{"id": int64(id), "org_id": int64(orgID), "title": title})
	}
	return out
}

func rowSetsEqual(a, b []map[string]any) bool {
	am := indexByID(a)
	bm := indexByID(b)
	if len(am) != len(bm) {
		return false
	}
	for id, ar := range am {
		br, ok := bm[id]
		if !ok {
			return false
		}
		if fmt.Sprint(ar["org_id"]) != fmt.Sprint(br["org_id"]) {
			return false
		}
		if fmt.Sprint(ar["title"]) != fmt.Sprint(br["title"]) {
			return false
		}
	}
	return true
}

func indexByID(rows []map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any, len(rows))
	for _, r := range rows {
		out[fmt.Sprint(r["id"])] = r
	}
	return out
}

func summarize(rows []map[string]any) string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, fmt.Sprintf("%s:%s", fmt.Sprint(r["id"]), fmt.Sprint(r["title"])))
	}
	sort.Strings(ids)
	b, _ := json.Marshal(ids)
	return string(b)
}

func stripReplication(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func jsonMap(t *testing.T, b []byte) map[string]any {
	t.Helper()
	if len(b) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}
