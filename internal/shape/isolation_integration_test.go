//go:build integration

package shape_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/shape"
	"github.com/anuwatthisuka/tether/internal/wal"
)

var shapeSeq atomic.Uint64

func TestShapeFilterIsolation(t *testing.T) {
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

	n := shapeSeq.Add(1)
	id := fmt.Sprintf("s%d%d", time.Now().Unix()%100000, n)
	table := "shape_p2_" + id
	slot := "shape_slot_" + id
	pub := "shape_pub_" + id
	consumerID := "shape_consumer_" + id

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

	repl, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.EnsureSlot(ctx, repl, slot); err != nil {
		_ = repl.Close(ctx)
		t.Fatal(err)
	}
	_ = repl.Close(ctx)
	t.Cleanup(func() { _ = wal.DropSlot(context.Background(), pool, slot) })

	eng, err := tether.New(norm)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Shape(table, func(c tether.Claims) tether.Filter {
		return tether.Where("org_id = ?", c.(int64))
	}, tether.Table("public", table)); err != nil {
		t.Fatal(err)
	}
	def, ok := eng.Registry().Get(table)
	if !ok {
		t.Fatal("missing shape")
	}

	orgA, err := shape.NewInstance(def, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	orgB, err := shape.NewInstance(def, int64(2))
	if err != nil {
		t.Fatal(err)
	}

	cfg := wal.Config{
		ConsumerID:  consumerID,
		SlotName:    slot,
		Publication: pub,
		Tables:      tables,
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO public.%s (id, org_id, title) VALUES (1, 1, 'a'), (2, 2, 'b')`, table,
	)); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	r, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	c, err := wal.NewConsumer(pool, r, cfg)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Run(runCtx) }()

	deadline := time.Now().Add(12 * time.Second)
	applied := map[int64]bool{}
	for time.Now().Before(deadline) {
		rows, err := pool.Query(ctx, `
SELECT id, op, schema_name, table_name, relation_fingerprint, old_row, new_row
FROM tether.change_log
WHERE consumer_id = $1 AND table_name = $2
ORDER BY id`, consumerID, table)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for rows.Next() {
			var (
				rowID               int64
				op, schema, tbl, fp string
				oldJSON, newJSON    []byte
			)
			if err := rows.Scan(&rowID, &op, &schema, &tbl, &fp, &oldJSON, &newJSON); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if applied[rowID] {
				continue
			}
			ch := wal.Change{
				Schema:              schema,
				Table:               tbl,
				Op:                  wal.Op(op),
				RelationFingerprint: fp,
				Old:                 jsonMap(t, oldJSON),
				New:                 jsonMap(t, newJSON),
			}
			if _, err := orgA.Apply(ch); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if _, err := orgB.Apply(ch); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			applied[rowID] = true
		}
		rows.Close()
		if len(applied) >= 2 {
			cancel()
			break
		}
		select {
		case err := <-errCh:
			if err != nil && runCtx.Err() == nil {
				t.Fatalf("consumer: %v", err)
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if len(applied) < 2 {
		t.Fatalf("only applied %d wal changes, want 2", len(applied))
	}

	if orgA.Log.Len() != 1 {
		t.Fatalf("orgA events = %d, want 1; events=%v", orgA.Log.Len(), orgA.Log.Events())
	}
	if orgB.Log.Len() != 1 {
		t.Fatalf("orgB events = %d, want 1; events=%v", orgB.Log.Len(), orgB.Log.Events())
	}
	if got := fmt.Sprint(orgA.Log.Events()[0].Row["org_id"]); got != "1" {
		t.Fatalf("orgA saw org_id=%s", got)
	}
	if got := fmt.Sprint(orgB.Log.Events()[0].Row["org_id"]); got != "2" {
		t.Fatalf("orgB saw org_id=%s", got)
	}
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
