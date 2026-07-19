//go:build integration

package wal_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether/internal/wal"
)

var lagSeq atomic.Uint64

func TestSlotLag_AbandonedSlotDropped(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	n := lagSeq.Add(1)
	id := fmt.Sprintf("lag%d%d", time.Now().Unix()%100000, n)
	table := "lag_t_" + id
	slot := "lag_slot_" + id
	pub := "lag_pub_" + id

	norm, err := normalDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, norm)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE public.%s (
	id INT PRIMARY KEY,
	payload BYTEA NOT NULL
)`, table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS public.%s", table))
		_ = wal.DropSlot(context.Background(), pool, slot)
		_ = wal.DropPublication(context.Background(), pool, pub)
	})

	if err := wal.EnsurePublication(ctx, pool, pub, []wal.TableRef{{Schema: "public", Name: table}}); err != nil {
		t.Fatal(err)
	}

	replURL, err := addReplication(dsn)
	if err != nil {
		t.Fatal(err)
	}
	repl, err := pgconn.Connect(ctx, replURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.EnsureSlot(ctx, repl, slot); err != nil {
		_ = repl.Close(ctx)
		t.Fatal(err)
	}
	_ = repl.Close(ctx)

	// Incompressible payload so WAL growth is roughly proportional to inserts.
	const chunk = 64 * 1024
	payload := make([]byte, chunk)
	for i := 0; i < 20; i++ {
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO public.%s (id, payload) VALUES ($1, $2)`, table,
		), i+1, payload); err != nil {
			t.Fatal(err)
		}
	}

	lag, err := wal.SlotLagBytes(ctx, pool, slot)
	if err != nil {
		t.Fatal(err)
	}
	const maxLag = 256 * 1024
	if lag <= maxLag {
		t.Fatalf("lag=%d, want > %d", lag, maxLag)
	}

	if err := wal.DropSlot(ctx, pool, slot); err != nil {
		t.Fatal(err)
	}
	exists, err := wal.SlotExists(ctx, pool, slot)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("slot still present after drop; WAL remains pinned")
	}
}

func addReplication(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
}
