# Embedding tether

Minimal guide for hosting `tether` inside an existing Go HTTP server.

## Requirements

- Go 1.22+
- PostgreSQL 14+ with `wal_level = logical`
- Every synced table has a primary key (or unique index)
- A DB role that can create publications / replication slots

## Skeleton

```go
engine, err := tether.New(pgURL,
    tether.WithAuth(func(r *http.Request) (tether.Claims, error) {
        // Resolve tenant/user from your session/JWT. Never from client filters.
        return claimsFromRequest(r)
    }),
    tether.MaxSlotLag(2*1024*1024*1024), // drop slot before disk fills
    tether.MaxClientIdle(24*time.Hour),
    tether.WithLogger(slog.Default()),
    tether.WithMetrics(promMetrics), // optional; see Metrics below
)
if err != nil {
    log.Fatal(err)
}
defer engine.Close()

// Filters come only from Claims (Invariant 2).
if err := engine.Shape("tasks", func(c tether.Claims) tether.Filter {
    return tether.Where("org_id = ?", c.(MyClaims).OrgID)
}, tether.Table("public", "tasks")); err != nil {
    log.Fatal(err)
}

engine.OnMutation(func(ctx context.Context, tx pgx.Tx, m tether.Mutation) error {
    // Apply the write; use m.Key for your own audit if needed.
    // tether already dedupes m.Key in the same transaction.
    return applyTaskMutation(ctx, tx, m)
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go func() {
    if err := engine.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Fatal(err)
    }
}()

mux.Handle("/sync", engine.Handler())
```

## Operational rules hosts must respect

1. **Persist before ack** — handled inside `Run`. Do not wrap the replication
   connection with custom LSN acks.
2. **Shape filters are server-side** — clients send shape names only.
3. **Mutations are idempotent by key** — clients may replay after reconnect.
4. **`must_resnapshot` / `shape_halted`** — clients must wipe local shape state
   and subscribe again. Do not try to resume by offset across these errors.
5. **Slot lag** — if lag exceeds `MaxSlotLag`, tether drops the slot, tells
   clients to resnapshot, and recreates the slot. Prefer a fresh snapshot over
   pinning WAL.

## Metrics (no Prometheus dependency)

`tether` exposes a small `Metrics` interface. Wire it to your stack:

```go
type promAdapter struct{ lag, clients prometheus.Gauge /* … */ }

func (p *promAdapter) ReplicationLagBytes(n int64) { p.lag.Set(float64(n)) }
func (p *promAdapter) ClientsConnected(n int)      { p.clients.Set(float64(n)) }
func (p *promAdapter) ClientOffset(clientID, shape string, offset int64) {
    // e.g. GaugeVec with labels client_id, shape
}

engine, err := tether.New(pgURL, tether.WithMetrics(&promAdapter{…}))
```

`ReplicationLagBytes` is restart_lsn lag vs the WAL tip (bytes) — the same
signal as the slot lag guard. `ClientOffset` is last successfully *buffered*
outbound offset (not a client ack). `ClientDisconnected` fires for
server-initiated `bye` (e.g. `slow_client`, `idle_client`).

## Backpressure

Each client has a fixed outbound channel (`WithClientBuffer`, default 64).
If a client cannot keep up, tether sends `bye: slow_client` and closes that
socket. Other clients and the WAL consumer continue. The disconnected client
should reconnect with its last applied offset (or take a fresh snapshot).

Do not raise the buffer to "fix" slow phones — that only delays OOM. Measure
`ClientDisconnected("slow_client")` and tune product UX (resume, backoff).

## Non-goals

See the root README. Do not expect joins in shapes, CRDTs, presence, or
multi-region coordination from this library.
