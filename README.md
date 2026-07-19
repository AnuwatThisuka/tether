# tether

**A Postgres sync engine that lives inside your Go server.**

No sidecar. No second runtime. No extra box on the architecture diagram.
Import a package, register a shape, and your clients get a local replica of
exactly the rows they're allowed to see — kept live over a WebSocket.

```go
engine := tether.New(pgURL)

engine.Shape("tasks", func(c tether.Claims) tether.Filter {
    return tether.Where("org_id = ?", c.OrgID)
})

engine.OnMutation(handleWrite)

mux.Handle("/sync", engine.Handler())
```

That's the whole integration.

> **Status: pre-alpha.** The protocol is unstable and the API will change
> without notice. Not ready for production. See [Roadmap](#roadmap) for what
> works today.

---

## Why this exists

Every sync engine in this space — PowerSync, Zero, Triplit, InstantDB —
assumes your backend is Node. If you write Go, adopting one means deploying
and operating a service in a language your team doesn't use, with its own
config, its own failure modes, and its own on-call runbook.

`tether` is the sync engine for people who already have a Go server and
don't want a second one.

**The design bets:**

- **Embedded, not deployed.** A library, not a service. Ships in your binary.
- **WebSocket from day one.** Not long polling. Go's concurrency model is
  built for holding tens of thousands of idle connections; this is the one
  place the language choice is a real advantage.
- **Server-authoritative writes.** No CRDTs. Your handler decides what's
  valid, inside a real Postgres transaction. Conflict resolution is business
  logic, and business logic belongs to you.
- **Boring on purpose.** Single table shapes. No joins. No auto-merge.
  The scope that stays small is the scope that stays correct.

---

## How it works

```
Postgres WAL ──▶ CDC reader ──▶ shape log ──▶ WebSocket ──▶ client replica
                                                  │
   mutation handler ◀── your Go code ◀────────────┘
```

1. `tether` opens a logical replication slot and decodes the WAL via
   [`pglogrepl`](https://github.com/jackc/pglogrepl).
2. Changes are matched against registered **shapes** and appended to a
   per-shape log with a monotonic offset.
3. Connected clients receive deltas over WebSocket. On reconnect a client
   sends its last-seen offset and resumes from exactly that point.
4. Client writes arrive as **mutations** with an idempotency key. Your
   handler validates and applies them in a transaction, then accepts or
   rejects. Rejected mutations roll back the client's optimistic state.

### Shapes

A shape is one table plus a `WHERE` clause derived from the caller's auth
claims. The filter is resolved **server-side, every time**. Clients cannot
send their own predicates.

```go
engine.Shape("invoices", func(c tether.Claims) tether.Filter {
    return tether.Where("org_id = ? AND deleted_at IS NULL", c.OrgID)
})
```

Deliberately not supported: joins, aggregates, cross-table shapes. If you
need a denormalised view, make it a table.

### Mutations

```go
func handleWrite(ctx context.Context, tx pgx.Tx, m tether.Mutation) error {
    switch m.Op {
    case "task.complete":
        if !m.Claims.Can("task:write") {
            return tether.Reject("forbidden")
        }
        _, err := tx.Exec(ctx,
            `UPDATE tasks SET done = true WHERE id = $1 AND org_id = $2`,
            m.Arg("id"), m.Claims.OrgID)
        return err
    }
    return tether.Reject("unknown op")
}
```

Mutations queued while offline are replayed in original order on reconnect.
Idempotency keys mean a replayed mutation is applied at most once.

---

## Operational safety

Two failure modes will take down a Postgres instance if a sync engine
ignores them. `tether` treats both as first-class concerns:

**Replication slot bloat.** An inactive slot pins WAL forever and fills the
disk. `tether` monitors slot lag and drops slots that exceed a configured
threshold, forcing affected clients to re-snapshot rather than taking the
database down.

```go
engine := tether.New(pgURL,
    tether.MaxSlotLag(2*1024*1024*1024),  // 2 GiB
    tether.MaxClientIdle(24*time.Hour),
)
```

**Schema drift.** Logical replication does not carry DDL. When `tether`
detects a schema change affecting a synced table, it **halts that shape and
surfaces a loud error**. It will not guess, and it will not silently
continue with a stale column map.

Host skeleton and operational rules: [docs/embed.md](docs/embed.md).

---

## Requirements

- Go 1.22+
- PostgreSQL 14+ with `wal_level = logical`
- Every synced table needs a primary key or unique index
- A replication-capable role

Views, materialized views, and foreign tables cannot be synced. This is a
Postgres constraint, not ours.

---

## Correctness

The bar this project holds itself to, run in CI on every commit:

> Two clients edit the same rows **while offline**. Both stay offline for
> ten minutes. One is killed mid-write and restarted cold. Both reconnect.
>
> They converge to identical state. No write is lost. No mutation is
> applied twice.

Every release states which parts of this test pass. If a claim isn't
covered by a test, it isn't in this README.

Covered by `TestConvergence` in `internal/e2e` (`make test-integration`).

---

## Roadmap

**v0.1 — correctness**

- [x] WAL ingest, LSN checkpointing, resume after restart
- [x] Single-table shapes with auth-bound filters
- [x] Gapless initial snapshot handoff to live stream
- [x] WebSocket transport with offset-based resume
- [x] Server-authoritative mutations with idempotency keys
- [x] Slot lag guard and schema-drift halt
- [x] Correctness / convergence (`TestConvergence`)

**v0.2 — operability**

- [ ] Metrics: replication lag, slot size, per-client offsets
- [ ] Backpressure and slow-client eviction
- [ ] Structured logging, OpenTelemetry traces

**Later, if users ask**

- [ ] Private networking for databases that cannot be reached from the
      public internet (embedded userspace WireGuard via `tsnet`)
- [ ] Additional client SDKs

**Explicit non-goals**
CRDTs and automatic merge. Joins in shapes. Presence and cursors.
Multi-region coordination. Databases other than Postgres. An admin UI.

---

## Prior art

Genuinely good projects solving adjacent problems. Read them before you
pick anything, including this.

| Project                                                 | Shape of it                                                      |
| ------------------------------------------------------- | ---------------------------------------------------------------- |
| [ElectricSQL](https://github.com/electric-sql/electric) | Elixir service, read-path only, writes are your problem          |
| [PowerSync](https://powersync.com)                      | Postgres to SQLite with a write-back path, hosted or self-hosted |
| [Zero](https://zero.rocicorp.dev)                       | Query-based sync, server-authoritative mutations, TypeScript     |
| [Debezium](https://debezium.io)                         | Industrial-strength CDC into Kafka. Different problem, done well |

If your backend is TypeScript, one of the above is very likely the better
choice today. `tether` exists for the case where it isn't.

---

## Contributing

Early enough that the most useful contribution is a bug report with a
reproduction. Correctness issues take priority over features.

## License

Apache-2.0
