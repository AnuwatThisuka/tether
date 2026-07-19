# IMPLEMENT_PLAN — v0.2 Operability

Master plan for the **v0.2** track. Complements [`IMPLEMENT_PLAN.md`](./IMPLEMENT_PLAN.md)
(v0.1 correctness). Does **not** reopen Invariants 1–7 unless a bug is found.

## Goal

Make an embedded deployment operable: visible lag, slot pressure, and
per-client progress; clearer backpressure; structured logs. Optional
OpenTelemetry traces stay deferred until someone needs them.

Target release: **v0.2.0** (SemVer MINOR — backward-compatible features).

## Non-goals (still)

Same as README: CRDTs, joins, presence, multi-region, non-Postgres, admin UI.
No new third-party deps unless explicitly approved (AGENTS.md).

## Dependency rule for telemetry

**Hooks / interfaces in the root package.** Hosts wire Prometheus, StatsD,
or OTel themselves. Zero new module dependencies for metrics.

```go
// Sketch — exact API lives in the Slice 1 ExecPlan.
type Metrics interface {
    SetReplicationLagBytes(n int64)
    SetSlotRestartLagBytes(n int64) // or slot size if exposed
    SetClientOffset(clientID, shape string, offset int64)
    // counters: disconnect_slow, disconnect_idle, mutation_ok, mutation_dup, …
}
```

Default: no-op implementation.

## Slices

| Slice | README bullet                           | Deliverable                                              |
| ----- | --------------------------------------- | -------------------------------------------------------- |
| 1     | Metrics: lag, slot size, client offsets | `Metrics` interface + engine sampling; unit/integration  |
| 2     | Backpressure / slow-client eviction     | Tunables + metrics events; docs for buffer sizing        |
| 3     | Structured logging                      | Consistent `slog` attrs (shape, LSN, client, slot)       |
| 4     | OpenTelemetry traces (optional / later) | Only if requested; may need `go.opentelemetry.io/*` deps |

## Slice 1 — Metrics (this track's first ExecPlan)

**Purpose:** surface the numbers operators watch when Postgres is about to
fill the disk or a client is stuck.

Minimum gauges / observations:

1. **Replication lag (bytes)** — already computable via `wal.SlotLagBytes`
2. **Slot pressure** — `pg_replication_slots` restart_lsn lag and/or
   `pg_wal_lsn_diff` (document which); prefer bytes over opaque LSNs for hosts
3. **Per-client offset** — last successfully enqueued/applied offset per
   `(client, shape)` when fan-out advances (best-effort; may be in-memory)

Acceptance:

- Host can install `WithMetrics(m Metrics)` (name TBD in ExecPlan)
- Integration or unit test with a recording `Metrics` sees lag + offset updates
- No new deps; README / `docs/embed.md` show a 10-line Prometheus adapter sketch
- `make test` / `make lint` / `make test-integration` green
- CHANGELOG under Unreleased → ships in v0.2.0

Invariants: do not acknowledge LSN earlier; do not block WAL reader on metrics.

## Slice 2 — Backpressure polish

Phase 4 already disconnects on full buffer (`ReasonSlowClient`). v0.2 adds:

- Explicit option documentation / maybe `WithClientBuffer` guidance
- Metrics counters for slow-client disconnects
- Possibly configurable bye reason logging

No change that blocks the WAL reader (Invariant 7).

## Slice 3 — Structured logging

`slog` already used. Polish:

- `WithLogger(*slog.Logger)` if not already exported
- Standard keys: `slot`, `shape`, `client_id`, `lsn`, `offset`, `err`
- Loud events: schema halt, lag drop, idle disconnect

OTel traces: **out of slice 3** unless deps approved.

## Phase map vs README

| README v0.2 bullet                           | Slice           |
| -------------------------------------------- | --------------- |
| Metrics: replication lag, slot size, offsets | 1               |
| Backpressure and slow-client eviction        | 2               |
| Structured logging, OpenTelemetry traces     | 3 (+4 optional) |

## Definition of done (v0.2.0)

- [x] Slice 1–3 accepted with tests and CHANGELOG entries
- [x] README v0.2 checkboxes checked with named tests / adapter example
- [x] Direct deps unchanged (`pgx`, `pglogrepl`, `websocket`)
- [x] Tag `v0.2.0`; `tether.Version` updated

OpenTelemetry traces remain deferred (Slice 4 / later).

## Revision history

| Date       | Change                          |
| ---------- | ------------------------------- |
| 2026-07-19 | Initial v0.2 operability plan   |
| 2026-07-19 | Slices 1–3 shipped as v0.2.0    |
