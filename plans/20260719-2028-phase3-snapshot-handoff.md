# Phase 3: Gapless snapshot → stream handoff

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current.

Reference: `AGENTS.md`, `IMPLEMENT_PLAN.md` Phase 3. Invariants touched: **4** (snapshot at LSN _N_, stream strictly after _N_).

## Purpose / Big Picture

A new subscriber must receive a consistent table snapshot at WAL LSN _N_, then live shape events only for commits with LSN > _N_. Off-by-one here silently duplicates or drops rows. After this phase, an integration test takes a filtered snapshot under concurrent writes, applies later WAL changes, and asserts **exact** row-set equality against a post-catch-up `SELECT` with the same filter.

## Assumptions

- Snapshot uses a **normal** `pgxpool` connection (not the replication conn).
- `REPEATABLE READ READ ONLY` + `pg_current_wal_lsn()` at transaction start defines _N_. Document residual race; prefer this until per-subscriber EXPORT_SNAPSHOT slots (Phase 6 territory).
- `wal.Change` gains `CommitLSN` so shape apply can skip ≤ _N_.
- Shape `Filter` exposes SQL for the snapshot `SELECT`.

## Progress

- [x] (2026-07-19 20:28+07) ExecPlan authored.
- [x] (2026-07-19 20:35+07) Add `CommitLSN` on `wal.Change`; set on commit in consumer.
- [x] (2026-07-19 20:35+07) `Filter.SQLClause` + `snapshot.Take`.
- [x] (2026-07-19 20:35+07) `Instance.LoadSnapshot` + skip `CommitLSN <= snapshotLSN` in Apply.
- [x] (2026-07-19 20:35+07) `TestSnapshotStreamHandoff` integration.
- [x] (2026-07-19 20:35+07) CHANGELOG / README / Outcomes; commit.
- [ ] Move plan to `plans/done/` when PR lands.

## Surprises & Discoveries

- Observation: Catch-up must not wait for `confirmed_lsn >= pg_current_wal_lsn()` — tip advances without publication commits. Wait for checkpoint ≥ max(change_log.lsn) and stable count.
  Evidence: `TestSnapshotStreamHandoff` 20s timeout until catch-up helper changed.

## Decision Log

- Decision: Snapshot LSN = `pg_current_wal_lsn()` immediately after `BEGIN REPEATABLE READ READ ONLY`, before row SELECT.
  Rationale: Separate pool; no new slot per subscriber in Phase 3. Stream applies only `CommitLSN > N`.
  Date/Author: 2026-07-19 / plan author

- Decision: Seed shape via `LoadSnapshot` (cache + insert events), then `Apply` WAL with LSN gate.
  Rationale: Matches subscriber mental model; testable without WebSocket.
  Date/Author: 2026-07-19 / plan author

## Outcomes & Retrospective

Phase 3 acceptance passed:

- Unit: LSN skip after `LoadSnapshot`; `SQLClause` rendering.
- Integration: `TestSnapshotStreamHandoff` — concurrent writers, snapshot, stream apply, exact row-set equality vs filtered `SELECT`.
- `make test` / `make lint` / `make test-integration` green.
- README v0.1 snapshot handoff checkbox checked.

Gaps: no WebSocket yet; snapshot still uses RR+`pg_current_wal_lsn` rather than per-subscriber `EXPORT_SNAPSHOT` slots.

## Plan of Work

1. `wal.Change.CommitLSN`; assign in consumer on Commit before persist.
2. `shape.Filter.SQLClause() (clause string, args []any, err error)`.
3. `snapshot.Take(ctx, pool, req)` → `{LSN, Rows}`.
4. Shape handoff helpers + unit test for LSN skip.
5. Integration: concurrent writers, snapshot, catch up, exact set equality.

## Validation

    make test && make lint
    make db-up && make test-integration && make db-down

Must include `TestSnapshotStreamHandoff` asserting exact row-set equality.

## Interfaces

```go
// snapshot
type Request struct {
    Schema, Table string
    Columns []string // empty => *
    Filter shape.Filter
}
type Result struct {
    LSN pglogrepl.LSN
    Rows []map[string]any
}
func Take(ctx context.Context, pool *pgxpool.Pool, req Request) (Result, error)

// shape
func (inst *Instance) LoadSnapshot(lsn pglogrepl.LSN, rows []map[string]any) error
// Apply skips when ch.CommitLSN != 0 && ch.CommitLSN <= snapshotLSN
```

No new third-party deps.

---

## Revision note

2026-07-19: Initial Phase 3 ExecPlan.
