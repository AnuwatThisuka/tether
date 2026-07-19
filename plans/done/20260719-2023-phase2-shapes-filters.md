# Phase 2: Shapes with server-side filters

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

Reference: `AGENTS.md`, `IMPLEMENT_PLAN.md` Phase 2, `.agents/commands/create-plan.md`. Invariants touched: **2** (primary), **5** (per-shape schema fingerprint halt).

## Purpose / Big Picture

Phase 1 persists raw WAL changes. After Phase 2, a registered **shape** turns those changes into a per-claims, per-shape append log: only rows matching a filter built from **server-side auth claims** appear, with monotonic offsets. An update that moves a row across the filter boundary emits leave (delete) then enter (insert). Two orgs' claims never see each other's rows.

WebSocket delivery and snapshots are still later phases.

## Assumptions

- Phase 1 `wal.Consumer` and durable change log remain; Phase 2 consumes decoded `wal.Change` values (in-process), not by re-reading change_log (that bridge can stay test-local).
- Shape name defaults to the Postgres table name in `public` (matches README `Shape("tasks", …)`).
- Default primary key column is `id` unless overridden via `PrimaryKey(...)`.
- Partial UPDATE correctness uses an in-memory row cache keyed by PK; DELETE membership uses the cache when Old is key-only.
- `Where` supports a restricted predicate grammar only (`col = ?`, `AND`, `IS NULL` / `IS NOT NULL`, `!=`). Reject anything else loudly.

## Open Questions

_(none blocking)_

## Progress

- [x] (2026-07-19 20:23+07) ExecPlan authored.
- [x] (2026-07-19 20:30+07) `internal/shape` filter parse/eval + unit tests.
- [x] (2026-07-19 20:30+07) Registry, per-shape log, apply with enter/leave + schema halt.
- [x] (2026-07-19 20:30+07) Public `Claims`, `Filter`, `Where`, `Engine.Shape`, `New` succeeds for registration.
- [x] (2026-07-19 20:30+07) Integration: org isolation; unit: boundary delete+insert.
- [x] (2026-07-19 20:30+07) CHANGELOG / README checkbox / Outcomes; commit.
- [x] Move plan to `plans/done/` when PR lands.

## Surprises & Discoveries

- Observation: JSON round-trip through `change_log` yields float64 numbers; filter equality must coerce int/float.
  Evidence: isolation test matches `org_id` via `valuesEqual` numeric coercion.

## Decision Log

- Decision: Shape name = default table name (`public.<name>`).
  Rationale: Matches README; avoids expanding Shape signature before needed.
  Date/Author: 2026-07-19 / plan author

- Decision: Restricted `Where` grammar; never accept filter text from the wire (Invariant 2). Filters exist only as values returned from host `bind(Claims)`.
  Rationale: Server-side security boundary.
  Date/Author: 2026-07-19 / plan author

- Decision: In-memory per-shape log + row cache for Phase 2; durable client offsets wait for transport (Phase 4).
  Rationale: Prove filter semantics without prematurely freezing shape-log schema.
  Date/Author: 2026-07-19 / plan author

- Decision: Default PK `id`; optional `tether.PrimaryKey("…")` shape option.
  Rationale: Needed to patch partial UPDATEs and key-only DELETEs.
  Date/Author: 2026-07-19 / plan author

## Outcomes & Retrospective

Phase 2 acceptance passed:

- Unit: filter reject, match, boundary leave/enter, schema drift halt.
- Integration: `TestShapeFilterIsolation` — org A and B each see only their row via WAL → shape apply.
- `make test` / `make lint` / `make test-integration` green.
- README v0.1 shapes checkbox checked.

Gaps: in-memory shape log (not durable); no snapshot/stream; `Run`/`Handler` still unimplemented.

## Context and Orientation

- `internal/wal` produces `Change` with Old/New maps and `RelationFingerprint`.
- `internal/shape` is doc-only today.
- Root `New` returns `ErrNotImplemented`.

Packages touched: `internal/shape/*`, root `tether.go` / new API files, `CHANGELOG.md`, `README.md` roadmap checkbox for shapes. No WAL ack changes (Invariant 1 untouched).

## Plan of Work

1. Filter: parse `Where`, `Match(row map[string]any) (bool, error)`.
2. Shape definition + Registry; `Apply(change, claims) ([]Event, error)` with cache; halt on fingerprint mismatch (`ErrSchemaDrift` or `ErrShapeHalted`).
3. In-memory `Log` with monotonic `Offset` starting at 1.
4. Public API wiring; `New` returns usable `*Engine` for `Shape` registration; `Run`/`Handler`/`OnMutation` still `ErrNotImplemented`.
5. Tests: unit boundary; integration org isolation using wal consumer → shape apply.

## Concrete Steps

    make test
    make lint
    make db-up && make test-integration && make db-down

## Validation and Acceptance

- Unit: update crossing `org_id` filter → shape events `delete` then `insert` (or leave/enter ops).
- Integration: org A claims never receive org B rows in shape log.
- `Where` with disallowed SQL → error at bind/parse time, not silent accept.
- Schema fingerprint change → shape halted error.

## Idempotence and Recovery

Shape log is in-memory; tests construct fresh Engine/Registry. DB harness reuse Phase 1 cleanup patterns.

## Interfaces and Dependencies

No new third-party deps.

```go
type Claims any
func Where(clause string, args ...any) Filter
func (e *Engine) Shape(name string, bind func(Claims) Filter, opts ...ShapeOption)
func PrimaryKey(cols ...string) ShapeOption
```

Internal: `shape.Apply`, `shape.Log`, `shape.Event`.

---

## Revision note

2026-07-19: Initial Phase 2 ExecPlan.
