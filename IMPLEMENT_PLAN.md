# IMPLEMENT_PLAN.md

Master design and phased build plan for `tether`.

This document is the source of truth for _what we are building and in what
order_. Day-to-day agent work uses ExecPlans under `plans/`; those plans must
stay consistent with this file and with the Invariants in `AGENTS.md`.

**Status of the tree today:** greenfield. Only `README.md` and `AGENTS.md`
exist. No Go packages, Makefile, or Postgres harness yet. Everything below
is the intended shape of the first correct implementation — not a description
of code that already ships.

---

## 1. Purpose

`tether` is a Postgres sync engine that embeds into an existing Go HTTP
server. Embedders register **shapes** (one table + a server-resolved filter),
expose a WebSocket handler, and supply a **mutation** handler for writes.
Clients receive an initial snapshot, then live deltas keyed by a monotonic
offset, and send mutations with idempotency keys.

The product bet (see README): embedded library, not a sidecar; WebSocket from
day one; server-authoritative writes; deliberately small scope (no joins, no
CRDTs, no multi-region).

Correctness bar (README + AGENTS.md):

> Two clients edit the same rows while offline. Both stay offline. One is
> killed mid-write and restarted cold. Both reconnect. They converge to
> identical state. No write lost. No mutation applied twice.

Every claim in the README must be backed by a test. The `internal/e2e`
convergence test is the headline proof.

---

## 2. Non-goals (do not implement)

From README / AGENTS.md — stop and re-scope if a task appears to need one:

- CRDTs and automatic merge
- Joins / aggregates / cross-table shapes
- Presence and cursors
- Multi-region coordination
- Databases other than Postgres
- An admin UI
- Extra third-party deps beyond `jackc/pgx`, `jackc/pglogrepl`, `coder/websocket`
  without explicit user approval

v0.2 and later items (metrics, OTel, `tsnet`, client SDKs) are out of scope
for the v0.1 correctness track unless this document is revised.

---

## 3. Invariants (hard gates)

Restated from `AGENTS.md`. A design that breaks one is wrong even if tests pass.

| #   | Invariant                               | Implementation consequence                                                                      |
| --- | --------------------------------------- | ----------------------------------------------------------------------------------------------- |
| 1   | Never ack an LSN before durable record  | Shape-log (or equivalent durable store) write completes **before** `SendStandbyStatusUpdate`    |
| 2   | Shape filters from auth claims only     | `Shape(name, func(Claims) Filter)` — never accept client WHERE / filter params                  |
| 3   | Mutations idempotent by key             | Dedupe inside the same DB transaction as the handler effect                                     |
| 4   | Snapshot and stream: no overlap, no gap | Snapshot at LSN _N_; stream emits only changes with LSN > _N_; prove with row-set equality test |
| 5   | Schema drift halts the shape            | Relation OID/column set mismatch → stop shape, surface error; never remap                       |
| 6   | Slot lag bounded                        | Configurable max lag; on breach drop slot (and force resnapshot) rather than pin WAL forever    |
| 7   | Slow client isolated                    | Per-client send buffer; full → disconnect that client; WAL reader never blocks on `Write`       |

Domain traps that plans must account for: partial `UPDATE` columns, TOAST
unchanged-markers, per-source-transaction atomic append to the shape log,
replication conn cannot run ordinary queries (separate pool for snapshots),
reconnect/resume is the common path.

---

## 4. Target architecture

```
                    ┌─────────────────────────────────────────┐
                    │           host Go HTTP server           │
                    │  mux.Handle("/sync", engine.Handler())  │
                    └───────────────────┬─────────────────────┘
                                        │
┌──────────────┐   logical repl    ┌────▼─────┐   filter+append   ┌────────────┐
│   Postgres   │ ─────────────────▶│ wal      │ ────────────────▶│ shape log  │
│  WAL / slot  │   pgoutput        │ decoder  │                  │ (per shape)│
└──────▲───────┘                   └──────────┘                  └─────┬──────┘
       │ snapshot (normal pool)                                        │
       │                                                               │ fan-out
┌──────┴───────┐   mutations       ┌──────────┐   deltas+resume  ┌─────▼──────┐
│ mutation txn │ ◀─────────────────│ handler  │ ◀────────────────│ transport  │
│ + idempotency│                   │ (user)   │                  │ WebSocket  │
└──────────────┘                   └──────────┘                  └────────────┘
```

### Package layout (v0.1)

```
tether.go              # public API: New, Shape, OnMutation, Handler, options
options.go             # MaxSlotLag, MaxClientIdle, Logger, Auth, …
errors.go              # Reject, Halted, ErrSchemaDrift, …
CHANGELOG.md           # required for every root-package change

internal/wal/          # slot create/drop, StartReplication, pgoutput decode,
                       # persist-before-ack, lag monitor
internal/shape/        # registry, Claims→Filter, predicate eval, per-shape log,
                       # schema fingerprint / halt
internal/proto/        # wire messages, framing, offset codec
transport/             # HTTP upgrade, per-conn pump, buffer, resume by offset
internal/snapshot/     # consistent snapshot at LSN N via non-repl pool
internal/mutate/       # idempotency table + invoke user handler in one txn
internal/e2e/          # convergence test (headline correctness)

cmd/                   # harnesses / fixtures only — never library code

docker-compose.yml     # Postgres 14+ wal_level=logical, port 54321
Makefile               # test, test-integration, lint, fmt, db-up, db-down
```

**Import rules**

- Public symbols only in the root package.
- `cmd/` imports the library; the library never depends on `cmd/`.
- `transport` may depend on `internal/proto` and shape/stream interfaces;
  it must not talk to Postgres directly.
- `internal/wal` owns the replication connection; snapshot/mutate use a
  separate `pgxpool`.

### Public API sketch (compatibility surface)

```go
package tether

type Claims interface {
    // Host-defined. Shape callbacks and mutation handlers receive this.
}

type Filter struct { /* opaque; built via Where */ }

func Where(clause string, args ...any) Filter

type Mutation struct {
    Op     string
    Key    string // idempotency key
    Claims Claims
    // Arg accessors…
}

type Engine struct{ /* … */ }

func New(pgURL string, opts ...Option) (*Engine, error)
func (e *Engine) Shape(name string, bind func(Claims) Filter)
func (e *Engine) OnMutation(fn func(ctx context.Context, tx pgx.Tx, m Mutation) error)
func (e *Engine) Handler() http.Handler
func (e *Engine) Run(ctx context.Context) error // start WAL consumer + lag monitor
func (e *Engine) Close() error
```

Auth extraction (how `Claims` is obtained from the WebSocket handshake) is an
`Option` supplied by the host — tether does not invent an auth scheme.

Exact method sets may evolve during v0.1, but **behavior** is constrained by
the Invariants. Every root API change gets a `CHANGELOG.md` entry.

---

## 5. Core concepts

### Shape

One Postgres table + a `Filter` produced from `Claims` on the server. Clients
subscribe by shape name only. Membership changes (row enters/leaves the
filter) appear as insert/delete in the shape log.

### Offset

Opaque, monotonic position in a shape's log. Clients store the last applied
offset and send it on reconnect. Encoding lives in `internal/proto` and must
round-trip stably across process restarts.

### Shape log

Append-only, per-shape sequence of changes derived from committed WAL
transactions. Entire source transactions are applied atomically to the log
(no partial txn visible to clients). Durable enough that Invariant 1 holds
relative to LSN ack — for v0.1 the durability target is: process crash must
not ack an LSN whose changes are not recoverable on restart. Prefer a simple
durable append store (Postgres table or local durable log) decided in the
first WAL ExecPlan; do not silently use memory-only if ack advances.

### Snapshot handoff

1. Begin snapshot on a **normal** connection (not the replication conn).
2. Note consistent LSN _N_ for that snapshot.
3. Stream matching rows to the client as a snapshot batch.
4. Subscribe the client to the shape log **strictly after** _N_.
5. Integration test: concurrent writes during snapshot → client final
   row-set equals `SELECT` with the same filter at a later stable point
   (exact equality, not "eventually similar").

### Mutations

Client → server messages with `op`, payload, and idempotency `key`. Server
opens a transaction, records the key (unique constraint / upsert), runs the
user handler, commits. Duplicate key → success with no second effect
(handler not re-run, or re-run is a pure no-op — pick one and test it).

### Slot lag guard

Periodic check of `pg_replication_slots` lag (bytes behind). If over
`MaxSlotLag`, drop the slot, halt/resync affected shapes, and surface an
error so clients re-snapshot. Never wait forever for a stuck consumer.

### Schema drift

Cache relation column names/types/attnums when first seen. On `Relation`
message mismatch → halt that shape (Invariant 5). Resume only after operator
intervention / explicit reshape (v0.1: halt is enough; auto-migrate is a
non-goal).

---

## 6. Wire protocol (v0.1 draft)

Versioned JSON (or binary later) messages over WebSocket. Details belong in
`internal/proto` and a short `docs/protocol.md` when implemented. Minimum
message kinds:

**Client → server**

- `hello` — protocol version, auth material (or cookie already consumed),
  optional per-shape resume offsets
- `subscribe` — shape name(s) only (no filter)
- `mutation` — op, key, args
- `ack` — client applied up to offset (optional in v0.1 if server is
  push-only; prefer having it for backpressure signals)

**Server → client**

- `snapshot` — rows + snapshot LSN / starting offset
- `change` — insert/update/delete + offset
- `mutation_ok` / `mutation_reject`
- `error` — shape halted, must_resnapshot, unauthorized, etc.
- `bye` — slow-client disconnect / shutdown

Framing rules: one message per WebSocket data frame for v0.1; document
ordering guarantees (changes for a shape are totally ordered by offset).

---

## 7. Phased delivery

Phases are sequential. Do not start phase _N+1_ until phase _N_ acceptance
passes. Each phase should land as one or more focused PRs (one concern per
PR). Each phase that touches WAL/offsets/handoff needs `make test-integration`.

### Phase 0 — Scaffolding

**Goal:** empty but buildable library with CI hooks and a logical Postgres.

Deliver:

- `go.mod` (module path chosen; Go 1.22+)
- Root package stub (`New` returns ErrNotImplemented or minimal no-op only if
  tests need a type — prefer failing closed)
- `Makefile`: `test`, `test-integration`, `lint`, `fmt`, `db-up`, `db-down`
- `docker-compose.yml`: Postgres 14+, `wal_level=logical`, port `54321`,
  role/db `tether`/`tether`
- `CHANGELOG.md` starting at `Unreleased`
- golangci-lint + gofumpt config
- Placeholder packages with package docs: `internal/wal`, `internal/shape`,
  `internal/proto`, `transport`, `internal/e2e`

Acceptance:

```text
make test        # passes (may be empty packages)
make lint        # clean
make db-up && psql "$TETHER_TEST_DSN" -c 'show wal_level'  # logical
```

Invariants touched: none yet (no ack path).

### Phase 1 — WAL ingest + persist-before-ack

**Goal:** decode pgoutput for registered tables; durable append; ack LSN only
after persist (Invariant 1); restart resumes from checkpoint.

Deliver:

- Replication slot + publication management (create if missing; drop on
  guarded lag comes in Phase 6)
- pgoutput parse: Begin/Relation/Insert/Update/Delete/Commit
- Correct handling of partial UPDATE + TOAST markers
- Durable checkpoint of last applied LSN **after** durable change record
- Unit tests for decode edge cases; **integration** test: insert row →
  appear in durable log; kill process before ack → row still present after
  restart; never loses committed change that was acked

Acceptance:

```text
make db-up
export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
make test-integration   # includes persist-before-ack / crash-replay
make lint
```

Invariants: **1** (primary), **5** (detect relation; halt can stub to error).

### Phase 2 — Shapes + server-side filters

**Goal:** registry of named shapes; filter from Claims only; predicate eval on
decoded rows; per-shape log offsets.

Deliver:

- `Shape(name, bind)` API
- Filter evaluation for insert/update/delete including enter/leave shape
- Rejection / panic-level guard if any code path would take filter text from
  the wire (Invariant 2) — fuzz or negative API test in transport later
- Schema fingerprint stored per shape; mismatch → halt (finish Invariant 5)

Acceptance:

- Integration: two orgs' rows; claims for org A never observe org B
- Unit: update that moves a row across filter boundary emits delete+insert
  (or equivalent membership events) in order

Invariants: **2**, **5**.

### Phase 3 — Snapshot + gapless handoff

**Goal:** new subscriber gets snapshot at LSN _N_ then live log with LSN > _N_
only (Invariant 4).

Deliver:

- `internal/snapshot` using non-replication pool
- Coordination with WAL consumer so handoff LSNs are well-defined
- Integration test: concurrent writes during snapshot; assert **exact**
  row-set equality vs source table under the same filter

Acceptance:

```text
make test-integration   # TestSnapshotStreamHandoff (name illustrative)
```

Invariants: **4**.

### Phase 4 — WebSocket transport + offset resume

**Goal:** `Handler()` upgrades connections; subscribe; push snapshot+changes;
resume by offset; slow-client disconnect (Invariant 7).

Deliver:

- `coder/websocket` server in `transport/`
- Host-supplied auth → `Claims`
- Per-client buffered outbound channel; on full → close conn with
  `must_resnapshot` / resume hint; WAL/shape fan-out never blocks
- Integration or e2e: connect → snapshot → insert elsewhere → receive
  change; disconnect → reconnect with offset → no duplicate / no gap for
  subsequent changes

Acceptance:

- Resume test green
- Slow-client test: blocked reader does not stall a second client
  (Invariant 7)

Invariants: **7**, resume correctness (supports **4**).

### Phase 5 — Mutations + idempotency

**Goal:** client writes through user handler; idempotent by key (Invariant 3);
offline replay safe.

Deliver:

- Idempotency storage in the same transaction as handler execution
- `mutation_ok` / `mutation_reject` wire responses
- Integration: apply key K; apply K again → single row effect
- Prep for convergence: mutations visible via WAL → shapes → other clients

Acceptance:

```text
make test-integration   # idempotency + cross-client visibility via WAL
```

Invariants: **3**.

### Phase 6 — Slot lag guard + polish for v0.1

**Goal:** bounded slots (Invariant 6); wire options from README
(`MaxSlotLag`, `MaxClientIdle`); close gaps for correctness claim.

Deliver:

- Lag monitor; drop slot + force resnapshot path
- Idle client cleanup
- Loud errors for halted shapes
- Docs: minimal embedder guide aligning with README examples
- Fill README roadmap checkboxes only when tests prove each line

Acceptance:

- Integration: artificial lag / stuck consumer → slot dropped, clients
  instructed to resnapshot; Postgres disk not pinned indefinitely
- Full `make test` + `make test-integration` + `make lint` green

Invariants: **6**; regression check **1–5, 7**.

### Phase 7 — Convergence (v0.1 exit criteria)

**Goal:** headline test in `internal/e2e` passes without flaky retries.

Scenario (from README):

1. Two clients offline; both enqueue conflicting/overlapping mutations.
2. Wait (simulated) with both offline.
3. Kill one mid-write; cold restart.
4. Both reconnect; replay mutations; sync shapes.
5. Assert identical final client state; no lost write; no double-apply.

Acceptance:

```text
make test-integration   # e2e convergence green under -race
```

If this flakes, the bug is in earlier phases — fix the cause, do not add
sleeps/retries to greenwash.

**v0.1 is done only when Phase 7 passes and every checked README v0.1 bullet
has a named test.**

---

## 8. Phase map vs README roadmap

| README v0.1 bullet                                   | Phase               |
| ---------------------------------------------------- | ------------------- |
| WAL ingest, LSN checkpointing, resume after restart  | 1                   |
| Single-table shapes with auth-bound filters          | 2                   |
| Gapless initial snapshot handoff to live stream      | 3                   |
| WebSocket transport with offset-based resume         | 4                   |
| Server-authoritative mutations with idempotency keys | 5                   |
| Slot lag guard and schema-drift halt                 | 2 (drift) + 6 (lag) |
| Correctness / convergence claim                      | 7                   |

| README v0.2 (later)                 | Notes                                        |
| ----------------------------------- | -------------------------------------------- |
| Metrics                             | After v0.1; do not block correctness         |
| Backpressure / slow-client eviction | Partial in Phase 4; metrics + tuning in v0.2 |
| Structured logging / OTel           | `slog` from day one; OTel later              |

---

## 9. Testing strategy

| Layer                       | Command                                                    | When required                                                     |
| --------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------------- |
| Unit                        | `make test`                                                | Always                                                            |
| Integration (real Postgres) | `make db-up` + `TETHER_TEST_DSN` + `make test-integration` | Any change to WAL, LSN ack, offsets, snapshot handoff, slots, e2e |
| Race                        | `-race` in CI / Make                                       | Concurrency paths (fan-out, resume, mutate)                       |
| Lint / format               | `make lint`, `make fmt`                                    | Every PR                                                          |

Rules from AGENTS.md:

- Fake WAL messages alone are insufficient for decode/offset/handoff claims.
- Do not disable `-race`.
- Do not soften the convergence test with retries.

Minimum named integration coverage by v0.1 exit:

1. `TestPersistBeforeAck_CrashReplay` / crash replay
2. `TestShapeFilterIsolation` (auth claims)
3. `TestSnapshotStreamHandoff` (exact row-set equality)
4. `TestWebSocketResume` (resume by offset)
5. `TestEnqueue_FullBuffer` (slow client buffer; disconnect path)
6. `TestApply_IdempotentByKey` / `TestMutationIdempotentAndVisible`
7. `TestSlotLag_ForcesResnapshot` / `TestSlotLag_AbandonedSlotDropped`
8. `TestApply_SchemaDriftHalts` / `TestSchemaDriftStopsConsumer`
9. `TestConvergence` (headline)

---

## 10. Operational defaults (v0.1)

| Option             | Suggested default              | Purpose                      |
| ------------------ | ------------------------------ | ---------------------------- |
| `MaxSlotLag`       | 2 GiB                          | Invariant 6                  |
| `MaxClientIdle`    | 24h                            | Drop abandoned clients       |
| Client send buffer | small fixed (e.g. 64–256 msgs) | Invariant 7 — tune with test |
| Protocol           | version `1`                    | Reject newer/older cleanly   |

---

## 11. How to use this document with ExecPlans

1. Pick the **lowest unfinished phase** above.
2. Write an ExecPlan in `plans/<YYYYMMDD-HHmm>-<name>.md` using
   `.agents/commands/create-plan.md` / `.cursor/commands/create-plan.md`.
3. The ExecPlan must list Invariants touched and must not contradict this
   file. If the design needs to change, **update this file in the same PR**
   as the decision (Decision Log in the ExecPlan + short note here).
4. On PR merge that completes a phase, check off the matching README roadmap
   bullet only if the named acceptance tests exist.
5. Move finished ExecPlans to `plans/done/`.

Do not implement Phase 5 before Phase 4 has resume working. Mutations without
a resume-safe transport create false confidence.

---

## 12. Open decisions (resolve in early ExecPlans)

Record outcomes here when decided:

| Topic                        | Options / note                              | Status                                                         |
| ---------------------------- | ------------------------------------------- | -------------------------------------------------------------- |
| Module path                  | `github.com/anuwatthisuka/tether`           | Done (2026-07-19)                                              |
| Shape-log durability backend | Postgres tables (`tether.change_log` / `tether.checkpoint` in Phase 1) | Done (2026-07-19) — Phase 1 ExecPlan |
| Offset encoding              | LSN+seq vs monotonic integer per shape      | Open — decide in Phase 2/3; must be opaque to clients          |
| Wire format                  | JSON v1 vs compact binary                   | Prefer JSON v1 for debuggability; decide in Phase 4            |
| Idempotency table ownership  | tether-managed `tether.mutation_keys`       | Done (2026-07-19) — Phase 5 ExecPlan |
| Publication strategy         | one publication for all watched tables; filter in-process | Done (2026-07-19) — Phase 1 ExecPlan |

---

## 13. Definition of done (v0.1)

- [x] Phases 0–7 acceptance criteria all green under CI with `-race`
- [x] All nine minimum integration tests named above exist and pass
- [x] README v0.1 checklist fully checked, each bullet linked to a test name
      in CI or docs
- [x] `AGENTS.md` Invariants 1–7 each cited by at least one test or explicit
      code-path comment + test
- [x] Direct deps still only `pgx`, `pglogrepl`, `websocket` (or approved
      exceptions listed in CHANGELOG)
- [x] Public API documented in README matches `tether.go`; CHANGELOG current
- [x] No non-goal features merged "temporarily"

---

## 14. Revision history

| Date       | Change                                                      |
| ---------- | ----------------------------------------------------------- |
| 2026-07-19 | Initial master plan for greenfield tree (Phases 0–7 → v0.1) |
| 2026-07-19 | v0.1.0 released; definition of done checked; CI added       |
