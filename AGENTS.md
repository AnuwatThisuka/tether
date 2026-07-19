# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this is

`tether` is a Postgres sync engine distributed as a Go library. It embeds
into an existing Go HTTP server — it is not a standalone service. It reads
the Postgres WAL via logical replication, filters changes into per-client
**shapes**, streams them over WebSocket, and accepts writes back as
**mutations** applied by user-supplied handlers.

This is a **correctness-critical data system**. Silent data loss is the
worst possible outcome and is worse than a crash. When in doubt, fail loudly.

The master design and phased build order live in [`IMPLEMENT_PLAN.md`](./IMPLEMENT_PLAN.md).
Day-to-day work uses ExecPlans under `plans/`; those must stay consistent
with this file and with `IMPLEMENT_PLAN.md`.

## Commands

```bash
make test          # unit tests, no database required
make test-integration   # requires Postgres, see below
make lint          # golangci-lint
make fmt           # gofumpt
```

Integration tests need a Postgres with `wal_level = logical`:

```bash
make db-up         # docker compose, exposes :54321
export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
make test-integration
make db-down
```

If a command above does not exist yet, add it to the Makefile rather than
documenting a different invocation.

## Layout

```
cmd/           # dev tooling and test harnesses only, never library code
internal/wal/  # replication slot lifecycle, LSN checkpointing, pgoutput decoding
internal/shape/# shape registry, predicate evaluation, per-shape log
internal/proto/# wire format, framing, offset encoding
transport/     # WebSocket server and resume logic
tether.go      # public API surface
```

Public API lives in the root package only. Anything under `internal/` can
change freely; anything in the root package is a compatibility commitment
and needs a changelog entry.

---

## Invariants — do not violate these

These are the rules that make the system correct. Breaking one produces bugs
that surface days later as corrupted customer data. If a task seems to
require breaking one, stop and say so instead of proceeding.

**1. Never acknowledge an LSN before the change is durably recorded.**
`SendStandbyStatusUpdate` tells Postgres it may discard WAL. Advancing the
confirmed LSN ahead of our own persisted state means that data is gone
permanently after a crash. Persist first, acknowledge second, always.

**2. Shape filters are resolved server-side from auth claims. Always.**
A predicate must never originate from client input, not even partially,
not even as an optimization, not even behind a feature flag. This is the
security boundary of the entire system. A client sending its own `WHERE`
clause is a cross-tenant data leak.

**3. Mutations must be idempotent by key.**
Clients replay queued mutations after reconnect. Any handler path that can
apply the same idempotency key twice is a bug. Dedupe inside the same
transaction that applies the effect — never in a separate one.

**4. Snapshot and stream must not overlap or gap.**
A new client gets a snapshot at LSN _N_ and then deltas strictly after _N_.
Off-by-one here silently duplicates or drops rows. Any change to this
handoff needs a test that asserts exact row-set equality against the source
table.

**5. Schema drift halts the shape.**
Logical replication does not carry DDL. On detecting a column set that
disagrees with the cached relation, stop the shape and surface an error.
Never guess a mapping, never skip the unknown column, never continue.

**6. Slot lag is bounded.**
An abandoned replication slot pins WAL until the customer's disk fills and
their database stops accepting writes. Dropping a slot and forcing a client
to re-snapshot is always preferable. Never add a code path that can hold a
slot open indefinitely.

**7. One slow client must not stall the others.**
Fan-out is per-client buffered. When a buffer fills, disconnect that client
and let it resume by offset. Never block the WAL reader on a socket write.

---

## Traps specific to this domain

Things that look correct and are not:

- **`UPDATE` events may not contain every column.** Postgres sends only the
  changed columns unless `REPLICA IDENTITY FULL` is set. Code that assumes
  a complete row will write nulls over live data.
- **`TOAST`ed values arrive as unchanged-markers.** Treat them as "not
  present", never as "empty".
- **Transaction boundaries matter.** Changes must be applied to the shape
  log atomically per source transaction. Emitting a partial transaction
  exposes clients to states that never existed in the database.
- **A replication connection is not a normal connection.** It cannot run
  ordinary queries. Snapshotting needs a separate pool.
- **Reconnects are not rare.** Mobile and laptop clients reconnect
  constantly. Resume paths get exercised far more than the happy path and
  deserve proportionally more test coverage.

## Testing expectations

- Any change touching WAL decoding, offsets, or the snapshot handoff
  requires an integration test against real Postgres. Unit tests with fake
  WAL messages are insufficient — they encode our assumptions, and the bugs
  live where the assumptions are wrong.
- Concurrency-sensitive code runs under `-race` in CI. Do not disable it.
- The convergence test in `internal/e2e` is the project's headline
  correctness claim (two offline clients, conflicting edits, cold restart,
  reconnect, identical final state). If a change makes it flaky, the change
  is wrong — do not add retries to make it pass.

## Style

- Standard Go. `gofumpt`, no custom formatter.
- Wrap errors with `fmt.Errorf("...: %w", err)`. Preserve the chain.
- Exported identifiers get doc comments. Unexported ones only where the
  reasoning is non-obvious — comments explaining _why_, not _what_.
- No new third-party dependencies without asking first. Current direct
  deps: `jackc/pgx`, `jackc/pglogrepl`, `coder/websocket`. Keeping this
  list short is a feature of an embeddable library.
- Log with `log/slog`. No `fmt.Println` outside `cmd/`.

## Scope

The README lists explicit non-goals: CRDTs and automatic merge, joins in
shapes, presence and cursors, multi-region coordination, non-Postgres
databases, an admin UI.

Do not implement these, and do not add abstractions in anticipation of
them. If a task appears to require one, say so and stop — the answer is
usually that the task should be scoped differently.

## Versioning

Semantic Versioning 2.0.0 — see [`VERSIONING.md`](./VERSIONING.md).
`tether.Version` must match the release. Git tags are `vMAJOR.MINOR.PATCH`.
Root-package changes need a `CHANGELOG.md` entry under `Unreleased` (or the
release section when cutting a version).

## Pull requests

- One concern per PR.
- Describe the failure mode being fixed, not just the change.
- Changes to the root package need a changelog entry.
- Say plainly if something is untested. An honest gap is fine; an implied
  guarantee that does not exist is not.
- Breaking public API while MAJOR is 0 → MINOR bump at release time (not
  silently in a drive-by). After 1.0.0 → MAJOR.
