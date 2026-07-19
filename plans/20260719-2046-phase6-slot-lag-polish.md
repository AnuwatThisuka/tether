# Phase 6: Slot lag guard + polish for v0.1

Living ExecPlan. Invariant touched: **6** (bounded slot lag). Regression check: **1–5, 7**.

## Purpose

An abandoned or stuck replication slot pins WAL until Postgres disk fills.
`tether` monitors slot lag, drops the slot when it exceeds `MaxSlotLag`,
forces live clients to re-snapshot, recreates the slot, and continues.
Also wire `MaxClientIdle`, surface shape-halt errors to clients, and document
a minimal embedder guide.

## Progress

- [x] ExecPlan authored
- [x] `MaxSlotLag` / `MaxClientIdle` options (defaults 2 GiB / 24h)
- [x] `wal.SlotLagBytes` + force `DropSlot` (terminate active backend)
- [x] Engine: lag monitor, resnapshot broadcast, consumer restart
- [x] Idle client sweep
- [x] Loud `shape_halted` on schema drift apply
- [x] Integration tests + embedder doc
- [x] CHANGELOG / README checkbox / commit
- [ ] Move to `plans/done/` on PR

## Surprises & Discoveries

_(none yet)_

## Decision Log

- Decision: On lag exceed — cancel consumer, drop slot (terminating walsender),
  clear in-memory streams + session shape membership, delete consumer
  checkpoint so the new slot does not resume past a discarded LSN, broadcast
  `must_resnapshot`, recreate slot, restart consumer. Prefer recreate-and-
  continue over permanently stopping `Run`.
  Rationale: Invariant 6 prefers drop+resnapshot over pinning disk; embedders
  expect the engine to keep serving after a guard trip.
  Date/Author: 2026-07-19

- Decision: `MaxSlotLag(0)` / `MaxClientIdle(0)` disable the respective guards;
  defaults remain 2 GiB / 24h when unset.
  Rationale: Matches README option shape; tests can disable without huge limits.
  Date/Author: 2026-07-19

## Outcomes & Retrospective

Phase 6 acceptance passed:

- Integration: `TestSlotLag_AbandonedSlotDropped` — abandoned slot with
  incompressible WAL → drop unpins slot
- Integration: `TestSlotLag_ForcesResnapshot` — lag exceed → `must_resnapshot`
  + slot recreated
- Integration: `TestMaxClientIdle_Disconnects` — idle → `bye: idle_client`
- Prior: `TestSchemaDriftStopsConsumer`, `TestApply_SchemaDriftHalts`
- `make test` / `make lint` / `make test-integration` green

Next: Phase 7 e2e convergence.
