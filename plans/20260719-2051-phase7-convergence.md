# Phase 7: Convergence (v0.1 exit criteria)

Living ExecPlan. Touches all invariants end-to-end; no new engine features.

## Purpose

Prove the README headline: two clients edit overlapping rows offline, one is
killed mid-write and cold-restarted, both reconnect and replay, and they
converge to identical state with no lost write and no double-apply.

## Progress

- [x] ExecPlan authored
- [x] `TestConvergence` harness + scenario
- [x] `make test-integration` runs under `-race`
- [x] README / CHANGELOG note named test
- [x] Commit
- [ ] Move to `plans/done/` on PR

## Surprises & Discoveries

- Enabling `-race` on integration exposed data races on `session.shapes`
  and test `slotLagOverride` (fixed with session mutex / RWMutex).
- Default `max_replication_slots=10` exhausted under parallel package tests
  after race slowdown; raised compose to 64.
- `MaxSlotLag(1024)` in lag WS test could trip on setup WAL before the
  client connected; trip is now override-only against a high threshold.

## Decision Log

- Decision: "Ten minutes offline" is simulated with a short fixed wait, not
  a literal sleep. Scenario intent is offline queuing + reconnect replay.
  Rationale: CI cannot afford a real 10-minute wall clock; retries/sleeps to
  greenwash flakiness remain forbidden.
  Date/Author: 2026-07-19

- Decision: Client killed mid-write = close WebSocket after sending a mutation
  without awaiting `mutation_ok`. Cold restart = drop local shape state;
  mutation queue retained and fully replayed.
  Rationale: Matches reconnect/idempotency path clients exercise in production.
  Date/Author: 2026-07-19

- Decision: Convergence asserted via fresh snapshots after replay quiescence,
  compared to each other and to `SELECT` from the table. Double-apply asserted
  via handler apply counter + `tether.mutation_keys` cardinality.
  Rationale: Server-authoritative LWW titles may differ from either client's
  optimistic guess; identical snapshots are the correctness claim.
  Date/Author: 2026-07-19

## Outcomes & Retrospective

Phase 7 acceptance passed:

- `TestConvergence` — offline queues, kill mid-write, cold replay,
  identical snapshots vs DB, no double-apply on re-replay
- `make test-integration` uses `-race`; green (including ×3 convergence)
- README Correctness names `TestConvergence`; v0.1 checklist complete
