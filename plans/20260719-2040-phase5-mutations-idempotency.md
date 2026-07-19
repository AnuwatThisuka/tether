# Phase 5: Mutations with idempotency

Living ExecPlan. Invariant touched: **3** (idempotent by key; dedupe in same txn as effect).

## Purpose

Clients send mutations with an idempotency key over WebSocket. The server applies them through a host `OnMutation` handler inside a Postgres transaction that also records the key. Replays return `mutation_ok` without a second effect. Rejects roll back (including the key insert) so a corrected retry can proceed.

## Progress

- [x] ExecPlan authored
- [x] `Mutation`, `Reject`, `OnMutation`, proto messages
- [x] `internal/mutate` EnsureSchema + Apply
- [x] Wire into `Engine.serveWS`
- [x] Integration: double key + WAL visibility
- [x] CHANGELOG / README / commit
- [ ] Move to `plans/done/` on PR

## Surprises & Discoveries

_(none)_

## Decision Log

- Decision: Tether-managed table `tether.mutation_keys (idempotency_key TEXT PRIMARY KEY)`.
  Rationale: Matches IMPLEMENT_PLAN preference; keeps dedupe in-engine.
  Date/Author: 2026-07-19

- Decision: On `Reject`, rollback entire txn (key insert undone) so clients may retry the same key after fixing the cause.
  Rationale: A rejected mutation must not permanently consume the idempotency key.
  Date/Author: 2026-07-19

- Decision: Duplicate key → `mutation_ok` with `duplicate: true`, handler not invoked.
  Date/Author: 2026-07-19

## Outcomes & Retrospective

Phase 5 acceptance passed:

- Integration: `TestApply_IdempotentByKey`, `TestApply_RejectRollsBackKey`
- Integration: `TestMutationIdempotentAndVisible` — WS mutate twice → one row; subscriber sees change via WAL
- `make test` / `make lint` / `make test-integration` green

Gaps: slot lag guard (Phase 6); full e2e convergence (Phase 7).
