# Slice 2: Backpressure / slow-client eviction polish

Living ExecPlan for [`IMPLEMENT_PLAN_v0.2.md`](../IMPLEMENT_PLAN_v0.2.md).
Invariant **7**: one slow client must not stall others or the WAL reader.

## Purpose

Phase 4 already disconnects on full outbound buffer (`bye: slow_client`).
Slice 2 makes that operable: counters on `Metrics`, louder structured logs,
and clearer embedder guidance for `WithClientBuffer`.

## Progress

- [x] ExecPlan authored
- [x] `Metrics.ClientDisconnected(reason)` (+ idle/slow call sites)
- [x] slog on slow/idle disconnect with client_id + reason
- [x] Docs: buffer sizing guidance in embed.md / README
- [x] Tests for disconnect metric
- [x] CHANGELOG Unreleased
- [x] Commit
- [ ] Move to `plans/done/` on release

## Outcomes & Retrospective

Slice 2 accepted:

- `TestMetrics_SlowClientDisconnected` — tiny buffer → `ClientDisconnected(slow_client)`
- `disconnect` helper logs + metrics for slow/idle
- embed.md backpressure section
