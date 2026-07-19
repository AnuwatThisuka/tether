# Slice 1: Metrics hooks (replication lag, slot, client offsets)

Living ExecPlan for [`IMPLEMENT_PLAN_v0.2.md`](../IMPLEMENT_PLAN_v0.2.md).
No new third-party dependencies. Hosts adapt to Prometheus/OTel themselves.

## Purpose

Operators need lag, slot pressure, and per-client progress without tether
pulling a metrics SDK. Provide a small `Metrics` interface sampled from the
engine loop and fan-out path.

## Progress

- [x] ExecPlan authored
- [x] Public `Metrics` + `NopMetrics` + `WithMetrics`
- [x] Sample slot lag (reuse `wal.SlotLagBytes`) on Run ticker
- [x] Observe per-client shape offsets on successful enqueue
- [x] Unit test with recording metrics; optional integration lag sample
- [x] Docs: embed.md adapter sketch; CHANGELOG Unreleased
- [x] Commit
- [x] Move to `plans/done/` on release

## Surprises & Discoveries

_(none yet)_

## Decision Log

- Decision: Interface + no-op default; no Prometheus/OTel module deps.
  Rationale: AGENTS.md keeps the embeddable dep list short; hosts already
  have a metrics stack.
  Date/Author: 2026-07-19

- Decision: Sample lag on the existing Run ticker (with maxSlotLag /
  fan-out), not a separate goroutine.
  Rationale: Avoid extra scheduling; fail loudly if sample errors via slog.
  Date/Author: 2026-07-19

- Decision: "Slot size" in README maps to **restart_lsn lag bytes**
  (`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`), same as lag guard.
  Postgres does not expose a reliable "slot bytes on disk" without extensions;
  document the semantic clearly.
  Date/Author: 2026-07-19

- Decision: Per-client offset is reported when a change is successfully
  `Enqueue`d (server outbound progress), keyed by `client_id` + shape.
  Rationale: Matches "how far has this socket been offered data"; applied
  ack from client remains optional (not in v0.1 protocol).
  Date/Author: 2026-07-19

## Proposed public API (to confirm at implement)

```go
// Metrics receives operational observations from the engine.
// Implementations must be safe for concurrent use and must not block.
type Metrics interface {
    // ReplicationLagBytes is how far the replication slot lags the tip WAL.
    ReplicationLagBytes(n int64)
    // ClientOffset is the last offset successfully buffered for a client shape.
    ClientOffset(clientID, shape string, offset int64)
    // ClientsConnected is the current live WebSocket count (optional gauge).
    ClientsConnected(n int)
}

func WithMetrics(m Metrics) Option
```

Counters for disconnects/mutations can wait for Slice 2–3 if this stays small.

## Outcomes & Retrospective

Slice 1 accepted:

- `TestNopMetrics_NoPanic`, `TestWithMetrics_Accepted`
- `TestMetrics_LagAndClientsSampled` — lag override 42000, clients≥1, snapshot offset
- Docs in `docs/embed.md`; CHANGELOG Unreleased
