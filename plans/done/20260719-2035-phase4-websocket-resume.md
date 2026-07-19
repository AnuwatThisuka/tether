# Phase 4: WebSocket transport + offset resume

Living ExecPlan. Invariants: **7** (slow client isolated), resume correctness (supports **4**).

## Purpose

Clients connect over WebSocket, authenticate via host callback, subscribe to shapes by name only, receive snapshot then changes, and can reconnect with a last-seen offset. A full per-client send buffer disconnects that client without blocking WAL fan-out.

## Progress

- [x] ExecPlan authored
- [x] `internal/proto` JSON messages
- [x] `transport` Conn/Hub with non-blocking enqueue
- [x] `Engine.Handler` / `Run` / `WithAuth` / stream store for resume
- [x] `Log.After`; resume + buffer-full unit tests; `TestWebSocketResume`
- [x] CHANGELOG / README / commit
- [x] Move to `plans/done/` on PR

## Surprises & Discoveries

- Observation: Fan-out polls `tether.change_log` rather than hooking the consumer callback — keeps WAL persist-before-ack path unchanged.
  Evidence: `Engine.Run` ticker + `fanOutNewChanges`.

## Decision Log

- Decision: Shared in-process shape streams keyed by `(shapeName, claimsKey)` so reconnect can resume by offset without a durable shape log.
  Rationale: Phase 2 log is in-memory; process-local resume still exercises Invariant 7 + offset resume.
  Date/Author: 2026-07-19

- Decision: Default client buffer 64 messages; `WithClientBuffer` overrides.
  Date/Author: 2026-07-19

- Decision: Wire format JSON, one message per WebSocket frame (`coder/websocket`).
  Date/Author: 2026-07-19

## Outcomes & Retrospective

Phase 4 acceptance passed:

- Unit: `TestEnqueue_FullBuffer`, `TestLogAfter`
- Integration: `TestWebSocketResume` — snapshot, live change, reconnect with offset, next insert without gap
- Slow-client isolation covered by non-blocking enqueue unit test (Invariant 7)
- `make test` / `make lint` / `make test-integration` green

Gaps: mutations (Phase 5); durable shape logs across process restart; origin checks left to embedder (`InsecureSkipVerify` on Accept).
