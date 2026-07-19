# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see [VERSIONING.md](./VERSIONING.md)).

## Unreleased

## [0.2.0] - 2026-07-19

### Added

- Operability metrics hooks (`Metrics`, `WithMetrics`, `NopMetrics`): replication
  lag bytes, live client count, per-client buffered shape offsets, and
  `ClientDisconnected(reason)` for slow/idle eviction. No new dependencies —
  hosts adapt to Prometheus/OTel themselves (`docs/embed.md`).
- Backpressure docs: `WithClientBuffer` guidance; disconnects log
  `client_id` + reason via `slog`.
- `WithLogger` and consistent slog attrs (`slot`, `shape`, `client_id`,
  `lag_bytes`, `reason`, `err`) on fan-out, lag, halt, and disconnect paths.

## [0.1.0] - 2026-07-19

### Added

- GitHub Actions CI: unit tests, golangci-lint, and `make test-integration`
  under `-race` against Docker Postgres (`wal_level=logical`).
- Headline e2e convergence test (`internal/e2e.TestConvergence`): two offline
  clients, kill mid-write + cold restart, idempotent replay, identical final
  snapshots vs Postgres (`make test-integration` runs under `-race`).
- Slot lag guard (`MaxSlotLag`, default 2 GiB) and idle client sweep
  (`MaxClientIdle`, default 24h). Exceeding lag drops the replication slot,
  broadcasts `must_resnapshot`, clears in-memory streams, and recreates the
  slot (Invariant 6). Schema-drift shape apply errors are sent to clients as
  `shape_halted`. Minimal embedder guide in `docs/embed.md`.
- Server-authoritative mutations (`OnMutation`, `Reject`) with idempotency keys
  stored in `tether.mutation_keys` inside the same transaction as the handler
  (Invariant 3). Wire: `mutation` / `mutation_ok` / `mutation_reject`.
- WebSocket transport (`Engine.Handler`, `Engine.Run`) with `coder/websocket`:
  host `WithAuth`, per-client buffered fan-out (slow client → `bye`), and
  offset resume via in-process shape streams (Invariant 7).
- Gapless snapshot→stream handoff (`internal/snapshot`): REPEATABLE READ
  snapshot at LSN _N_ on a normal pool; shape `LoadSnapshot` + Apply skips
  `CommitLSN <= N` (Invariant 4).
- Shapes with server-side filters (`Engine.Shape`, `Where`, `Claims`): per-claims
  membership, enter/leave on filter boundary, schema-fingerprint halt
  (Invariants 2 and 5). In-memory per-shape log for now.
- Internal WAL consumer (`internal/wal`): pgoutput decode, Postgres-backed
  `tether.change_log` / `tether.checkpoint`, and persist-before-ack LSN
  advancement (Invariant 1).
- Dependencies: `jackc/pgx/v5`, `jackc/pglogrepl`, `coder/websocket`.
- Repository scaffolding: Go module, package placeholders, Makefile, and
  Docker Postgres for integration tests.
- Library SemVer via `tether.Version` and [VERSIONING.md](./VERSIONING.md).
