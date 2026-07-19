# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

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
