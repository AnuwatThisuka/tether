# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

- Shapes with server-side filters (`Engine.Shape`, `Where`, `Claims`): per-claims
  membership, enter/leave on filter boundary, schema-fingerprint halt
  (Invariants 2 and 5). In-memory per-shape log for now.
- Internal WAL consumer (`internal/wal`): pgoutput decode, Postgres-backed
  `tether.change_log` / `tether.checkpoint`, and persist-before-ack LSN
  advancement (Invariant 1). Not a public API promise — `Run`/`Handler` are
  still unimplemented.
- Dependencies: `jackc/pgx/v5`, `jackc/pglogrepl`.
- Repository scaffolding: Go module, package placeholders, Makefile, and
  Docker Postgres for integration tests.
