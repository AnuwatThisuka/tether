# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

- Internal WAL consumer (`internal/wal`): pgoutput decode, Postgres-backed
  `tether.change_log` / `tether.checkpoint`, and persist-before-ack LSN
  advancement (Invariant 1). Not a public API promise — embeds still get
  `ErrNotImplemented` from `tether.New`.
- Dependencies: `jackc/pgx/v5`, `jackc/pglogrepl`.
- Repository scaffolding: Go module, package placeholders, Makefile, and
  Docker Postgres for integration tests.
