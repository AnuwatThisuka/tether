// Package wal owns the Postgres logical replication slot lifecycle,
// pgoutput decoding, and LSN checkpointing. Persist before ack
// (see AGENTS.md Invariant 1).
package wal
