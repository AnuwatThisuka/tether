// Package wal owns the Postgres logical replication slot lifecycle,
// pgoutput decoding, durable change-log persistence, and LSN checkpointing.
//
// Persist before ack (AGENTS.md Invariant 1): change_log + checkpoint must
// COMMIT before SendStandbyStatusUpdate advances the confirmed LSN.
package wal
