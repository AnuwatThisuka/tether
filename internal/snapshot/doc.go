// Package snapshot takes a consistent table snapshot at LSN N on a normal
// (non-replication) connection pool so the live stream can start strictly
// after N (see AGENTS.md Invariant 4).
package snapshot
