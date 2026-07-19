package wal

import "errors"

// ErrSchemaDrift is returned when a Relation message disagrees with the
// cached column fingerprint for that relation OID. The consumer must stop;
// remapping columns is forbidden (AGENTS.md Invariant 5).
var ErrSchemaDrift = errors.New("wal: schema drift detected")

// ErrNotConfigured is returned when Config is missing required fields.
var ErrNotConfigured = errors.New("wal: not configured")
