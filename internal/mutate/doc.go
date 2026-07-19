// Package mutate applies client mutations through a user-supplied handler
// with idempotency-key dedupe inside the same database transaction
// (see AGENTS.md Invariant 3).
package mutate
