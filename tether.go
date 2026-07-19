// Package tether is a Postgres sync engine that embeds into a Go HTTP server.
//
// Status: pre-alpha scaffolding. The public API is incomplete; New returns
// ErrNotImplemented until later phases land (see IMPLEMENT_PLAN.md).
package tether

import (
	"fmt"
)

// Engine is the sync engine handle returned by New.
//
// Methods such as Shape, OnMutation, Handler, and Run will be added as
// phases land; they are intentionally absent until implemented.
type Engine struct{}

type options struct{}

// Option configures an Engine. Phase 0 defines the type only; concrete
// options arrive in later phases (MaxSlotLag, MaxClientIdle, etc.).
type Option func(*options)

// New constructs an Engine. Phase 0 validates pgURL and then fails closed
// with ErrNotImplemented — it does not open database connections.
func New(pgURL string, opts ...Option) (*Engine, error) {
	if pgURL == "" {
		return nil, fmt.Errorf("tether: pgURL is required")
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	_ = o
	return nil, fmt.Errorf("%w", ErrNotImplemented)
}
