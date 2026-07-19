package tether

import "errors"

// ErrNotImplemented is returned by API entry points that are not yet built.
// Prefer this loud failure over a half-working Engine.
var ErrNotImplemented = errors.New("tether: not implemented")
