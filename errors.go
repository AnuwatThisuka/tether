package tether

import "errors"

// ErrNotImplemented is returned by API entry points that are not yet built.
// Prefer this loud failure over a half-working Engine.
var ErrNotImplemented = errors.New("tether: not implemented")

// ErrSlotLagExceeded indicates a replication slot was dropped because lag
// exceeded MaxSlotLag. Clients receive must_resnapshot; Run recreates the slot.
var ErrSlotLagExceeded = errors.New("tether: replication slot lag exceeded")
