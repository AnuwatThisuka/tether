package tether

import (
	"context"

	"github.com/anuwatthisuka/tether/transport"
)

// SetSlotLagOverrideForTest replaces SlotLagBytes for integration tests.
func (e *Engine) SetSlotLagOverrideForTest(fn func(context.Context) (int64, error)) {
	e.slotLagMu.Lock()
	e.slotLagOverride = fn
	e.slotLagMu.Unlock()
}

// DisconnectForTest invokes the server-initiated disconnect path.
func (e *Engine) DisconnectForTest(conn *transport.Conn, reason string) {
	e.disconnect(context.Background(), conn, reason)
}
