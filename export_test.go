package tether

import "context"

// SetSlotLagOverrideForTest replaces SlotLagBytes for integration tests.
func (e *Engine) SetSlotLagOverrideForTest(fn func(context.Context) (int64, error)) {
	e.slotLagOverride = fn
}
