package tether

import (
	"errors"
	"fmt"
)

// Mutation is a client write delivered to OnMutation.
type Mutation struct {
	Op     string
	Key    string // idempotency key
	Claims Claims
	args   map[string]any
}

// Arg returns a named argument from the mutation payload.
func (m Mutation) Arg(name string) any {
	if m.args == nil {
		return nil
	}
	return m.args[name]
}

// Args returns a copy of the mutation arguments.
func (m Mutation) Args() map[string]any {
	if m.args == nil {
		return nil
	}
	out := make(map[string]any, len(m.args))
	for k, v := range m.args {
		out[k] = v
	}
	return out
}

// RejectError is returned from OnMutation to refuse a write.
type RejectError struct {
	Reason string
}

func (e *RejectError) Error() string {
	if e == nil || e.Reason == "" {
		return "tether: rejected"
	}
	return "tether: rejected: " + e.Reason
}

// Reject returns an error that rejects the mutation with a client-visible reason.
func Reject(reason string) error {
	return &RejectError{Reason: reason}
}

// IsReject reports whether err is or wraps a RejectError.
func IsReject(err error) bool {
	var r *RejectError
	return errors.As(err, &r)
}

// RejectReason extracts the reject reason, or empty if not a reject.
func RejectReason(err error) string {
	var r *RejectError
	if errors.As(err, &r) {
		return r.Reason
	}
	return ""
}

func newMutation(op, key string, claims Claims, args map[string]any) (Mutation, error) {
	if op == "" {
		return Mutation{}, fmt.Errorf("tether: mutation op is required")
	}
	if key == "" {
		return Mutation{}, fmt.Errorf("tether: mutation idempotency key is required")
	}
	return Mutation{Op: op, Key: key, Claims: claims, args: args}, nil
}
