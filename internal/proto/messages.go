// Package proto defines the tether WebSocket JSON wire format.
package proto

import (
	"encoding/json"
	"fmt"
)

const ProtocolVersion = 1

// Envelope is the common message wrapper.
type Envelope struct {
	Type string `json:"type"`
}

// Hello is sent by the client after the socket opens.
type Hello struct {
	Type     string           `json:"type"` // hello
	Protocol int              `json:"protocol"`
	Resume   map[string]int64 `json:"resume,omitempty"` // shape → last applied offset
}

// Subscribe requests shapes by name only (never a filter).
type Subscribe struct {
	Type   string   `json:"type"` // subscribe
	Shapes []string `json:"shapes"`
}

// Snapshot is the initial row set for a shape at handoff LSN.
type Snapshot struct {
	Type   string           `json:"type"` // snapshot
	Shape  string           `json:"shape"`
	LSN    string           `json:"lsn"`
	Offset int64            `json:"offset"` // last offset after seeding (0 if empty)
	Rows   []map[string]any `json:"rows"`
}

// Change is a live shape-log event.
type Change struct {
	Type   string         `json:"type"` // change
	Shape  string         `json:"shape"`
	Offset int64          `json:"offset"`
	Op     string         `json:"op"` // insert|update|delete
	Row    map[string]any `json:"row"`
}

// Error notifies the client of a hard failure for a shape or session.
type Error struct {
	Type    string `json:"type"` // error
	Code    string `json:"code"`
	Message string `json:"message"`
	Shape   string `json:"shape,omitempty"`
}

// Bye is sent before the server closes a slow or shutting-down client.
type Bye struct {
	Type   string `json:"type"` // bye
	Reason string `json:"reason"`
}

// Mutation is a client write request.
type Mutation struct {
	Type string         `json:"type"` // mutation
	ID   string         `json:"id"`   // client correlation id
	Op   string         `json:"op"`
	Key  string         `json:"key"` // idempotency key
	Args map[string]any `json:"args,omitempty"`
}

// MutationOK acknowledges a successful (or duplicate) apply.
type MutationOK struct {
	Type      string `json:"type"` // mutation_ok
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// MutationReject refuses a mutation with a client-visible reason.
type MutationReject struct {
	Type   string `json:"type"` // mutation_reject
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

const (
	CodeMustResnapshot = "must_resnapshot"
	CodeUnauthorized   = "unauthorized"
	CodeBadProtocol    = "bad_protocol"
	CodeUnknownShape   = "unknown_shape"
	CodeHalted         = "shape_halted"
	CodeNoHandler      = "no_mutation_handler"

	ReasonSlowClient = "slow_client"
	ReasonShutdown   = "shutdown"
)

// Marshal returns JSON bytes for a wire message.
func Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("proto: marshal: %w", err)
	}
	return b, nil
}

// UnmarshalType peeks the type field.
func UnmarshalType(data []byte) (string, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", fmt.Errorf("proto: unmarshal type: %w", err)
	}
	return env.Type, nil
}

// Decode unmarshals data into dest.
func Decode(data []byte, dest any) error {
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("proto: decode: %w", err)
	}
	return nil
}
