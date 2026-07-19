package shape

import (
	"fmt"
	"strings"
	"sync"
)

// EventOp is a shape-log operation as seen by a subscriber.
type EventOp string

const (
	EventInsert EventOp = "insert"
	EventUpdate EventOp = "update"
	EventDelete EventOp = "delete"
)

// Event is one append to a per-shape log.
type Event struct {
	Offset int64
	Op     EventOp
	Table  string
	Row    map[string]any // new row for insert/update; old/key row for delete
}

// Log is an in-memory append-only per-shape log with monotonic offsets.
type Log struct {
	mu     sync.Mutex
	next   int64
	events []Event
	halted error
}

// NewLog returns an empty shape log whose first offset is 1.
func NewLog() *Log {
	return &Log{next: 1}
}

// Halt permanently stops the log with err (Invariant 5).
func (l *Log) Halt(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.halted == nil {
		l.halted = err
	}
}

// Err returns the halt error, if any.
func (l *Log) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.halted
}

// Append adds an event and returns it with the assigned offset.
func (l *Log) Append(op EventOp, table string, row map[string]any) (Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.halted != nil {
		return Event{}, l.halted
	}
	ev := Event{
		Offset: l.next,
		Op:     op,
		Table:  table,
		Row:    cloneRow(row),
	}
	l.next++
	l.events = append(l.events, ev)
	return ev, nil
}

// Events returns a snapshot of the log.
func (l *Log) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	return out
}

// After returns events with Offset strictly greater than offset.
// ok is false when the log cannot satisfy resume (empty, or offset before
// the first retained event — client must resnapshot).
func (l *Log) After(offset int64) (events []Event, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.events) == 0 {
		if offset == 0 {
			return nil, true
		}
		return nil, false
	}
	first := l.events[0].Offset
	if offset > 0 && offset < first-1 {
		return nil, false
	}
	for _, ev := range l.events {
		if ev.Offset > offset {
			events = append(events, ev)
		}
	}
	return events, true
}

// Len returns the number of events.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// LastOffset returns the newest offset, or 0 if empty.
func (l *Log) LastOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.events) == 0 {
		return 0
	}
	return l.events[len(l.events)-1].Offset
}

func cloneRow(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}

func pkKey(pk []string, row map[string]any) (string, error) {
	if len(pk) == 0 {
		return "", fmt.Errorf("shape: primary key not configured")
	}
	parts := make([]string, len(pk))
	for i, col := range pk {
		v, ok := row[col]
		if !ok {
			return "", fmt.Errorf("shape: missing primary key column %q", col)
		}
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, "|"), nil
}
