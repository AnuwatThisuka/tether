package shape

import (
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"

	"github.com/anuwatthisuka/tether/internal/wal"
)

// ErrSchemaDrift is returned when a change's relation fingerprint disagrees
// with the fingerprint already recorded for this shape (Invariant 5).
var ErrSchemaDrift = errors.New("shape: schema drift")

// ErrHalted is returned when Apply is called on a halted shape.
var ErrHalted = errors.New("shape: halted")

// Definition is a registered shape bound to a claims→filter function.
type Definition struct {
	Name       string
	Schema     string
	Table      string
	PrimaryKey []string
	Bind       func(claims any) (Filter, error)
}

// Instance is a live shape for one claims principal: filter, log, row cache.
type Instance struct {
	Def    Definition
	Filter Filter
	Log    *Log

	mu          sync.Mutex
	fingerprint string
	// snapshotLSN is the handoff LSN from LoadSnapshot; Apply ignores
	// changes with CommitLSN <= snapshotLSN (Invariant 4).
	snapshotLSN pglogrepl.LSN
	hasSnapshot bool
	// rows currently in the shape, keyed by pkKey
	cache map[string]map[string]any
}

// NewInstance builds a shape instance by evaluating Bind(claims).
func NewInstance(def Definition, claims any) (*Instance, error) {
	if def.Bind == nil {
		return nil, fmt.Errorf("shape: bind is nil")
	}
	if def.Schema == "" {
		def.Schema = "public"
	}
	if def.Table == "" {
		def.Table = def.Name
	}
	if len(def.PrimaryKey) == 0 {
		def.PrimaryKey = []string{"id"}
	}
	f, err := def.Bind(claims)
	if err != nil {
		return nil, err
	}
	return &Instance{
		Def:    def,
		Filter: f,
		Log:    NewLog(),
		cache:  make(map[string]map[string]any),
	}, nil
}

// Apply projects a WAL change into shape-log events for this instance.
func (inst *Instance) Apply(ch wal.Change) ([]Event, error) {
	if err := inst.Log.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrHalted, err)
	}
	if !tableMatch(inst.Def, ch) {
		return nil, nil
	}

	inst.mu.Lock()
	if inst.hasSnapshot && ch.CommitLSN != 0 && ch.CommitLSN <= inst.snapshotLSN {
		inst.mu.Unlock()
		return nil, nil
	}
	inst.mu.Unlock()

	if err := inst.checkFingerprint(ch.RelationFingerprint); err != nil {
		inst.Log.Halt(err)
		return nil, err
	}

	switch ch.Op {
	case wal.OpInsert:
		return inst.applyInsert(ch.New)
	case wal.OpDelete:
		return inst.applyDelete(ch.Old)
	case wal.OpUpdate:
		return inst.applyUpdate(ch.Old, ch.New)
	default:
		return nil, fmt.Errorf("shape: unknown wal op %q", ch.Op)
	}
}

// LoadSnapshot seeds the shape from a consistent snapshot at lsn.
// Subsequent Apply calls ignore changes with CommitLSN <= lsn (Invariant 4).
func (inst *Instance) LoadSnapshot(lsn pglogrepl.LSN, rows []map[string]any) error {
	if err := inst.Log.Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrHalted, err)
	}
	inst.mu.Lock()
	inst.snapshotLSN = lsn
	inst.hasSnapshot = true
	inst.mu.Unlock()

	for _, row := range rows {
		ok, err := inst.Filter.Match(row)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		key, err := pkKey(inst.Def.PrimaryKey, row)
		if err != nil {
			return err
		}
		inst.mu.Lock()
		inst.cache[key] = cloneRow(row)
		inst.mu.Unlock()
		if _, err := inst.Log.Append(EventInsert, inst.Def.Table, row); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotLSN returns the handoff LSN if LoadSnapshot was called.
func (inst *Instance) SnapshotLSN() (pglogrepl.LSN, bool) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return inst.snapshotLSN, inst.hasSnapshot
}

// Materialized returns a copy of rows currently in the shape cache.
func (inst *Instance) Materialized() []map[string]any {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	out := make([]map[string]any, 0, len(inst.cache))
	for _, row := range inst.cache {
		out = append(out, cloneRow(row))
	}
	return out
}

func tableMatch(def Definition, ch wal.Change) bool {
	schema := ch.Schema
	if schema == "" {
		schema = "public"
	}
	return schema == def.Schema && ch.Table == def.Table
}

func (inst *Instance) checkFingerprint(fp string) error {
	if fp == "" {
		return nil
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.fingerprint == "" {
		inst.fingerprint = fp
		return nil
	}
	if inst.fingerprint != fp {
		return fmt.Errorf("%w: shape %q", ErrSchemaDrift, inst.Def.Name)
	}
	return nil
}

func (inst *Instance) applyInsert(neu map[string]any) ([]Event, error) {
	ok, err := inst.Filter.Match(neu)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	key, err := pkKey(inst.Def.PrimaryKey, neu)
	if err != nil {
		return nil, err
	}
	inst.mu.Lock()
	inst.cache[key] = cloneRow(neu)
	inst.mu.Unlock()
	ev, err := inst.Log.Append(EventInsert, inst.Def.Table, neu)
	if err != nil {
		return nil, err
	}
	return []Event{ev}, nil
}

func (inst *Instance) applyDelete(old map[string]any) ([]Event, error) {
	key, cached, err := inst.lookup(old)
	if err != nil {
		return nil, err
	}
	if cached == nil {
		// Not in shape — ignore (or evaluate Old if full row).
		if old != nil {
			ok, err := inst.Filter.Match(old)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, nil
			}
			// Matched filter but not cached (restart) — still emit delete.
		} else {
			return nil, nil
		}
	}
	row := cached
	if row == nil {
		row = old
	}
	inst.mu.Lock()
	delete(inst.cache, key)
	inst.mu.Unlock()
	ev, err := inst.Log.Append(EventDelete, inst.Def.Table, row)
	if err != nil {
		return nil, err
	}
	return []Event{ev}, nil
}

func (inst *Instance) applyUpdate(old, neu map[string]any) ([]Event, error) {
	key, cached, err := inst.lookup(firstNonNil(old, neu))
	if err != nil {
		return nil, err
	}

	beforeIn := cached != nil
	var beforeRow map[string]any
	if beforeIn {
		beforeRow = cached
	} else if old != nil {
		ok, err := inst.Filter.Match(old)
		if err != nil {
			return nil, err
		}
		beforeIn = ok
		if ok {
			beforeRow = old
		}
	}

	afterRow := patchRow(cached, old, neu)
	afterIn, err := inst.Filter.Match(afterRow)
	if err != nil {
		return nil, err
	}

	switch {
	case beforeIn && afterIn:
		inst.mu.Lock()
		inst.cache[key] = cloneRow(afterRow)
		inst.mu.Unlock()
		ev, err := inst.Log.Append(EventUpdate, inst.Def.Table, afterRow)
		if err != nil {
			return nil, err
		}
		return []Event{ev}, nil
	case beforeIn && !afterIn:
		inst.mu.Lock()
		delete(inst.cache, key)
		inst.mu.Unlock()
		ev, err := inst.Log.Append(EventDelete, inst.Def.Table, beforeRow)
		if err != nil {
			return nil, err
		}
		return []Event{ev}, nil
	case !beforeIn && afterIn:
		inst.mu.Lock()
		inst.cache[key] = cloneRow(afterRow)
		inst.mu.Unlock()
		ev, err := inst.Log.Append(EventInsert, inst.Def.Table, afterRow)
		if err != nil {
			return nil, err
		}
		return []Event{ev}, nil
	default:
		return nil, nil
	}
}

func (inst *Instance) lookup(row map[string]any) (string, map[string]any, error) {
	if row == nil {
		return "", nil, nil
	}
	key, err := pkKey(inst.Def.PrimaryKey, row)
	if err != nil {
		return "", nil, err
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()
	cached, ok := inst.cache[key]
	if !ok {
		return key, nil, nil
	}
	return key, cloneRow(cached), nil
}

func patchRow(cached, old, neu map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range cached {
		out[k] = v
	}
	for k, v := range old {
		if _, ok := out[k]; !ok {
			out[k] = v
		}
	}
	for k, v := range neu {
		out[k] = v
	}
	return out
}

func firstNonNil(a, b map[string]any) map[string]any {
	if a != nil {
		return a
	}
	return b
}

// Registry holds shape definitions registered on an Engine.
type Registry struct {
	mu   sync.Mutex
	defs map[string]Definition
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{defs: make(map[string]Definition)}
}

// Register adds a shape definition. Duplicate names error.
func (r *Registry) Register(def Definition) error {
	if def.Name == "" {
		return fmt.Errorf("shape: name is required")
	}
	if def.Bind == nil {
		return fmt.Errorf("shape: bind is required")
	}
	if def.Schema == "" {
		def.Schema = "public"
	}
	if def.Table == "" {
		def.Table = def.Name
	}
	if len(def.PrimaryKey) == 0 {
		def.PrimaryKey = []string{"id"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.defs[def.Name]; ok {
		return fmt.Errorf("shape: duplicate shape %q", def.Name)
	}
	r.defs[def.Name] = def
	return nil
}

// Get returns a definition by name.
func (r *Registry) Get(name string) (Definition, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.defs[name]
	return d, ok
}

// All returns a copy of registered definitions.
func (r *Registry) All() []Definition {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Definition, 0, len(r.defs))
	for _, d := range r.defs {
		out = append(out, d)
	}
	return out
}
