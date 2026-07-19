package tether

import (
	"fmt"

	"github.com/anuwatthisuka/tether/internal/shape"
)

// Claims is host-defined auth context passed to Shape binders.
// Typically a struct; tether never builds Claims from client filter input.
type Claims any

// Filter is an opaque server-side row predicate built by Where.
type Filter = shape.Filter

// Where builds a Filter from a restricted WHERE clause and args.
// Predicates must come only from Shape binders over Claims (Invariant 2).
// Invalid clauses panic — that is a programmer error at registration time.
func Where(clause string, args ...any) Filter {
	f, err := shape.ParseWhere(clause, args...)
	if err != nil {
		panic(err)
	}
	return f
}

// ShapeOption configures a registered shape.
type ShapeOption func(*shapeOptions)

type shapeOptions struct {
	schema     string
	table      string
	primaryKey []string
}

// PrimaryKey sets the primary key columns used for membership caching.
// Defaults to []string{"id"}.
func PrimaryKey(cols ...string) ShapeOption {
	return func(o *shapeOptions) {
		o.primaryKey = append([]string(nil), cols...)
	}
}

// Table overrides the Postgres table (default schema public, name = shape name).
func Table(schema, name string) ShapeOption {
	return func(o *shapeOptions) {
		if schema != "" {
			o.schema = schema
		}
		if name != "" {
			o.table = name
		}
	}
}

// Engine is the sync engine handle returned by New.
type Engine struct {
	pgURL string
	opts  options
	reg   *shape.Registry
}

type options struct{}

// Option configures an Engine.
type Option func(*options)

// New constructs an Engine. Phase 2 supports Shape registration; Run/Handler
// remain unimplemented.
func New(pgURL string, opts ...Option) (*Engine, error) {
	if pgURL == "" {
		return nil, fmt.Errorf("tether: pgURL is required")
	}
	o := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Engine{
		pgURL: pgURL,
		opts:  o,
		reg:   shape.NewRegistry(),
	}, nil
}

// Shape registers a named shape whose filter is resolved from Claims only.
func (e *Engine) Shape(name string, bind func(Claims) Filter, opts ...ShapeOption) error {
	if e == nil {
		return fmt.Errorf("tether: nil engine")
	}
	if name == "" {
		return fmt.Errorf("tether: shape name is required")
	}
	if bind == nil {
		return fmt.Errorf("tether: shape bind is required")
	}
	so := shapeOptions{schema: "public", table: name, primaryKey: []string{"id"}}
	for _, opt := range opts {
		if opt != nil {
			opt(&so)
		}
	}
	def := shape.Definition{
		Name:       name,
		Schema:     so.schema,
		Table:      so.table,
		PrimaryKey: so.primaryKey,
		Bind: func(claims any) (shape.Filter, error) {
			// Invariant 2: filter originates only here from host Claims.
			return bind(claims), nil
		},
	}
	return e.reg.Register(def)
}

// Registry exposes the shape registry for internal wiring and tests.
func (e *Engine) Registry() *shape.Registry {
	if e == nil {
		return nil
	}
	return e.reg
}
