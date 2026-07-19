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

// Option configures an Engine.
type Option func(*options)

func shapeDef(name string, bind func(Claims) Filter, opts ...ShapeOption) shape.Definition {
	so := shapeOptions{schema: "public", table: name, primaryKey: []string{"id"}}
	for _, opt := range opts {
		if opt != nil {
			opt(&so)
		}
	}
	return shape.Definition{
		Name:       name,
		Schema:     so.schema,
		Table:      so.table,
		PrimaryKey: so.primaryKey,
		Bind: func(claims any) (shape.Filter, error) {
			if bind == nil {
				return shape.Filter{}, fmt.Errorf("tether: nil bind")
			}
			return bind(claims), nil
		},
	}
}
