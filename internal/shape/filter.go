package shape

import (
	"fmt"
	"strings"
	"unicode"
)

// Filter is a server-built row predicate. Clients never supply Filter text
// (AGENTS.md Invariant 2).
type Filter struct {
	preds []predicate
}

type predicate struct {
	column string
	op     predOp
	value  any // ignored for null checks
}

type predOp int

const (
	opEq predOp = iota
	opNe
	opIsNull
	opIsNotNull
)

// ParseWhere builds a Filter from a restricted WHERE clause.
// Supported forms (AND-combined):
//
//	col = ?
//	col != ?
//	col IS NULL
//	col IS NOT NULL
func ParseWhere(clause string, args ...any) (Filter, error) {
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return Filter{}, fmt.Errorf("shape: empty where clause")
	}

	parts := splitAND(clause)
	argIdx := 0
	preds := make([]predicate, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		upper := strings.ToUpper(part)

		switch {
		case strings.HasSuffix(upper, " IS NOT NULL"):
			col := strings.TrimSpace(part[:len(part)-len(" IS NOT NULL")])
			if err := validateIdent(col); err != nil {
				return Filter{}, err
			}
			preds = append(preds, predicate{column: col, op: opIsNotNull})
		case strings.HasSuffix(upper, " IS NULL"):
			col := strings.TrimSpace(part[:len(part)-len(" IS NULL")])
			if err := validateIdent(col); err != nil {
				return Filter{}, err
			}
			preds = append(preds, predicate{column: col, op: opIsNull})
		default:
			col, op, err := parseComparison(part)
			if err != nil {
				return Filter{}, err
			}
			if argIdx >= len(args) {
				return Filter{}, fmt.Errorf("shape: not enough args for where clause")
			}
			preds = append(preds, predicate{column: col, op: op, value: args[argIdx]})
			argIdx++
		}
	}
	if argIdx != len(args) {
		return Filter{}, fmt.Errorf("shape: too many args for where clause")
	}
	return Filter{preds: preds}, nil
}

func parseComparison(part string) (string, predOp, error) {
	for _, cand := range []struct {
		sep string
		op  predOp
	}{
		{"!=", opNe},
		{"<>", opNe},
		{"=", opEq},
	} {
		i := strings.Index(part, cand.sep)
		if i < 0 {
			continue
		}
		col := strings.TrimSpace(part[:i])
		rhs := strings.TrimSpace(part[i+len(cand.sep):])
		if rhs != "?" {
			return "", 0, fmt.Errorf("shape: unsupported where fragment %q (only ? placeholders)", part)
		}
		if err := validateIdent(col); err != nil {
			return "", 0, err
		}
		return col, cand.op, nil
	}
	return "", 0, fmt.Errorf("shape: unsupported where fragment %q", part)
}

func splitAND(clause string) []string {
	upper := strings.ToUpper(clause)
	var parts []string
	start := 0
	for {
		i := strings.Index(upper[start:], " AND ")
		if i < 0 {
			parts = append(parts, clause[start:])
			return parts
		}
		parts = append(parts, clause[start:start+i])
		start = start + i + len(" AND ")
	}
}

func validateIdent(name string) error {
	if name == "" {
		return fmt.Errorf("shape: empty column name")
	}
	for i, r := range name {
		ok := unicode.IsLetter(r) || r == '_' || (i > 0 && unicode.IsDigit(r))
		if !ok {
			return fmt.Errorf("shape: invalid column identifier %q", name)
		}
	}
	return nil
}

// Match reports whether row satisfies the filter.
// Missing columns are treated as non-matching for equality checks (not as NULL),
// except IS NULL which matches missing or explicit nil.
func (f Filter) Match(row map[string]any) (bool, error) {
	if row == nil {
		return false, nil
	}
	for _, p := range f.preds {
		ok, err := p.match(row)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (p predicate) match(row map[string]any) (bool, error) {
	val, present := row[p.column]
	switch p.op {
	case opIsNull:
		return !present || val == nil, nil
	case opIsNotNull:
		return present && val != nil, nil
	case opEq:
		if !present {
			return false, nil
		}
		return valuesEqual(val, p.value), nil
	case opNe:
		if !present {
			return false, nil
		}
		return !valuesEqual(val, p.value), nil
	default:
		return false, fmt.Errorf("shape: unknown predicate op")
	}
}

func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case int:
		return numEqual(int64(av), b)
	case int32:
		return numEqual(int64(av), b)
	case int64:
		return numEqual(av, b)
	case float64:
		return numEqualFloat(av, b)
	case string:
		return fmt.Sprint(b) == av
	case []byte:
		return fmt.Sprint(b) == string(av)
	default:
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}

func numEqual(a int64, b any) bool {
	switch bv := b.(type) {
	case int:
		return a == int64(bv)
	case int32:
		return a == int64(bv)
	case int64:
		return a == bv
	case float64:
		return float64(a) == bv
	case string:
		return fmt.Sprintf("%d", a) == bv
	default:
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}

func numEqualFloat(a float64, b any) bool {
	switch bv := b.(type) {
	case float64:
		return a == bv
	case int:
		return a == float64(bv)
	case int64:
		return a == float64(bv)
	default:
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}

// SQLClause renders the filter as a SQL boolean expression with $n placeholders
// for use on a normal (non-replication) connection.
func (f Filter) SQLClause() (string, []any, error) {
	if len(f.preds) == 0 {
		return "", nil, fmt.Errorf("shape: empty filter")
	}
	parts := make([]string, 0, len(f.preds))
	args := make([]any, 0, len(f.preds))
	argN := 1
	for _, p := range f.preds {
		if err := validateIdent(p.column); err != nil {
			return "", nil, err
		}
		col := quoteIdent(p.column)
		switch p.op {
		case opEq:
			parts = append(parts, fmt.Sprintf("%s = $%d", col, argN))
			args = append(args, p.value)
			argN++
		case opNe:
			parts = append(parts, fmt.Sprintf("%s <> $%d", col, argN))
			args = append(args, p.value)
			argN++
		case opIsNull:
			parts = append(parts, col+" IS NULL")
		case opIsNotNull:
			parts = append(parts, col+" IS NOT NULL")
		default:
			return "", nil, fmt.Errorf("shape: unknown predicate op")
		}
	}
	return strings.Join(parts, " AND "), args, nil
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
