package wal

import (
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

type changeOp string

const (
	opInsert changeOp = "insert"
	opUpdate changeOp = "update"
	opDelete changeOp = "delete"
)

// Change is one row mutation decoded from pgoutput, ready to persist.
type Change struct {
	Schema              string
	Table               string
	Op                  changeOp
	RelationFingerprint string
	// Old/New omit columns that were not present in the WAL message
	// (partial UPDATE / TOAST unchanged-marker). Absent ≠ JSON null.
	Old map[string]any
	New map[string]any
}

type relationMeta struct {
	Namespace   string
	Relation    string
	Fingerprint string
	Columns     []*pglogrepl.RelationMessageColumn
}

func fingerprintRelation(rel *pglogrepl.RelationMessage) string {
	parts := make([]string, 0, len(rel.Columns)*3+2)
	parts = append(parts, rel.Namespace, rel.RelationName)
	for _, col := range rel.Columns {
		parts = append(
			parts,
			col.Name,
			fmt.Sprintf("%d", col.DataType),
			fmt.Sprintf("%d", col.TypeModifier),
			fmt.Sprintf("%d", col.Flags),
		)
	}
	return strings.Join(parts, "|")
}

func decodeTuple(
	typeMap *pgtype.Map,
	rel *relationMeta,
	tuple *pglogrepl.TupleData,
) (map[string]any, error) {
	if tuple == nil {
		return nil, nil
	}
	out := make(map[string]any, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			return nil, fmt.Errorf("wal: tuple column index %d out of range for relation %s.%s",
				idx, rel.Namespace, rel.Relation)
		}
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			out[colName] = nil
		case 'u': // unchanged TOAST — not present
			continue
		case 't', 'b':
			val, err := decodeColumnValue(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("wal: decode column %q: %w", colName, err)
			}
			out[colName] = val
		default:
			return nil, fmt.Errorf("wal: unknown tuple data type %q for column %q", col.DataType, colName)
		}
	}
	return out, nil
}

func decodeColumnValue(typeMap *pgtype.Map, data []byte, dataTypeOID uint32) (any, error) {
	if typeMap == nil {
		return string(data), nil
	}
	dt, ok := typeMap.TypeForOID(dataTypeOID)
	if !ok {
		return string(data), nil
	}
	val, err := dt.Codec.DecodeValue(typeMap, dataTypeOID, pgtype.TextFormatCode, data)
	if err != nil {
		return string(data), nil
	}
	return val, nil
}
