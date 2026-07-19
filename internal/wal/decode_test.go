package wal

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestDecodeTuple_OmitsUnchangedToast(t *testing.T) {
	t.Parallel()

	rel := &relationMeta{
		Namespace: "public",
		Relation:  "items",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "blob", DataType: pgtype.TextOID},
			{Name: "name", DataType: pgtype.TextOID},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},
			{DataType: 'u'}, // unchanged TOAST — must not appear
			{DataType: 't', Data: []byte("widget")},
		},
	}

	got, err := decodeTuple(pgtype.NewMap(), rel, tuple)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["blob"]; ok {
		t.Fatalf("blob present in decoded map: %#v (TOAST unchanged must be omitted)", got)
	}
	if got["name"] != "widget" {
		t.Fatalf("name = %#v, want widget", got["name"])
	}
}

func TestFingerprintRelation_ChangesOnAddColumn(t *testing.T) {
	t.Parallel()

	a := &pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "items",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
		},
	}
	b := &pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "items",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "extra", DataType: pgtype.TextOID},
		},
	}
	if fingerprintRelation(a) == fingerprintRelation(b) {
		t.Fatal("fingerprints should differ after ADD COLUMN")
	}
}
