package shape_test

import (
	"testing"

	"github.com/anuwatthisuka/tether/internal/shape"
	"github.com/anuwatthisuka/tether/internal/wal"
)

func TestParseWhere_rejectsInjection(t *testing.T) {
	t.Parallel()
	_, err := shape.ParseWhere("org_id = ?; DROP TABLE x", 1)
	if err == nil {
		t.Fatal("expected error for unsupported clause")
	}
}

func TestFilterMatch(t *testing.T) {
	t.Parallel()
	f, err := shape.ParseWhere("org_id = ? AND deleted_at IS NULL", int64(7))
	if err != nil {
		t.Fatal(err)
	}
	ok, err := f.Match(map[string]any{"org_id": int64(7), "deleted_at": nil})
	if err != nil || !ok {
		t.Fatalf("want match, ok=%v err=%v", ok, err)
	}
	ok, err = f.Match(map[string]any{"org_id": int64(8)})
	if err != nil || ok {
		t.Fatalf("want no match for other org, ok=%v err=%v", ok, err)
	}
}

func TestApply_UpdateCrossesBoundary(t *testing.T) {
	t.Parallel()

	def := shape.Definition{
		Name:       "tasks",
		Table:      "tasks",
		PrimaryKey: []string{"id"},
		Bind: func(claims any) (shape.Filter, error) {
			org := claims.(int64)
			return shape.ParseWhere("org_id = ?", org)
		},
	}
	inst, err := shape.NewInstance(def, int64(1))
	if err != nil {
		t.Fatal(err)
	}

	_, err = inst.Apply(wal.Change{
		Schema:              "public",
		Table:               "tasks",
		Op:                  wal.OpInsert,
		RelationFingerprint: "fp1",
		New:                 map[string]any{"id": 10, "org_id": int64(1), "title": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}

	evs, err := inst.Apply(wal.Change{
		Schema:              "public",
		Table:               "tasks",
		Op:                  wal.OpUpdate,
		RelationFingerprint: "fp1",
		Old:                 map[string]any{"id": 10, "org_id": int64(1)},
		New:                 map[string]any{"org_id": int64(2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Op != shape.EventDelete {
		t.Fatalf("leave shape: got %#v, want single delete", evs)
	}

	// Enter org 1 from org 2
	inst2, err := shape.NewInstance(def, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	evs, err = inst2.Apply(wal.Change{
		Schema:              "public",
		Table:               "tasks",
		Op:                  wal.OpUpdate,
		RelationFingerprint: "fp1",
		Old:                 map[string]any{"id": 11, "org_id": int64(2), "title": "b"},
		New:                 map[string]any{"id": 11, "org_id": int64(1), "title": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Op != shape.EventInsert {
		t.Fatalf("enter shape: got %#v, want single insert", evs)
	}
}

func TestApply_SchemaDriftHalts(t *testing.T) {
	t.Parallel()
	def := shape.Definition{
		Name:  "tasks",
		Table: "tasks",
		Bind: func(any) (shape.Filter, error) {
			return shape.ParseWhere("org_id = ?", 1)
		},
	}
	inst, err := shape.NewInstance(def, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = inst.Apply(wal.Change{
		Table: "tasks", Op: wal.OpInsert, RelationFingerprint: "a",
		New: map[string]any{"id": 1, "org_id": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = inst.Apply(wal.Change{
		Table: "tasks", Op: wal.OpInsert, RelationFingerprint: "b",
		New: map[string]any{"id": 2, "org_id": 1},
	})
	if err == nil {
		t.Fatal("expected schema drift")
	}
}
