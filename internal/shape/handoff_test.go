package shape_test

import (
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/anuwatthisuka/tether/internal/shape"
	"github.com/anuwatthisuka/tether/internal/wal"
)

func TestLoadSnapshot_SkipsWALAtOrBeforeLSN(t *testing.T) {
	t.Parallel()

	def := shape.Definition{
		Name:  "tasks",
		Table: "tasks",
		Bind: func(any) (shape.Filter, error) {
			return shape.ParseWhere("org_id = ?", int64(1))
		},
	}
	inst, err := shape.NewInstance(def, nil)
	if err != nil {
		t.Fatal(err)
	}

	snapLSN := pglogrepl.LSN(100)
	if err := inst.LoadSnapshot(snapLSN, []map[string]any{
		{"id": 1, "org_id": int64(1), "title": "seed"},
	}); err != nil {
		t.Fatal(err)
	}

	// At snapshot LSN — ignored
	evs, err := inst.Apply(wal.Change{
		Table: "tasks", Op: wal.OpInsert, CommitLSN: snapLSN,
		New: map[string]any{"id": 2, "org_id": int64(1), "title": "dup"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("expected skip at snapshot lsn, got %#v", evs)
	}

	// After snapshot — applied
	evs, err = inst.Apply(wal.Change{
		Table: "tasks", Op: wal.OpInsert, CommitLSN: snapLSN + 1,
		RelationFingerprint: "fp",
		New:                 map[string]any{"id": 3, "org_id": int64(1), "title": "new"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Op != shape.EventInsert {
		t.Fatalf("expected insert after snapshot, got %#v", evs)
	}

	rows := inst.Materialized()
	if len(rows) != 2 {
		t.Fatalf("materialized=%d, want 2 (seed+new)", len(rows))
	}
}

func TestFilterSQLClause(t *testing.T) {
	t.Parallel()
	f, err := shape.ParseWhere("org_id = ? AND title IS NOT NULL", int64(9))
	if err != nil {
		t.Fatal(err)
	}
	clause, args, err := f.SQLClause()
	if err != nil {
		t.Fatal(err)
	}
	if clause != `"org_id" = $1 AND "title" IS NOT NULL` {
		t.Fatalf("clause=%q", clause)
	}
	if len(args) != 1 || args[0] != int64(9) {
		t.Fatalf("args=%v", args)
	}
}
