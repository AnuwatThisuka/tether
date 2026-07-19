package shape_test

import (
	"testing"

	"github.com/anuwatthisuka/tether/internal/shape"
)

func TestLogAfter(t *testing.T) {
	t.Parallel()
	l := shape.NewLog()
	_, _ = l.Append(shape.EventInsert, "t", map[string]any{"id": 1})
	_, _ = l.Append(shape.EventInsert, "t", map[string]any{"id": 2})
	evs, ok := l.After(1)
	if !ok || len(evs) != 1 || evs[0].Offset != 2 {
		t.Fatalf("After(1)=%v ok=%v", evs, ok)
	}
	_, ok = l.After(0)
	if !ok {
		t.Fatal("After(0) should be ok")
	}
}
