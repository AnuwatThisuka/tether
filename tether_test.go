package tether_test

import (
	"errors"
	"testing"

	"github.com/anuwatthisuka/tether"
)

func TestNew_EmptyURL(t *testing.T) {
	t.Parallel()

	eng, err := tether.New("")
	if eng != nil {
		t.Fatalf("New(\"\"): engine = %v, want nil", eng)
	}
	if err == nil {
		t.Fatal("New(\"\"): err = nil, want error")
	}
}

func TestNew_AllowsShapeRegistration(t *testing.T) {
	t.Parallel()

	eng, err := tether.New("postgres://tether:tether@localhost:54321/tether")
	if err != nil {
		t.Fatal(err)
	}
	err = eng.Shape("tasks", func(c tether.Claims) tether.Filter {
		org := c.(string)
		return tether.Where("org_id = ?", org)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := eng.Registry().Get("tasks"); !ok {
		t.Fatal("shape not registered")
	}
}

func TestWhere_InvalidPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = tether.Where("org_id = 1 OR true")
}

func TestErrNotImplementedStillDefined(t *testing.T) {
	t.Parallel()
	if !errors.Is(tether.ErrNotImplemented, tether.ErrNotImplemented) {
		t.Fatal("sentinel missing")
	}
}
