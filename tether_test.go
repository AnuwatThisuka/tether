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
	if errors.Is(err, tether.ErrNotImplemented) {
		t.Fatalf("New(\"\"): err = %v, should not be ErrNotImplemented", err)
	}
}

func TestNew_NotImplemented(t *testing.T) {
	t.Parallel()

	eng, err := tether.New("postgres://tether:tether@localhost:54321/tether")
	if eng != nil {
		t.Fatalf("New(...): engine = %v, want nil", eng)
	}
	if !errors.Is(err, tether.ErrNotImplemented) {
		t.Fatalf("New(...): err = %v, want ErrNotImplemented", err)
	}
}
