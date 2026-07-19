package tether_test

import (
	"testing"
	"time"

	"github.com/anuwatthisuka/tether"
)

func TestOptions_Defaults(t *testing.T) {
	eng, err := tether.New("postgres://localhost/db")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Zero disables; non-zero options must stick.
	eng2, err := tether.New(
		"postgres://localhost/db",
		tether.MaxSlotLag(0),
		tether.MaxClientIdle(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()

	eng3, err := tether.New(
		"postgres://localhost/db",
		tether.MaxSlotLag(1024),
		tether.MaxClientIdle(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer eng3.Close()
}
