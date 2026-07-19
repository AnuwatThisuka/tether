package tether_test

import (
	"errors"
	"testing"

	"github.com/anuwatthisuka/tether"
)

func TestReject(t *testing.T) {
	t.Parallel()
	err := tether.Reject("forbidden")
	if !tether.IsReject(err) {
		t.Fatal("expected IsReject")
	}
	if tether.RejectReason(err) != "forbidden" {
		t.Fatalf("reason=%q", tether.RejectReason(err))
	}
	if !errors.Is(err, err) {
		t.Fatal("errors.Is self")
	}
}
