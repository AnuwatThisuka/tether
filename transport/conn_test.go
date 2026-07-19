package transport_test

import (
	"testing"

	"github.com/anuwatthisuka/tether/transport"
)

func TestEnqueue_FullBuffer(t *testing.T) {
	t.Parallel()
	c := transport.NewConn("t1", nil, nil, 2)
	if !c.Enqueue([]byte("a")) || !c.Enqueue([]byte("b")) {
		t.Fatal("expected first two enqueues to succeed")
	}
	if c.Enqueue([]byte("c")) {
		t.Fatal("expected buffer-full enqueue to fail (Invariant 7)")
	}
}
