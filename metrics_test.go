package tether_test

import (
	"sync"
	"testing"

	"github.com/anuwatthisuka/tether"
)

type recordingMetrics struct {
	mu      sync.Mutex
	lag     []int64
	clients []int
	disc    []string
	offsets []struct {
		client, shape string
		offset        int64
	}
}

func (m *recordingMetrics) ReplicationLagBytes(n int64) {
	m.mu.Lock()
	m.lag = append(m.lag, n)
	m.mu.Unlock()
}

func (m *recordingMetrics) ClientOffset(clientID, shape string, offset int64) {
	m.mu.Lock()
	m.offsets = append(m.offsets, struct {
		client, shape string
		offset        int64
	}{clientID, shape, offset})
	m.mu.Unlock()
}

func (m *recordingMetrics) ClientsConnected(n int) {
	m.mu.Lock()
	m.clients = append(m.clients, n)
	m.mu.Unlock()
}

func (m *recordingMetrics) ClientDisconnected(reason string) {
	m.mu.Lock()
	m.disc = append(m.disc, reason)
	m.mu.Unlock()
}

func TestNopMetrics_NoPanic(t *testing.T) {
	var n tether.NopMetrics
	n.ReplicationLagBytes(1)
	n.ClientOffset("c1", "tasks", 2)
	n.ClientsConnected(3)
	n.ClientDisconnected("slow_client")
}

func TestWithMetrics_Accepted(t *testing.T) {
	rec := &recordingMetrics{}
	eng, err := tether.New("postgres://localhost/db", tether.WithMetrics(rec))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng2, err := tether.New("postgres://localhost/db", tether.WithMetrics(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
}
