package tether

// Metrics receives operational observations from the engine.
// Implementations must be safe for concurrent use and must not block:
// a slow Metrics sink must not stall WAL ingest or client fan-out
// (Invariant 7).
//
// Hosts adapt this to Prometheus, OpenTelemetry, StatsD, etc. tether does
// not depend on those libraries.
type Metrics interface {
	// ReplicationLagBytes is how far the replication slot lags the WAL tip
	// (pg_wal_lsn_diff(current, restart_lsn)), in bytes. This is also the
	// "slot pressure" signal referred to as slot size in the README.
	ReplicationLagBytes(n int64)

	// ClientOffset is the last shape-log offset successfully buffered for
	// outbound send to a connected client (not a client-applied ack).
	ClientOffset(clientID, shape string, offset int64)

	// ClientsConnected is the number of live WebSocket sessions.
	ClientsConnected(n int)
}

// NopMetrics discards all observations.
type NopMetrics struct{}

// ReplicationLagBytes implements Metrics.
func (NopMetrics) ReplicationLagBytes(int64) {}

// ClientOffset implements Metrics.
func (NopMetrics) ClientOffset(string, string, int64) {}

// ClientsConnected implements Metrics.
func (NopMetrics) ClientsConnected(int) {}

// WithMetrics sets the metrics sink. Default is NopMetrics.
func WithMetrics(m Metrics) Option {
	return func(o *options) {
		if m == nil {
			o.metrics = NopMetrics{}
			return
		}
		o.metrics = m
	}
}
