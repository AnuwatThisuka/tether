// Package transport serves the WebSocket sync endpoint, per-client buffered
// fan-out, and offset-based resume. A full buffer disconnects that client;
// the WAL reader must never block on a socket write (AGENTS.md Invariant 7).
package transport
