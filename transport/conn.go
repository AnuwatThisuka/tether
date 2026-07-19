package transport

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/anuwatthisuka/tether/internal/proto"
)

// AuthFunc extracts Claims from the HTTP upgrade request.
// Returning an error rejects the handshake with 401.
type AuthFunc func(*http.Request) (any, error)

// Conn is one WebSocket client with a non-blocking outbound buffer.
type Conn struct {
	id     string
	ws     *websocket.Conn
	claims any
	send   chan []byte
	closed atomic.Bool
	once   sync.Once
}

// NewConn wraps an upgraded websocket with a send buffer of size bufSize.
func NewConn(id string, ws *websocket.Conn, claims any, bufSize int) *Conn {
	if bufSize <= 0 {
		bufSize = 64
	}
	return &Conn{
		id:     id,
		ws:     ws,
		claims: claims,
		send:   make(chan []byte, bufSize),
	}
}

// ID returns the connection id.
func (c *Conn) ID() string { return c.id }

// Claims returns the auth claims from the handshake.
func (c *Conn) Claims() any { return c.claims }

// Enqueue tries to buffer a message without blocking.
// Returns false if the buffer is full or the conn is closed (Invariant 7).
func (c *Conn) Enqueue(msg []byte) bool {
	if c.closed.Load() {
		return false
	}
	select {
	case c.send <- msg:
		return true
	default:
		return false
	}
}

// WritePump drains the send buffer to the socket until ctx ends or close.
func (c *Conn) WritePump(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-c.send:
			if !ok {
				return nil
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.ws.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return fmt.Errorf("transport: write: %w", err)
			}
		}
	}
}

// ReadMessage reads the next client text frame.
func (c *Conn) ReadMessage(ctx context.Context) ([]byte, error) {
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("transport: read: %w", err)
	}
	return data, nil
}

// SendBye best-effort writes a bye frame then closes the connection.
func (c *Conn) SendBye(ctx context.Context, reason string) {
	c.shutdown(ctx, reason, true)
}

// Close closes the websocket without a bye payload.
func (c *Conn) Close(reason string) {
	c.shutdown(context.Background(), reason, false)
}

func (c *Conn) shutdown(ctx context.Context, reason string, bye bool) {
	c.once.Do(func() {
		c.closed.Store(true)
		if bye {
			msg, err := proto.Marshal(proto.Bye{Type: "bye", Reason: reason})
			if err == nil {
				wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				_ = c.ws.Write(wctx, websocket.MessageText, msg)
				cancel()
			}
			_ = c.ws.Close(websocket.StatusPolicyViolation, reason)
		} else {
			_ = c.ws.Close(websocket.StatusNormalClosure, reason)
		}
		close(c.send)
	})
}

// Upgrade performs the WebSocket handshake.
func Upgrade(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // host terminates TLS; embedders gate origin
	})
	if err != nil {
		return nil, fmt.Errorf("transport: accept: %w", err)
	}
	return ws, nil
}

// Hub tracks live connections for WAL fan-out.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Conn
	nextID  atomic.Uint64
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[string]*Conn)}
}

// Add registers a connection and returns it.
func (h *Hub) Add(ws *websocket.Conn, claims any, bufSize int) *Conn {
	id := fmt.Sprintf("c%d", h.nextID.Add(1))
	c := NewConn(id, ws, claims, bufSize)
	h.mu.Lock()
	h.clients[id] = c
	h.mu.Unlock()
	return c
}

// Remove unregisters a connection.
func (h *Hub) Remove(id string) {
	h.mu.Lock()
	delete(h.clients, id)
	h.mu.Unlock()
}

// Snapshot returns a copy of current connections.
func (h *Hub) Snapshot() []*Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*Conn, 0, len(h.clients))
	for _, c := range h.clients {
		out = append(out, c)
	}
	return out
}
