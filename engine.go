package tether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuwatthisuka/tether/internal/mutate"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/internal/shape"
	"github.com/anuwatthisuka/tether/internal/snapshot"
	"github.com/anuwatthisuka/tether/internal/wal"
	"github.com/anuwatthisuka/tether/transport"
)

// AuthFunc resolves Claims from the WebSocket upgrade request.
type AuthFunc func(*http.Request) (Claims, error)

// ClaimsKeyFunc derives a stable key for resume streams from Claims.
type ClaimsKeyFunc func(Claims) string

type options struct {
	auth          AuthFunc
	claimsKey     ClaimsKeyFunc
	clientBuffer  int
	slotName      string
	publication   string
	logger        *slog.Logger
	onMutation    MutationHandler
	maxSlotLag    int64
	maxClientIdle time.Duration
	metrics       Metrics
}

// MutationHandler applies a client mutation inside a host-controlled transaction.
type MutationHandler func(ctx context.Context, tx pgx.Tx, m Mutation) error

// WithAuth sets the handshake auth callback. If nil, Claims are nil.
func WithAuth(fn AuthFunc) Option {
	return func(o *options) { o.auth = fn }
}

// WithClaimsKey sets how Claims map to a resume-stream key.
// Default is fmt.Sprint(claims).
func WithClaimsKey(fn ClaimsKeyFunc) Option {
	return func(o *options) { o.claimsKey = fn }
}

// WithClientBuffer sets the per-client outbound buffer size (default 64).
// When the buffer is full, that client is disconnected with bye reason
// slow_client so the WAL reader and other clients are not stalled
// (Invariant 7). Prefer a small buffer and let clients resume by offset
// over a huge buffer that hides a stuck consumer.
func WithClientBuffer(n int) Option {
	return func(o *options) { o.clientBuffer = n }
}

// WithSlotName overrides the replication slot name (default tether_slot).
func WithSlotName(name string) Option {
	return func(o *options) { o.slotName = name }
}

// WithPublication overrides the publication name (default tether_pub).
func WithPublication(name string) Option {
	return func(o *options) { o.publication = name }
}

// MaxSlotLag sets the maximum replication slot lag in bytes before the slot
// is dropped and clients are forced to re-snapshot (Invariant 6).
// Zero disables the guard. Default is 2 GiB.
func MaxSlotLag(n int64) Option {
	return func(o *options) { o.maxSlotLag = n }
}

// MaxClientIdle sets how long a connected client may be idle before it is
// disconnected. Zero disables the sweep. Default is 24h.
func MaxClientIdle(d time.Duration) Option {
	return func(o *options) { o.maxClientIdle = d }
}

// WithLogger sets the slog logger. Default is slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// Engine is the sync engine handle returned by New.
type Engine struct {
	pgURL string
	opts  options
	reg   *shape.Registry
	hub   *transport.Hub
	log   *slog.Logger

	mu       sync.Mutex
	pool     *pgxpool.Pool
	streams  map[string]*shape.Instance // shape\x00claimsKey
	sessions map[string]*session

	// slotLagOverride, when set (tests), replaces wal.SlotLagBytes.
	slotLagMu       sync.RWMutex
	slotLagOverride func(context.Context) (int64, error)
}

type session struct {
	conn   *transport.Conn
	claims Claims

	mu         sync.Mutex
	resume     map[string]int64
	shapes     map[string]struct{}
	lastActive time.Time
}

func (s *session) touch() {
	s.mu.Lock()
	s.lastActive = time.Now()
	s.mu.Unlock()
}

func (s *session) isIdle(cutoff time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActive.Before(cutoff)
}

func (s *session) setResume(r map[string]int64) {
	s.mu.Lock()
	s.resume = r
	s.mu.Unlock()
}

func (s *session) resumeAt(shapeName string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resume[shapeName]
}

func (s *session) addShape(name string) {
	s.mu.Lock()
	s.shapes[name] = struct{}{}
	s.mu.Unlock()
}

func (s *session) hasShape(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.shapes[name]
	return ok
}

func (s *session) clearMembership() {
	s.mu.Lock()
	s.shapes = make(map[string]struct{})
	s.resume = make(map[string]int64)
	s.mu.Unlock()
}

// New constructs an Engine.
func New(pgURL string, opts ...Option) (*Engine, error) {
	if pgURL == "" {
		return nil, fmt.Errorf("tether: pgURL is required")
	}
	o := options{
		clientBuffer:  64,
		slotName:      "tether_slot",
		publication:   "tether_pub",
		logger:        slog.Default(),
		claimsKey:     func(c Claims) string { return fmt.Sprint(c) },
		maxSlotLag:    2 * 1024 * 1024 * 1024, // 2 GiB
		maxClientIdle: 24 * time.Hour,
		metrics:       NopMetrics{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Engine{
		pgURL:    pgURL,
		opts:     o,
		reg:      shape.NewRegistry(),
		hub:      transport.NewHub(),
		log:      o.logger,
		streams:  make(map[string]*shape.Instance),
		sessions: make(map[string]*session),
	}, nil
}

// Shape registers a named shape whose filter is resolved from Claims only.
func (e *Engine) Shape(name string, bind func(Claims) Filter, opts ...ShapeOption) error {
	if e == nil {
		return fmt.Errorf("tether: nil engine")
	}
	return e.reg.Register(shapeDef(name, bind, opts...))
}

// OnMutation registers the write handler. Must be set before clients send mutations.
func (e *Engine) OnMutation(fn MutationHandler) {
	if e == nil {
		return
	}
	e.opts.onMutation = fn
}

// Registry exposes the shape registry for tests.
func (e *Engine) Registry() *shape.Registry {
	if e == nil {
		return nil
	}
	return e.reg
}

// Handler returns the WebSocket HTTP handler.
func (e *Engine) Handler() http.Handler {
	return http.HandlerFunc(e.serveWS)
}

func (e *Engine) serveWS(w http.ResponseWriter, r *http.Request) {
	var claims Claims
	if e.opts.auth != nil {
		c, err := e.opts.auth(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims = c
	}

	ws, err := transport.Upgrade(w, r)
	if err != nil {
		return
	}
	conn := e.hub.Add(ws, claims, e.opts.clientBuffer)
	defer e.hub.Remove(conn.ID())

	ctx := r.Context()
	sess := &session{
		conn:       conn,
		claims:     claims,
		resume:     map[string]int64{},
		shapes:     map[string]struct{}{},
		lastActive: time.Now(),
	}
	e.mu.Lock()
	e.sessions[conn.ID()] = sess
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.sessions, conn.ID())
		e.mu.Unlock()
		conn.Close(proto.ReasonShutdown)
	}()

	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = conn.WritePump(writeCtx) }()

	for {
		data, err := conn.ReadMessage(ctx)
		if err != nil {
			cancel()
			return
		}
		sess.touch()
		typ, err := proto.UnmarshalType(data)
		if err != nil {
			e.sendErr(conn, proto.CodeBadProtocol, err.Error(), "")
			continue
		}
		switch typ {
		case "hello":
			var msg proto.Hello
			if err := proto.Decode(data, &msg); err != nil {
				e.sendErr(conn, proto.CodeBadProtocol, err.Error(), "")
				continue
			}
			if msg.Protocol != 0 && msg.Protocol != proto.ProtocolVersion {
				e.sendErr(conn, proto.CodeBadProtocol, "unsupported protocol", "")
				continue
			}
			if msg.Resume != nil {
				sess.setResume(msg.Resume)
			}
		case "subscribe":
			var msg proto.Subscribe
			if err := proto.Decode(data, &msg); err != nil {
				e.sendErr(conn, proto.CodeBadProtocol, err.Error(), "")
				continue
			}
			if err := e.handleSubscribe(ctx, sess, msg.Shapes); err != nil {
				e.sendErr(conn, proto.CodeHalted, err.Error(), "")
			}
		case "mutation":
			var msg proto.Mutation
			if err := proto.Decode(data, &msg); err != nil {
				e.sendErr(conn, proto.CodeBadProtocol, err.Error(), "")
				continue
			}
			e.handleMutation(ctx, sess, msg)
		default:
			e.sendErr(conn, proto.CodeBadProtocol, "unknown message type "+typ, "")
		}
	}
}

func (e *Engine) handleMutation(ctx context.Context, sess *session, msg proto.Mutation) {
	reply := func(v any) {
		b, err := proto.Marshal(v)
		if err != nil {
			return
		}
		_ = sess.conn.Enqueue(b)
	}

	if e.opts.onMutation == nil {
		e.sendErr(sess.conn, proto.CodeNoHandler, "OnMutation not configured", "")
		return
	}
	if err := e.ensurePool(ctx); err != nil {
		e.sendErr(sess.conn, proto.CodeHalted, err.Error(), "")
		return
	}
	if err := mutate.EnsureSchema(ctx, e.pool); err != nil {
		e.sendErr(sess.conn, proto.CodeHalted, err.Error(), "")
		return
	}

	m, err := newMutation(msg.Op, msg.Key, sess.claims, msg.Args)
	if err != nil {
		reply(proto.MutationReject{Type: "mutation_reject", ID: msg.ID, Reason: err.Error()})
		return
	}

	res, err := mutate.Apply(
		ctx, e.pool, m.Key, m.Op, m.Claims, m.Args(),
		func(ctx context.Context, tx pgx.Tx, op, key string, claims any, args map[string]any) error {
			return e.opts.onMutation(ctx, tx, Mutation{
				Op: op, Key: key, Claims: claims, args: args,
			})
		},
		IsReject,
		RejectReason,
	)
	if err != nil {
		e.sendErr(sess.conn, proto.CodeHalted, err.Error(), "")
		return
	}
	if res.Rejected {
		reply(proto.MutationReject{Type: "mutation_reject", ID: msg.ID, Reason: res.Reason})
		return
	}
	reply(proto.MutationOK{Type: "mutation_ok", ID: msg.ID, Duplicate: res.Duplicate})
}

func (e *Engine) handleSubscribe(ctx context.Context, sess *session, names []string) error {
	if err := e.ensurePool(ctx); err != nil {
		return err
	}
	for _, name := range names {
		def, ok := e.reg.Get(name)
		if !ok {
			e.sendErr(sess.conn, proto.CodeUnknownShape, "unknown shape", name)
			continue
		}
		if err := e.subscribeShape(ctx, sess, def); err != nil {
			return err
		}
		sess.addShape(name)
	}
	return nil
}

func (e *Engine) subscribeShape(ctx context.Context, sess *session, def shape.Definition) error {
	key := streamKey(def.Name, e.opts.claimsKey(sess.claims))
	resumeAt := sess.resumeAt(def.Name)

	e.mu.Lock()
	inst, exists := e.streams[key]
	e.mu.Unlock()

	if exists && resumeAt > 0 {
		events, ok := inst.Log.After(resumeAt)
		if ok {
			for _, ev := range events {
				if !e.enqueueChange(sess.conn, def.Name, ev) {
					e.disconnect(ctx, sess.conn, proto.ReasonSlowClient)
					return fmt.Errorf("tether: slow client")
				}
			}
			return nil
		}
		e.sendErr(sess.conn, proto.CodeMustResnapshot, "resume offset not available", def.Name)
	}

	if exists && resumeAt == 0 {
		msg, err := proto.Marshal(proto.Snapshot{
			Type:   "snapshot",
			Shape:  def.Name,
			LSN:    "0/0",
			Offset: inst.Log.LastOffset(),
			Rows:   inst.Materialized(),
		})
		if err != nil {
			return err
		}
		if !sess.conn.Enqueue(msg) {
			e.disconnect(ctx, sess.conn, proto.ReasonSlowClient)
			return fmt.Errorf("tether: slow client")
		}
		e.noteClientOffset(sess.conn.ID(), def.Name, inst.Log.LastOffset())
		return nil
	}

	inst, err := shape.NewInstance(def, sess.claims)
	if err != nil {
		return err
	}
	snap, err := snapshot.Take(ctx, e.pool, snapshot.Request{
		Schema: def.Schema,
		Table:  def.Table,
		Filter: inst.Filter,
	})
	if err != nil {
		return err
	}
	if err := inst.LoadSnapshot(snap.LSN, snap.Rows); err != nil {
		return err
	}

	e.mu.Lock()
	e.streams[key] = inst
	e.mu.Unlock()

	msg, err := proto.Marshal(proto.Snapshot{
		Type:   "snapshot",
		Shape:  def.Name,
		LSN:    snap.LSN.String(),
		Offset: inst.Log.LastOffset(),
		Rows:   snap.Rows,
	})
	if err != nil {
		return err
	}
	if !sess.conn.Enqueue(msg) {
		e.disconnect(ctx, sess.conn, proto.ReasonSlowClient)
		return fmt.Errorf("tether: slow client")
	}
	e.noteClientOffset(sess.conn.ID(), def.Name, inst.Log.LastOffset())
	return nil
}

func (e *Engine) disconnect(ctx context.Context, conn *transport.Conn, reason string) {
	e.log.Warn(
		"tether client disconnect",
		"slot", e.opts.slotName,
		"client_id", conn.ID(),
		"reason", reason,
	)
	if m := e.opts.metrics; m != nil {
		m.ClientDisconnected(reason)
	}
	conn.SendBye(ctx, reason)
}

func (e *Engine) noteClientOffset(clientID, shapeName string, offset int64) {
	if m := e.opts.metrics; m != nil {
		m.ClientOffset(clientID, shapeName, offset)
	}
}

func (e *Engine) enqueueChange(conn *transport.Conn, shapeName string, ev shape.Event) bool {
	msg, err := proto.Marshal(proto.Change{
		Type:   "change",
		Shape:  shapeName,
		Offset: ev.Offset,
		Op:     string(ev.Op),
		Row:    ev.Row,
	})
	if err != nil {
		return true
	}
	if !conn.Enqueue(msg) {
		return false
	}
	e.noteClientOffset(conn.ID(), shapeName, ev.Offset)
	return true
}

func (e *Engine) sendErr(conn *transport.Conn, code, message, shapeName string) {
	msg, err := proto.Marshal(proto.Error{
		Type:    "error",
		Code:    code,
		Message: message,
		Shape:   shapeName,
	})
	if err != nil {
		return
	}
	_ = conn.Enqueue(msg)
}

func streamKey(shapeName, claimsKey string) string {
	return shapeName + "\x00" + claimsKey
}

func (e *Engine) ensurePool(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pool != nil {
		return nil
	}
	norm, err := normalDSN(e.pgURL)
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, norm)
	if err != nil {
		return fmt.Errorf("tether: pool: %w", err)
	}
	e.pool = pool
	return nil
}

func normalDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func replicationDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Run starts the WAL consumer and fans changes out to live sessions.
// Blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.ensurePool(ctx); err != nil {
		return err
	}
	if err := wal.EnsureSchema(ctx, e.pool); err != nil {
		return err
	}
	if err := mutate.EnsureSchema(ctx, e.pool); err != nil {
		return err
	}

	defs := e.reg.All()
	if len(defs) == 0 {
		return fmt.Errorf("tether: no shapes registered")
	}
	tables := make([]wal.TableRef, 0, len(defs))
	seen := map[string]bool{}
	for _, d := range defs {
		k := d.Schema + "." + d.Table
		if seen[k] {
			continue
		}
		seen[k] = true
		tables = append(tables, wal.TableRef{Schema: d.Schema, Name: d.Table})
	}
	if err := wal.EnsurePublication(ctx, e.pool, e.opts.publication, tables); err != nil {
		return err
	}

	replURL, err := replicationDSN(e.pgURL)
	if err != nil {
		return err
	}

	var lastID int64
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.runConsumerCycle(ctx, replURL, tables, ticker, &lastID); err != nil {
			if errors.Is(err, errRestartConsumer) {
				continue
			}
			return err
		}
		return nil
	}
}

// errRestartConsumer signals Run to recreate the slot and restart the consumer.
var errRestartConsumer = errors.New("tether: restart consumer")

func (e *Engine) runConsumerCycle(
	ctx context.Context,
	replURL string,
	tables []wal.TableRef,
	ticker *time.Ticker,
	lastID *int64,
) error {
	repl, err := pgconn.Connect(ctx, replURL)
	if err != nil {
		return fmt.Errorf("tether: replication connect: %w", err)
	}
	if err := wal.EnsureSlot(ctx, repl, e.opts.slotName); err != nil {
		_ = repl.Close(ctx)
		return err
	}
	_ = repl.Close(ctx)

	repl, err = pgconn.Connect(ctx, replURL)
	if err != nil {
		return fmt.Errorf("tether: replication reconnect: %w", err)
	}

	consumer, err := wal.NewConsumer(e.pool, repl, wal.Config{
		ConsumerID:  "tether-engine",
		SlotName:    e.opts.slotName,
		Publication: e.opts.publication,
		Tables:      tables,
	})
	if err != nil {
		_ = repl.Close(ctx)
		return err
	}

	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()

	errCh := make(chan error, 1)
	go func() { errCh <- consumer.Run(consCtx) }()

	for {
		select {
		case err := <-errCh:
			_ = repl.Close(ctx)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		case <-ctx.Done():
			consCancel()
			<-errCh
			_ = repl.Close(ctx)
			return ctx.Err()
		case <-ticker.C:
			if err := e.fanOutNewChanges(ctx, lastID); err != nil {
				e.log.Error("tether fanout", "slot", e.opts.slotName, "err", err)
			}
			e.sweepIdleClients(ctx)
			e.reportClientCount()

			// Sample lag once per tick for both metrics and the lag guard so
			// one-shot test overrides (and expensive queries) are not doubled.
			lag, lagErr := e.slotLagBytes(ctx)
			if lagErr != nil {
				if ctx.Err() == nil && !errors.Is(lagErr, context.Canceled) {
					e.log.Error("tether slot lag", "slot", e.opts.slotName, "err", lagErr)
				}
				continue
			}
			if m := e.opts.metrics; m != nil {
				m.ReplicationLagBytes(lag)
			}
			if e.opts.maxSlotLag > 0 && lag > e.opts.maxSlotLag {
				e.log.Error("tether slot lag exceeded",
					"slot", e.opts.slotName,
					"lag_bytes", lag,
					"max_bytes", e.opts.maxSlotLag)
				consCancel()
				<-errCh
				_ = repl.Close(ctx)
				if err := e.handleSlotLagExceeded(ctx, lastID); err != nil {
					return err
				}
				return errRestartConsumer
			}
		}
	}
}

func (e *Engine) reportClientCount() {
	m := e.opts.metrics
	if m == nil {
		return
	}
	e.mu.Lock()
	n := len(e.sessions)
	e.mu.Unlock()
	m.ClientsConnected(n)
}

func (e *Engine) slotLagBytes(ctx context.Context) (int64, error) {
	e.slotLagMu.RLock()
	fn := e.slotLagOverride
	e.slotLagMu.RUnlock()
	if fn != nil {
		return fn(ctx)
	}
	e.mu.Lock()
	pool := e.pool
	e.mu.Unlock()
	if pool == nil {
		return 0, fmt.Errorf("tether: pool closed")
	}
	return wal.SlotLagBytes(ctx, pool, e.opts.slotName)
}

func (e *Engine) handleSlotLagExceeded(ctx context.Context, lastID *int64) error {
	e.broadcastMustResnapshot(ctx)

	e.mu.Lock()
	e.streams = make(map[string]*shape.Instance)
	for _, s := range e.sessions {
		s.clearMembership()
	}
	e.mu.Unlock()

	if err := wal.DropSlot(ctx, e.pool, e.opts.slotName); err != nil {
		return fmt.Errorf("tether: drop lagged slot: %w", err)
	}
	if _, err := e.pool.Exec(ctx, `
DELETE FROM tether.checkpoint WHERE consumer_id = $1`, "tether-engine"); err != nil {
		return fmt.Errorf("tether: clear checkpoint after lag drop: %w", err)
	}
	if err := e.resetFanoutCursor(ctx, lastID); err != nil {
		return err
	}
	return nil
}

func (e *Engine) resetFanoutCursor(ctx context.Context, lastID *int64) error {
	var maxID int64
	err := e.pool.QueryRow(ctx, `
SELECT COALESCE(MAX(id), 0) FROM tether.change_log WHERE consumer_id = $1`,
		"tether-engine").Scan(&maxID)
	if err != nil {
		return fmt.Errorf("tether: reset fanout cursor: %w", err)
	}
	*lastID = maxID
	return nil
}

func (e *Engine) broadcastMustResnapshot(ctx context.Context) {
	msg, err := proto.Marshal(proto.Error{
		Type:    "error",
		Code:    proto.CodeMustResnapshot,
		Message: ErrSlotLagExceeded.Error(),
	})
	if err != nil {
		return
	}
	e.mu.Lock()
	sessions := make([]*session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.mu.Unlock()
	for _, s := range sessions {
		if !s.conn.Enqueue(msg) {
			e.disconnect(ctx, s.conn, proto.ReasonSlowClient)
		}
	}
}

func (e *Engine) sweepIdleClients(ctx context.Context) {
	if e.opts.maxClientIdle <= 0 {
		return
	}
	cutoff := time.Now().Add(-e.opts.maxClientIdle)
	e.mu.Lock()
	var idle []*session
	for id, s := range e.sessions {
		if s.isIdle(cutoff) {
			idle = append(idle, s)
			delete(e.sessions, id)
		}
	}
	e.mu.Unlock()
	for _, s := range idle {
		e.disconnect(ctx, s.conn, proto.ReasonIdleClient)
		e.hub.Remove(s.conn.ID())
	}
}

func (e *Engine) fanOutNewChanges(ctx context.Context, lastID *int64) error {
	e.mu.Lock()
	pool := e.pool
	e.mu.Unlock()
	if pool == nil {
		return fmt.Errorf("tether: pool closed")
	}
	rows, err := pool.Query(ctx, `
SELECT id, lsn, op, schema_name, table_name, relation_fingerprint, old_row, new_row
FROM tether.change_log
WHERE consumer_id = $1 AND id > $2
ORDER BY id`, "tether-engine", *lastID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id                    int64
			lsn, op, sch, tbl, fp string
			oldJSON, newJSON      []byte
		)
		if err := rows.Scan(&id, &lsn, &op, &sch, &tbl, &fp, &oldJSON, &newJSON); err != nil {
			return err
		}
		*lastID = id
		commitLSN, err := pglogrepl.ParseLSN(lsn)
		if err != nil {
			return err
		}
		ch := wal.Change{
			Schema:              sch,
			Table:               tbl,
			Op:                  wal.Op(op),
			RelationFingerprint: fp,
			CommitLSN:           commitLSN,
			Old:                 decodeJSONMap(oldJSON),
			New:                 decodeJSONMap(newJSON),
		}
		e.dispatchChange(ctx, ch)
	}
	return rows.Err()
}

func (e *Engine) dispatchChange(ctx context.Context, ch wal.Change) {
	e.mu.Lock()
	streams := make(map[string]*shape.Instance, len(e.streams))
	for k, v := range e.streams {
		streams[k] = v
	}
	sessions := make([]*session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.mu.Unlock()

	// Apply to each stream once, then notify sessions subscribed to that shape.
	type applied struct {
		shapeName string
		events    []shape.Event
	}
	var results []applied
	for key, inst := range streams {
		shapeName := key
		if i := indexNull(key); i >= 0 {
			shapeName = key[:i]
		}
		evs, err := inst.Apply(ch)
		if err != nil {
			e.log.Error(
				"shape apply",
				"slot", e.opts.slotName,
				"shape", shapeName,
				"err", err,
			)
			if errors.Is(err, shape.ErrSchemaDrift) || errors.Is(err, shape.ErrHalted) {
				for _, s := range sessions {
					if s.hasShape(shapeName) {
						e.sendErr(s.conn, proto.CodeHalted, err.Error(), shapeName)
					}
				}
			}
			continue
		}
		if len(evs) > 0 {
			results = append(results, applied{shapeName: shapeName, events: evs})
		}
	}

	for _, s := range sessions {
		for _, r := range results {
			if !s.hasShape(r.shapeName) {
				continue
			}
			for _, ev := range r.events {
				if !e.enqueueChange(s.conn, r.shapeName, ev) {
					e.disconnect(ctx, s.conn, proto.ReasonSlowClient)
					break
				}
			}
		}
	}
}

func indexNull(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return i
		}
	}
	return -1
}

func decodeJSONMap(b []byte) map[string]any {
	if len(b) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}

// Close releases the database pool.
//
// Must not hold e.mu across pool.Close: fan-out borrows a pool conn then takes
// e.mu in dispatchChange. Holding the mutex while Close waits for that conn
// deadlocks until the caller (or go test) times out.
func (e *Engine) Close() {
	e.mu.Lock()
	pool := e.pool
	e.pool = nil
	e.mu.Unlock()
	if pool != nil {
		pool.Close()
	}
}
