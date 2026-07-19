package wal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Consumer reads pgoutput WAL, persists changes, then acknowledges LSN.
type Consumer struct {
	pool    *pgxpool.Pool
	repl    *pgconn.PgConn
	cfg     Config
	types   *pgtype.Map
	rels    map[uint32]*relationMeta
	durable pglogrepl.LSN

	// AfterDurableCommit is called after the durable txn commits and before
	// SendStandbyStatusUpdate. Tests use it to simulate crash-before-ack.
	AfterDurableCommit func(lsn pglogrepl.LSN) error
}

// NewConsumer builds a Consumer. Call EnsureSchema / EnsurePublication /
// EnsureSlot before Run.
func NewConsumer(pool *pgxpool.Pool, repl *pgconn.PgConn, cfg Config) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if pool == nil || repl == nil {
		return nil, fmt.Errorf("wal: pool and replication conn are required")
	}
	return &Consumer{
		pool:  pool,
		repl:  repl,
		cfg:   cfg,
		types: pgtype.NewMap(),
		rels:  make(map[uint32]*relationMeta),
	}, nil
}

// Run starts logical replication and blocks until ctx is cancelled or an error.
func (c *Consumer) Run(ctx context.Context) error {
	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", c.cfg.Publication),
	}

	startLSN, err := c.resolveStartLSN(ctx)
	if err != nil {
		return err
	}
	c.durable = startLSN

	if err := pglogrepl.StartReplication(ctx, c.repl, c.cfg.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	}); err != nil {
		return fmt.Errorf("wal: start replication: %w", err)
	}

	standbyTimeout := 10 * time.Second
	nextStandby := time.Now().Add(standbyTimeout)

	var (
		pending []Change
		curXid  uint32
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if time.Now().After(nextStandby) {
			if err := c.sendStatus(ctx); err != nil {
				return err
			}
			nextStandby = time.Now().Add(standbyTimeout)
		}

		msgCtx, cancel := context.WithDeadline(ctx, nextStandby)
		raw, err := c.repl.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			return fmt.Errorf("wal: receive: %w", err)
		}

		if errMsg, ok := raw.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("wal: postgres error: %s (%s)", errMsg.Message, errMsg.Code)
		}

		cd, ok := raw.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		if len(cd.Data) == 0 {
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("wal: parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				nextStandby = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("wal: parse xlogdata: %w", err)
			}
			logical, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("wal: parse logical: %w", err)
			}

			switch m := logical.(type) {
			case *pglogrepl.RelationMessage:
				if err := c.handleRelation(m); err != nil {
					return err
				}
			case *pglogrepl.BeginMessage:
				pending = pending[:0]
				curXid = m.Xid
			case *pglogrepl.InsertMessage:
				ch, err := c.decodeInsert(m)
				if err != nil {
					return err
				}
				pending = append(pending, ch)
			case *pglogrepl.UpdateMessage:
				ch, err := c.decodeUpdate(m)
				if err != nil {
					return err
				}
				pending = append(pending, ch)
			case *pglogrepl.DeleteMessage:
				ch, err := c.decodeDelete(m)
				if err != nil {
					return err
				}
				pending = append(pending, ch)
			case *pglogrepl.CommitMessage:
				commitLSN := m.TransactionEndLSN
				if commitLSN == 0 {
					commitLSN = m.CommitLSN
				}
				if err := c.persistAndAck(ctx, commitLSN, curXid, pending); err != nil {
					return err
				}
				pending = pending[:0]
			}
		}
	}
}

func (c *Consumer) resolveStartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	raw, err := loadCheckpoint(ctx, c.pool, c.cfg.ConsumerID)
	if err != nil {
		return 0, err
	}
	if raw != "" {
		lsn, err := pglogrepl.ParseLSN(raw)
		if err != nil {
			return 0, fmt.Errorf("wal: parse checkpoint lsn %q: %w", raw, err)
		}
		return lsn, nil
	}
	// No checkpoint yet: start from the slot's restart position (0/0), not
	// IdentifySystem's current tip — that would skip already-committed rows.
	return pglogrepl.LSN(0), nil
}

func (c *Consumer) handleRelation(m *pglogrepl.RelationMessage) error {
	fp := fingerprintRelation(m)
	if prev, ok := c.rels[m.RelationID]; ok && prev.Fingerprint != fp {
		return fmt.Errorf("%w: relation %s.%s oid=%d", ErrSchemaDrift, m.Namespace, m.RelationName, m.RelationID)
	}
	c.rels[m.RelationID] = &relationMeta{
		Namespace:   m.Namespace,
		Relation:    m.RelationName,
		Fingerprint: fp,
		Columns:     m.Columns,
	}
	return nil
}

func (c *Consumer) rel(id uint32) (*relationMeta, error) {
	rel, ok := c.rels[id]
	if !ok {
		return nil, fmt.Errorf("wal: unknown relation id %d", id)
	}
	return rel, nil
}

func (c *Consumer) decodeInsert(m *pglogrepl.InsertMessage) (Change, error) {
	rel, err := c.rel(m.RelationID)
	if err != nil {
		return Change{}, err
	}
	neu, err := decodeTuple(c.types, rel, m.Tuple)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Schema:              rel.Namespace,
		Table:               rel.Relation,
		Op:                  opInsert,
		RelationFingerprint: rel.Fingerprint,
		New:                 neu,
	}, nil
}

func (c *Consumer) decodeUpdate(m *pglogrepl.UpdateMessage) (Change, error) {
	rel, err := c.rel(m.RelationID)
	if err != nil {
		return Change{}, err
	}
	oldRow, err := decodeTuple(c.types, rel, m.OldTuple)
	if err != nil {
		return Change{}, err
	}
	newRow, err := decodeTuple(c.types, rel, m.NewTuple)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Schema:              rel.Namespace,
		Table:               rel.Relation,
		Op:                  opUpdate,
		RelationFingerprint: rel.Fingerprint,
		Old:                 oldRow,
		New:                 newRow,
	}, nil
}

func (c *Consumer) decodeDelete(m *pglogrepl.DeleteMessage) (Change, error) {
	rel, err := c.rel(m.RelationID)
	if err != nil {
		return Change{}, err
	}
	oldRow, err := decodeTuple(c.types, rel, m.OldTuple)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Schema:              rel.Namespace,
		Table:               rel.Relation,
		Op:                  opDelete,
		RelationFingerprint: rel.Fingerprint,
		Old:                 oldRow,
	}, nil
}

func (c *Consumer) persistAndAck(ctx context.Context, lsn pglogrepl.LSN, xid uint32, changes []Change) error {
	if err := c.persist(ctx, lsn, xid, changes); err != nil {
		return err
	}

	if c.AfterDurableCommit != nil {
		if err := c.AfterDurableCommit(lsn); err != nil {
			return fmt.Errorf("wal: after durable commit hook: %w", err)
		}
	}

	// Invariant 1: never SendStandbyStatusUpdate before change_log+checkpoint
	// COMMIT returns. Ack-ahead allows Postgres to discard WAL we have not
	// durably recorded — silent data loss after crash.
	c.durable = lsn
	if err := c.sendStatus(ctx); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) persist(ctx context.Context, lsn pglogrepl.LSN, xid uint32, changes []Change) error {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("wal: begin durable txn: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	lsnStr := lsn.String()
	for i, ch := range changes {
		var oldJSON, newJSON []byte
		if ch.Old != nil {
			oldJSON, err = json.Marshal(ch.Old)
			if err != nil {
				return fmt.Errorf("wal: marshal old_row: %w", err)
			}
		}
		if ch.New != nil {
			newJSON, err = json.Marshal(ch.New)
			if err != nil {
				return fmt.Errorf("wal: marshal new_row: %w", err)
			}
		}
		_, err = tx.Exec(ctx, `
INSERT INTO tether.change_log (
	consumer_id, lsn, xid, seq, schema_name, table_name, op,
	relation_fingerprint, old_row, new_row
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (consumer_id, lsn, seq) DO NOTHING
`, c.cfg.ConsumerID, lsnStr, int64(xid), i, ch.Schema, ch.Table, string(ch.Op),
			ch.RelationFingerprint, nullableJSON(oldJSON), nullableJSON(newJSON))
		if err != nil {
			return fmt.Errorf("wal: insert change_log: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
INSERT INTO tether.checkpoint (consumer_id, confirmed_lsn)
VALUES ($1, $2)
ON CONFLICT (consumer_id) DO UPDATE
SET confirmed_lsn = EXCLUDED.confirmed_lsn,
    updated_at = now()
`, c.cfg.ConsumerID, lsnStr)
	if err != nil {
		return fmt.Errorf("wal: upsert checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("wal: commit durable txn: %w", err)
	}
	return nil
}

func nullableJSON(b []byte) any {
	if b == nil {
		return nil
	}
	return b
}

func (c *Consumer) sendStatus(ctx context.Context) error {
	// Only acknowledge the durable checkpoint LSN — never a received-but-not-persisted position.
	err := pglogrepl.SendStandbyStatusUpdate(ctx, c.repl, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: c.durable,
		WALFlushPosition: c.durable,
		WALApplyPosition: c.durable,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("wal: standby status: %w", err)
	}
	return nil
}
