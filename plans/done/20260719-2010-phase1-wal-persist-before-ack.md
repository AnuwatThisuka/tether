# Phase 1: WAL ingest with persist-before-ack

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

Reference: This plan follows conventions from `AGENTS.md` (root), master design in `IMPLEMENT_PLAN.md` (Phase 1), and `.agents/commands/create-plan.md`. Invariants touched: **1** (primary), **5** (relation fingerprint; halt on mismatch — minimal stub OK).

## Purpose / Big Picture

After Phase 0, the tree builds and can start Postgres with `wal_level = logical`, but nothing reads the WAL. After this plan, tether can:

1. Create (or reuse) a replication slot and publication for a configured set of tables.
2. Decode `pgoutput` Insert/Update/Delete for those tables, including partial UPDATE columns and TOAST unchanged-markers.
3. Persist each decoded change to a durable Postgres-backed change log **before** calling `SendStandbyStatusUpdate` (Invariant 1).
4. Restart and resume consumption from the last durable LSN checkpoint without silent loss.

An embedder still cannot sync clients over WebSocket (Phase 4) or apply shape filters (Phase 2). The observable win is: insert a row in a watched table → row appears in tether's durable log at a recorded LSN → crash before ack does not lose it; ack never runs ahead of durability.

## Assumptions

- Phase 0 acceptance still holds (`make test`, `make lint`, `make db-up` / `db-check`).
- Module path remains `github.com/anuwatthisuka/tether`.
- Adding `jackc/pgx` and `jackc/pglogrepl` is approved (listed in `AGENTS.md` / `IMPLEMENT_PLAN.md` allow-list). Do **not** add `coder/websocket` in this phase.
- Integration tests use `TETHER_TEST_DSN` with `replication=database` for the replication connection, and a second normal DSN (same URL without `replication=database`, or derived) for setup SQL and durable-log queries.
- Host Postgres role `tether` can create publications, replication slots, and tables in the `tether` database (true for the Phase 0 compose defaults).

## Open Questions

_(none blocking — durability backend and publication strategy decided below.)_

## Progress

- [x] (2026-07-19 20:10+07) ExecPlan authored; awaiting approval / implementation.
- [x] (2026-07-19 20:20+07) Add `jackc/pgx/v5` and `jackc/pglogrepl` to `go.mod`.
- [x] (2026-07-19 20:20+07) Implement `internal/wal` slot + publication lifecycle.
- [x] (2026-07-19 20:20+07) Implement pgoutput decode path (Begin/Relation/Insert/Update/Delete/Commit).
- [x] (2026-07-19 20:20+07) Implement Postgres-backed durable change log + LSN checkpoint (persist-before-ack).
- [x] (2026-07-19 20:20+07) Wire `wal.Consumer` for integration tests (public Engine still fails closed).
- [x] (2026-07-19 20:20+07) Unit tests for TOAST unchanged-marker omission + fingerprint drift.
- [x] (2026-07-19 20:25+07) Integration: ingest, restart resume, persist-before-ack crash-replay, schema drift.
- [x] (2026-07-19 20:25+07) Update `IMPLEMENT_PLAN.md` open decisions; `CHANGELOG.md`; close Outcomes.
- [ ] Move plan to `plans/done/` when PR lands.

## Surprises & Discoveries

- Observation: Starting replication at `IdentifySystem().XLogPos` after inserts skips them. First-run start must be `0/0` (slot restart) unless a durable checkpoint exists.
  Evidence: `TestIngestInsertPersisted` timed out until `resolveStartLSN` was fixed.

- Observation: A cancelled consumer must close the replication connection before another `Run` on the same slot, or Postgres returns SQLSTATE 55006 (slot active).
  Evidence: `TestRestartResumesWithoutGap` / crash-replay failures until tests deferred `repl.Close`.

## Decision Log

- Decision: **Durable change log lives in Postgres tables** managed by tether (schema `tether` or prefixed table names `tether_change_log` / `tether_checkpoint`), not an embedded local log.
  Rationale: Same failure domain as source data; no new dependency; satisfies Invariant 1 with ordinary `COMMIT` before ack; fits an embeddable library that already requires Postgres.
  Date/Author: 2026-07-19 / plan author

- Decision: **One publication** for all watched tables (`tether_<slot>` or fixed name `tether_pub`); filter tables in-process for Phase 1 (shape filters come in Phase 2).
  Rationale: Matches `IMPLEMENT_PLAN.md` preference; fewer Postgres objects; Phase 2 adds predicate filtering without reshaping publications constantly.
  Date/Author: 2026-07-19 / plan author

- Decision: Phase 1 **does not** expand the public `Engine` API beyond what tests need. Prefer an internal test helper / `wal.Consumer` constructed in integration tests. Optional: `Engine.Run` remains absent; document in CHANGELOG that WAL plumbing is internal-only until Phase 2+.
  Rationale: One concern per PR; avoid committing to a public Run API before shapes exist.
  Date/Author: 2026-07-19 / plan author

- Decision: Temporary durable "change log" rows store decoded row images as JSONB keyed by LSN + xid + change seq within the transaction. This is **not** the final per-shape client log — Phase 2 will introduce shape logs. Phase 1 log exists to prove persist-before-ack and restart.
  Rationale: Avoid premature shape-log schema; keep Invariant 1 testable now.
  Date/Author: 2026-07-19 / plan author

- Decision: Use pgoutput `proto_version '1'` (no in-progress streaming) for Phase 1.
  Rationale: Committed transactions arrive as Begin…Commit units; simpler and sufficient for Invariant 1.
  Date/Author: 2026-07-19 / implementer

## Outcomes & Retrospective

Phase 1 acceptance passed:

- `make test` — decode unit tests green.
- `make lint` — 0 issues.
- `make test-integration` — `TestIngestInsertPersisted`, `TestRestartResumesWithoutGap`, `TestPersistBeforeAck_CrashReplay`, `TestSchemaDriftStopsConsumer` green.
- README v0.1 checkbox for WAL ingest checked.
- Public `tether.New` still returns `ErrNotImplemented` (intentional).

Gaps / follow-ups (expected):

- Shape filters, snapshot handoff, WebSocket, mutations, slot lag drop — Phases 2–6.
- Schema drift is in-process only (relation cache in one `Run`); persistence of fingerprints can wait for Phase 2 shape halt UX.
- Move this ExecPlan to `plans/done/` on PR.

Purpose met: inserts into watched tables appear in `tether.change_log` at a durable LSN; crash-before-ack does not lose them; ack never runs ahead of durability.
## Context and Orientation

Current tree (post Phase 0):

- Root: `tether.New` returns `ErrNotImplemented`; `CHANGELOG.md` notes scaffolding.
- `internal/wal/doc.go` — package doc only.
- `docker-compose.yml` — Postgres 16, `wal_level=logical`, port 54321.
- `Makefile` — `test`, `test-integration`, `db-up`, `db-check`, etc.
- No `pgx` / `pglogrepl` in `go.mod` yet.

**Packages this plan changes:**

| Path | Change |
|------|--------|
| `internal/wal/` | Main work: slot, publication, decode, consumer loop, checkpoint |
| `internal/wal` test files | Unit + integration tests |
| `go.mod` / `go.sum` | Add `jackc/pgx/v5`, `jackc/pglogrepl` |
| `CHANGELOG.md` | Note internal WAL ingest scaffolding |
| `IMPLEMENT_PLAN.md` §12 | Mark durability + publication decisions Done |
| root / `internal/e2e` | Only if needed for shared test DSN helpers — prefer helpers under `internal/wal` or a tiny `internal/testutil` **only if** duplication hurts; avoid new packages unless necessary |

**Does not touch:** `transport/`, public Shape/Handler API, snapshot handoff (Phase 3), mutations (Phase 5), WebSocket.

**Terms:**

- **LSN** (Log Sequence Number): Postgres WAL position. Advancing the confirmed flush LSN via `SendStandbyStatusUpdate` tells Postgres it may discard older WAL — hence persist-first.
- **pgoutput**: Builtin logical decoding plugin. Emits typed messages (Relation, Insert, …).
- **Replication slot**: Named cursor on the WAL. Pins WAL until the consumer acknowledges LSNs.
- **Publication**: Set of tables whose changes are decoded for subscribers.
- **TOAST unchanged-marker**: In logical decoding, an unchanged TOASTed value may arrive as a toast marker rather than bytes — treat as "column not present", never as empty/null overwrite.
- **Partial UPDATE**: Without `REPLICA IDENTITY FULL`, UPDATE messages may include only changed columns for the new tuple. Code must not invent nulls for omitted columns in the durable log representation.

**Invariant 1 call order (mandatory):**

    BEGIN (durable store txn, normal pool OR same durable conn)
    INSERT into change_log …
    UPSERT checkpoint confirmed_lsn = …
    COMMIT
    SendStandbyStatusUpdate(… confirmed flush/apply LSN …)

Never reverse the last two steps. Never ack inside an uncommitted transaction that might roll back.

**Invariant 5 (minimal):** On `Relation` message, fingerprint column names + types (+ attnums). If a later Relation for the same OID disagrees, return a loud error and stop the consumer loop. Do not remap. Full "halt shape" UX waits for Phase 2 shapes; Phase 1 stops the WAL consumer.

## Plan of Work

### Milestone 1: Dependencies and test DSN helpers

1. `go get github.com/jackc/pgx/v5 github.com/jackc/pglogrepl`
2. Add a small helper in `internal/wal` tests (or unexported test file) to:
   - Parse `TETHER_TEST_DSN`
   - Derive a **normal** DSN by stripping `replication=database` for DDL, inserts, and querying the durable log
   - Open `pgx` pool (normal) and replication `pgconn` separately
3. Replace or extend Phase 0 `TestIntegrationHarnessWired` only if needed — keep it; add real tests alongside.

### Milestone 2: Schema for durable log + checkpoint

Create SQL applied by an exported or package-level `EnsureSchema(ctx, pool)`:

```sql
CREATE SCHEMA IF NOT EXISTS tether;

CREATE TABLE IF NOT EXISTS tether.checkpoint (
  consumer_id TEXT PRIMARY KEY,
  confirmed_lsn TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tether.change_log (
  id BIGSERIAL PRIMARY KEY,
  consumer_id TEXT NOT NULL,
  lsn TEXT NOT NULL,
  xid xid,                    -- or TEXT/BIGINT if xid binding is awkward
  seq INT NOT NULL,           -- order within commit
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  op TEXT NOT NULL,            -- insert|update|delete
  relation_fingerprint TEXT NOT NULL,
  old_row JSONB,
  new_row JSONB,
  UNIQUE (consumer_id, lsn, seq)
);
```

Exact column types may be adjusted during implementation; keep uniqueness that prevents double-append on restart replay.

Document that this schema is **internal** and may change before v0.1 public stability.

### Milestone 3: Slot and publication

In `internal/wal`:

- Types: `Config` with `SlotName`, `Publication`, `ConsumerID`, `Tables []TableRef` (`Schema`, `Name`).
- `EnsurePublication(ctx, pool, name, tables)` — `CREATE PUBLICATION … FOR TABLE …` or alter to add tables idempotently.
- `EnsureSlot(ctx, replConn, slotName)` — create logical slot with plugin `pgoutput` if missing; read consistent start LSN.
- Prefer `pglogrepl.CreateReplicationSlot` / `StartReplication` patterns consistent with current `pglogrepl` API.

Idempotence: calling Ensure* twice must not fail.

### Milestone 4: Decode pgoutput

Implement a decoder that maintains a relation cache (OID → columns + fingerprint).

Handle message types needed for correctness:

- Begin / Commit — track xid and commit LSN
- Relation — update cache; on fingerprint mismatch → `ErrSchemaDrift` (new sentinel in `internal/wal` or root `errors.go`)
- Insert / Update / Delete — produce an in-memory `Change` with:
  - schema, table, op
  - old/new maps of column → value
  - omitted columns absent from the map (not present as null) for partial UPDATE / TOAST markers

Unit-test with crafted pgoutput payloads **or** structured tables of decoded field presence if crafting binary messages is too brittle — but prefer at least one integration assertion for a partial update with `REPLICA IDENTITY DEFAULT` (unchanged wide column not wiped). If unit tests use fakes, the integration test remains authoritative for "no null overwrite" semantics in stored JSONB.

### Milestone 5: Consumer loop (persist-before-ack)

`Consumer.Run(ctx)`:

1. Load checkpoint LSN for `consumer_id` (or slot consistent point on first run).
2. `StartReplication` from that LSN with publication names.
3. Read messages; batch changes until Commit.
4. On Commit LSN `L`:
   - In one normal-pool transaction: insert all changes for the txn into `tether.change_log`, upsert `tether.checkpoint` to `L`.
   - **After** commit succeeds: `SendStandbyStatusUpdate` acknowledging `L`.
5. On ctx cancel: clean shutdown without ack-ahead.
6. Keepalive / standby status replies as required by the replication protocol so the connection stays healthy — but never advance confirmed LSN past durable checkpoint.

Comment at the ack call site citing Invariant 1.

### Milestone 6: Integration tests (required)

All under `//go:build integration` in `internal/wal`.

**TestIngestInsertPersisted**

- Ensure schema, publication for a temp table `wal_phase1_items`, slot, consumer.
- INSERT a row via normal pool.
- Run consumer until change_log contains the row (timeout ~10s).
- Assert op/table/new_row fields.

**TestRestartResumesWithoutGap**

- Ingest at least one change; stop consumer.
- INSERT another row; start new consumer with same slot/consumer_id.
- Assert both rows present exactly once in change_log.

**TestPersistBeforeAck_CrashReplay** (headline for Invariant 1)

Implement with a test hook, not a real SIGKILL of the whole test binary:

- Add an optional `AfterPersistBeforeAck func(lsn)` or `AckHook` on Consumer used only from tests.
- Hook: after durable COMMIT, **before** `SendStandbyStatusUpdate`, return a injected error / cancel (simulating crash).
- Restart consumer; assert the change is still in `change_log` and is not duplicated incorrectly; assert Postgres still has WAL for replay (consumer redelivers and upsert/unique handles duplicates).
- Control case: without hook, ack advances and a third start does not duplicate.

Do **not** weaken this into a unit-only mock of ack ordering — the durable tables must be real Postgres.

**TestSchemaDriftStopsConsumer**

- Create table; ingest; `ALTER TABLE … ADD COLUMN`; produce a change (UPDATE); expect consumer to return `ErrSchemaDrift` (or wrapped) and not silently continue.

### Milestone 7: Docs and bookkeeping

- `CHANGELOG.md` Unreleased: internal WAL consumer + durable log (not a public API promise).
- `IMPLEMENT_PLAN.md` §12: mark durability backend and publication strategy Done.
- Do **not** check off README v0.1 "WAL ingest…" until these integration tests exist and pass (they will after this phase — then check that one bullet).
- Update this plan Progress / Outcomes; on PR creation move to `plans/done/`.

## Concrete Steps

From repo root:

    go get github.com/jackc/pgx/v5@v5.7.4
    go get github.com/jackc/pglogrepl@v0.0.0-20250509230407-a9884f6bd78a
    # pin to current stable versions available at implement time; run go mod tidy

Implement packages/files roughly as:

    internal/wal/schema.go          # EnsureSchema
    internal/wal/publication.go     # EnsurePublication
    internal/wal/slot.go            # EnsureSlot
    internal/wal/decode.go          # pgoutput → Change
    internal/wal/decode_test.go     # unit tests
    internal/wal/consumer.go        # Run loop, persist-before-ack
    internal/wal/consumer_integration_test.go

Then:

    make fmt
    make test
    make lint

    make db-up
    export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
    make test-integration
    make db-down

Expected: all new integration tests pass; Phase 0 harness still passes; lint clean.

## Validation and Acceptance

        make test
        make lint
        make db-up
        make test-integration
        # Must include:
        #   TestIngestInsertPersisted
        #   TestRestartResumesWithoutGap
        #   TestPersistBeforeAck_CrashReplay
        #   TestSchemaDriftStopsConsumer

Manual spot-check (optional):

        make db-up
        # run a small cmd or go test -run TestIngest -v
        docker compose exec -T postgres psql -U tether -d tether \
          -c 'SELECT op, table_name, new_row FROM tether.change_log ORDER BY id'

Honest gaps after Phase 1 (do not claim otherwise):

- No shape filters (Invariant 2 not yet enforced end-to-end).
- No snapshot/stream handoff (Invariant 4).
- No WebSocket / slow-client isolation (Invariant 7).
- Slot lag drop (Invariant 6) deferred to Phase 6.
- Durable log is not the final client-facing shape log.

## Idempotence and Recovery

- `EnsureSchema` / `EnsurePublication` / `EnsureSlot` must be safe to re-run.
- Integration tests should use unique table/slot suffixes per test (or truncate tether.* + drop slot in cleanup) so re-runs do not collide.
- Recovery from a wedged slot: `SELECT pg_drop_replication_slot(...)` then re-run tests; document in test helpers.
- `make db-down && make db-up` resets everything.

## Artifacts and Notes

Invariant 1 comment to place near ack:

    // Invariant 1: never SendStandbyStatusUpdate before change_log+checkpoint
    // COMMIT returns. Ack-ahead allows Postgres to discard WAL we have not
    // durably recorded — silent data loss after crash.

Persist-before-ack hook sketch:

    type Consumer struct {
        // AfterDurableCommit, if set, is called after the durable txn commits
        // and before standby status update. Used by tests to simulate crash.
        AfterDurableCommit func(lsn pglogrepl.LSN) error
    }

## Interfaces and Dependencies

**Dependencies (allowed):**

- `github.com/jackc/pgx/v5`
- `github.com/jackc/pglogrepl`

No other new modules without asking.

**Suggested exported surface in `internal/wal` (unexported ok if tests stay in-package):**

    type TableRef struct{ Schema, Name string }

    type Config struct {
        ConsumerID  string
        SlotName    string
        Publication string
        Tables      []TableRef
    }

    func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error
    func NewConsumer(pool *pgxpool.Pool, repl *pgconn.PgConn, cfg Config) (*Consumer, error)
    func (c *Consumer) Run(ctx context.Context) error

    var ErrSchemaDrift error

Decoded `Change` stays unexported or exported only if Phase 2 needs it — prefer exporting a minimal read API for tests:

    // Test helpers query tether.change_log via SQL rather than exporting Change.

---

## Revision note

2026-07-19: Initial ExecPlan for IMPLEMENT_PLAN.md Phase 1 (WAL ingest + persist-before-ack).
