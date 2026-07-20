# Production-readiness audit — tether v0.2.0

Audit date: 2026-07-20. Auditor: read-only review + empirical tests against
Postgres 16 (`docker-compose.yml`, port 54321, `wal_level=logical`,
`max_replication_slots=64`). No source file was modified. Every claim below
cites `file.go:LINE` or a test name; anything I could not verify is labelled
`UNVERIFIED` and collected in §8.

Invariants referenced by number are defined in `AGENTS.md:59-101`
("Invariants — do not violate these", items 1–7).

---

## 1. Verdict

`tether` v0.2.0 is a **single-instance, single-writer** embedded sync engine that
is correct on its happy path but is not production-hardened: it has no automatic
reconnect when the replication connection drops (`engine.go:601-612`), an
unbounded in-memory per-shape log that is never freed while the process lives
(`shape/log.go:26-71`, cleared only on lag-drop at `engine.go:737`), and a
resume-offset scheme that is process-local with no epoch/identity, so any
multi-instance deployment can silently diverge (§Q1). It is usable today for a
**single host process** syncing **modest shapes** to reconnecting clients, where
an operator watches the process and restarts it on WAL errors. It is **not**
ready for horizontal scaling, large snapshots, or unattended operation, and the
project's own documented integration command (`make test-integration`) does not
currently pass (§3).

---

## 2. Answers to the blocking questions

### Q1 — More than one host replica: **single-instance only.**

Two host processes cannot share a slot, and giving them distinct slots makes
resume offsets diverge silently. Do not run more than one replica against one
slot name.

Evidence:

- **Slot name is fixed-or-configured, process-global, not per-process.** Default
  `tether_slot` (`engine.go:181`), overridable once per Engine via
  `WithSlotName` (`engine.go:69-72`). The consumer id is the hard-coded constant
  `"tether-engine"` (`engine.go:641`). Nothing derives either from the process,
  host, or PID.
- **Two processes, same config → the second fails fast.** I ran a throwaway
  program (two `tether.Engine`s, same slot/publication, same DB). Result:

  ```
  ENGINE B Run returned: wal: start replication: ERROR: replication slot
  "q1_slot_q131481" is active for PID 2616 (SQLSTATE 55006)
  after 6s: active-walsenders-on-slot=1
  ```

  `EnsureSlot` swallows "already exists" (`wal/publication.go:133-139`), so the
  second process proceeds to `StartReplication` on the live slot, which Postgres
  rejects with `55006`. That error is returned from `Consumer.Run` →
  `runConsumerCycle` (`engine.go:659-664`) → `Run` returns it (it is **not**
  `errRestartConsumer`, so no retry — `engine.go:606-611`). The host skeleton
  calls `log.Fatal` on a `Run` error (`docs/embed.md:46-48`), so the second
  replica **crashes on boot**. It does not share, and it does not corrupt the
  first (the first keeps the slot: `active-walsenders=1`).

- **The per-shape log offset is process-local.** `shape.Log` is an in-memory
  slice with a counter that starts at 1 on every `NewLog` (`shape/log.go:26-37`).
  Offsets are not persisted and not namespaced to any instance. A restart resets
  the counter to 1.

- **Cross-instance reconnect: safe only when the target has no stream; silently
  wrong when it does.** In `subscribeShape` the resume path only runs when the
  target instance already holds a stream for that `(shape, claimsKey)` key
  (`engine.go:392-408`). Two sub-cases, using distinct slots (the only viable
  multi-instance topology, since same-slot is impossible per above):
  - Target instance has **no** stream for the key → `exists==false`, control
    falls through to a fresh snapshot (`engine.go:429-464`). The client is
    force-resnapshotted. **Safe**, but every load-balancer reconnect to a cold
    instance is a full snapshot.
  - Target instance **has** a stream for the key (another client of the same
    tenant is already connected there) → `Log.After(foreignOffset)` runs against
    a different sequence space (`shape/log.go:85-104`). Trace with a foreign
    offset 50 against a target log holding offsets 1..30: `first=1`; the stale
    guard `offset < first-1` is `50 < 0` → false; the loop appends no event with
    `Offset > 50`; it returns `(nil, true)`. The server tells the client "you
    are caught up," then delivers future events numbered 31, 32, … which the
    client has already seen as *different rows* from instance 1. **Silent
    divergence. The stale offset is not detectable in the overlapping range.**

- **`max_replication_slots` ceiling.** Distinct-slot replicas each open one slot.
  When the ceiling is reached, `CreateReplicationSlot` returns an error that
  `EnsureSlot` does **not** swallow (only `42710`/"already exists" is swallowed,
  `wal/publication.go:133-139`), so `Run` returns it and the replica fails to
  start. Practical ceiling = `max_replication_slots` minus other consumers
  (compose sets 64; Postgres default is 10). Exact error string at the ceiling:
  UNVERIFIED (§8).

**Conclusion: single-instance only.** This sentence belongs in the README.

### Q2 — What bounds the initial snapshot: **nothing.**

The snapshot is fully materialized in memory and shipped as one WebSocket frame.
There is no row cap, no byte cap, no cursor, no pagination.

- **Fully materialized.** `snapshot.Take` runs one `SELECT … WHERE …` and
  appends every row to `out []map[string]any` (`snapshot/take.go:96-107`). A
  5,000,000-row shape is 5,000,000 `map[string]any` in RAM, then marshalled
  again into a single `proto.Snapshot` and enqueued as one buffer slot
  (`engine.go:449-462`). No `LIMIT`, no cursor, no streaming.
- **No cap.** There is no row-count or byte guard anywhere in `Take` or
  `subscribeShape`. Exceeding "reasonable" size is bounded only by OOM.
- **Snapshot vs. the slot-lag guard.** `Take` holds a `REPEATABLE READ READ ONLY`
  transaction open across the entire scan and marshal
  (`snapshot/take.go:69-113`). The lag guard measures
  `pg_current_wal_lsn() - restart_lsn` (`wal/lag.go:23-26`); `restart_lsn` is
  advanced by the consumer goroutine's acks (`wal/consumer.go:292-296`), which
  run independently of the snapshot. So a long snapshot does **not** retard
  `restart_lsn` and does **not** trip the guard. The hazard is the inverse: a
  long snapshot transaction pins the database's xmin horizon (blocking vacuum /
  bloating WAL), and the byte-lag guard is blind to that. **The guard does not
  protect against snapshot-induced pinning.** (xmin-pinning consequence is
  standard Postgres semantics, not measured by this code — see §8.)
- **Disconnect mid-snapshot.** `e.streams[key]` is set only *after* `Take`
  succeeds (`engine.go:441-447`). `Take` uses `r.Context()` (`engine.go:251`,
  `433`), which cancels on disconnect, so the query aborts, no stream is stored,
  and the client re-snapshots cleanly on reconnect. No partial state is exposed.

### Q3 — Is the filter path a security boundary: **yes, and it holds — with two caveats.**

The clause is parsed by a real (tiny) grammar and rebuilt with `$N`
placeholders; there is no textual `?`→`$N` substitution, so the jsonb-operator
corruption class does not apply. Client-supplied values never reach the SQL
string. Two caveats: `Where` can panic on a request path (the doc comment says
otherwise), and the one "isolation" test does not exercise the wire boundary.

- **Grammar.** `ParseWhere` accepts only AND-combined fragments of the forms
  `col = ?`, `col != ?` / `col <> ?`, `col IS NULL`, `col IS NOT NULL`
  (`shape/filter.go:30-79`). The right-hand side of a comparison must be exactly
  the token `?` or the fragment is rejected (`shape/filter.go:96-98`). Column
  identifiers are validated to `[A-Za-z_][A-Za-z0-9_]*` (`shape/filter.go:122-133`).
  Everything else — string literals, `LIKE`, casts, functions, `OR`, jsonb
  operators `?|` `?&` — is rejected as "unsupported where fragment."
- **No client value reaches the SQL string.** Values are carried as
  `predicate.value` and emitted only into the args slice by `SQLClause`
  (`shape/filter.go:230-260`); the SQL text contains only validated identifiers
  and `$N`. Because the RHS must be a bare `?`, there is no code path that
  substitutes a value into text. The "naive `?`→`$N` replacement corrupts `?|`"
  concern does not apply: there is no such replacement, and `?|` is rejected by
  the grammar before it could matter.
- **Filters cannot originate from the client (Invariant 2).** `subscribe`
  carries shape names only (`proto/messages.go:23-27`), and the filter is built
  by the host binder from `Claims` (`engine.go:388-429` → `shape.NewInstance` →
  `def.Bind(claims)` at `shape/apply.go:59`). Confirmed enforced.
- **Caveat A — `Where` CAN fire on a request path.** `tether.go:19` says
  *"Invalid clauses panic — that is a programmer error at registration time."*
  But the binder runs at **subscribe** time, not registration:
  `subscribeShape` → `shape.NewInstance(def, claims)` (`engine.go:429`) →
  `def.Bind(claims)` (`shape/apply.go:59`) → the host's
  `func(Claims) Filter{ return tether.Where(...) }`. If a binder builds a clause
  from claims that turns out invalid for some tenant, `Where` panics on the
  request path. It is contained — `serveWS` runs in the `net/http` handler
  goroutine, which recovers the panic and tears down that one connection (no
  process crash; there is no `recover()` in tether itself — the only `recover`
  in the tree is a test, `tether_test.go:44`). But the doc comment is wrong and
  the failure is a dropped connection, not a registration-time error.
- **Caveat B — host `fmt.Sprintf` into a clause is possible and undocumented as a
  host responsibility.** Nothing stops a host writing
  `tether.Where(fmt.Sprintf("org_id = %s", claim))`. If `claim` derives from a
  client-controlled token, that is injection — but it is injection the *host*
  wrote, outside tether's parser. `AGENTS.md:70-75` (Invariant 2) covers "client
  sends its own WHERE"; neither the README nor `docs/embed.md` explicitly warns
  hosts not to interpolate claim values into the clause string. Should be
  documented as a host responsibility.
- **`TestShapeFilterIsolation` is a partial test.** It constructs
  `shape.NewInstance` objects directly and feeds them decoded change_log rows
  (`shape/isolation_integration_test.go:94-201`); it asserts org 1 sees only the
  org-1 row and org 2 only the org-2 row. It does **not** exercise the WebSocket
  subscribe path, does not attempt a crafted/hostile claim, does not attempt a
  client-supplied predicate, and does not assert that a tenant-A socket can never
  receive a tenant-B row end-to-end. It proves per-instance filter routing, not
  the transport-level security boundary.

---

## 3. Baseline

All timings on the audit host (darwin/arm64, Go 1.26.3), Postgres 16 in Docker.

| Command | Result | Wall time |
|---|---|---|
| `go test ./...` (unit) | **PASS** | 2.87s |
| `go vet ./...` | **PASS** (0 issues) | — |
| `golangci-lint run` | **PASS** (0 issues) | — |
| `make test-integration` (`go test -race -tags=integration ./...`) | **FAIL** | ~17–51s/run |
| `make bench` | ran; numbers below | 39.4s |

Unit suite (`go test ./...`): all packages `ok`. `cmd/bench`, `internal/e2e`,
`internal/mutate`, `internal/proto`, `internal/snapshot` have no non-integration
test files.

**`make test-integration` does not pass.** Across four full runs it failed every
time, with a shifting set of tests:

| Test | Package | Failures / 4 full runs | Isolated |
|---|---|---|---|
| `TestSlotLag_ForcesResnapshot` | root | 4/4 | 3/3 PASS |
| `TestApply_IdempotentByKey` | internal/mutate | 3/3 (after first-ever run) | now FAILS |
| `TestApply_RejectRollsBackKey` | internal/mutate | 3/3 (after first-ever run) | now FAILS |
| `TestSnapshotStreamHandoff` | internal/snapshot | 1/4 | PASS |
| `TestConvergence` | internal/e2e | 0/4 | 3/3 PASS |

Three distinct root causes, all verified:

1. **`internal/mutate` — leaked idempotency keys (deterministic, not flaky).**
   The tests use stable keys `"k1-"+t.Name()` and `"k-reject-"+t.Name()`
   (`internal/mutate/apply_integration_test.go:36,56`) and never delete them.
   `tether.mutation_keys` persists across runs. On the failing run the *first*
   apply already returns `Duplicate:true`:
   ```
   apply_integration_test.go:38: first apply: res={Duplicate:true ...} err=<nil>
   ```
   I confirmed the leak directly:
   ```
   SELECT idempotency_key FROM tether.mutation_keys WHERE idempotency_key LIKE 'k1-%' ...
     k1-TestApply_IdempotentByKey
     k-reject-TestApply_RejectRollsBackKey
   ```
   The package passed only on the very first run against a fresh database; every
   run afterward fails, in isolation too. This is a test-hygiene defect, not an
   engine bug — but it means the documented command is red.

2. **`TestSlotLag_ForcesResnapshot` — flaky under the parallel suite
   (availability test).** Passes 3/3 in isolation (1.8s each) but fails 4/4 when
   `go test ./...` runs all packages concurrently against the one Postgres. The
   guard *does* fire (log: `slot lag exceeded … lag_bytes=…905 max_bytes=…904`)
   but the client does not observe `must_resnapshot` within the test's 10s
   deadline under load. A flaky availability test is itself a finding
   (`AGENTS.md:130-132` applies the same rule to convergence).

3. **`TestSnapshotStreamHandoff` — flaky handoff (correctness test).** Failed
   1/4 with an off-by-one row-set mismatch under concurrent writers:
   ```
   handoff_integration_test.go:208: row-set mismatch
     want (25): [... "113:t-113","114:t-114","116:t-116" ...]
     got  (24): [... "113:t-113","116:t-116" ...]   # id=114 missing
   ```
   This is the test that guards Invariant 4 (gapless handoff). Whether the miss
   is a real handoff gap or the test's `waitConsumerCaughtUp` heuristic being
   fooled under load, a flaky Invariant-4 test is a finding (§8 for what would
   settle which).

**`TestConvergence` is not flaky:** 3/3 PASS in isolation (~1.9s) and PASS in all
4 full runs.

**Bench** (`make bench`, 1 client, 5000 rows, batch=1):
```
insert_wall: 27.63s (185 rows/s commit throughput)
e2e_wall:    29.733s (172 rows/s insert→all clients)
commit_to_client_lag: p50=2.30s p95=3.91s p99=4.79s max=5.37s
```
The multi-second commit→client lag and ~185 rows/s ceiling reflect the fan-out
design: a 50ms ticker polls `tether.change_log` with `id > lastID`
(`engine.go:598,810-821`) rather than pushing decoded changes. Not wrong, but
worth stating plainly — this is a polling relay, not a low-latency push, and the
README's "kept live over a WebSocket" should be read with that latency in mind.

Flakiness across three consecutive runs: `TestConvergence` PASS/PASS/PASS;
`TestSlotLag_ForcesResnapshot` FAIL in every full-suite run.

---

## 4. README accuracy

Each roadmap claim marked **proven** (test exists and asserts the claim),
**partial** (asserts something weaker), or **unproven**.

| README claim | Cited test | Verdict | Note |
|---|---|---|---|
| WAL ingest, LSN checkpointing, resume after restart | `TestPersistBeforeAck_CrashReplay`, `TestRestartResumesWithoutGap` | **proven** | Both exist (`wal/consumer_integration_test.go:229,252`). Crash is simulated via `AfterDurableCommit` returning an error after the durable commit, before ack (`wal/consumer.go:283-296`); replay dedupe is real (`ON CONFLICT DO NOTHING`, `consumer.go:326`). It models crash-before-ack, not a process kill — a fair compression. |
| Single-table shapes with auth-bound filters | `TestShapeFilterIsolation` | **partial** | Proves per-instance filter routing; bypasses the wire path and has no hostile-claim or cross-tenant end-to-end case (see §Q3). |
| Gapless initial snapshot handoff | `TestSnapshotStreamHandoff` | **partial** | Asserts row-set equality (`handoff_integration_test.go:205-210`) but is flaky under load (§3). The claim's own guard test is not reliably green. |
| WebSocket transport with offset resume | `TestWebSocketResume` | **partial** | Proves resume *within one process/stream* (`handler_integration_test.go:114-125`). Does not cover resume across a restart or across instances — where offsets are a different sequence space (§Q1). |
| Server-authoritative mutations with idempotency | `TestMutationIdempotentAndVisible`, `TestApply_IdempotentByKey` | **proven-but-red** | Logic is correct (`mutate/apply.go:78-104`); the unit test currently fails from its own leaked keys (§3). |
| Slot lag guard and schema-drift halt | `TestSlotLag_ForcesResnapshot`, `TestApply_SchemaDriftHalts` | **partial** | Drift halt is proven (`shape/apply.go:87-90`; `TestSchemaDriftStopsConsumer` also asserts `ErrSchemaDrift`). Lag guard test is flaky under the suite (§3). |
| Correctness / convergence | `TestConvergence` | **partial (compressed, and correctly disclosed)** | See below. |
| Metrics: lag, slot size, per-client offsets | `TestMetrics_LagAndClientsSampled` | **proven** | Exists; metrics sampled per tick (`engine.go:686-688`). "slot size" is really restart-lsn lag, which the code comment admits (`metrics.go:12-13`). |
| Backpressure and slow-client eviction | `TestMetrics_SlowClientDisconnected`, `TestEnqueue_FullBuffer` | **proven** | Both exist; eviction path `engine.go:459-462,906-909`. |
| Structured logging | `TestWithLogger_DisconnectIncludesAttrs` | **proven** | Exists (`logger_test.go`). |

**The headline convergence claim, precisely.** `TestConvergence`
(`internal/e2e/convergence_integration_test.go:36`) does run two clients editing
overlapping rows (ids 1,2,3), one killed mid-write and cold-restarted, both
replaying full queues, and it asserts (a) no lost write, (b) exact apply count =
6 with no double-apply, (c) both clients and the DB converge, (d) a full re-replay
yields all-duplicates with unchanged apply count. That is a real convergence test.
Two compressions the README does not spell out:

- **"Both stay offline for ten minutes" is a `50ms` sleep**, explicitly commented
  as "README 'ten minutes' — not wall-clock" (`convergence_integration_test.go:136-137`).
  Fine as a compression; the README's Correctness section (`README.md:153-158`)
  states "ten minutes" without noting it is simulated.
- **"Killed and restarted cold" is the *client* restarting, not the server.** The
  engine runs continuously throughout; the kill is a WebSocket close mid-second-write
  and the "cold restart" is dropping client-local state and replaying the queue
  (`convergence_integration_test.go:139-152`). Durability across a *server* crash
  is covered separately by `TestPersistBeforeAck_CrashReplay`, not here. The README
  phrasing ("One is killed mid-write and restarted cold") reads as a process crash;
  the test models a client reconnect.

**Behaviour the README describes that no longer matches the code:**

- `tether.go:19` — "Invalid clauses panic … at registration time." The binder,
  and therefore `Where`, runs at subscribe time (§Q3 Caveat A).
- `README.md:12,14` show `tether.New(pgURL)` and `engine.Shape(...)` returning no
  error; the real signatures return `(*Engine, error)` and `error`
  (`engine.go:174,205`). Illustrative snippet, but it will not compile.
- `README.md:5,9` "kept live over a WebSocket" implies push latency; the transport
  is a 50ms table poll with multi-second p50 lag (§3). Not false, but easy to
  over-read.

---

## 5. Failure-mode inventory

Behaviour + citation, then risk. No fixes here (see §6).

1. **Postgres restarts / connection drops mid-stream.** `Consumer.Run` returns
   the receive error (`wal/consumer.go:102`), `runConsumerCycle` returns it
   (`engine.go:659-664`), and `Run` returns it because it is not
   `errRestartConsumer` (`engine.go:606-611`). **There is no reconnect or
   backoff.** The engine exits; the host skeleton `log.Fatal`s
   (`docs/embed.md:46-48`), so the process dies. Risk: **availability — high.**
   A routine Postgres failover takes tether down.

2. **Replication slot dropped externally by a DBA.** Same exit path (the next
   receive or `StartReplication` errors → `Run` returns). On restart,
   `resolveStartLSN` loads the stale checkpoint LSN (`wal/consumer.go:180-195`)
   and starts replication from it on the *recreated* slot. The lag-drop path
   deletes the checkpoint precisely to avoid this (`engine.go:746-749`), but the
   external-drop path gets no such cleanup, so the window between the stale LSN
   and the new slot's creation point can be **skipped** — a silent gap — or the
   start LSN is rejected. Risk: **data-loss / correctness — high** (exact PG
   behaviour on start-before-slot: §8).

3. **Slot-lag guard fires (`ErrSlotLagExceeded`).** Path: tick samples lag
   (`engine.go:679-701`) → cancels consumer → `handleSlotLagExceeded`
   (`engine.go:733-754`): broadcast `must_resnapshot` to all sessions, clear
   `e.streams`, clear each session's membership, `DropSlot`, delete checkpoint,
   reset the fan-out cursor to `MAX(id)` → return `errRestartConsumer` → `Run`
   loops and recreates the slot (`engine.go:601-612,629`). This is the one
   self-healing path and it is coherent: clients are told to resnapshot *before*
   the slot is dropped, and the fan-out cursor is advanced past the old change_log
   so stale deltas are not replayed. Gap risk is low **provided** the client
   honours `must_resnapshot` by wiping state (host responsibility,
   `docs/embed.md:60-61`). Risk: **availability — medium** (works, but every
   affected client re-snapshots; with no snapshot bound, §Q2, a lag event on a
   big shape is a thundering re-snapshot).

4. **Schema drift.** Halt is **per-shape** at the fingerprint check
   (`shape/apply.go:87-90,163-177`; `Log.Halt` at `shape/log.go:39-46`), and the
   *consumer* also halts globally if drift is seen at decode time
   (`wal/consumer.go:197-209` returns `ErrSchemaDrift` → `Run` exits). So a DDL
   on one synced table halts that shape for clients *and* can stop the whole
   consumer. Recovery is undocumented: there is no operator procedure, no
   "resume shape after migration" API. Risk: **operability — medium.**

5. **A `Metrics` implementation blocks or panics (Invariant 7).** Documented as
   "must not block" (`metrics.go:4-9`) but **not enforced.** Calls are synchronous
   and on the ingest path: `ReplicationLagBytes` in the Run tick
   (`engine.go:686-688`), `ClientOffset` inside fan-out
   (`engine.go:480-484,500`). A blocking sink stalls the tick loop (fan-out, idle
   sweep, lag sampling all live there). A panicking sink panics the `Run`
   goroutine — which is **not** wrapped by `net/http`, so it crashes the process.
   Risk: **availability — medium** (a bad host metrics adapter takes the engine
   down; Invariant 7 is documentation, not a guardrail).

6. **`OnMutation` panics, blocks, or leaks the tx.** Runs in the per-connection
   `serveWS` goroutine via `mutate.Apply` (`engine.go:349-358`). **Panic:**
   recovered by `net/http`; `defer tx.Rollback` (`mutate/apply.go:76`) runs during
   unwind, so the tx is released and only that client drops. **Blocks forever:**
   only that client's read loop stalls, but it holds an open transaction and a
   pooled connection for the whole time — there is no handler timeout. Enough
   slow/stuck handlers exhaust the pool and stall all mutations. **Tx leak:** not
   on panic (deferred rollback); but a handler that spawns work referencing `tx`
   after return would use-after-commit — undocumented. Risk: **availability —
   medium.**

7. **Malformed / enormous / high-rate frames.** Malformed JSON → `bad_protocol`
   error, connection kept (`engine.go:280-283,300-303`). Enormous frame → the
   `coder/websocket` default read limit (32 KiB; no `SetReadLimit` anywhere,
   `transport/conn.go:92-98,133-141`) makes `Read` error → the connection is
   closed. High rate → **no rate limiting**; each mutation does a DB transaction,
   so a client can drive unbounded DB load on its own connection. Risk:
   **availability — low/medium** (per-connection DoS; no global limiter).

8. **Slow-client eviction — buffered-state cleanup.** Eviction sends `bye` and
   closes the socket (`engine.go:467-478` → `transport/conn.go:101-130`); the
   send channel is closed, and when the read loop returns the `serveWS` defers
   remove the session and hub entry and cancel the write pump
   (`engine.go:249,262-267,269-271`). Goroutines are cleaned. **But the stream
   Instance is never removed:** `e.streams` is only ever cleared wholesale on a
   lag-drop (`engine.go:737`); there is no per-key deletion on disconnect, and
   `shape.Log` is append-only with no trim (`shape/log.go:55-71`). So each
   `(shape, claimsKey)` accumulates an unbounded in-memory event log that lives
   for the whole process lifetime even after every subscriber leaves. `-race`
   integration runs reported no data races; there is no `goleak` in the tree
   (§8), so goroutine-leak freedom is unverified, but the **memory** leak is
   structural and certain. Risk: **availability — high** (steady memory growth;
   OOM on a long-lived process with churn or large shapes).

9. **SIGTERM mid-snapshot / mid-mutation.** tether installs no signal handler;
   graceful shutdown depends on the host cancelling `Run`'s ctx and calling
   `Close` (`docs/embed.md:43-49`). On ctx-cancel: an in-flight snapshot query
   aborts and stores nothing (§Q2); an in-flight mutation tx aborts via deferred
   rollback; the consumer only ever acked durable LSNs (Invariant 1,
   `wal/consumer.go:289-296`), so no data is lost; the slot is left intact and
   resumable. `Close` deliberately drops the pool without holding `e.mu`
   (`engine.go:935-948`, regression-tested by `TestClose_WhileFanOut`). Shutdown
   is graceful **iff** the host wires ctx+Close; there is no in-library drain or
   `bye`-to-all on shutdown. Risk: **operability — low.**

---

## 6. Gap list

Sorted by risk. "Blocks v1?" is judged against the README's own scope and
non-goals (`README.md:191-193`), not against larger products.

| # | Gap | Evidence | Risk | Blocks v1? | Effort |
|---|-----|----------|------|------------|--------|
| 1 | Cross-instance resume silently diverges: process-local offsets with no epoch/identity; `After()` returns empty-ok for a foreign in-range offset | `shape/log.go:26-37,85-104`; `engine.go:392-408`; §Q1 | data-loss / correctness | **Yes** — but only if multi-instance is ever allowed. Fix = document + enforce single-instance, or add offset epochs | M (doc/guard) – L (epochs) |
| 2 | External slot drop → stale checkpoint → silent WAL gap on restart (no checkpoint cleanup on external drop) | `wal/consumer.go:180-195`; `engine.go:746-749` (contrast) | data-loss / correctness | **Yes** | M |
| 3 | Unbounded in-memory per-shape log; streams never freed on disconnect | `shape/log.go:55-71`; `engine.go:737` only clear site | availability | **Yes** | M |
| 4 | No reconnect/backoff on WAL connection loss → engine exits on Postgres restart/failover | `engine.go:606-611`; `wal/consumer.go:102` | availability | **Yes** | M |
| 5 | Initial snapshot fully materialized, no row/byte cap, single frame → OOM on large shapes | `snapshot/take.go:96-107`; `engine.go:449-462` | availability | **Yes** | M–L |
| 6 | `make test-integration` red: mutate tests leak stable keys; lag/handoff tests flaky under the suite | §3; `mutate/apply_integration_test.go:36,56` | operability (CI credibility) | **Yes** | S (mutate) / M (flakes) |
| 7 | Invariant 7 not enforced: blocking/panicking `Metrics` stalls or crashes ingest | `metrics.go:4-9`; `engine.go:686-688,480-484` | availability | No | S (isolate calls) |
| 8 | `Where` panics on the request path; doc says registration-time only | `tether.go:19`; `engine.go:429`; `shape/apply.go:59` | operability / DX | No | S |
| 9 | `OnMutation` has no timeout; a blocking handler holds a tx + pooled conn indefinitely | `engine.go:349-358`; `mutate/apply.go:72-104` | availability | No | S–M |
| 10 | No per-connection rate limit on mutations | `engine.go:308-314` | availability | No | M |
| 11 | Schema-drift recovery undocumented; can also stop the whole consumer, not just the shape | `wal/consumer.go:197-209`; `shape/apply.go:87-90` | operability | No | S (doc) |
| 12 | `TestShapeFilterIsolation` does not cover the wire boundary or hostile claims | `shape/isolation_integration_test.go` | security (test coverage) | No | M |
| 13 | Protocol version declared but not enforced (mismatch is non-fatal, socket stays usable) | `engine.go:292-295`; `proto/messages.go:9` | DX | No | S |
| 14 | README examples don't compile; "ten minutes"/"killed cold" over-state the convergence test | `README.md:12-21,153-158`; §4 | DX | No | S |

Proposed fix for the cheapest correctness-adjacent item (#6, mutate leak) — a
recommendation, **not applied**:

```go
// internal/mutate/apply_integration_test.go — clean the shared table per test
func TestApply_IdempotentByKey(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := mutate.EnsureSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	key := "k1-" + t.Name()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM tether.mutation_keys WHERE idempotency_key = $1`, key)
	})
	// ... use `key` instead of the inline literal ...
}
```

Proposed guard for #1/#3 (single-instance intent made explicit) —
recommendation only:

```go
// engine.go — free the stream when its last subscriber leaves, and refuse a
// resume offset that predates this process (offsets are process-local).
// Sketch; real change needs subscriber refcounting per streamKey.
func (e *Engine) releaseStream(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.streams[key]; ok && s.subscribers == 0 {
		delete(e.streams, key)
	}
}
```

---

## 7. Adoption

### Wire protocol (reconstructed from `internal/proto/messages.go` + `transport/conn.go` + `engine.go`)

Transport: WebSocket **text** frames, one JSON object per frame, discriminated by
`"type"`. Server read limit 32 KiB (coder/websocket default; not overridden).
`ProtocolVersion = 1` (`proto/messages.go:9`).

| Type | Dir | Fields | Notes / ordering |
|---|---|---|---|
| `hello` | C→S | `protocol int`, `resume {shape:offset}?` | Send first if resuming. Sets session resume map (`engine.go:286-298`). **No server ack.** `protocol` other than 0 or 1 → `bad_protocol` error, but the socket stays open and usable (not enforced). |
| `subscribe` | C→S | `shapes []string` | Per shape: unknown → `error unknown_shape`; else snapshot or resume (`engine.go:299-307,370-386`). |
| `mutation` | C→S | `id`, `op`, `key`, `args {}?` | `key` = idempotency key; `id` = client correlation id echoed back (`engine.go:308-314,321-368`). |
| `snapshot` | S→C | `shape`, `lsn`, `offset`, `rows []{}` | Full row set at handoff. `offset` = last offset after seeding; resume from it. `lsn` is `"0/0"` when replaying an existing in-memory stream (`engine.go:410-416`) — informational only, not resumable. |
| `change` | S→C | `shape`, `offset`, `op` (insert/update/delete), `row {}` | Monotonic per (shape, claimsKey) **within one process only**. |
| `error` | S→C | `code`, `message`, `shape?` | Codes: `must_resnapshot`, `unauthorized`, `bad_protocol`, `unknown_shape`, `shape_halted`, `no_mutation_handler` (`proto/messages.go:84-90`). On `must_resnapshot`/`shape_halted` the client must wipe local state and resubscribe (`docs/embed.md:58-61`). |
| `bye` | S→C | `reason` | `slow_client`, `idle_client`, `shutdown` (`proto/messages.go:92-94`). Sent before the socket closes. |
| `mutation_ok` | S→C | `id`, `duplicate?` | `duplicate:true` = idempotency key already applied (`engine.go:367`). |
| `mutation_reject` | S→C | `id`, `reason` | Business reject; client rolls back optimistic state (`engine.go:363-366`). |

Resume semantics: client persists the highest `offset` it has fully applied and
sends it in `hello.resume`. On resubscribe, if the server process still holds
that stream and the offset is in range → deltas after it; if the offset predates
the retained window → `must_resnapshot`; if the process has no stream for the key
→ a fresh `snapshot` (the offset is ignored). **Offsets are meaningful only to the
process that issued them** (§Q1) — there is no epoch, so a client cannot detect
that it is talking to a different process.

### Could a competent TypeScript dev build a client from this repo alone?

Read-path: **yes, in about a day.** The format is plain JSON over a WS text
socket and the message set is small. Write-path and resume-correctness:
**not safely, without guessing**, because the following are undocumented and only
discoverable by reading Go:

- Mutations queued offline must be replayed **in original order** with stable
  idempotency keys — stated in prose (`README.md:106-107`) but not in any protocol
  doc, and ordering is entirely the client's responsibility.
- The offset is "last *applied* offset," and the epoch hazard above is invisible
  from the wire — a naive client that reuses an offset across a server restart or
  LB hop can silently desync (§Q1).
- The 32 KiB frame limit is undocumented; a large mutation just disconnects.
- `snapshot.lsn` is sometimes `"0/0"` and must be ignored for resume.
- `internal/proto` cannot be imported (Go `internal/`), so there is no shared
  type source; a TS client must hand-mirror the structs and keep them in sync
  manually.

Missing for real adoption: a versioned protocol spec (this table is a
reconstruction, not a contract), an offset-persistence + reconnect state-machine
doc, and at least one reference client.

### Is the protocol versioned? v0.2 client vs v0.3 server?

Versioned in name only. `hello.protocol` is checked against
`ProtocolVersion=1`, and a mismatch sends a `bad_protocol` error — but then
`continue`s the read loop (`engine.go:292-295`), leaving the socket open and
still processing `subscribe`/`mutation`. So a future v2 server would *warn* a v1
client and then serve it anyway (or vice-versa), with no negotiated downgrade and
no hard stop. A version bump today would not cleanly reject incompatible peers.

---

## 8. What I could not verify (UNVERIFIED)

Section 8 is deliberately exhaustive; an audit claiming total coverage is not
credible.

1. **`max_replication_slots` ceiling error string / exact failure at the ceiling
   (§Q1).** I verified by code that a non-"already exists" `CreateReplicationSlot`
   error propagates out of `Run` (`wal/publication.go:133-139`), but I did not
   exhaust the 64 configured slots to observe the exact SQLSTATE/message. *To
   verify:* set `max_replication_slots` low (e.g. 2), start N+1 distinct-slot
   engines, capture the (N+1)th `Run` error.

2. **Snapshot xmin/WAL pinning (§Q2).** I verified what the lag guard measures
   (`restart_lsn` only, `wal/lag.go:23-26`) and therefore that a long snapshot
   does not trip it. I did **not** measure the snapshot transaction's effect on
   the database's xmin horizon / vacuum / WAL retention. *To verify:* open a
   `REPEATABLE READ` snapshot over a multi-million-row shape and watch
   `pg_stat_activity.backend_xmin`, `pg_database_size`, and dead-tuple bloat while
   it runs.

3. **External-slot-drop gap vs. start error (Failure mode 2).** I verified the
   engine exits and that the external-drop path skips the checkpoint cleanup the
   lag path does (`engine.go:746-749`). I did **not** reproduce a DBA drop +
   process restart to observe whether Postgres skips the gap silently or rejects
   the stale start LSN. *To verify:* run the engine, `pg_drop_replication_slot`
   out-of-band, insert rows, restart the process, diff delivered rows vs. table.

4. **`TestSnapshotStreamHandoff` root cause (§3).** I observed a 1/4 off-by-one
   row-set mismatch under load but did not determine whether it is a genuine
   Invariant-4 handoff gap or the test's `waitConsumerCaughtUp` heuristic
   declaring catch-up early. *To verify:* run the test in a loop in isolation with
   the concurrent-writer load fixed to a deterministic schedule, and log the
   change_log tail vs. snapshot LSN at the mismatch.

5. **Cross-instance divergence end-to-end (§Q1, gap #1).** I proved the mechanism
   by reading `Log.After` (`shape/log.go:85-104`) and traced a concrete foreign
   offset by hand; I did **not** stage two distinct-slot engines behind a load
   balancer and observe a client receiving wrong deltas, because I could not
   import `internal/shape` from an out-of-tree throwaway. *To verify:* add an
   in-tree throwaway test (temporarily) that appends 30 events to one `Log` and
   calls `After(50)`, asserting `(nil, true)`; then a two-engine e2e.

6. **Goroutine-leak freedom under slow-client eviction (Failure mode 8).**
   `-race` integration runs reported no data races, but there is no `goleak` in
   the module (`grep` found none) and I did not add one. The **memory** leak
   (unbounded `e.streams` / append-only logs) is verified by code; goroutine
   cleanliness is not independently confirmed. *To verify:* wrap the slow-client
   and idle tests with `goleak.VerifyNone`.

7. **`Metrics` panic crashing the process (Failure mode 5).** I traced that the
   tick-path metric calls run in the `Run` goroutine (`engine.go:686-688`), which
   is not `net/http`-wrapped, so a panic there is unrecovered. I did not run a
   panicking `Metrics` to observe the crash. *To verify:* inject a `Metrics` whose
   `ReplicationLagBytes` panics and confirm `Run`'s goroutine takes the process
   down.

8. **Behaviour of a real process kill mid-write (README "killed cold").** The
   convergence test simulates this at the client layer with a WS close
   (`convergence_integration_test.go:139-152`); I did not `kill -9` a host process
   mid-mutation and restart it. Durability is argued from Invariant 1 +
   `TestPersistBeforeAck_CrashReplay`, not from a real crash.

9. **`bench` numbers are one host, one run, batch=1.** p50≈2.3s reflects the 50ms
   poll relay under a specific machine load; I did not run `make bench-lag` (the
   batched COPY variant) or repeat for variance. Treat the numbers as directional.
```
