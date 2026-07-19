# Benchmarking tether

Tether-only microbenches against local Postgres (`wal_level=logical`).
Use them to regression-check builds and size buffers on **your** hardware —
not to claim wins against Electric, PowerSync, Zero, or Debezium.

Harness lives in [`cmd/bench`](../cmd/bench). Phases A–B are implemented;
C–F are specified here for implementation and for recording results once
the targets land in the Makefile.

## Scorecard (North Star)

Targets are **aspirational** for a well-tuned LAN deploy. Local `make bench`
numbers today will usually miss the latency goals until the harness and
ingest path are improved (batch inserts, less fsync contention, warm paths).

| Metric                                |            Target | Status                                                                                                          |
| ------------------------------------- | ----------------: | --------------------------------------------------------------------------------------------------------------- |
| Commit → Client latency (p50/p95/p99) | &lt; 50 ms on LAN | **Phase B** — `make bench-lag`: lag = WS recv − wall clock when INSERT/COPY returned; reports p50/p95/p99. LAN &lt;50ms still aspirational on stock laptop + sync WAL. |
| Max concurrent WebSocket clients      |              10k+ | **Phase E** — not measured yet; needs soak harness + OS `ulimit` / fd tuning.                                   |
| Resume after reconnect                |       &lt; 100 ms | **Phase C** — not measured yet; connect → `hello.resume` → first delta.                                         |
| Snapshot 1M rows                      |   time in seconds | **Phase D** — not measured yet; `LoadSnapshot` + WS snapshot send wall clock.                                   |
| Memory per connection                 |         KB / conn | **Phase E** — not measured yet; `runtime.MemStats` before/after N conns.                                        |
| CPU at 1k / 5k / 10k clients          |        % or cores | **Phase E** — not measured yet; steady idle + light traffic profiles.                                           |
| WAL ingest throughput                 |              MB/s | **Phase F** — not measured yet; bulk load + `pg_current_wal_lsn` delta / time.                                  |
| Broadcast latency                     |         p50 / p99 | **Phase B** — same series as commit→client for single-shape fan-out.                                            |

### Legend

| Status       | Meaning                                                                      |
| ------------ | ---------------------------------------------------------------------------- |
| Phase B      | Automated via `make bench` / `make bench-lag`                                |
| Phase C–F    | Specified below; Makefile target pending unless noted                        |
| Not measured | No automated command yet                                                     |
| Planned      | Intended next harness work                                                   |

## Prerequisites (all phases)

```bash
make db-up
export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
```

Postgres must report `wal_level=logical` (`make db-check` asserts this).
Override flags with `BENCH_ARGS='...'` on Make targets, or `go run ./cmd/bench ...`.

**Known bias (shared):** localhost Docker Postgres with default
`synchronous_commit`. Lag and throughput include WAL decode + durable shape
log persist + fan-out. Treat results as a **regression check**, not a
marketing ceiling.

---

## Phased harness overview

| Phase | Command (proposed)    | Status        | Covers                                                         |
| ----- | --------------------- | ------------- | -------------------------------------------------------------- |
| A     | `make bench`          | **Shipped**   | Rough commit→WS lag + rows/s                                   |
| B     | `make bench-lag`      | **Shipped**   | Commit-aligned lag, p50/p95/p99, `-batch` COPY inserts         |
| C     | `make bench-resume`   | Spec only     | Reconnect + offset → first change &lt; 100 ms target           |
| D     | `make bench-snapshot` | Spec only     | Snapshot wall clock for N rows (incl. 1M)                      |
| E     | `make bench-scale`    | Spec only     | N clients (1k/5k/10k): mem/conn, CPU%, disconnect rate         |
| F     | `make bench-wal`      | Spec only     | WAL MB/s ingest with subscribers=0 or sink-only                |

Phases C–F need careful machine prep (`ulimit -n`, Docker shm, enough RAM).
Do not run 10k clients on a default laptop and declare failure.

---

## Phase A — Baseline insert → WebSocket

### Purpose

Smoke-level end-to-end path: engine + WebSocket subscribers see rows after
inserts. Useful as a quick regression after WAL/transport changes when you
do not care about percentile lag under batched commits.

### Metric definitions

| Metric              | Definition                                                                 |
| ------------------- | -------------------------------------------------------------------------- |
| `insert_wall`       | Wall clock of all INSERT/COPY calls (warmup + measured rows).              |
| Commit throughput   | `(warmup + rows) / insert_wall` (rows/s).                                  |
| `e2e_wall`          | Wall from first insert start until every client has seen every row.        |
| E2E throughput      | `rows / e2e_wall` (rows/s; uses measured row count only in the rate label). |
| `commit_to_client_lag` | Per-row: WS `change` recv − wall clock when that row’s commit returned. Warmup IDs excluded from lag samples. |

### Command

```bash
make bench
# defaults (cmd/bench): -rows=5000 -clients=1 -warmup=100 -batch=1 -buffer=256
# or:
go run ./cmd/bench -batch=1 -rows=5000 -clients=2 -warmup=100
```

### Flags (`cmd/bench`)

| Flag       | Default | Meaning                                              |
| ---------- | ------: | ---------------------------------------------------- |
| `-dsn`     | env     | Postgres DSN (`TETHER_TEST_DSN` if empty)            |
| `-rows`    |    5000 | Measured inserts after warmup                        |
| `-clients` |       1 | Concurrent WebSocket subscribers on one shape        |
| `-warmup`  |     100 | Insert discarded from lag percentiles                 |
| `-batch`   |       1 | Rows per commit (`1` = single `INSERT`)              |
| `-buffer`  |     256 | `WithClientBuffer` size                              |

### How to read results

Example:

```text
bench: table=bench_… clients=1 warmup=100 rows=5000 batch=1
received: 5100 / 5100 (clients×rows including warmup)
insert_wall: 2.1s (2429 rows/s commit throughput)
e2e_wall:    2.4s (2083 rows/s insert→all clients)
commit_to_client_lag: n=5000 p50=… p95=… p99=… max=…
```

- Prefer comparing **two git SHAs** on the same machine and Docker image.
- `batch=1` stresses single-row commit latency; expect higher lag than Phase B.

### Pass / fail

| Check                         | Pass condition                                      |
| ----------------------------- | --------------------------------------------------- |
| Completeness                  | `received == clients × (warmup + rows)`             |
| Process exit                  | Exit `0` when all samples received; `1` otherwise   |
| Scorecard (&lt;50 ms p99 LAN) | **Not required** for Phase A; treat as regression baseline only |

### Machine prep

Default laptop + `make db-up` is enough. No special `ulimit` needed.

---

## Phase B — Commit-aligned lag percentiles

### Purpose

Measure **commit → client** lag the way the scorecard defines it: clock starts
when the writer’s `INSERT`/`CopyFrom` returns (durable from the app client’s
point of view), ends when a subscriber’s WebSocket receives the matching
`change`. Batched COPY amortizes commit cost so lag reflects the sync path
more than Postgres fsync-per-row.

Also covers **broadcast latency** for a single shape (same sample series when
`-clients` &gt; 1, one sample per client×row).

### Metric definitions

Same as Phase A, plus:

| Metric     | Definition                                                                 |
| ---------- | -------------------------------------------------------------------------- |
| p50 / p95 / p99 / max | Nearest-rank percentiles over lag samples after warmup.                |
| Lag origin | `commitAt[id]` = `time.Now()` immediately after commit return for that id (shared timestamp for all ids in a COPY batch). |

### Command

```bash
make bench-lag
# Phase B defaults: -batch=100 -rows=2000 -warmup=100 -clients=1
# override:
make bench-lag BENCH_ARGS='-clients=4 -rows=5000'
```

### Flags

Same as Phase A. Phase B’s Make target pins batch/rows/warmup/clients for
comparable nightlies; override via `BENCH_ARGS`.

### How to read results

```text
commit_to_client_lag: n=2000 p50=12.4ms p95=28.1ms p99=41.0ms max=55.2ms
note: lag = WS recv − wall clock when INSERT/COPY commit returned …
```

- Track **p99** across releases; cliffs usually mean fan-out stall, buffer
  pressure, or WAL persist regression.
- Multi-client runs: sample count ≈ `clients × rows` (each client contributes).

### Pass / fail

| Check            | Pass condition                                                                 |
| ---------------- | ------------------------------------------------------------------------------ |
| Completeness     | All measured rows received on all clients                                      |
| Scorecard target | p99 &lt; 50 ms on a **tuned LAN** with low fsync contention — aspirational locally |
| Regression       | Fail CI/nightly if p99 regresses &gt;2× vs last known-good SHA on same host    |

Hitting &lt;50 ms p99 on Docker + default `synchronous_commit` is **not**
expected; do not treat a laptop miss as a product bug by itself.

### Machine prep

Same as Phase A. Prefer a quiet machine (no concurrent heavy docker workloads).

---

## Phase C — Resume after reconnect

**Status:** Spec only — add `make bench-resume` (likely a mode or sibling under
`cmd/bench`).

### Purpose

Reconnects dominate real client lifetimes (Invariant: resume paths get more
exercise than the happy path). Measure time from reconnect handshake to the
**first live `change`**, when the client already holds a valid offset from a
prior session.

Wire protocol (existing):

1. First session: `hello` → `subscribe` → consume `snapshot` + `change`s;
   remember last `change.offset` (or `snapshot.offset` if no changes).
2. Disconnect.
3. Second session: `hello` with `resume: { "<shape>": <offset> }` →
   `subscribe` → expect **no full snapshot** when offset is still in the
   in-process stream; first subsequent `change` is the resume hit.

### Metric definitions

| Metric              | Definition                                                                 |
| ------------------- | -------------------------------------------------------------------------- |
| `resume_to_first`   | Clock starts after successful WS dial on reconnect (or after `subscribe` write — pick one and document in the harness README line); ends when first `change` with `offset > resume` is received. |
| Warm path           | At least one live insert already flowing / stream hot before disconnect.   |
| Cold resume         | Optional second mode: idle stream, then insert after reconnect (reports separately). |

Default scorecard metric is **warm path** `resume_to_first` p50/p99 over
`-iters` reconnect cycles.

### Command (proposed)

```bash
make bench-resume
# proposed defaults:
go run ./cmd/bench -mode=resume -iters=50 -clients=1 -warmup=10
```

### Flags (proposed)

| Flag       | Default | Meaning                                              |
| ---------- | ------: | ---------------------------------------------------- |
| `-mode`    | resume  | Select resume harness                                |
| `-iters`   |      50 | Reconnect cycles after initial snapshot              |
| `-clients` |       1 | Parallel resume clients (fan-out resume pressure)    |
| `-gap-ms`  |       0 | Sleep between disconnect and reconnect               |
| `-buffer`  |     256 | Per-client buffer (same as A/B)                      |

Each iteration: ensure an insert after resume so a `change` is guaranteed
(unless measuring pure catch-up of already-buffered events — then insert
before disconnect and resume to the buffered tail; report both if useful).

### How to read results

```text
resume_to_first: n=50 p50=12ms p95=40ms p99=80ms max=95ms
must_resnapshot: 0
```

- Any `error` with `must_resnapshot` means the offset fell out of the
  in-memory stream — count them; they invalidate “resume &lt; 100 ms” and
  indicate buffer/retention limits, not latency.

### Pass / fail

| Check              | Pass condition                                      |
| ------------------ | --------------------------------------------------- |
| Completeness       | Every iteration receives a first `change` or an explicit `must_resnapshot` |
| Scorecard          | Warm `resume_to_first` p99 &lt; 100 ms on LAN       |
| Correctness        | No gap/dup vs inset row ids (optional assert)       |
| `must_resnapshot`  | 0 under default buffer for short disconnects        |

### Machine prep

Same as A/B. Do not combine with Phase E 10k-client runs.

---

## Phase D — Snapshot wall clock

**Status:** Spec only — add `make bench-snapshot`.

### Purpose

Time the gapless handoff path clients always hit on first subscribe (Invariant
4): filtered `SELECT` at LSN _N_, WS `snapshot` delivery, then live stream.
Scorecard cares especially about **1M rows**.

### Metric definitions

| Metric                | Definition                                                                 |
| --------------------- | -------------------------------------------------------------------------- |
| `snapshot_query`      | Wall inside server/harness around snapshot take (`LoadSnapshot` / SQL).    |
| `snapshot_ws_send`    | Wall from snapshot ready until client finishes reading the `snapshot` frame (or last chunk if framed later). |
| `snapshot_e2e`        | Client: after `subscribe` write → fully decoded `snapshot` message.        |
| Row count / bytes     | `len(rows)` and approximate JSON payload size.                             |

Split query vs WS send so a slow encode path is distinguishable from a slow
`SELECT`.

### Command (proposed)

```bash
make bench-snapshot
# proposed:
go run ./cmd/bench -mode=snapshot -rows=1000000 -pad-bytes=64 -clients=1
```

Pre-populate the table (or shape filter) to exactly `-rows` matching rows
**before** opening the WebSocket, so timed work is snapshot-only (no insert
storm during the measured window).

### Flags (proposed)

| Flag         | Default | Meaning                                           |
| ------------ | ------: | ------------------------------------------------- |
| `-mode`      | snapshot| Snapshot harness                                  |
| `-rows`      | 1000000 | Target populated row count                        |
| `-pad-bytes` |      64 | Extra payload per row (`pad` column)              |
| `-clients`   |       1 | Concurrent first-time subscribers (fan-out snap)  |
| `-filter`    | org=1   | Keep filter server-side from claims (never client)|

### How to read results

```text
snapshot: rows=1000000 bytes≈… query=12.4s ws_send=3.1s e2e=15.6s
```

Report seconds (or ms for small N). Always publish row width (`pad-bytes`)
with the number — 1M empty rows ≠ 1M wide rows.

### Pass / fail

| Check         | Pass condition                                                      |
| ------------- | ------------------------------------------------------------------- |
| Completeness  | Client `snapshot` row count equals DB filtered count                |
| Handoff       | Following live inserts appear once, no dup of snapshotted ids       |
| Scorecard     | Record `snapshot_e2e` for 1M; no hard pass SLA yet — track trend    |

Integration correctness remains `TestSnapshotStreamHandoff`; this phase is
timing only.

### Machine prep

- Enough RAM for 1M-row result sets in Go + WS buffers (several GB depending
  on `pad-bytes`).
- Raise Docker memory / Postgres `shared_buffers` if the query phase stalls.
- Prefer SSD-backed Docker volumes.

---

## Phase E — Concurrent client scale

**Status:** Spec only — add `make bench-scale`.

### Purpose

Size embedders for connection count: memory per connection, CPU at 1k / 5k /
10k idle (and light traffic) clients, and disconnect rate under backpressure
(Invariant 7: full buffer → disconnect that client, never stall WAL).

### Metric definitions

| Metric             | Definition                                                                 |
| ------------------ | -------------------------------------------------------------------------- |
| `clients_ok`       | WebSockets that completed hello+subscribe and remain open.                 |
| `mem_per_conn`     | `(HeapAlloc_after − HeapAlloc_before) / clients_ok` (and optionally `Sys`). Force `runtime.GC()` before each sample. |
| `cpu_pct`          | Process CPU during steady window (e.g. `time`, `rusage`, or external profile). |
| `disconnect_rate`  | `bye` / `slow_client` closes per minute under a defined insert rate.       |
| Light traffic      | Fixed inserts/s across the shape while N clients are connected.            |

Run at least three presets: **1000**, **5000**, **10000** clients (skip
tiers the machine cannot sustain and record why).

### Command (proposed)

```bash
make bench-scale
# proposed tiers:
go run ./cmd/bench -mode=scale -clients=1000 -hold=60s -traffic-rps=100
go run ./cmd/bench -mode=scale -clients=5000 -hold=60s -traffic-rps=100
go run ./cmd/bench -mode=scale -clients=10000 -hold=60s -traffic-rps=0   # idle first
```

### Flags (proposed)

| Flag           | Default | Meaning                                         |
| -------------- | ------: | ----------------------------------------------- |
| `-mode`        | scale   | Scale harness                                   |
| `-clients`     |    1000 | Target concurrent subscribers                   |
| `-hold`        |     60s | Steady-state measurement window                 |
| `-traffic-rps` |     100 | Insert insert rate during hold (`0` = idle)       |
| `-buffer`      |      64 | Smaller buffer = sooner backpressure (optional) |
| `-dial-rate`   |     200 | Max new dials/s during ramp (avoid thundering)  |

### How to read results

```text
scale: target=10000 ok=9987 failed_dial=13
mem_per_conn: 48 KiB (ΔHeapAlloc)
cpu: 180% (≈1.8 cores) idle; 320% at traffic-rps=100
disconnects: 0 idle; 12 at traffic-rps=500 (slow_client)
```

Publish OS limits with the result (`ulimit -n`, maxfiles).

### Pass / fail

| Check        | Pass condition                                                      |
| ------------ | ------------------------------------------------------------------- |
| Scorecard    | Sustain 10k+ `clients_ok` on a prepared host without OOM            |
| Isolation    | Under overload, disconnect slow clients; WAL reader stays healthy   |
| Regression   | `mem_per_conn` and idle CPU within ~20% of prior SHA on same host   |

A laptop that cannot open 10k fds is an **environment skip**, not a fail.

### Machine prep

```bash
ulimit -n 65536   # or higher
# macOS: may need additional maxfiles sysctls for 10k
# Linux: file-max, somaxconn; Docker: --ulimit nofile=65536:65536
```

- RAM: plan several GB above `(mem_per_conn × N)`.
- Prefer dedicated machine; disable background compile/indexers.
- Do not run Phase E inside tiny CI runners and expect 10k.

---

## Phase F — WAL ingest throughput

**Status:** Spec only — add `make bench-wal`.

### Purpose

Isolate **ingest** from WebSocket fan-out: how fast the consumer can decode
pgoutput, filter shapes, and durably advance the checkpoint (Invariant 1:
persist before ack). This is the ceiling fan-out cannot exceed.

### Metric definitions

| Metric           | Definition                                                                 |
| ---------------- | -------------------------------------------------------------------------- |
| `wal_bytes`      | `pg_current_wal_lsn()` (or slot `restart_lsn`/`confirmed_flush_lsn`) delta over the window, converted to bytes. |
| `wal_mb_s`       | `wal_bytes / elapsed` in MiB/s.                                            |
| `rows_s`         | Inserted/copied rows / elapsed (sanity cross-check).                       |
| `ack_lag`        | Optional: write LSN − confirmed flush during/after burst.                  |

Modes:

1. **Sink-only** — engine running, **zero** WebSocket clients (pure ingest).
2. **Subscribers=0 vs few** — optional comparison to show fan-out tax (report both).

### Command (proposed)

```bash
make bench-wal
# proposed:
go run ./cmd/bench -mode=wal -rows=500000 -batch=1000 -clients=0 -pad-bytes=128
```

Bulk-load with large `-batch` (COPY) so the writer is not the bottleneck;
measure until the engine’s durable LSN catches the producer (or for a fixed
`-hold` with continuous COPY).

### Flags (proposed)

| Flag         | Default | Meaning                                          |
| ------------ | ------: | ------------------------------------------------ |
| `-mode`      | wal     | WAL ingest harness                               |
| `-rows`      |  500000 | Rows to load (or omit if `-hold` streaming)      |
| `-batch`     |    1000 | COPY batch size                                  |
| `-clients`   |       0 | Keep 0 for scorecard WAL MB/s                    |
| `-pad-bytes` |     128 | Row width → WAL volume                           |
| `-hold`      |       0 | If &gt;0, continuous load for this duration      |

### How to read results

```text
wal_ingest: clients=0 rows=500000 wal=842 MiB elapsed=14.2s → 59.3 MiB/s (35100 rows/s)
confirmed_flush caught producer: yes
```

Always report **pad width** and whether subscribers were attached.

### Pass / fail

| Check         | Pass condition                                                    |
| ------------- | ----------------------------------------------------------------- |
| Correctness   | No crash; after catch-up, shape log / DB agree for filtered rows  |
| Scorecard     | Record max `wal_mb_s` on reference hardware; track regressions    |
| Invariant 1   | Never claim throughput if LSN was acked past durable state (harness must not cheat) |

### Machine prep

- Disk fast enough that Postgres WAL write is not the only story (still expect
  fsync to dominate unless tuned).
- Optional Postgres knobs for **upper-bound experiments only** (document if
  changed): `synchronous_commit=off`, larger `wal_buffers` — never silently
  compare tuned vs untuned runs.

---

## Shared output conventions

When implementing C–F, keep Phase A/B readable style:

```text
<phase>: key=value …
<metric>: n=… p50=… p95=… p99=… max=…
note: <one-line definition of the clock>
```

Exit non-zero on incomplete receipt or invariant violations. Do not exit
non-zero solely for missing aspirational scorecard latency on a laptop.

Record with every published number:

- git SHA / `tether.Version`
- `go version`, `GOMAXPROCS`
- Postgres version + `synchronous_commit` setting
- Docker vs bare metal
- Command line exactly as run

---

## Why this is not a fair bake-off vs peers

| System                                                  | Shape of comparison                                     |
| ------------------------------------------------------- | ------------------------------------------------------- |
| **tether**                                              | Go library in-process with your HTTP server             |
| [ElectricSQL](https://github.com/electric-sql/electric) | Separate Elixir service; HTTP shape API + sync protocol |
| [PowerSync](https://powersync.com)                      | Service (+ optional self-host); SQLite clients          |
| [Zero](https://zero.rocicorp.dev)                       | TypeScript query sync; different write path             |
| [Debezium](https://debezium.io)                         | CDC into Kafka — different problem                      |

Apples-to-apples would require matching **client count, payload size, network
hop count, auth, persistence durability, and “lag” definition**. Those systems
add at least one network hop (app → sync service → client) that tether often
skips. Publishing “tether is Nx faster than Electric” from this harness would
be marketing, not science.

Use this bench to:

- regression-check tether after changes (`-rows` / p99 lag shouldn’t cliff)
- size `WithClientBuffer` and client count on _your_ hardware
- compare **two tether builds** (git SHAs), not tether vs another product
- track progress against the **scorecard** above over releases

## Fair comparison protocol (if you ever do a peer study)

1. Same Postgres instance, same table, same row width, same insert generator.
2. Same subscriber count and geo (all localhost or all cross-AZ — pick one).
3. Define lag the same way: commit visible on client local replica.
4. Exclude cold start / schema publish from timed window.
5. Publish full command lines, versions, and machine specs.

Until that exists, treat peer numbers from blogs as anecdotes.
