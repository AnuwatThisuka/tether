# Benchmarking tether

## Scorecard (North Star)

Targets are **aspirational** for a well-tuned LAN deploy. Local `make bench`
numbers today will usually miss the latency goals until the harness and
ingest path are improved (batch inserts, less fsync contention, warm paths).

| Metric                                |            Target | Status                                                                                                          |
| ------------------------------------- | ----------------: | --------------------------------------------------------------------------------------------------------------- |
| Commit → Client latency (p50/p95/p99) | &lt; 50 ms on LAN | **Phase B** — `make bench-lag`: lag = WS recv − wall clock when INSERT/COPY returned; reports p50/p95/p99. LAN &lt;50ms still aspirational on stock laptop + sync WAL. |
| Max concurrent WebSocket clients      |              10k+ | **Not measured** — needs soak harness + OS `ulimit` / fd tuning.                                                |
| Resume after reconnect                |       &lt; 100 ms | **Not measured** — planned: connect → subscribe with offset → first delta.                                      |
| Snapshot 1M rows                      |   time in seconds | **Not measured** — planned: `snapshot.Take` + WS snapshot send wall clock.                                      |
| Memory per connection                 |         KB / conn | **Not measured** — planned: `runtime.MemStats` / `GODEBUG` before/after N conns.                                |
| CPU at 1k / 5k / 10k clients          |        % or cores | **Not measured** — planned: steady idle + light traffic profiles.                                               |
| WAL ingest throughput                 |              MB/s | **Not measured** — planned: bulk load + `pg_current_wal_lsn` delta / time (separate from WS).                   |
| Broadcast latency                     |         p50 / p99 | **Phase B** — same series as commit→client for single-shape fan-out.                                         |

### Legend

| Status       | Meaning                                                                      |
| ------------ | ---------------------------------------------------------------------------- |
| Partial      | Something exists in `cmd/bench`; definition or completeness still incomplete |
| Not measured | No automated command yet                                                     |
| Planned      | Intended next harness work (see below)                                       |

## What `make bench` / `make bench-lag` measure

End-to-end **tether** microbench (`cmd/bench`):

1. Start an embedded engine + WebSocket handler against local Postgres
   (`wal_level=logical`).
2. Connect `-clients` subscribers to one shape.
3. Insert `-rows` (+ `-warmup`) with `-batch` rows per commit (`-batch=1`
   single `INSERT`; `-batch>1` uses `COPY`).
4. Record **commit time** as wall clock when the INSERT/COPY call returns
   (durable from the client’s POV).
5. Report:
   - commit throughput (rows/s)
   - wall-clock until every client has seen every row
   - **commit → client lag**: p50 / **p95** / p99 / max

```bash
make db-up
export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
make bench          # defaults: batch=1
make bench-lag      # Phase B defaults: batch=100, rows=2000, p50/p95/p99
# or:
go run ./cmd/bench -batch=100 -rows=5000 -clients=2 -warmup=100
```

**Known bias:** localhost Docker Postgres with default `synchronous_commit`;
lag includes WAL decode + persist + fan-out. Treat as a **regression check**,
not a marketing ceiling. Hitting &lt;50 ms p99 needs a tuned LAN + lighter load.

## Phased harness plan

| Phase   | Command (proposed)    | Covers                                                      |
| ------- | --------------------- | ----------------------------------------------------------- |
| A       | `make bench`          | Rough commit→WS lag + rows/s                                |
| B (now) | `make bench-lag`      | Commit-aligned lag, p50/p95/p99, `-batch` COPY inserts      |
| C       | `make bench-resume`   | Reconnect + offset → first change &lt; 100 ms target        |
| D       | `make bench-snapshot` | Snapshot wall clock for N rows (incl. 1M)                   |
| E       | `make bench-scale`    | N clients (1k/5k/10k): mem/conn, CPU%, disconnect rate      |
| F       | `make bench-wal`      | WAL MB/s ingest with subscribers=0 or sink-only             |

Phases C–F need careful machine prep (`ulimit -n`, Docker shm, enough RAM).
Do not run 10k clients on a default laptop and declare failure.

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
