# Benchmarking tether

## What `make bench` measures

End-to-end **tether** microbench (`cmd/bench`):

1. Start an embedded engine + WebSocket handler against local Postgres
   (`wal_level=logical`).
2. Connect `-clients` subscribers to one shape.
3. `INSERT` `-rows` rows (plus `-warmup`) with a `sent_ns` timestamp column.
4. Report:
   - insert-only throughput (rows/s)
   - wall-clock throughput until every client has seen every row
   - shape delivery lag: `recv_time - sent_ns` → **p50 / p99 / max**

```bash
make db-up
export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
make bench
# or:
go run ./cmd/bench -rows=10000 -clients=4 -warmup=200
```

This path exercises WAL decode → persist → fan-out → WebSocket. It does **not**
claim to be a substitute for production load tests.

## Why this is not a fair bake-off vs peers

| System | Shape of comparison |
| ------ | ------------------- |
| **tether** | Go library in-process with your HTTP server |
| [ElectricSQL](https://github.com/electric-sql/electric) | Separate Elixir service; HTTP shape API + sync protocol |
| [PowerSync](https://powersync.com) | Service (+ optional self-host); SQLite clients |
| [Zero](https://zero.rocicorp.dev) | TypeScript query sync; different write path |
| [Debezium](https://debezium.io) | CDC into Kafka — different problem |

Apples-to-apples would require matching **client count, payload size, network
hop count, auth, persistence durability, and “lag” definition**. Those systems
add at least one network hop (app → sync service → client) that tether often
skips. Publishing “tether is Nx faster than Electric” from this harness would
be marketing, not science.

Use this bench to:

- regression-check tether after changes (`-rows` / p99 lag shouldn’t cliff)
- size `WithClientBuffer` and client count on *your* hardware
- compare **two tether builds** (git SHAs), not tether vs another product

## Fair comparison protocol (if you ever do a peer study)

1. Same Postgres instance, same table, same row width, same insert generator.
2. Same subscriber count and geo (all localhost or all cross-AZ — pick one).
3. Define lag the same way: commit visible on client local replica.
4. Exclude cold start / schema publish from timed window.
5. Publish full command lines, versions, and machine specs.

Until that exists, treat peer numbers from blogs as anecdotes.
