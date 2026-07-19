# Phase 0: Scaffold the tether Go library

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

Reference: This plan follows conventions from `AGENTS.md` (root), master design in `IMPLEMENT_PLAN.md` (Phase 0), and `.agents/commands/create-plan.md`. Invariants touched: none (no LSN ack path, no filters, no mutations yet).

## Purpose / Big Picture

Today the repository is documentation only (`README.md`, `AGENTS.md`, `IMPLEMENT_PLAN.md`, agent command files). After this plan, a developer (or agent) can clone the repo, run `make test` and `make lint` successfully, bring up a Postgres 14+ instance with `wal_level = logical` on port 54321, and find empty but correctly named packages ready for Phase 1 WAL work. No sync behavior ships yet — this phase only removes "there is no codebase" as a blocker.

## Assumptions

- Docker is available on the implementer's machine for `make db-up`.
- Go 1.22 or newer is installed and on `PATH`.
- `golangci-lint` and `gofumpt` will be invoked via Make; if missing locally, the Makefile should print a clear install hint rather than silently no-oping.
- The GitHub module path `github.com/anuwatthisuka/tether` is acceptable even if the remote does not exist yet — `go.mod` does not require the remote to resolve for local builds.

## Open Questions

_(none remaining — module path resolved in Decision Log before implementation.)_

## Progress

- [x] (2026-07-19 20:05+07) ExecPlan authored; awaiting approval / implementation.
- [x] (2026-07-19 20:10+07) Create `go.mod` / `go.sum` with module `github.com/anuwatthisuka/tether`, Go 1.22+.
- [x] (2026-07-19 20:10+07) Add root package stub (`tether.go`, `errors.go`, `CHANGELOG.md`) failing closed.
- [x] (2026-07-19 20:10+07) Add package placeholders: `internal/wal`, `internal/shape`, `internal/proto`, `internal/snapshot`, `internal/mutate`, `transport`, `internal/e2e`.
- [x] (2026-07-19 20:10+07) Add `docker-compose.yml` for Postgres with logical WAL on `:54321`.
- [x] (2026-07-19 20:10+07) Add `Makefile` targets: `test`, `test-integration`, `lint`, `fmt`, `db-up`, `db-down`, `db-check`.
- [x] (2026-07-19 20:10+07) Add golangci-lint and gofumpt config; root smoke test + integration DSN probe.
- [x] (2026-07-19 20:10+07) Update `IMPLEMENT_PLAN.md` open-decision row for module path to Done.
- [x] (2026-07-19 20:12+07) Validate acceptance criteria; fill Outcomes & Retrospective.
- [ ] Move plan to `plans/done/` when PR lands.

## Surprises & Discoveries

- Observation: Repo was not a git repository during scaffolding validation; initialized afterward as `main` with root commit `1593582`.
  Evidence: `git init -b main` + first commit on 2026-07-19.

- Observation: `gofumpt` was not on PATH initially; installed via `go install mvdan.cc/gofumpt@latest`. `golangci-lint` was already available and reported 0 issues (includes gofumpt formatter).
  Evidence: acceptance run transcript 2026-07-19.

## Decision Log

- Decision: Go module path is `github.com/anuwatthisuka/tether`.
  Rationale: Matches the local username; user confirmed for Phase 0. Can be retagged later if the public remote differs, but avoid renaming mid-phase.
  Date/Author: 2026-07-19 / plan author (user choice)

- Decision: Root `New` fails closed with a sentinel error rather than returning a no-op engine.
  Rationale: Prefer loud failure over a fake Engine that embeds would mistake for working sync (`AGENTS.md` — fail loudly; `IMPLEMENT_PLAN.md` Phase 0).
  Date/Author: 2026-07-19 / plan author

- Decision: Include stub packages `internal/snapshot` and `internal/mutate` in Phase 0 even though `IMPLEMENT_PLAN.md` Phase 0 list names five placeholders — align with the architecture map in §4 of `IMPLEMENT_PLAN.md`.
  Rationale: Avoids a second scaffolding PR before Phase 3/5; empty package docs only.
  Date/Author: 2026-07-19 / plan author

- Decision: Phase 0 DB probe uses `make db-check` (psql in container) rather than adding `pgx`.
  Rationale: Keep direct deps empty until Phase 1 needs them; harness still proves `wal_level=logical`.
  Date/Author: 2026-07-19 / implementer

## Outcomes & Retrospective

Phase 0 acceptance passed locally:

- `make test` — root stub tests pass; placeholder packages list as no test files.
- `make lint` — 0 issues.
- `make fmt` — gofumpt applied after install.
- `make db-up` + `make db-check` — `wal_level=logical`.
- `make test-integration` — `TestIntegrationHarnessWired` passes with Makefile DSN.
- `make db-down` — tears down cleanly.
- `go.mod` has module `github.com/anuwatthisuka/tether` and no third-party requires.
- `IMPLEMENT_PLAN.md` module path marked Done.

Gaps / follow-ups:

- No git remote or CI yet — initialize repo / add Actions in a later PR if desired.
- Move this ExecPlan to `plans/done/` when the scaffolding PR is created.
- Phase 1 can start: WAL ingest + persist-before-ack (will introduce `pgx` / `pglogrepl`).

Compared to purpose: developers can now build, test, lint, and bring up logical Postgres — the "no codebase" blocker is gone.

## Context and Orientation

The working tree at plan authoring time contains:

- `README.md` — product description and v0.1 roadmap (all unchecked)
- `AGENTS.md` — agent rules and seven Invariants
- `IMPLEMENT_PLAN.md` — master phased design; Phase 0 is the target of this plan
- `.agents/` / `.cursor/` — agent commands (not product code)
- No `*.go`, no `go.mod`, no `Makefile`, no `docker-compose.yml`

**Packages this plan creates (stubs only):**

| Path                  | Role                                         |
| --------------------- | -------------------------------------------- |
| root (`tether.go`, …) | Public API surface — stub + changelog        |
| `internal/wal/`       | Future replication slot / pgoutput / LSN ack |
| `internal/shape/`     | Future registry, filters, shape log          |
| `internal/proto/`     | Future wire format / offsets                 |
| `internal/snapshot/`  | Future gapless snapshot at LSN N             |
| `internal/mutate/`    | Future idempotent mutation txn               |
| `transport/`          | Future WebSocket server / resume             |
| `internal/e2e/`       | Future convergence test                      |

This plan does **not** touch WAL decoding, LSN ack ordering, offsets, or snapshot handoff. No Invariant enforcement code yet. Public API gets a minimal stub and a `CHANGELOG.md` Unreleased section noting scaffolding only.

**wal_level = logical** means Postgres writes enough information to the write-ahead log for logical decoding / replication consumers (like tether will be). Without it, `pgoutput` replication cannot start. The compose file must set this via command or config.

**TETHER_TEST_DSN** is the environment variable integration tests will use. Phase 0 adds a tiny probe test that skips when unset and checks `wal_level` when set — proving the harness works before Phase 1.

## Plan of Work

### Milestone 1: Module and root stub

Create `go.mod`:

    module github.com/anuwatthisuka/tether

    go 1.22

Do not add third-party requires yet (`pgx`, `pglogrepl`, `websocket` arrive when first used in later phases). Run `go mod tidy` after the first test file exists so `go.sum` is present (may be empty of deps).

Add `tether.go` in the root package exporting a minimal API that fails closed:

- `var ErrNotImplemented = errors.New("tether: not implemented")` (or place in `errors.go`)
- `type Engine struct{}` — empty; no behavior
- `func New(pgURL string, opts ...Option) (*Engine, error)` — validate `pgURL != ""`, then return `nil, fmt.Errorf("%w", ErrNotImplemented)` (or return the sentinel directly). Do not connect to Postgres.
- `type Option func(*options)` and unexported `options` struct — empty for now so later phases add options without changing the `New` signature pattern.
- Doc comments on all exported identifiers.

Add `errors.go` if it keeps `tether.go` readable. Add `CHANGELOG.md`:

    # Changelog

    ## Unreleased

    ### Added

    - Repository scaffolding: Go module, package placeholders, Makefile, Docker Postgres for integration tests.

No real Shape / Handler / OnMutation methods yet — adding empty methods that panic is worse than omitting them until Phase 2/4/5. Optional: document in package comment that the public API is incomplete.

Add `tether_test.go` with a table-driven or simple test: `New("")` errors; `New("postgres://…")` returns `ErrNotImplemented` via `errors.Is`.

### Milestone 2: Package placeholders

For each of `internal/wal`, `internal/shape`, `internal/proto`, `internal/snapshot`, `internal/mutate`, `transport`, `internal/e2e`, add a single `doc.go` (or `<pkg>.go`) containing only the package clause and a package doc comment describing the future responsibility in one short paragraph. No exported API required.

Example for `internal/wal/doc.go`:

    // Package wal owns the Postgres logical replication slot lifecycle,
    // pgoutput decoding, and LSN checkpointing. Persist before ack
    // (see AGENTS.md Invariant 1).
    package wal

Do not put library code under `cmd/`. Optionally add `cmd/.gitkeep` with no Go files, or omit `cmd/` until a harness is needed — prefer omit to avoid empty noise.

### Milestone 3: Postgres via Docker Compose

Add `docker-compose.yml` at repo root:

- Image: `postgres:16` (or `postgres:14`) — 14+ is the stated requirement; 16 is fine.
- Container name / service name: `tether-pg` / `postgres` (either; document in Makefile).
- Ports: `54321:5432`
- Environment: `POSTGRES_USER=tether`, `POSTGRES_PASSWORD=tether`, `POSTGRES_DB=tether`
- Command must enable logical WAL, e.g.:

      command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]

- Healthcheck using `pg_isready` so `make db-up` can wait.

Document the DSN in a short comment in the Makefile:

    postgres://tether:tether@localhost:54321/tether?replication=database

Note: a DSN with `replication=database` is for the replication connection. A normal query probe can use the same URL without that query param, or with it — `SHOW wal_level` works on a normal connection. Prefer a non-replication DSN for the Phase 0 probe:

    postgres://tether:tether@localhost:54321/tether

Keep `TETHER_TEST_DSN` as specified in `AGENTS.md` / `IMPLEMENT_PLAN.md` for future replication tests; the probe test may strip or ignore the replication param when opening a normal `database/sql` or `pgx` connection. **Do not add `jackc/pgx` in Phase 0 solely for the probe** — use `database/sql` + `lib/pq` only if necessary. Prefer:

- Phase 0 probe implemented with the `psql` CLI in a Make target `db-check`, **and**
- A Go integration test under `internal/e2e` or a root `integration_test.go` that is gated on `//go:build integration` and uses only the standard library by shelling out — actually that's awkward.

Cleaner Phase 0 approach (prescriptive):

1. `make db-check` runs `docker compose exec` / `psql` to `SHOW wal_level` — no new Go dep.
2. `make test-integration` depends on the integration build tag and, for Phase 0, runs only tests that skip if `TETHER_TEST_DSN` is unset. The Phase 0 Go integration test may be a placeholder that `t.Skip` unless DSN set, and if set, uses `os/exec` to run `psql -Atc 'SHOW wal_level'` — ugly.

**Chosen approach:** Add no `pgx` yet. Phase 0 Go `test-integration` target runs `go test -tags=integration ./...` and includes one file `internal/e2e/harness_integration_test.go` built with `//go:build integration` that skips when `TETHER_TEST_DSN` is empty, and when set uses `net` + undocumented protocol — too hard.

Simplest correct approach matching AGENTS.md ("If a command does not exist yet, add it to the Makefile"):

- `make test-integration` in Phase 0 runs `make db-check` (psql) and then `go test -tags=integration ./...` where the only integration test is:

      //go:build integration

      func TestIntegrationHarnessWired(t *testing.T) {
          if os.Getenv("TETHER_TEST_DSN") == "" {
              t.Fatal("TETHER_TEST_DSN unset")
          }
          t.Log("DSN present; Postgres wal_level verified by make db-check")
      }

And `make test-integration` is defined as:

    test-integration: db-check
        TETHER_TEST_DSN=... go test -tags=integration ./...

With `db-check` using docker/psql. That proves the harness without a new dependency. When Phase 1 adds `pgx`, replace the placeholder with real tests.

**Do not** add `lib/pq` or `pgx` in Phase 0.

### Milestone 4: Makefile and tooling config

Add `Makefile` with `.PHONY` targets:

| Target             | Behavior                                                                                       |
| ------------------ | ---------------------------------------------------------------------------------------------- |
| `test`             | `go test ./...` (unit only; no integration tag)                                                |
| `test-integration` | ensure DSN env default, `db-check`, then `go test -tags=integration ./...`                     |
| `lint`             | `golangci-lint run ./...`                                                                      |
| `fmt`              | `gofumpt -w .`                                                                                 |
| `db-up`            | `docker compose up -d --wait` (or up -d + wait for health)                                     |
| `db-down`          | `docker compose down -v` (document that `-v` wipes data — acceptable for test DB)              |
| `db-check`         | `psql` or `docker compose exec -T … psql -Atc 'SHOW wal_level'` and assert output is `logical` |

Use a variable:

    TETHER_TEST_DSN ?= postgres://tether:tether@localhost:54321/tether?replication=database

Add `.golangci.yml` with sensible defaults for a small library (errcheck, govet, staticcheck; enable gofumpt via formatters if supported by the pinned golangci-lint major version). Keep the config minimal — do not cargo-cult dozens of linters.

Add `.gofumpt` is not a file — gofumpt has flags; `make fmt` invoking `gofumpt -w .` is enough. Optionally set `GOFLAGS` nowhere.

Add `.gitignore`:

    /bin/
    /dist/
    coverage.out
    *.test
    .idea/
    .vscode/
    *.swp

Do not ignore `go.sum`.

### Milestone 5: Cross-links and IMPLEMENT_PLAN update

In `IMPLEMENT_PLAN.md` §12 Open decisions, set the module path row Status to Done with value `github.com/anuwatthisuka/tether`.

Optionally add one line under README Contributing or Requirements pointing maintainers at `make db-up` — only if it does not bloat the README; prefer keeping README user-facing and letting `AGENTS.md` / Makefile comments carry ops detail.

### Out of scope for this plan

- Any `pgx` / `pglogrepl` / `websocket` dependency
- Real `Engine` behavior, shapes, WAL, transport
- GitHub Actions CI YAML (nice-to-have; add only if trivial — prefer a follow-up PR so this PR stays one concern: scaffolding). If CI is added, restrict to `make test` + `make lint` without Docker to keep Phase 0 reviewable.

## Concrete Steps

Work from the repository root `/Users/anuwatthisuka/Documents/products/tether` (or the clone root).

1.  Write files as described in Plan of Work (Milestones 1–5).

2.  Format and tidy:

        gofumpt -w .
        go mod tidy
        go test ./...

    Expected: tests pass; `TestNew_NotImplemented` (or equivalent) passes.

3.  Lint:

        make lint
        # or: golangci-lint run ./...

    Expected: no issues.

4.  Database harness:

        make db-up
        make db-check
        # Expected: wal_level is logical

        export TETHER_TEST_DSN='postgres://tether:tether@localhost:54321/tether?replication=database'
        make test-integration
        # Expected: integration tag tests pass; db-check succeeded

5.  Tear down (optional after validation):

        make db-down

6.  Update this plan's Progress checkboxes and Outcomes; when creating the PR, `git mv` this file to `plans/done/`.

## Validation and Acceptance

All of the following must hold before calling Phase 0 done:

        make test
        # Expected: ok for root and all packages; no failures

        make lint
        # Expected: exit 0

        make fmt
        # Expected: no further diffs from gofumpt (run twice or check git status clean for format)

        make db-up
        make db-check
        # Expected: prints or asserts wal_level=logical

        make test-integration
        # Expected: exit 0 with TETHER_TEST_DSN set by the Makefile default

        # Tree expectations:
        # - go.mod module github.com/anuwatthisuka/tether
        # - CHANGELOG.md has Unreleased scaffolding note
        # - Placeholder packages exist with package docs
        # - No jackc/pgx, jackc/pglogrepl, or coder/websocket in go.mod yet
        # - IMPLEMENT_PLAN.md module path decision marked Done

Honest gap: Phase 0 does not prove replication slots work — only `wal_level`. Slot create/drop is Phase 1.

## Idempotence and Recovery

- Re-running `make db-up` is safe (compose recreates/reuses the container).
- `make db-down` then `make db-up` is the recovery path for a broken Postgres data volume (uses `-v` — wipes DB).
- Re-running `go mod tidy`, `make fmt`, `make test` is always safe.
- If `golangci-lint` or `gofumpt` is missing, install per their upstream docs; do not vendor copies into this repo in Phase 0.

## Artifacts and Notes

Expected `go.mod` after Phase 0:

    module github.com/anuwatthisuka/tether

    go 1.22

Expected Makefile fragment for DSN:

    TETHER_TEST_DSN ?= postgres://tether:tether@localhost:54321/tether?replication=database

    .PHONY: test test-integration lint fmt db-up db-down db-check

## Interfaces and Dependencies

**Root package (end of Phase 0):**

- `var ErrNotImplemented error`
- `type Engine struct{}`
- `type Option func(*options)`
- `func New(pgURL string, opts ...Option) (*Engine, error)` — returns `ErrNotImplemented` when `pgURL` non-empty; error on empty URL

No `Shape`, `OnMutation`, `Handler`, or `Run` yet.

**Dependencies:** none beyond the Go standard library. Do not add third-party modules in this phase.

**Tools (not Go module deps):** Docker Compose, `psql` (via docker exec is enough), `golangci-lint`, `gofumpt`.

---

## Revision note

2026-07-19: Initial ExecPlan for IMPLEMENT_PLAN.md Phase 0 (scaffolding). Module path fixed to `github.com/anuwatthisuka/tether` per user confirmation.
