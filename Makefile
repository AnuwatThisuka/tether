TETHER_TEST_DSN ?= postgres://tether:tether@localhost:54321/tether?replication=database

.PHONY: test test-integration lint fmt db-up db-down db-check bench bench-lag

test:
	go test ./...

# Requires Docker. db-check asserts wal_level=logical; Go tests use -tags=integration.
test-integration: db-check
	TETHER_TEST_DSN='$(TETHER_TEST_DSN)' go test -race -tags=integration ./...

# End-to-end insert→WebSocket microbench. Requires db-up + TETHER_TEST_DSN.
bench: db-check
	TETHER_TEST_DSN='$(TETHER_TEST_DSN)' go run ./cmd/bench $(BENCH_ARGS)

# Phase B: commit-aligned lag (p50/p95/p99) with batched COPY inserts.
bench-lag: db-check
	TETHER_TEST_DSN='$(TETHER_TEST_DSN)' go run ./cmd/bench -batch=100 -rows=2000 -warmup=100 -clients=1 $(BENCH_ARGS)

lint:
	@command -v golangci-lint >/dev/null || { echo 'golangci-lint not found; see https://golangci-lint.run/welcome/install/'; exit 1; }
	golangci-lint run ./...

fmt:
	@command -v gofumpt >/dev/null || { echo 'gofumpt not found; install: go install mvdan.cc/gofumpt@latest'; exit 1; }
	gofumpt -w .

db-up:
	docker compose up -d --wait

db-down:
	docker compose down -v

# Uses psql inside the container — no host psql or Go DB driver required.
db-check:
	@wal_level=$$(docker compose exec -T postgres psql -U tether -d tether -Atc 'SHOW wal_level'); \
	if [ "$$wal_level" != "logical" ]; then \
		echo "wal_level=$$wal_level, want logical"; \
		exit 1; \
	fi; \
	echo "wal_level=logical"
