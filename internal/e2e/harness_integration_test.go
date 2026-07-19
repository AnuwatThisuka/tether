//go:build integration

package e2e_test

import (
	"os"
	"testing"
)

// TestIntegrationHarnessWired proves the Phase 0 integration Make target
// exports TETHER_TEST_DSN. Real Postgres checks run via `make db-check`
// until pgx is introduced in Phase 1.
func TestIntegrationHarnessWired(t *testing.T) {
	if os.Getenv("TETHER_TEST_DSN") == "" {
		t.Fatal("TETHER_TEST_DSN unset")
	}
	t.Log("DSN present; Postgres wal_level verified by make db-check")
}
