package tether_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/anuwatthisuka/tether"
	"github.com/anuwatthisuka/tether/internal/proto"
	"github.com/anuwatthisuka/tether/transport"
)

func TestWithLogger_DisconnectIncludesAttrs(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	eng, err := tether.New(
		"postgres://localhost/db",
		tether.WithLogger(log),
		tether.WithSlotName("log_slot"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	conn := transport.NewConn("c99", nil, nil, 4)
	eng.DisconnectForTest(conn, proto.ReasonSlowClient)

	out := buf.String()
	for _, want := range []string{"tether client disconnect", "slot=log_slot", "client_id=c99", "reason=slow_client"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q\n%s", want, out)
		}
	}
}
