package tether_test

import (
	"regexp"
	"testing"

	"github.com/anuwatthisuka/tether"
)

func TestVersion_SemVer(t *testing.T) {
	// Strict SemVer core: MAJOR.MINOR.PATCH (optional pre-release/build not used yet).
	re := regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)
	if !re.MatchString(tether.Version) {
		t.Fatalf("Version %q is not MAJOR.MINOR.PATCH", tether.Version)
	}
}
