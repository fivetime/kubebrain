package main

import (
	"testing"
)

func TestValidateMode(t *testing.T) {
	for _, mode := range []string{"put", "stream", "delprefix"} {
		if err := validateMode(mode); err != nil {
			t.Fatalf("validateMode(%q): %v", mode, err)
		}
	}
	for _, mode := range []string{"", "del", "unknown"} {
		want := `unsupported mode "` + mode + `" (want put, stream, or delprefix)`
		if err := validateMode(mode); err == nil || err.Error() != want {
			t.Fatalf("validateMode(%q) error = %v, want %q", mode, err, want)
		}
	}
}
