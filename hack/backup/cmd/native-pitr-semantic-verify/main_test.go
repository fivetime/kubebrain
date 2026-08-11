package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecuteRejectsIncompleteOptionsBeforeExternalAccess(t *testing.T) {
	err := execute(context.Background(), options{}, &strings.Builder{}, time.Now)
	require.ErrorContains(t, err, "required")
}

func TestParseAddrsIsStrict(t *testing.T) {
	got, err := parseAddrs("pd-a:2379, pd-b:2379")
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, got)
	for _, raw := range []string{"", "pd:2379,pd:2379", "http://pd:2379", "pd:2379,"} {
		_, err := parseAddrs(raw)
		require.Error(t, err, raw)
	}
}

func TestReadStableRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxInputBytes+1), 0o600))
	_, err := readStable(path)
	require.ErrorContains(t, err, "exceeds")
}
