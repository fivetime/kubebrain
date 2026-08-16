package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecuteValidatesOptionsBeforeConnecting(t *testing.T) {
	require.ErrorContains(t, execute(context.Background(), options{}, io.Discard), "preflight is required")
	require.ErrorContains(t, execute(context.Background(), options{preflight: "unused", timeout: -1}, io.Discard), "timeout must be positive")
}

func TestReadBoundedRejectsOversizedPreflight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preflight.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "exceeds 8 MiB")
}
