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
	require.ErrorContains(t, execute(context.Background(), options{}, io.Discard), "task-create and pd are required")
	require.ErrorContains(t, execute(context.Background(), options{taskCreate: "unused", pd: "pd", timeout: -1}, io.Discard), "timeout must be positive")
}

func TestReadBoundedRejectsOversizedTaskCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task-create.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "exceeds 8 MiB")
}
