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
	require.ErrorContains(t, execute(context.Background(), options{}, io.Discard), "task-create, task-ready, and pd are required")
	require.ErrorContains(t, execute(context.Background(), options{taskCreate: "unused", taskReady: "unused", pd: "pd", timeout: -1}, io.Discard), "timeout must be positive")
}

func TestReadBoundedRejectsOversizedTaskReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task-ready.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := readBounded(path, "task ready receipt")
	require.ErrorContains(t, err, "exceeds 8 MiB")
}
