package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBoundedRejectsOversizedEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "exceeds 8 MiB")
}

func TestWriteExclusiveDoesNotOverwriteOrLeaveTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "authorization.json")
	require.NoError(t, writeExclusive(path, []byte("first\n")))
	require.Error(t, writeExclusive(path, []byte("second\n")))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first\n", string(data))
	temps, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temps)
}
