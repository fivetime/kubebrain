package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyRequiresAllBaseBindingsBeforeReading(t *testing.T) {
	err := verify(options{})
	require.ErrorContains(t, err, "all receipt binding inputs")
}

func TestReadBoundedRejectsOversizedBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	require.NoError(t, os.WriteFile(path, make([]byte, (8<<20)+1), 0o600))
	_, err := readBounded(path)
	require.ErrorContains(t, err, "exceeds 8 MiB")
}
