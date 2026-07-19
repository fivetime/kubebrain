package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunReturnsExecutorStatus(t *testing.T) {
	dir := t.TempDir()
	ok := filepath.Join(dir, "ok")
	require.NoError(t, os.WriteFile(ok, []byte("#!/bin/sh\nexit 0\n"), 0o700))
	require.NoError(t, run(context.Background(), ok))

	fail := filepath.Join(dir, "fail")
	require.NoError(t, os.WriteFile(fail, []byte("#!/bin/sh\nexit 7\n"), 0o700))
	require.ErrorContains(t, run(context.Background(), fail), "exit status 7")
}

func TestRunCancelsExecutor(t *testing.T) {
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	require.NoError(t, os.WriteFile(block, []byte("#!/bin/sh\nsleep 30\n"), 0o700))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, run(ctx, block), context.Canceled)
}
