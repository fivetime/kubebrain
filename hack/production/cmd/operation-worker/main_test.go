package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestRunCancellationKillsExecutorDescendants(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	survived := filepath.Join(dir, "survived")
	block := filepath.Join(dir, "block")
	require.NoError(t, os.WriteFile(block, []byte(
		"#!/bin/sh\n(sleep 0.2; echo survived > "+survived+") &\n"+
			"echo ready > "+ready+"\nwait\n",
	), 0o700))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, block)
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	time.Sleep(300 * time.Millisecond)
	_, err := os.Stat(survived)
	require.ErrorIs(t, err, os.ErrNotExist)
}
