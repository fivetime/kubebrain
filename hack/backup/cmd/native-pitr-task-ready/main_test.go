package main

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecuteValidatesOptionsBeforeConnecting(t *testing.T) {
	require.ErrorContains(t, execute(context.Background(), options{}, io.Discard), "task-create and pd are required")
	require.ErrorContains(t, execute(context.Background(), options{taskCreate: "unused", pd: "pd", timeout: -1}, io.Discard), "timeout must be positive")
}
