package main

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecuteValidatesOptionsBeforeConnecting(t *testing.T) {
	require.ErrorContains(t, execute(context.Background(), options{}, io.Discard), "preflight is required")
	require.ErrorContains(t, execute(context.Background(), options{preflight: "unused", timeout: -1}, io.Discard), "timeout must be positive")
}
