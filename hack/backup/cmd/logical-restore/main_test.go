package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteKey(t *testing.T) {
	require.Equal(t, []byte("/target/a"), rewriteKey([]byte("/source/a"), "/source", "/target"))
	require.Equal(t, []byte("/other/a"), rewriteKey([]byte("/other/a"), "/source", "/target"))
	require.Equal(t, []byte("/source/a"), rewriteKey([]byte("/source/a"), "", "/target"))
}

func TestEnvBool(t *testing.T) {
	old := os.Getenv("ALLOW_OVERWRITE")
	defer os.Setenv("ALLOW_OVERWRITE", old)

	for _, value := range []string{"true", "TRUE", "1", "yes", " yes "} {
		require.NoError(t, os.Setenv("ALLOW_OVERWRITE", value))
		require.True(t, envBool("ALLOW_OVERWRITE"))
	}

	for _, value := range []string{"", "false", "0", "no", "maybe"} {
		require.NoError(t, os.Setenv("ALLOW_OVERWRITE", value))
		require.False(t, envBool("ALLOW_OVERWRITE"))
	}
}
