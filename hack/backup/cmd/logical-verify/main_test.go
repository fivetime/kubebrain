package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteKey(t *testing.T) {
	require.Equal(t, []byte("/target/a"), rewriteKey([]byte("/source/a"), "/source", "/target"))
	require.Equal(t, []byte("/other/a"), rewriteKey([]byte("/other/a"), "/source", "/target"))
	require.Equal(t, []byte("/source/a"), rewriteKey([]byte("/source/a"), "", "/target"))
}
