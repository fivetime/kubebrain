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

func TestReceiptTargetPrefix(t *testing.T) {
	target, err := receiptTargetPrefix("/source", "", "")
	require.NoError(t, err)
	require.Equal(t, "/source", target)

	target, err = receiptTargetPrefix("/source", "/source", "/target")
	require.NoError(t, err)
	require.Equal(t, "/target", target)

	_, err = receiptTargetPrefix("/source", "/source/subtree", "/target")
	require.ErrorContains(t, err, "to equal artifact prefix")
	_, err = receiptTargetPrefix("/source", "/source", "")
	require.ErrorContains(t, err, "non-empty REWRITE_TO")
}
