package main

import (
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/keyrewrite"
	"github.com/stretchr/testify/require"
)

func TestRewriteKey(t *testing.T) {
	require.Equal(t, []byte("/target/a"), keyrewrite.Rewrite([]byte("/source/a"), "/source", "/target"))
	require.Equal(t, []byte("/other/a"), keyrewrite.Rewrite([]byte("/other/a"), "/source", "/target"))
	require.Equal(t, []byte("/source/a"), keyrewrite.Rewrite([]byte("/source/a"), "", "/target"))
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

func TestRunReturnsEnvironmentValidationError(t *testing.T) {
	t.Setenv("REWRITE_FROM", "")
	t.Setenv("REWRITE_TO", "/target")

	require.ErrorContains(t, run(), "REWRITE_TO requires REWRITE_FROM")
}
