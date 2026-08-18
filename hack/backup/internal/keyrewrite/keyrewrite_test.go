package keyrewrite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteUnique(t *testing.T) {
	seen := make(map[string]struct{})
	target, err := RewriteUnique([]byte("/source/a"), "/source", "/target", seen)
	require.NoError(t, err)
	require.Equal(t, []byte("/target/a"), target)

	_, err = RewriteUnique([]byte("/target/a"), "/source", "/target", seen)
	require.ErrorContains(t, err, `duplicate target key "/target/a"`)

	_, err = RewriteUnique([]byte("/source"), "/source", "", make(map[string]struct{}))
	require.ErrorContains(t, err, "empty target key")
}
