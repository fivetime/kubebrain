package semanticverify

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRevokeProbeLeasePropagatesCleanupFailure(t *testing.T) {
	revokeErr := errors.New("revoke transport failed")
	err := revokeProbeLease(func(ctx context.Context) error {
		require.NoError(t, ctx.Err())
		return revokeErr
	})
	require.ErrorIs(t, err, revokeErr)
}

func TestValidateProbePrefix(t *testing.T) {
	require.NoError(t, ValidateProbePrefix("/kubebrain-restore-probe"))
	for _, prefix := range []string{"", "/", "relative", "/registry", "/registry/pods", "/bad\nkey"} {
		require.Error(t, ValidateProbePrefix(prefix), prefix)
	}
}

func TestEqualStrings(t *testing.T) {
	require.True(t, equalStrings([]string{"a", "b"}, []string{"a", "b"}))
	require.False(t, equalStrings([]string{"a"}, []string{"b"}))
	require.False(t, equalStrings([]string{"a"}, []string{"a", "b"}))
}
