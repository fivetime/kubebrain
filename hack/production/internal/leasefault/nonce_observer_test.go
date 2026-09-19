package leasefault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testNoncePreparation(t *testing.T, ctx context.Context, p FaultPreparation, mode string) {
	t.Helper()
	podPath := filepath.Join(p.Directory, "observer-pod.json")
	require.NoError(t, os.WriteFile(podPath, p.Network.PodBefore, 0600))
	scripts := t.TempDir()
	deadline, _ := ctx.Deadline()
	script := `set -eu
[[ $# == 5 && $2 == "$1/observer-pod.json" && $3 == "$ACTIVE" && $4 == "$RESERVED" && $5 == "$DEADLINE" ]] || exit 99
printf 'nonce scan fixture\n'
if [[ $MODE == nonce-pending ]]; then exit 75; fi
if [[ $MODE == nonce-input-during ]]; then printf changed > "$2"; fi
`
	require.NoError(t, os.WriteFile(filepath.Join(scripts, "observe-local-nonces.sh"), []byte(script), 0600))
	retained := 0
	ownerLost := mode == "nonce-owner-lost"
	o := NetworkObserver{Directory: p.Directory, StatefulSetName: p.StatefulSetName, ScriptDirectory: scripts,
		Network: p.Network, Client: p.Client,
		Env: []string{"MODE=" + mode, "ACTIVE=" + p.Network.Nonce, "RESERVED=" + p.Network.ReservedNonce, fmt.Sprintf("DEADLINE=%d", deadline.UnixNano())},
		Admit: func(context.Context) error {
			if ownerLost {
				return errors.New("claim lost")
			}
			return nil
		},
		Retain: func(stage string, output []byte, observed error) error {
			retained++
			if mode == "nonce-owner-after" {
				ownerLost = true
			}
			require.Equal(t, "nonces", stage)
			require.Equal(t, "nonce scan fixture\n", string(output))
			if mode == "nonce-pending" {
				require.ErrorIs(t, observed, ErrObservationPending)
			} else {
				require.NoError(t, observed)
			}
			if mode == "nonce-retain-fail" {
				return errors.New("cannot retain evidence")
			}
			return nil
		},
	}
	if mode == "nonce-input-before" {
		require.NoError(t, os.WriteFile(podPath, []byte("changed"), 0600))
	}
	p.NoncesSafe = o.NoncesSafe
	err := PrepareFault(ctx, p)
	if mode == "nonce-matched" {
		require.NoError(t, err)
		require.Greater(t, retained, 1, "scan is checked between preparation mutations")
		require.Error(t, VerifyProtocolRecovery(ctx, p.Protocol, p.Connection))
		require.NoError(t, RecoverFault(ctx, FaultRecovery{Directory: p.Directory, StatefulSetName: p.StatefulSetName, Network: p.Network, Protocol: p.Protocol,
			Client: p.Client, Connection: p.Connection, Own: p.Own, Join: p.Own, NetworkRestored: p.Own, IdentityRestored: p.Own}))
		require.NoError(t, VerifyProtocolRecovery(ctx, p.Protocol, p.Connection))
		return
	}
	require.Error(t, err)
	require.NoError(t, VerifyProtocolRecovery(ctx, p.Protocol, p.Connection))
	if mode == "nonce-owner-lost" || mode == "nonce-input-before" {
		require.Zero(t, retained)
	} else {
		require.Equal(t, 1, retained, "no implicit read retry or mutation on failed first scan")
	}
	_, err = LoadNetworkRecovery(p.Directory, p.Network)
	require.ErrorIs(t, err, os.ErrNotExist, "first nonce scan must pass before reservation intent")
}
