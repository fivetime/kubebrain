package leasefault

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Real runtime, lifecycle, child supervision and memkv recovery RPCs; simulated
// Kubernetes, Cilium script boundaries and successor Status. Not live acceptance.
func nativeNetworkRuntimeFixture(t *testing.T, l FaultLifecycle, release func(), joined func() bool, mode string) NetworkFaultRuntime {
	t.Helper()
	p := l.Preparation
	l.Preparation.NoncesSafe = nil
	l.Preparation.ReservedReady = nil
	l.ObserveFault = nil
	l.NetworkRestored = nil
	l.IdentityRestored = nil
	scripts := t.TempDir()
	write := func(path, data string) { require.NoError(t, os.WriteFile(path, []byte(data), 0600)) }
	write(filepath.Join(p.Directory, "observer-pod.json"), string(p.Network.PodBefore))
	targets := "[]"
	write(filepath.Join(p.Directory, "observer-targets.json"), targets)
	digest := sha256.Sum256([]byte(targets))
	write(filepath.Join(scripts, "observe-local-nonces.sh"), `set -eu
[[ $# == 5 && $2 == "$1/observer-pod.json" && $5 =~ ^[0-9]{19}$ ]] || exit 99
printf 'fixture nonce scan\n'
`)
	write(filepath.Join(scripts, "observe-local-network-restored.sh"), `set -eu
[[ $# == 6 && ( $2 == absent || $2 == absent-unlabelled ) ]] || exit 99
printf 'fixture restoration\n'
`)
	write(filepath.Join(scripts, "observe-local-policy-state.sh"), `set -eu
[[ $# == 5 && $2 == present ]] || exit 99
printf 'fixture active policy\n'
`)
	write(filepath.Join(scripts, "observe-local-backend-drops.sh"), `set -eu
umask 077
[[ $# == 5 && $4 == 1 ]] || exit 99
printf '%s\n' "$5" > "$1/fault.origin"
[[ $SCENARIO != lifecycle-native-runtime-drops-fail ]] || exit 23
out=$(mktemp -d "$1/backend-drops.XXXXXXXX")
printf '0\n' > "$out/observation.exit"
printf '%s\n' "$5" > "$out/origin"
printf 'EVIDENCE=%s\nSAME_SOURCE_PD_AND_TIKV_POLICY_DROPS_NOT_TERM_OR_RPC_PROOF\n' "$out"
`)
	initial := l.Observation.Initial
	healthy := initial.InitialMemberID + 1
	if healthy == 0 {
		healthy = 1
	}
	stages := map[string]int{}
	retained := 0
	conn := &successorConnection{read: func(context.Context, int) (*pb.StatusResponse, error) {
		require.False(t, joined())
		require.Equal(t, 1, stages["drops"], "Status follows retained drop result")
		return &pb.StatusResponse{Header: &pb.ResponseHeader{ClusterId: initial.ClusterID, MemberId: healthy, RaftTerm: 3}, Leader: healthy}, nil
	}}
	t.Cleanup(func() {
		require.Greater(t, stages["nonces"], 1, "repeat nonce scans throughout preparation and before activation")
		// Protocol preflight, intent admission, three before-write gates and
		// the final preparation check all re-observe the reserved network.
		require.Equal(t, 6, stages["prepared"])
		require.Equal(t, 1, stages["drops"])
		// Withdrawal is rechecked throughout protocol and label recovery,
		// rather than accepted once and then cached across recovery writes.
		require.Greater(t, stages["restored"], 1)
		require.Equal(t, 1, stages["unlabelled"])
		if mode == "lifecycle-native-runtime-success" {
			require.Equal(t, 2, stages["active"])
			require.Equal(t, 1, retained)
			require.Equal(t, 1, conn.calls)
		} else {
			require.Equal(t, 1, stages["active"])
			require.Zero(t, retained)
			require.Zero(t, conn.calls)
		}
	})
	return NetworkFaultRuntime{
		Lifecycle: l, ScriptDirectory: scripts, TargetsSHA256: hex.EncodeToString(digest[:]),
		Env:          []string{"PATH=/usr/bin:/bin", "SCENARIO=" + mode},
		AdmitNetwork: func(context.Context) error { return nil },
		RetainNetwork: func(stage string, _ []byte, err error) error {
			stages[stage]++
			if stage == "nonces" {
				require.Zero(t, stages["active"], "nonce scan must precede fault observations")
				require.Zero(t, stages["drops"])
			}
			if stage == "restored" || stage == "unlabelled" {
				require.True(t, joined(), "concrete restoration must follow actual child join")
			} else {
				require.False(t, joined())
			}
			if stage == "drops" && mode == "lifecycle-native-runtime-drops-fail" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			return nil
		},
		SuccessorConnection: conn,
		Successor:           SuccessorBinding{ClusterID: initial.ClusterID, OldLeaderID: initial.InitialMemberID, OldTerm: initial.InitialTerm, ObserverMemberID: healthy},
		CaptureSeconds:      1, AdmitSuccessor: func(context.Context) error { require.False(t, joined()); return nil },
		RetainStatus: func(context.Context, SuccessorSample) error { retained++; release(); return nil },
	}
}
