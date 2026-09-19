package leasefault

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The subprocess here is a boundary fixture, not a substitute for the real
// shell observer tests. Kubernetes is simulated; protocol recovery uses real RPCs.
func testNetworkObserver(t *testing.T, ctx context.Context, p FaultPreparation, mode string) {
	t.Helper()
	dir := p.Directory
	require.NoError(t, os.WriteFile(filepath.Join(dir, "observer-pod.json"), p.Network.PodBefore, 0600))
	targets := []byte(`[{"name":"independently-admitted-fixture"}]`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "observer-targets.json"), targets, 0600))
	digest := sha256.Sum256(targets)
	scripts := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(scripts, "observe-local-policy-state.sh"), []byte(`set -eu
[[ $# == 5 && $2 == present && $3 == created-uid && $4 == fault-policy && $5 == "$1/observer-pod.json" ]] || exit 99
printf 'fixture observer result\n'
if [[ $SCENARIO == observer-active-pending && ! -e "$1/active-seen" ]]; then touch "$1/active-seen"; exit 75; fi
`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(scripts, "observe-local-network-restored.sh"), []byte(`set -eu
[[ $# == 6 && $3 == created-uid && $4 == fault-policy && $5 == "$1/observer-pod.json" && $6 == "$1/observer-targets.json" ]] || exit 99
[[ $2 == absent || $2 == absent-unlabelled ]] || exit 99
printf '%s\n' "$2" >> "$1/observer-modes"
printf 'fixture observer result\n'
if [[ $SCENARIO == observer-pending && ! -f "$1/seen-pending" ]]; then
 touch "$1/seen-pending"
 exit 75
fi
if [[ $SCENARIO == observer-input-during ]]; then printf changed > "$6"; fi
`), 0600))
	retained, pending := 0, 0
	stages := map[string]bool{}
	o := NetworkObserver{Directory: dir, StatefulSetName: p.StatefulSetName, ScriptDirectory: scripts, TargetsSHA256: hex.EncodeToString(digest[:]), Network: p.Network, Client: p.Client, Env: []string{"SCENARIO=" + mode, "PATH=/usr/bin:/bin"},
		Admit: func(context.Context) error {
			if mode == "observer-owner-lost" {
				return errors.New("ownership lost")
			}
			return nil
		},
		Retain: func(stage string, output []byte, observed error) error {
			retained++
			stages[stage] = true
			require.Contains(t, string(output), "fixture observer result")
			if errors.Is(observed, ErrObservationPending) {
				pending++
			}
			if mode == "observer-retain-fail" {
				return errors.New("evidence retention failed")
			}
			return nil
		},
	}
	if mode == "observer-input-before" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "observer-targets.json"), []byte("changed"), 0600))
	}
	if strings.HasPrefix(mode, "observer-active") {
		origin := time.Now()
		faultCtx, cancel := context.WithDeadline(ctx, origin.Add(30*time.Second))
		defer cancel()
		if mode != "observer-active-inactive" {
			require.NoError(t, ActivateNetwork(faultCtx, p.Client, dir, p.Network, origin, p.Own))
		}
		if mode == "observer-active-replaced" {
			resource := p.Client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(p.Network.Namespace)
			policy, err := resource.Get(ctx, p.Network.PolicyName, metav1.GetOptions{})
			require.NoError(t, err)
			policy.SetUID("other")
			_, err = resource.Update(ctx, policy, metav1.UpdateOptions{})
			require.NoError(t, err)
		}
		err := o.Active(faultCtx, origin)
		if mode == "observer-active-inactive" || mode == "observer-active-replaced" {
			require.Error(t, err)
			require.Zero(t, retained)
			return
		}
		require.NoError(t, err)
		require.Equal(t, map[string]bool{"active": true}, stages)
		if mode == "observer-active-pending" {
			require.Equal(t, 1, pending)
			require.Equal(t, 2, retained)
		} else {
			require.Equal(t, 1, retained)
		}
		_, err = os.Stat(filepath.Join(dir, "observer-modes"))
		require.ErrorIs(t, err, os.ErrNotExist, "active mode must not invoke restored/TCP observer")
		return
	}
	err := o.Prepared(ctx)
	if mode != "observer-matched" && mode != "observer-pending" {
		require.Error(t, err)
		if mode == "observer-input-before" || mode == "observer-owner-lost" {
			require.Zero(t, retained)
		} else {
			require.Equal(t, 1, retained)
		}
		return
	}
	require.NoError(t, err)
	if mode == "observer-pending" {
		require.Equal(t, 1, pending)
		require.Equal(t, 2, retained)
	}
	// Connect concrete observer adapters to the complete recovery sequence.
	require.NoError(t, RecoverFault(ctx, FaultRecovery{Directory: dir, StatefulSetName: p.StatefulSetName, Network: p.Network, Protocol: p.Protocol, Client: p.Client, Connection: p.Connection,
		Own: p.Own, Join: func(context.Context) error { return nil }, NetworkRestored: o.Restored, IdentityRestored: o.Unlabelled}))
	require.NoError(t, VerifyProtocolRecovery(ctx, p.Protocol, p.Connection))
	require.Equal(t, map[string]bool{"prepared": true, "restored": true, "unlabelled": true}, stages)
	modes, err := os.ReadFile(filepath.Join(dir, "observer-modes"))
	require.NoError(t, err)
	require.Contains(t, string(modes), "absent\n")
	require.Contains(t, string(modes), "absent-unlabelled\n")
}
