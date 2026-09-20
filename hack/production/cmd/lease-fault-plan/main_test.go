package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestOnlineReleasePreflightDispatch(t *testing.T) {
	for _, mode := range []string{"success", "refused", "missing-hash", "missing-plan", "bindings-only"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"--command-plan", "/command.json", "--approve-sha256", "command-hash"}
			if mode == "bindings-only" {
				args[0] = "--bindings"
			}
			if mode != "missing-plan" {
				args = append(args, "--release-plan", "/release.json")
			}
			if mode != "missing-hash" {
				args = append(args, "--release-approve-sha256", "release-hash")
			}
			calls := 0
			refused := errors.New("release authentication refused")
			// Isolate option dispatch only. Real plan/download validation is not
			// bypassable through CLI options and is tested in the internal packages.
			auth := func(ctx context.Context, path, digest, release, releaseDigest string) error {
				calls++
				require.Equal(t, t.Context(), ctx)
				require.Equal(t, []string{"/command.json", "command-hash", "/release.json", "release-hash"}, []string{path, digest, release, releaseDigest})
				if mode == "refused" {
					return refused
				}
				return nil
			}
			var out bytes.Buffer
			err := runWithRelease(t.Context(), args, &out, auth)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.Equal(t, "COMMAND_RELEASE_AUTHENTICATED_NOT_LIVE_OR_FAULT_ADMISSION\n", out.String())
			} else {
				require.Error(t, err)
				require.Empty(t, out.String())
				if mode == "refused" {
					require.ErrorIs(t, err, refused)
					require.Equal(t, 1, calls)
				} else {
					require.Zero(t, calls)
				}
			}
		})
	}
}

func TestLocalBindingsMarker(t *testing.T) {
	minimum := uint64(1)
	n := leasefault.NetworkRecovery{Owner: "owner", Namespace: "test", NamespaceUID: "ns", StatefulSetUID: "sts", PodName: "brain-0", PodUID: "pod", PolicyName: "fault", Nonce: "active", ReservedNonce: "reserved", PodBefore: json.RawMessage(`{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"test","name":"brain-0","uid":"pod","resourceVersion":"1"}}`), ApprovedPolicy: json.RawMessage(`{"apiVersion":"cilium.io/v2","kind":"CiliumNetworkPolicy","metadata":{"namespace":"test","name":"fault"},"spec":{"endpointSelector":{"matchLabels":{"kubebrain.io/fault-owner":"active"}}}}`)}
	p := leasefault.NativeExperimentBindings{Version: 1, Network: n, Protocol: leasefault.ProtocolRecovery{Owner: "owner", NamespaceUID: "ns", StatefulSetUID: "sts", ClusterID: 1, AlarmMemberID: 2, LeaseID: 3, Key: "/acceptance/test"}, InitialTerm: 4, ObserverMemberID: 5, Source: strings.Repeat("a", 40), SourceHash: strings.Repeat("b", 64), Image: "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("c", 64), StackPorts: []int{18588, 18589, 18590, 18591}, Metrics: []retirementmetrics.WorkerExpectation{{Binding: retirementmetrics.CaptureBinding{NamespaceUID: "ns", StatefulSetUID: "sts", PodUID: "pod", SpecSHA256: strings.Repeat("d", 64), Cluster: "test"}, Key: retirementmetrics.Key{Stage: "local", Outcome: "confirmed"}, MinimumCount: &minimum}}}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "bindings.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	var out bytes.Buffer
	require.NoError(t, run([]string{"--bindings", path, "--approve-sha256", planinput.SHA256(data)}, &out))
	require.Equal(t, "LOCAL_BINDINGS_VALID_NOT_EXPERIMENT_ADMISSION\n", out.String())
}

func TestRejectIncompleteOrExecutingPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	data := []byte(`{"version":1}`)
	require.NoError(t, os.WriteFile(path, data, 0600))
	for _, args := range [][]string{
		nil, {"--bindings", path}, {"--bindings", path, "--approve-sha256", planinput.SHA256(data)},
		{"--command-plan", path}, {"--command-plan", path, "--approve-sha256", planinput.SHA256(data)},
		{"--bindings", path, "--command-plan", path, "--approve-sha256", planinput.SHA256(data)},
		{"--execute"}, {"extra"},
	} {
		var out bytes.Buffer
		require.Error(t, run(args, &out))
		require.Empty(t, out.String(), "invalid/incomplete input must not produce success marker")
	}
}
