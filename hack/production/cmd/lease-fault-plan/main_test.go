package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

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
