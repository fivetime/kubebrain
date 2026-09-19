package leasefault

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNetworkRecoveryFeedsLabelPlan(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	admitted := networkPlan()
	require.NoError(t, ArmNetworkRecovery(dir, admitted))
	saved, err := LoadNetworkRecovery(dir, admitted)
	require.NoError(t, err)
	// Synthetic fresh observations: no API calls, no data-plane recovery proof.
	current := json.RawMessage(strings.Replace(string(saved.PodBefore), `"app":"brain"`, `"app":"brain","kubebrain.io/fault-owner":"active"`, 1))
	input := map[string]interface{}{
		"binding": map[string]string{"namespace": saved.Namespace, "namespaceUID": saved.NamespaceUID,
			"name": saved.PodName, "uid": saved.PodUID, "nonce": saved.Nonce, "policyName": saved.PolicyName},
		"namespace": map[string]interface{}{"apiVersion": "v1", "kind": "Namespace",
			"metadata": map[string]string{"name": saved.Namespace, "uid": saved.NamespaceUID}},
		"expected": saved.PodBefore, "current": current,
		"policies": json.RawMessage(`{"apiVersion":"v1","kind":"List","items":[]}`),
	}
	data, err := json.Marshal(input)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "jq", "-e", "-f", "../../fault-label-restore-plan.jq")
	cmd.Stdin = bytes.NewReader(data)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	require.JSONEq(t, `[
{"op":"test","path":"/metadata/uid","value":"pod-uid"},
{"op":"test","path":"/metadata/resourceVersion","value":"18446744073709551615"},
{"op":"test","path":"/metadata/labels/kubebrain.io~1fault-owner","value":"active"},
{"op":"remove","path":"/metadata/labels/kubebrain.io~1fault-owner"}
]`, string(output))
}

func networkPlan() NetworkRecovery {
	return NetworkRecovery{
		Owner: "fresh-owner", Namespace: "test-ns", NamespaceUID: "ns-uid", StatefulSetUID: "sts-uid",
		PodName: "brain-0", PodUID: "pod-uid", PolicyName: "fault-policy", Nonce: "active", ReservedNonce: "reserved",
		PodBefore:      json.RawMessage(`{ "apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test-ns","uid":"pod-uid","resourceVersion":"18446744073709551615","labels":{"app":"brain"}},"spec":{"containers":[]},"large":18446744073709551615 }`),
		ApprovedPolicy: json.RawMessage(`{"apiVersion":"cilium.io/v2","kind":"CiliumNetworkPolicy","metadata":{"name":"fault-policy","namespace":"test-ns"},"spec":{"endpointSelector":{"matchLabels":{"kubebrain.io/fault-owner":"active"}},"egressDeny":[{"toCIDR":["10.0.0.1/32"]}]}}`),
	}
}

func TestNetworkRecoveryCreateOnce(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	plan := networkPlan()
	require.NoError(t, ArmNetworkRecovery(dir, plan))
	got, err := LoadNetworkRecovery(dir, plan)
	require.NoError(t, err)
	require.JSONEq(t, string(plan.PodBefore), string(got.PodBefore))
	require.Contains(t, string(got.PodBefore), `"large":18446744073709551615`)
	require.JSONEq(t, string(plan.ApprovedPolicy), string(got.ApprovedPolicy))
	path := filepath.Join(dir, networkRecoveryFile)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), st.Mode().Perm())
	require.Error(t, ArmNetworkRecovery(dir, plan))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	plan.StatefulSetUID = "replacement-sts"
	_, err = LoadNetworkRecovery(dir, plan)
	require.Error(t, err)
}

func TestNetworkRecoveryRejectsInvalidIntent(t *testing.T) {
	for _, mode := range []string{"missing-owner", "same-nonce", "pod-replacement", "pod-deleting", "pod-labeled", "pod-case", "pod-duplicate", "policy-duplicate", "policy-name", "policy-owner", "policy-specs", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			plan := networkPlan()
			switch mode {
			case "missing-owner":
				plan.Owner = ""
			case "same-nonce":
				plan.ReservedNonce = plan.Nonce
			case "pod-replacement":
				plan.PodUID = "replacement"
			case "pod-deleting":
				plan.PodBefore = []byte(strings.Replace(string(plan.PodBefore), `"labels":`, `"deletionTimestamp":"2026-09-19T00:00:00Z","labels":`, 1))
			case "pod-labeled":
				plan.PodBefore = []byte(strings.Replace(string(plan.PodBefore), `"app":"brain"`, `"kubebrain.io/fault-owner":"active"`, 1))
			case "pod-case":
				plan.PodBefore = []byte(strings.Replace(string(plan.PodBefore), `"uid":`, `"UID":`, 1))
			case "pod-duplicate":
				plan.PodBefore = []byte(strings.Replace(string(plan.PodBefore), `"app":"brain"`, `"app":"other","app":"brain"`, 1))
			case "policy-duplicate":
				plan.ApprovedPolicy = []byte(strings.Replace(string(plan.ApprovedPolicy), `"toCIDR":`, `"toCIDR":[],"toCIDR":`, 1))
			case "policy-name":
				plan.PolicyName = "other-policy"
			case "policy-owner":
				plan.Nonce = "other-owner"
			case "policy-specs":
				plan.ApprovedPolicy = []byte(strings.Replace(string(plan.ApprovedPolicy), `"spec":`, `"specs":[],"spec":`, 1))
			case "oversize":
				plan.PodBefore = []byte(strings.Repeat(" ", networkRecoveryLimit+1))
			}
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			require.Error(t, ArmNetworkRecovery(dir, plan))
			_, err := os.Lstat(filepath.Join(dir, networkRecoveryFile))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestNetworkRecoveryRejectsUnsafeRecord(t *testing.T) {
	for _, mode := range []string{"missing", "truncated", "unknown", "duplicate", "public", "symlink", "fifo", "oversize", "changed-original", "public-dir", "dir-link"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := networkPlan()
			require.NoError(t, ArmNetworkRecovery(dir, plan))
			path := filepath.Join(dir, networkRecoveryFile)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			switch mode {
			case "missing", "symlink", "fifo":
				require.NoError(t, os.Remove(path))
				if mode == "symlink" {
					require.NoError(t, os.Symlink("missing-target", path))
				} else if mode == "fifo" {
					require.NoError(t, syscall.Mkfifo(path, 0600))
				}
			case "truncated":
				require.NoError(t, os.WriteFile(path, data[:len(data)/2], 0600))
			case "unknown", "duplicate":
				prefix := `{"unexpected":true,`
				if mode == "duplicate" {
					prefix = `{"owner":"fresh-owner",`
				}
				require.NoError(t, os.WriteFile(path, append([]byte(prefix), data[1:]...), 0600))
			case "public":
				require.NoError(t, os.Chmod(path, 0644))
			case "oversize":
				require.NoError(t, os.WriteFile(path, []byte(strings.Repeat(" ", networkRecoveryLimit+1)), 0600))
			case "changed-original":
				require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(data), `"app":"brain"`, `"app":"new"`, 1)), 0600))
			case "public-dir":
				require.NoError(t, os.Chmod(dir, 0755))
			case "dir-link":
				link := filepath.Join(t.TempDir(), "link")
				require.NoError(t, os.Symlink(dir, link))
				dir = link
			}
			got, err := LoadNetworkRecovery(dir, plan)
			require.Error(t, err)
			require.Zero(t, got)
		})
	}
}
