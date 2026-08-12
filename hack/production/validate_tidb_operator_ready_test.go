package production_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateTiDBOperatorReadyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *"get deployment tidb-controller-manager -o json"*)
    count=0; [[ ! -f "$FAKE_DEPLOYMENT_CALL_COUNT" ]] || count="$(<"$FAKE_DEPLOYMENT_CALL_COUNT")"
    printf '%s' "$((count + 1))" >"$FAKE_DEPLOYMENT_CALL_COUNT"
    if ((count > 0)) && [[ -n "${FAKE_FINAL_DEPLOYMENT_JSON:-}" ]]; then printf '%s' "$FAKE_FINAL_DEPLOYMENT_JSON"; else printf '%s' "$FAKE_DEPLOYMENT_JSON"; fi ;;
  *"get replicasets -o json"*) printf '%s' "$FAKE_REPLICASETS_JSON" ;;
  *"get pods -o json"*) printf '%s' "$FAKE_PODS_JSON" ;;
  *) printf 'unexpected kubectl call: %s\n' "$*" >&2; exit 1 ;;
esac
`), 0o755))

	deployment := fakeOperatorDeploymentJSON("uid-operator", "pingcap/tidb-operator:v1.6.5", true)
	replicaSets := fakeOperatorReplicaSetsJSON("uid-operator", "pingcap/tidb-operator:v1.6.5")
	pods := fakeOperatorPodsJSON("docker-pullable://pingcap/tidb-operator@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true)
	base := []string{
		"KUBE_CONTEXT=production",
		"KUBECTL=" + kubectl,
		"JQ=jq",
		"EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID=uid-operator",
		"EXPECTED_TIDB_OPERATOR_IMAGE=pingcap/tidb-operator:v1.6.5",
		"EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"EXPECTED_TIDB_OPERATOR_POD_TEMPLATE_HASH=abc",
		"FAKE_DEPLOYMENT_JSON=" + deployment,
		"FAKE_REPLICASETS_JSON=" + replicaSets,
		"FAKE_PODS_JSON=" + pods,
		"FAKE_DEPLOYMENT_CALL_COUNT=" + filepath.Join(dir, "deployment-calls"),
	}

	require.NoError(t, os.RemoveAll(filepath.Join(dir, "deployment-calls")))
	output, err := runProductionScriptCommand(t, "validate-tidb-operator-ready.sh", base)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiDB Operator release gate passed")

	for _, tc := range []struct {
		name string
		env  string
		want string
	}{
		{name: "runtime digest drift", env: "FAKE_PODS_JSON=" + fakeOperatorPodsJSON("containerd://sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", true), want: "runtime release mismatch"},
		{name: "Pod unready", env: "FAKE_PODS_JSON=" + fakeOperatorPodsJSON("containerd://sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false), want: "runtime release mismatch"},
		{name: "Deployment unready", env: "FAKE_DEPLOYMENT_JSON=" + fakeOperatorDeploymentJSON("uid-operator", "pingcap/tidb-operator:v1.6.5", false), want: "Deployment release mismatch"},
		{name: "Deployment UID drift", env: "FAKE_DEPLOYMENT_JSON=" + fakeOperatorDeploymentJSON("uid-other", "pingcap/tidb-operator:v1.6.5", true), want: "Deployment release mismatch"},
		{name: "second ReplicaSet starts rollout", env: "FAKE_REPLICASETS_JSON=" + fakeOperatorReplicaSetsDuringRolloutJSON("uid-operator", "pingcap/tidb-operator:v1.6.5"), want: "ReplicaSet rollout is not quiescent"},
		{name: "rollout starts during validation", env: "FAKE_FINAL_DEPLOYMENT_JSON=" + fakeOperatorDeploymentGenerationJSON("uid-operator", "pingcap/tidb-operator:v1.6.6", 4), want: "Deployment changed during validation"},
		{name: "Pod template hash is required", env: "EXPECTED_TIDB_OPERATOR_POD_TEMPLATE_HASH=", want: "must be a DNS label"},
		{name: "Pod template hash drift", env: "EXPECTED_TIDB_OPERATOR_POD_TEMPLATE_HASH=approved", want: "ReplicaSet pod-template-hash mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.RemoveAll(filepath.Join(dir, "deployment-calls")))
			failedOutput, failedErr := runProductionScriptCommand(t, "validate-tidb-operator-ready.sh", append(base, tc.env))
			require.Error(t, failedErr, string(failedOutput))
			require.Contains(t, string(failedOutput), tc.want)
		})
	}
}

func fakeOperatorDeploymentJSON(uid, image string, ready bool) string {
	readyReplicas := 1
	if !ready {
		readyReplicas = 0
	}
	return mustJSON(map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "tidb-controller-manager", "uid": uid, "generation": 3},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "tidb-controller-manager", "image": image}}}}},
		"status":   map[string]any{"observedGeneration": 3, "readyReplicas": readyReplicas, "updatedReplicas": 1, "availableReplicas": readyReplicas},
	})
}

func fakeOperatorDeploymentGenerationJSON(uid, image string, generation int) string {
	return mustJSON(map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "tidb-controller-manager", "uid": uid, "generation": generation},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "tidb-controller-manager", "image": image}}}}},
		"status":   map[string]any{"observedGeneration": 3, "readyReplicas": 1, "updatedReplicas": 1, "availableReplicas": 1},
	})
}

func fakeOperatorReplicaSetsJSON(deploymentUID, image string) string {
	return mustJSON(map[string]any{"items": []map[string]any{{
		"metadata": map[string]any{"name": "tidb-controller-manager-abc", "uid": "uid-rs", "labels": map[string]any{"pod-template-hash": "abc"}, "ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "Deployment", "name": "tidb-controller-manager", "uid": deploymentUID, "controller": true}}},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "tidb-controller-manager", "image": image}}}}},
		"status":   map[string]any{"readyReplicas": 1, "availableReplicas": 1},
	}}})
}

func fakeOperatorReplicaSetsDuringRolloutJSON(deploymentUID, image string) string {
	base := map[string]any{
		"metadata": map[string]any{"name": "tidb-controller-manager-abc", "uid": "uid-rs", "labels": map[string]any{"pod-template-hash": "abc"}, "ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "Deployment", "name": "tidb-controller-manager", "uid": deploymentUID, "controller": true}}},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "tidb-controller-manager", "image": image}}}}},
		"status":   map[string]any{"replicas": 1, "readyReplicas": 1, "availableReplicas": 1},
	}
	newReplicaSet := map[string]any{
		"metadata": map[string]any{"name": "tidb-controller-manager-new", "uid": "uid-rs-new", "labels": map[string]any{"pod-template-hash": "new"}, "ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "Deployment", "name": "tidb-controller-manager", "uid": deploymentUID, "controller": true}}},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": "tidb-controller-manager", "image": "pingcap/tidb-operator:v1.6.6"}}}}},
		"status":   map[string]any{"replicas": 0, "readyReplicas": 0, "availableReplicas": 0},
	}
	return mustJSON(map[string]any{"items": []map[string]any{base, newReplicaSet}})
}

func fakeOperatorPodsJSON(imageID string, ready bool) string {
	return mustJSON(map[string]any{"items": []map[string]any{{
		"metadata": map[string]any{"name": "tidb-controller-manager-abc-0", "labels": map[string]any{"pod-template-hash": "abc"}, "ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "tidb-controller-manager-abc", "uid": "uid-rs", "controller": true}}},
		"spec":     map[string]any{"containers": []map[string]any{{"name": "tidb-controller-manager", "image": "pingcap/tidb-operator:v1.6.5"}}},
		"status":   map[string]any{"phase": "Running", "conditions": []map[string]any{{"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[ready]}}, "containerStatuses": []map[string]any{{"name": "tidb-controller-manager", "ready": ready, "imageID": imageID}}},
	}}})
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
