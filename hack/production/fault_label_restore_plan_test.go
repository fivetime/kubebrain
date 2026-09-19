package production_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFaultLabelRestorePlan(t *testing.T) {
	for _, mode := range []string{"owned", "absent", "null-labels", "replacement", "namespace-replaced", "pod-deleting", "namespace-deleting", "original-owned-label", "label-changed", "missing-rv", "policy-remains", "nonce-remains", "paginated", "missing-policies", "invalid-list", "absent-with-policy"} {
		t.Run(mode, func(t *testing.T) {
			pod := func(labels any) map[string]any {
				return map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": "test-ns", "name": "target-0", "uid": "pod-uid", "resourceVersion": "9007199254740993", "labels": labels}}
			}
			binding := map[string]any{"namespace": "test-ns", "namespaceUID": "ns-uid", "name": "target-0", "uid": "pod-uid", "nonce": "fresh-owner", "policyName": "owned-policy"}
			nsMeta := map[string]any{"name": "test-ns", "uid": "ns-uid"}
			before := pod(map[string]any{"app": "preserve"})
			labels := map[string]any{"app": "preserve", "kubebrain.io/fault-owner": "fresh-owner"}
			current := pod(labels)
			policies := map[string]any{"apiVersion": "cilium.io/v2", "kind": "CiliumNetworkPolicyList", "metadata": map[string]any{}, "items": []any{}}
			policy := map[string]any{"apiVersion": "cilium.io/v2", "kind": "CiliumNetworkPolicy", "metadata": map[string]any{"namespace": "test-ns", "name": "owned-policy"}, "spec": map[string]any{}}
			input := map[string]any{"binding": binding, "namespace": map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": nsMeta}, "expected": before, "current": current, "policies": policies}
			switch mode {
			case "absent":
				delete(labels, "kubebrain.io/fault-owner")
			case "null-labels":
				current["metadata"].(map[string]any)["labels"] = nil
			case "replacement":
				current["metadata"].(map[string]any)["uid"] = "new-uid"
			case "namespace-replaced":
				nsMeta["uid"] = "new-namespace"
			case "pod-deleting":
				current["metadata"].(map[string]any)["deletionTimestamp"] = "2026-09-19T00:00:00Z"
			case "namespace-deleting":
				nsMeta["deletionTimestamp"] = "2026-09-19T00:00:00Z"
			case "original-owned-label":
				before["metadata"].(map[string]any)["labels"] = labels
			case "label-changed":
				labels["kubebrain.io/fault-owner"] = "other-owner"
			case "missing-rv":
				delete(current["metadata"].(map[string]any), "resourceVersion")
			case "policy-remains":
				policies["items"] = []any{policy}
			case "absent-with-policy":
				policies["items"] = []any{policy}
				delete(labels, "kubebrain.io/fault-owner")
			case "nonce-remains":
				policy["metadata"].(map[string]any)["name"] = "different-policy"
				policy["specs"] = []any{map[string]any{"endpointSelector": map[string]any{"matchLabels": map[string]any{"kubebrain.io/fault-owner": "fresh-owner"}}}}
				policies["items"] = []any{policy}
			case "paginated":
				policies["metadata"].(map[string]any)["continue"] = "next-page"
			case "missing-policies":
				delete(input, "policies")
			case "invalid-list":
				policies["items"] = nil
			}
			data, err := json.Marshal(input)
			require.NoError(t, err)
			cmd := exec.Command("jq", "-e", "-f", "fault-label-restore-plan.jq")
			cmd.Stdin = bytes.NewReader(data)
			var out, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &stderr
			err = cmd.Run()
			if mode != "owned" && mode != "absent" && mode != "null-labels" {
				require.Error(t, err)
				require.Empty(t, out.String())
				return
			}
			require.NoError(t, err, stderr.String())
			if mode != "owned" {
				require.JSONEq(t, `[]`, out.String())
				return
			}
			require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"pod-uid"},{"op":"test","path":"/metadata/resourceVersion","value":"9007199254740993"},{"op":"test","path":"/metadata/labels/kubebrain.io~1fault-owner","value":"fresh-owner"},{"op":"remove","path":"/metadata/labels/kubebrain.io~1fault-owner"}]`, out.String())
		})
	}
}
