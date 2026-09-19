package production_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFaultPolicyDeletePlan(t *testing.T) {
	for _, mode := range []string{"active", "inactive", "absent", "replacement", "changed-spec", "namespace-replaced", "namespace-deleting", "bad-receipt", "missing-rv", "wrong-approved-name", "same-nonce", "extra-specs", "absent-bad-receipt", "missing-current"} {
		t.Run(mode, func(t *testing.T) {
			policy := func(nonce string) map[string]any {
				return map[string]any{"apiVersion": "cilium.io/v2", "kind": "CiliumNetworkPolicy",
					"metadata": map[string]any{"name": "owned-policy", "namespace": "test-ns", "uid": "created-uid", "resourceVersion": "9007199254740993"},
					"spec":     map[string]any{"endpointSelector": map[string]any{"matchLabels": map[string]any{"kubebrain.io/fault-owner": nonce}}, "egressDeny": []any{map[string]any{"toCIDR": []string{"10.0.0.1/32"}}}}}
			}
			binding := map[string]any{"namespace": "test-ns", "namespaceUID": "ns-uid", "name": "owned-policy", "nonce": "fresh-owner", "reservedNonce": "fresh-reserved"}
			nsMeta := map[string]any{"name": "test-ns", "uid": "ns-uid"}
			approved, receipt, current := policy("fresh-owner"), policy("fresh-reserved"), policy("fresh-owner")
			input := map[string]any{"binding": binding, "namespace": map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": nsMeta}, "approved": approved, "expected": receipt, "current": current}
			switch mode {
			case "inactive":
				input["current"] = policy("fresh-reserved")
			case "absent":
				input["current"] = nil
			case "missing-current":
				delete(input, "current")
			case "replacement":
				current["metadata"].(map[string]any)["uid"] = "new-uid"
			case "changed-spec":
				current["spec"].(map[string]any)["egressDeny"] = []any{}
			case "namespace-replaced":
				nsMeta["uid"] = "new-namespace"
			case "namespace-deleting":
				nsMeta["deletionTimestamp"] = "2026-09-19T00:00:00Z"
			case "bad-receipt":
				input["expected"] = policy("fresh-owner")
			case "absent-bad-receipt":
				input["expected"] = policy("fresh-owner")
				input["current"] = nil
			case "missing-rv":
				delete(current["metadata"].(map[string]any), "resourceVersion")
			case "wrong-approved-name":
				approved["metadata"].(map[string]any)["name"] = "unrelated"
			case "same-nonce":
				binding["reservedNonce"] = binding["nonce"]
			case "extra-specs":
				current["specs"] = []any{}
			}
			data, err := json.Marshal(input)
			require.NoError(t, err)
			cmd := exec.Command("jq", "-e", "-f", "fault-policy-delete-plan.jq")
			cmd.Stdin = bytes.NewReader(data)
			var out, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &stderr
			err = cmd.Run()
			if mode != "active" && mode != "inactive" && mode != "absent" {
				require.Error(t, err)
				require.Empty(t, out.String())
				return
			}
			require.NoError(t, err, stderr.String())
			if mode == "absent" {
				require.JSONEq(t, `{"absent":true}`, out.String())
				return
			}
			require.JSONEq(t, `{"apiVersion":"cilium.io/v2","resource":"ciliumnetworkpolicies","namespace":"test-ns","name":"owned-policy","uid":"created-uid","resourceVersion":"9007199254740993"}`, out.String())
		})
	}
}
