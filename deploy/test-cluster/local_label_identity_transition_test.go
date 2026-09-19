package testcluster_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalLabelIdentityTransition(t *testing.T) {
	for _, tc := range []struct {
		name, mode, state, mutation, want string
		ids                               [3]int
		owned                             [3]bool
	}{
		{"present", "present", "ready", "", "matched", [3]int{2, 2, 2}, [3]bool{true, true, true}},
		{"absent", "absent", "ready", "", "matched", [3]int{1, 1, 1}, [3]bool{}},
		{"waiting", "present", "waiting-for-identity", "", "pending", [3]int{1, 1, 1}, [3]bool{}},
		{"regenerating", "present", "regenerating", "", "pending", [3]int{2, 2, 2}, [3]bool{true, true, true}},
		{"cep-lags-add", "present", "regenerating", "", "pending", [3]int{1, 2, 2}, [3]bool{false, true, true}},
		{"cep-lags-remove", "absent", "regenerating", "", "pending", [3]int{2, 1, 1}, [3]bool{true, false, false}},
		{"ready-but-cep-lags", "absent", "ready", "", "pending", [3]int{2, 1, 1}, [3]bool{true, false, false}},
		{"ready-wrong-label", "present", "ready", "", "pending", [3]int{1, 1, 1}, [3]bool{}},
		{"unknown-state", "present", "disconnected", "", "", [3]int{1, 1, 1}, [3]bool{}},
		{"same-id-different-labels", "present", "ready", "", "", [3]int{1, 1, 1}, [3]bool{false, true, true}},
		{"different-id-same-labels", "present", "ready", "", "", [3]int{1, 2, 1}, [3]bool{}},
		{"foreign-owner", "present", "regenerating", "foreign", "", [3]int{1, 2, 1}, [3]bool{false, true, false}},
		{"non-owner-change", "present", "regenerating", "base", "", [3]int{1, 2, 1}, [3]bool{false, true, false}},
		{"duplicate-label", "present", "ready", "duplicate", "", [3]int{1, 1, 1}, [3]bool{}},
		{"missing-labels", "present", "ready", "missing", "", [3]int{1, 1, 1}, [3]bool{}},
		{"cep-replaced", "present", "ready", "uid", "", [3]int{1, 1, 1}, [3]bool{}},
		{"endpoint-replaced", "present", "ready", "endpoint", "", [3]int{1, 1, 1}, [3]bool{}},
		{"network-change", "present", "ready", "network", "", [3]int{1, 1, 1}, [3]bool{}},
		{"owner-change", "present", "ready", "owner", "", [3]int{1, 1, 1}, [3]bool{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity := func(i int) map[string]any {
				labels := []string{"k8s:app=kubebrain", "k8s:namespace=test"}
				if tc.owned[i] {
					labels = append(labels, "k8s:kubebrain.io/fault-owner=term-test")
				}
				return map[string]any{"id": tc.ids[i], "labels": labels}
			}
			cep := func(i int) map[string]any {
				return map[string]any{"metadata": map[string]any{"uid": "cep", "ownerReferences": []any{map[string]any{"kind": "Pod", "uid": "pod"}}}, "status": map[string]any{"id": 42, "identity": identity(i), "networking": map[string]any{"ip": "10.0.0.1"}}}
			}
			before, after := cep(0), cep(2)
			ei := identity(1)
			endpoint := map[string]any{"id": 42, "status": map[string]any{"state": tc.state, "identity": ei}}
			switch tc.mutation {
			case "foreign":
				ei["labels"] = []string{"k8s:app=kubebrain", "k8s:namespace=test", "k8s:kubebrain.io/fault-owner=term-foreign"}
			case "base":
				ei["labels"] = []string{"k8s:app=other", "k8s:namespace=test"}
			case "duplicate":
				ei["labels"] = []string{"k8s:app=kubebrain", "k8s:app=kubebrain", "k8s:namespace=test"}
			case "missing":
				delete(ei, "labels")
			case "uid":
				after["metadata"].(map[string]any)["uid"] = "replacement"
			case "endpoint":
				endpoint["id"] = 43
			case "network":
				after["status"].(map[string]any)["networking"] = map[string]any{"ip": "other"}
			case "owner":
				after["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"kind": "Pod", "uid": "replacement"}}
			}
			input, err := json.Marshal(map[string]any{"cep_before": before, "endpoint": endpoint, "cep_after": after})
			require.NoError(t, err)
			cmd := exec.Command("jq", "-e", "--arg", "mode", tc.mode, "--arg", "token", "term-test", "-f", "local-label-identity-transition.jq")
			cmd.Stdin = bytes.NewReader(input)
			output, err := cmd.CombinedOutput()
			if tc.want == "" {
				require.Error(t, err, string(output))
				return
			}
			require.NoError(t, err, string(output))
			var result struct {
				State string `json:"state"`
			}
			require.NoError(t, json.Unmarshal(output, &result))
			require.Equal(t, tc.want, result.State)
		})
	}
}
