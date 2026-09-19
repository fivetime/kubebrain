package testcluster_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNonceEndpointSnapshots(t *testing.T) {
	for _, mode := range []string{"unlabelled", "target-active", "pending-active", "reserved", "foreign-active", "foreign-source", "unprefixed-reserved", "stale-derived", "stale-disabled", "stale-user", "stale-realized", "target-foreign-owner", "missing-agent", "duplicate-agent", "replaced-agent", "missing-target", "duplicate-endpoint", "wrong-pod", "wrong-ip", "missing-labels", "malformed-labels"} {
		t.Run(mode, func(t *testing.T) {
			const active = "k8s:kubebrain.io/fault-owner=term-test"
			endpoint := func(id int, pod string) map[string]any {
				return map[string]any{"id": id, "status": map[string]any{"identity": map[string]any{"labels": []string{"k8s:app=brain"}}, "external-identifiers": map[string]any{"k8s-namespace": "kubebrain-dbaas-test", "k8s-pod-name": pod}, "networking": map[string]any{"addressing": []any{map[string]any{"ipv4": "10.0.0.1"}}}}}
			}
			target, other := endpoint(42, "kubebrain-local-0"), endpoint(43, "other")
			targetStatus := target["status"].(map[string]any)
			otherStatus := other["status"].(map[string]any)
			setIdentity := func(status map[string]any, label string) {
				status["identity"] = map[string]any{"labels": []string{"k8s:app=brain", label}}
			}
			agent := map[string]any{"uid": "agent-1", "node": "node-1", "endpoints": []any{target}}
			otherAgent := map[string]any{"uid": "agent-2", "node": "node-2", "endpoints": []any{other}}
			agents := []any{agent, otherAgent}
			switch mode {
			case "target-active":
				setIdentity(targetStatus, active)
			case "pending-active":
				targetStatus["labels"] = map[string]any{"security-relevant": []string{active}}
			case "reserved":
				setIdentity(targetStatus, "k8s:kubebrain.io/fault-owner=reserved-test")
			case "foreign-active":
				setIdentity(otherStatus, active)
			case "foreign-source":
				setIdentity(otherStatus, "container:kubebrain.io/fault-owner=term-test")
			case "unprefixed-reserved":
				setIdentity(otherStatus, "kubebrain.io/fault-owner=reserved-test")
			case "stale-derived", "stale-disabled":
				field := "derived"
				if mode == "stale-disabled" {
					field = "disabled"
				}
				otherStatus["labels"] = map[string]any{field: []string{active}}
			case "stale-user":
				other["spec"] = map[string]any{"label-configuration": map[string]any{"user": []string{active}}}
			case "stale-realized":
				otherStatus["realized"] = map[string]any{"label-configuration": map[string]any{"user": []string{active}}}
			case "target-foreign-owner":
				setIdentity(targetStatus, "k8s:kubebrain.io/fault-owner=term-other")
			case "missing-agent":
				agents = agents[:1]
			case "duplicate-agent":
				agents = append(agents, agent)
			case "replaced-agent":
				agent["uid"] = "other"
			case "missing-target":
				agent["endpoints"] = []any{}
			case "duplicate-endpoint":
				agent["endpoints"] = []any{target, target}
			case "wrong-pod":
				targetStatus["external-identifiers"].(map[string]any)["k8s-pod-name"] = "other"
			case "wrong-ip":
				targetStatus["networking"] = map[string]any{"addressing": []any{map[string]any{"ipv4": "other"}}}
			case "missing-labels":
				delete(otherStatus, "identity")
			case "malformed-labels":
				otherStatus["labels"] = map[string]any{"derived": active}
			}
			input, err := json.Marshal(map[string]any{"agents": agents, "expected_agents": []any{map[string]any{"uid": "agent-1", "node": "node-1"}, map[string]any{"uid": "agent-2", "node": "node-2"}}, "target": map[string]any{"node": "node-1", "endpoint_id": 42, "namespace": "kubebrain-dbaas-test", "pod": "kubebrain-local-0", "ipv4": "10.0.0.1"}})
			require.NoError(t, err)
			cmd := exec.Command("jq", "-e", "--arg", "active", "term-test", "--arg", "reserved", "reserved-test", "-f", "local-nonce-endpoints.jq")
			cmd.Stdin = bytes.NewReader(input)
			out, err := cmd.CombinedOutput()
			if mode == "unlabelled" || mode == "target-active" || mode == "pending-active" {
				require.NoError(t, err, string(out))
				require.Contains(t, string(out), "nonce_snapshot_only_not_enforcement")
			} else {
				require.Error(t, err, string(out))
			}
		})
	}
}
