package compat

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneAuditPolicy(t *testing.T) {
	for _, enabled := range []string{"false", "true"} {
		t.Run(enabled, func(t *testing.T) {
			out, err := runCompatCommandContext(t, context.Background(), "jq", []string{
				"-n", "--argjson", "kwok", enabled, "-f", filepath.Join("..", "scale-lab", "controlplane-audit-policy.jq"),
			}, nil)
			require.NoError(t, err, "%s", out)
			var policy struct {
				APIVersion string            `json:"apiVersion"`
				Kind       string            `json:"kind"`
				OmitStages []string          `json:"omitStages"`
				Rules      []json.RawMessage `json:"rules"`
			}
			require.NoError(t, json.Unmarshal([]byte(out), &policy))
			require.Equal(t, "audit.k8s.io/v1", policy.APIVersion)
			require.Equal(t, "Policy", policy.Kind)
			require.Equal(t, []string{"RequestReceived"}, policy.OmitStages)
			rules := policy.Rules
			if enabled == "true" {
				require.Len(t, rules, 4)
				// Exact scope: no watch response stream, Secret, token, other user,
				// namespace, resource, or wildcard body logging is introduced.
				require.JSONEq(t, `{"level":"RequestResponse","users":["kubebrain-test-kwok"],"verbs":["get","list"],"namespaces":["kube-node-lease"],"resources":[{"group":"coordination.k8s.io","resources":["leases"]}]}`, string(rules[0]))
				require.JSONEq(t, `{"level":"RequestResponse","users":["kubebrain-test-kwok"],"verbs":["patch","update"],"resources":[{"group":"","resources":["nodes/status","pods/status"]},{"group":"coordination.k8s.io","resources":["leases"]}]}`, string(rules[1]))
				rules = rules[2:]
			}
			require.Len(t, rules, 2)
			require.JSONEq(t, `{"level":"RequestResponse","verbs":["create"],"namespaces":["controlplane-smoke"],"resources":[{"group":"apps","resources":["replicasets"]},{"group":"","resources":["pods"]}]}`, string(rules[0]))
			require.JSONEq(t, `{"level":"Metadata"}`, string(rules[1]))
		})
	}
}
