package testcluster_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalFrontendIsIsolatedAndUsesAuditedDefault2PC(t *testing.T) {
	data, err := os.ReadFile("kubebrain-local.json")
	require.NoError(t, err)
	var list struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(data, &list))
	require.Len(t, list.Items, 6)
	kinds := map[string]int{}
	for _, obj := range list.Items {
		kinds[obj["kind"].(string)]++
		name := field(t, obj, "metadata", "name").(string)
		require.True(t, strings.HasPrefix(name, "kubebrain-local"))
		require.Equal(t, "kubebrain-dbaas-test", field(t, obj, "metadata", "namespace"))
		require.NotContains(t, obj, "status")
		if obj["kind"] == "Service" {
			require.Equal(t, "kubebrain-local", field(t, obj, "spec", "selector", "app.kubernetes.io/instance"))
		}
		if obj["kind"] != "StatefulSet" {
			continue
		}
		require.Equal(t, float64(3), field(t, obj, "spec", "replicas"))
		require.Equal(t, "kubebrain-local", field(t, obj, "spec", "selector", "matchLabels", "app.kubernetes.io/instance"))
		require.Equal(t, "kubebrain-local", field(t, obj, "spec", "template", "metadata", "labels", "app.kubernetes.io/instance"))
		pod := field(t, obj, "spec", "template", "spec").(map[string]any)
		require.Equal(t, false, field(t, pod, "automountServiceAccountToken"))
		containers := field(t, pod, "containers").([]any)
		require.Len(t, containers, 1)
		c := containers[0].(map[string]any)
		require.Equal(t, "ghcr.io/fivetime/kubebrain@sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537", c["image"])
		args := c["args"].([]any)
		for _, a := range []string{"--pd-addrs=kb-local-pd.kubebrain-dbaas-test.svc:2379", "--keyspace=kubebrain-local", "--cluster-name=kubebrain-local", "--allow-insecure=false", "--peer-allow-insecure=false", "--client-cert-auth=true", "--peer-client-cert-auth=true"} {
			require.Contains(t, args, a)
		}
		for _, a := range args {
			s := a.(string)
			require.NotContains(t, s, "1pc")
			require.NotContains(t, s, "async-commit")
			require.NotContains(t, s, "=kb-pd.")
			require.NotContains(t, s, "kubebrain-peer.")
			require.NotContains(t, s, "kubebrain-client.")
		}
		secrets, ephemeral := 0, 0
		for _, raw := range pod["volumes"].([]any) {
			v := raw.(map[string]any)
			if _, ok := v["secret"]; ok {
				secrets++
				require.True(t, strings.HasPrefix(field(t, v, "secret", "secretName").(string), "kubebrain-local-"))
			} else {
				ephemeral++
				require.Equal(t, "kubebrain-local-lvm", field(t, v, "ephemeral", "volumeClaimTemplate", "spec", "storageClassName"))
			}
		}
		require.Equal(t, 3, secrets)
		require.Equal(t, 2, ephemeral)
	}
	require.Equal(t, map[string]int{"StatefulSet": 1, "Service": 2, "ServiceAccount": 1, "NetworkPolicy": 1, "PodDisruptionBudget": 1}, kinds)
}
