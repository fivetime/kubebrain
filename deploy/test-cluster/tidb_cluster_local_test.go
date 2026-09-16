package testcluster_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLocalBackendIsIndependentRetainedAndBaselineShaped(t *testing.T) {
	data, err := os.ReadFile("tidb-cluster-local.yaml")
	require.NoError(t, err)
	var v map[string]any
	require.NoError(t, yaml.Unmarshal(data, &v))
	require.Equal(t, "kb-local", field(t, v, "metadata", "name"))
	require.Equal(t, "kubebrain-dbaas-test", field(t, v, "metadata", "namespace"))
	require.Equal(t, "v8.5.3", field(t, v, "spec", "version"))
	require.Equal(t, "Retain", field(t, v, "spec", "pvReclaimPolicy"))
	require.Equal(t, false, field(t, v, "spec", "enablePVReclaim"))
	for _, c := range []string{"pd", "tikv"} {
		spec := field(t, v, "spec", c).(map[string]any)
		require.Equal(t, 3, field(t, spec, "replicas"))
		require.Equal(t, 0, field(t, spec, "maxFailoverCount"))
		require.Equal(t, "kubebrain-local-lvm", field(t, spec, "storageClassName"))
		require.Equal(t, []any{map[string]any{"matchExpressions": []any{
			map[string]any{"key": "kubernetes.io/hostname", "operator": "In", "values": []any{"k8s3-worker1", "k8s3-worker2", "k8s3-worker3"}},
			map[string]any{"key": "kubebrain.io/topolvm", "operator": "In", "values": []any{"enabled"}},
		}}}, field(t, spec, "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"))
		require.Equal(t, []any{map[string]any{
			"labelSelector": map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/component": c, "app.kubernetes.io/instance": "kb-local"}},
			"topologyKey":   "kubernetes.io/hostname",
		}}, field(t, spec, "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution"))
	}
	require.Equal(t, map[string]any{"cpu": "1", "memory": "2Gi", "storage": "20Gi"}, field(t, v, "spec", "pd", "requests"))
	require.Equal(t, map[string]any{"cpu": "2", "memory": "4Gi"}, field(t, v, "spec", "pd", "limits"))
	require.Equal(t, map[string]any{"cpu": "4", "memory": "8Gi", "storage": "100Gi"}, field(t, v, "spec", "tikv", "requests"))
	require.Equal(t, map[string]any{"cpu": "8", "memory": "16Gi"}, field(t, v, "spec", "tikv", "limits"))
	require.Equal(t, "", field(t, v, "spec", "pd", "config"))
	require.Equal(t, "[storage]\n  max-key-size = 2621440\n", field(t, v, "spec", "tikv", "config"))
}
