package testcluster_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func values(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile("topolvm-values.yaml")
	require.NoError(t, err)
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var result map[string]any
	require.NoError(t, decoder.Decode(&result))
	var extra any
	require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
	return result
}

func field(t *testing.T, object map[string]any, path ...string) any {
	t.Helper()
	var result any = object
	for _, key := range path {
		mapping, ok := result.(map[string]any)
		require.True(t, ok, "non-mapping in path %v at %s", path, key)
		result, ok = mapping[key]
		require.True(t, ok, "missing explicit value %v at %s", path, key)
	}
	return result
}

func TestTopoLVMHasNoOptionalClusterWideAdmissionOrScheduler(t *testing.T) {
	v := values(t)
	for _, path := range [][]string{
		{"useLegacy"}, {"scheduler", "enabled"}, {"webhook", "certManager"},
		{"webhook", "podMutatingWebhook", "enabled"}, {"cert-manager", "enabled"},
		{"snapshot", "enabled"}, {"node", "lvmdEmbedded"},
	} {
		require.Equal(t, false, field(t, v, path...), "path %v must be explicitly disabled", path)
	}
	require.Equal(t, true, field(t, v, "lvmd", "managed"))
	require.Equal(t, true, field(t, v, "controller", "storageCapacityTracking", "enabled"))
}

func TestTopoLVMAllWorkloadsRequireExactDedicatedNodes(t *testing.T) {
	v := values(t)
	var expected map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(`requiredDuringSchedulingIgnoredDuringExecution:
  nodeSelectorTerms:
    - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values: [k8s3-worker1, k8s3-worker2, k8s3-worker3]
`), &expected))
	for _, component := range []string{"lvmd", "node", "controller"} {
		t.Run(component, func(t *testing.T) {
			require.Equal(t, map[string]any{"kubebrain.io/topolvm": "enabled"}, field(t, v, component, "nodeSelector"))
			var affinity map[string]any
			if component == "controller" {
				text, ok := field(t, v, component, "affinity").(string)
				require.True(t, ok)
				require.NoError(t, yaml.Unmarshal([]byte(text), &affinity))
			} else {
				var ok bool
				affinity, ok = field(t, v, component, "affinity").(map[string]any)
				require.True(t, ok)
			}
			require.Equal(t, expected, field(t, affinity, "nodeAffinity"), "extra OR terms could allow other nodes")
		})
	}
	lvmd := field(t, v, "lvmd").(map[string]any)
	require.NotContains(t, lvmd, "additionalConfigs", "additional lvmd deployments need their own scope audit")
	require.Equal(t, 2, field(t, v, "controller", "replicaCount"))
}

func TestTopoLVMStorageIsNonDefaultRetainedAndTopologyBound(t *testing.T) {
	v := values(t)
	classes, ok := field(t, v, "storageClasses").([]any)
	require.True(t, ok)
	require.Len(t, classes, 1)
	sc, ok := classes[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "kubebrain-local-lvm", field(t, sc, "name"))
	require.Equal(t, map[string]any{
		"fsType": "ext4", "reclaimPolicy": "Retain", "isDefaultClass": false,
		"volumeBindingMode": "WaitForFirstConsumer", "allowVolumeExpansion": true,
		"additionalParameters": map[string]any{"topolvm.io/device-class": "local-test"},
	}, field(t, sc, "storageClass"))
	require.Equal(t, []any{map[string]any{
		"name": "local-test", "volume-group": "kubebrain-local", "default": true, "spare-gb": 20,
	}}, field(t, v, "lvmd", "deviceClasses"))
}

func TestTopoLVMUsesReviewedImmutableImage(t *testing.T) {
	v := values(t)
	require.Equal(t, map[string]any{
		"reference": "0.41.1@sha256:70548dbe0c6addcccf79a557f29e95db2e6dc2cba2102988c91f30086004d0fc",
	}, field(t, v, "image"), "image override changes require a new chart and rendered-image review")
}
