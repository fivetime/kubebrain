package production_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestProductionManifestsProvideStableCompleteMembership(t *testing.T) {
	for _, tc := range []struct {
		file   string
		scheme string
	}{
		{file: "kubebrain.yaml", scheme: "http"},
		{file: "kubebrain-tls.yaml", scheme: "https"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			objects := decodeManifest(t, tc.file)
			workload := objectByKindAndName(t, objects, "StatefulSet", "kubebrain")
			require.Equal(t, "kubebrain-peer", nestedString(t, workload, "spec", "serviceName"))
			require.EqualValues(t, 3, nestedInt64(t, workload, "spec", "replicas"))
			require.False(t, nestedBool(t, workload, "spec", "template", "spec", "automountServiceAccountToken"))
			require.True(t, nestedBool(t, workload, "spec", "template", "spec", "securityContext", "runAsNonRoot"))
			require.EqualValues(t, 65532, nestedInt64(t, workload, "spec", "template", "spec", "securityContext", "runAsUser"))
			require.Equal(t, "RuntimeDefault", nestedString(t, workload, "spec", "template", "spec", "securityContext", "seccompProfile", "type"))

			requiredAntiAffinity, found, err := unstructured.NestedSlice(
				workload.Object, "spec", "template", "spec", "affinity", "podAntiAffinity",
				"requiredDuringSchedulingIgnoredDuringExecution",
			)
			require.NoError(t, err)
			require.True(t, found)
			require.NotEmpty(t, requiredAntiAffinity)

			containers, found, err := unstructured.NestedSlice(workload.Object, "spec", "template", "spec", "containers")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, containers, 1)
			container := containers[0].(map[string]any)
			containerObject := &unstructured.Unstructured{Object: container}
			require.False(t, nestedBool(t, containerObject, "securityContext", "allowPrivilegeEscalation"))
			require.True(t, nestedBool(t, containerObject, "securityContext", "readOnlyRootFilesystem"))
			dropped, found, err := unstructured.NestedStringSlice(container, "securityContext", "capabilities", "drop")
			require.NoError(t, err)
			require.True(t, found)
			require.Contains(t, dropped, "ALL")
			require.Equal(t, "500m", nestedString(t, containerObject, "resources", "requests", "cpu"))
			require.Equal(t, "1Gi", nestedString(t, containerObject, "resources", "requests", "memory"))
			require.Equal(t, "2", nestedString(t, containerObject, "resources", "limits", "cpu"))
			require.Equal(t, "4Gi", nestedString(t, containerObject, "resources", "limits", "memory"))
			args, found, err := unstructured.NestedStringSlice(container, "args")
			require.NoError(t, err)
			require.True(t, found)
			require.Contains(t, args, "--advertise-host=$(POD_NAME).kubebrain-peer.kubebrain-system.svc")
			require.Contains(t, args, "--initial-cluster="+expectedInitialCluster(tc.scheme))
			require.Contains(t, args, "--max-requests-inflight=1024")
			require.Contains(t, args, "--max-request-rate=2000")
			require.Contains(t, args, "--request-rate-burst=4000")
			require.Contains(t, args, "--max-watches=10000")

			env, found, err := unstructured.NestedSlice(container, "env")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, env, 1)
			podName := env[0].(map[string]any)
			require.Equal(t, "POD_NAME", podName["name"])
			require.Equal(t, "metadata.name", nestedString(t, &unstructured.Unstructured{Object: podName}, "valueFrom", "fieldRef", "fieldPath"))

			serviceAccount := objectByKindAndName(t, objects, "ServiceAccount", "kubebrain")
			require.False(t, nestedBool(t, serviceAccount, "automountServiceAccountToken"))

			peer := objectByKindAndName(t, objects, "Service", "kubebrain-peer")
			require.Equal(t, "None", nestedString(t, peer, "spec", "clusterIP"))
			require.True(t, nestedBool(t, peer, "spec", "publishNotReadyAddresses"))
			client := objectByKindAndName(t, objects, "Service", "kubebrain-client")
			clusterIP, _, err := unstructured.NestedString(client.Object, "spec", "clusterIP")
			require.NoError(t, err)
			require.NotEqual(t, "None", clusterIP)
		})
	}
}

func expectedInitialCluster(scheme string) string {
	return "kubebrain-0=" + scheme + "://kubebrain-0.kubebrain-peer.kubebrain-system.svc:3380," +
		"kubebrain-1=" + scheme + "://kubebrain-1.kubebrain-peer.kubebrain-system.svc:3380," +
		"kubebrain-2=" + scheme + "://kubebrain-2.kubebrain-peer.kubebrain-system.svc:3380"
}

func decodeManifest(t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var objects []*unstructured.Unstructured
	for {
		var raw map[string]any
		err = decoder.Decode(&raw)
		if err == io.EOF {
			return objects
		}
		require.NoError(t, err)
		if len(raw) != 0 {
			objects = append(objects, &unstructured.Unstructured{Object: raw})
		}
	}
}

func objectByKindAndName(t *testing.T, objects []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	for _, object := range objects {
		if object.GetKind() == kind && object.GetName() == name {
			return object
		}
	}
	t.Fatalf("%s/%s not found", kind, name)
	return nil
}

func nestedString(t *testing.T, object *unstructured.Unstructured, fields ...string) string {
	t.Helper()
	value, found, err := unstructured.NestedString(object.Object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	return value
}

func nestedInt64(t *testing.T, object *unstructured.Unstructured, fields ...string) int64 {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object.Object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	switch number := value.(type) {
	case int64:
		return number
	case float64:
		integer := int64(number)
		require.Equal(t, number, float64(integer))
		return integer
	default:
		t.Fatalf("%v is not a number", value)
		return 0
	}
}

func nestedBool(t *testing.T, object *unstructured.Unstructured, fields ...string) bool {
	t.Helper()
	value, found, err := unstructured.NestedBool(object.Object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	return value
}
