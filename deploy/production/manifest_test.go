package production_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
			require.Equal(t, "RollingUpdate", nestedString(t, workload, "spec", "updateStrategy", "type"))
			require.EqualValues(t, 30, nestedInt64(t, workload, "spec", "template", "spec", "terminationGracePeriodSeconds"))
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
			require.Contains(t, args, "--advertise-host=$(POD_NAME).kubebrain-peer.kubebrain-system.svc.cluster.local")
			require.Contains(t, args, "--initial-cluster="+expectedInitialCluster(tc.scheme))
			require.Contains(t, args, "--enable-count-index=true")
			require.Contains(t, args, "--count-index-max-keys=5000000")
			require.Contains(t, args, "--enable-storage-metrics=true")
			require.Contains(t, args, "--max-requests-inflight=1024")
			require.Contains(t, args, "--max-request-rate=2000")
			require.Contains(t, args, "--request-rate-burst=4000")
			require.Contains(t, args, "--max-delete-range-keys=1024")
			require.Contains(t, args, "--max-watches=10000")
			require.Equal(t, "/ready", nestedString(t, containerObject, "readinessProbe", "httpGet", "path"))
			require.Equal(t, "/ping", nestedString(t, containerObject, "livenessProbe", "httpGet", "path"))
			require.Equal(t, "/ping", nestedString(t, containerObject, "startupProbe", "httpGet", "path"))

			env, found, err := unstructured.NestedSlice(container, "env")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, env, 1)
			podName := env[0].(map[string]any)
			require.Equal(t, "POD_NAME", podName["name"])
			require.Equal(t, "metadata.name", nestedString(t, &unstructured.Unstructured{Object: podName}, "valueFrom", "fieldRef", "fieldPath"))

			serviceAccount := objectByKindAndName(t, objects, "ServiceAccount", "kubebrain")
			require.False(t, nestedBool(t, serviceAccount, "automountServiceAccountToken"))

			pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kubebrain")
			require.EqualValues(t, 2, nestedInt64(t, pdb, "spec", "minAvailable"))
			require.Equal(t, "kubebrain", nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/name"))

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

func TestProductionMonitoringTracksStatefulSetReadiness(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)

	var readinessRule map[string]any
	for _, rawGroup := range groups {
		group := rawGroup.(map[string]any)
		rules, ok := group["rules"].([]any)
		require.True(t, ok)
		for _, rawRule := range rules {
			candidate := rawRule.(map[string]any)
			if candidate["alert"] == "KubeBrainReadinessUnavailable" {
				readinessRule = candidate
			}
		}
	}
	require.NotNil(t, readinessRule)
	require.Equal(t,
		`(kube_statefulset_status_replicas_ready{namespace="kubebrain-system",statefulset="kubebrain"} or on() vector(0)) < 3`,
		readinessRule["expr"],
	)
	require.NotContains(t, readinessRule["expr"], "kube_deployment_")

	overflowRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexOverflowed")
	require.Equal(t, `max(count_index_overflowed{namespace="kubebrain-system"}) > 0`, overflowRule["expr"])
	require.Equal(t, "1m", overflowRule["for"])
	require.Equal(t, "warning", overflowRule["labels"].(map[string]any)["severity"])

	rebuildRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexRebuildFailures")
	require.Equal(t,
		`sum(increase(count_index_rebuild_err{namespace="kubebrain-system"}[10m])) > 0`,
		rebuildRule["expr"])
	require.Equal(t, "0m", rebuildRule["for"])

	grpcRule := prometheusRuleByAlert(t, groups, "KubeBrainGrpcErrors")
	require.Equal(t,
		`sum(rate(grpc_server_handled_total{namespace="kubebrain-system",grpc_code=~"Unknown|Internal|DataLoss"}[5m])) > 0`,
		grpcRule["expr"])
	require.NotContains(t, grpcRule["expr"], `grpc_code!="OK"`)

	writeRule := prometheusRuleByAlert(t, groups, "KubeBrainWriteFailures")
	require.Equal(t,
		`sum(rate(write{namespace="kubebrain-system",success="false",errclass=~"deadline|other"}[5m])) > 0`,
		writeRule["expr"])
}

func TestProductionAlertMetricsExist(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)

	emitted := emittedMetricNames(t, "../../pkg")
	for _, external := range []string{
		"grpc_server_handled_total",
		"grpc_server_handling_seconds_bucket",
		"kube_statefulset_status_replicas_ready",
		"up",
	} {
		emitted[external] = struct{}{}
	}

	metricRE := regexp.MustCompile(`\b([a-zA-Z_:][a-zA-Z0-9_:]*)\{`)
	for _, rawGroup := range groups {
		group := rawGroup.(map[string]any)
		rules, ok := group["rules"].([]any)
		require.True(t, ok)
		for _, rawRule := range rules {
			candidate := rawRule.(map[string]any)
			alert, _ := candidate["alert"].(string)
			expr, _ := candidate["expr"].(string)
			for _, match := range metricRE.FindAllStringSubmatch(expr, -1) {
				_, ok := emitted[match[1]]
				require.Truef(t, ok, "alert %s references metric %s, which is not emitted", alert, match[1])
			}
		}
	}
}

func emittedMetricNames(t *testing.T, root string) map[string]struct{} {
	t.Helper()
	emitRE := regexp.MustCompile(`Emit(Counter|Gauge|Histogram)\("([^"]+)"`)
	names := make(map[string]struct{})
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, match := range emitRE.FindAllSubmatch(data, -1) {
			name := strings.ReplaceAll(string(match[2]), ".", "_")
			names[name] = struct{}{}
			if string(match[1]) == "Histogram" {
				names[name+"_bucket"] = struct{}{}
				names[name+"_count"] = struct{}{}
				names[name+"_sum"] = struct{}{}
			}
		}
		return nil
	})
	require.NoError(t, err)
	return names
}

func prometheusRuleByAlert(t *testing.T, groups []any, alert string) map[string]any {
	t.Helper()
	for _, rawGroup := range groups {
		group := rawGroup.(map[string]any)
		rules, ok := group["rules"].([]any)
		require.True(t, ok)
		for _, rawRule := range rules {
			candidate := rawRule.(map[string]any)
			if candidate["alert"] == alert {
				return candidate
			}
		}
	}
	t.Fatalf("alert %q not found", alert)
	return nil
}

func TestProductionTiDBClusterProvidesDurableHAStorage(t *testing.T) {
	objects := decodeManifest(t, "tidb-cluster.yaml")
	cluster := objectByKindAndName(t, objects, "TidbCluster", "kb")
	require.Equal(t, "v8.5.3", nestedString(t, cluster, "spec", "version"))
	require.Equal(t, "Retain", nestedString(t, cluster, "spec", "pvReclaimPolicy"))
	require.True(t, nestedBool(t, cluster, "spec", "enableDynamicConfiguration"))
	require.Equal(t, "RollingUpdate", nestedString(t, cluster, "spec", "configUpdateStrategy"))

	for _, component := range []struct {
		name             string
		terminationGrace int64
		cpuRequest       string
		memoryRequest    string
		storageRequest   string
		cpuLimit         string
		memoryLimit      string
	}{
		{name: "pd", terminationGrace: 60, cpuRequest: "1", memoryRequest: "2Gi", storageRequest: "100Gi", cpuLimit: "2", memoryLimit: "4Gi"},
		{name: "tikv", terminationGrace: 300, cpuRequest: "4", memoryRequest: "8Gi", storageRequest: "500Gi", cpuLimit: "8", memoryLimit: "16Gi"},
	} {
		t.Run(component.name, func(t *testing.T) {
			require.EqualValues(t, 3, nestedInt64(t, cluster, "spec", component.name, "replicas"))
			require.EqualValues(t, 3, nestedInt64(t, cluster, "spec", component.name, "maxFailoverCount"))
			require.Equal(t, "RollingUpdate", nestedString(t, cluster, "spec", component.name, "statefulSetUpdateStrategy"))
			require.EqualValues(t, component.terminationGrace,
				nestedInt64(t, cluster, "spec", component.name, "terminationGracePeriodSeconds"))
			require.Equal(t, component.cpuRequest, nestedString(t, cluster, "spec", component.name, "requests", "cpu"))
			require.Equal(t, component.memoryRequest, nestedString(t, cluster, "spec", component.name, "requests", "memory"))
			require.Equal(t, component.storageRequest, nestedString(t, cluster, "spec", component.name, "requests", "storage"))
			require.Equal(t, component.cpuLimit, nestedString(t, cluster, "spec", component.name, "limits", "cpu"))
			require.Equal(t, component.memoryLimit, nestedString(t, cluster, "spec", component.name, "limits", "memory"))

			antiAffinity, found, err := unstructured.NestedSlice(
				cluster.Object, "spec", component.name, "affinity", "podAntiAffinity",
				"requiredDuringSchedulingIgnoredDuringExecution",
			)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, antiAffinity, 1)
			require.Equal(t, "kubernetes.io/hostname",
				nestedString(t, &unstructured.Unstructured{Object: antiAffinity[0].(map[string]any)}, "topologyKey"))

			pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kb-"+component.name)
			require.EqualValues(t, 2, nestedInt64(t, pdb, "spec", "minAvailable"))
			require.Equal(t, component.name,
				nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/component"))
			require.Equal(t, "kb",
				nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/instance"))
		})
	}
	require.Equal(t, "10m", nestedString(t, cluster, "spec", "tikv", "evictLeaderTimeout"))
}

func TestDevManifestProvidesStableCompleteMembership(t *testing.T) {
	objects := decodeManifest(t, "../dev/kubebrain-tikv.yaml")
	workload := objectByKindAndName(t, objects, "StatefulSet", "kubebrain")
	require.Equal(t, "kubebrain-peer", nestedString(t, workload, "spec", "serviceName"))
	require.EqualValues(t, 3, nestedInt64(t, workload, "spec", "replicas"))

	containers, found, err := unstructured.NestedSlice(workload.Object, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := containers[0].(map[string]any)
	args, found, err := unstructured.NestedStringSlice(container, "args")
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, args, "--advertise-host=$(POD_NAME).kubebrain-peer.kubebrain-dev.svc.cluster.local")
	require.Contains(t, args, "--enable-count-index=true")
	require.Contains(t, args, "--count-index-max-keys=5000000")
	require.Contains(t, args, "--enable-storage-metrics=true")
	require.Contains(t, args,
		"--initial-cluster=kubebrain-0=http://kubebrain-0.kubebrain-peer.kubebrain-dev.svc.cluster.local:3380,"+
			"kubebrain-1=http://kubebrain-1.kubebrain-peer.kubebrain-dev.svc.cluster.local:3380,"+
			"kubebrain-2=http://kubebrain-2.kubebrain-peer.kubebrain-dev.svc.cluster.local:3380",
	)

	peer := objectByKindAndName(t, objects, "Service", "kubebrain-peer")
	require.Equal(t, "None", nestedString(t, peer, "spec", "clusterIP"))
	require.True(t, nestedBool(t, peer, "spec", "publishNotReadyAddresses"))
	client := objectByKindAndName(t, objects, "Service", "kubebrain")
	require.Equal(t, "NodePort", nestedString(t, client, "spec", "type"))
}

func expectedInitialCluster(scheme string) string {
	return "kubebrain-0=" + scheme + "://kubebrain-0.kubebrain-peer.kubebrain-system.svc.cluster.local:3380," +
		"kubebrain-1=" + scheme + "://kubebrain-1.kubebrain-peer.kubebrain-system.svc.cluster.local:3380," +
		"kubebrain-2=" + scheme + "://kubebrain-2.kubebrain-peer.kubebrain-system.svc.cluster.local:3380"
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
