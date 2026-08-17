package production_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
			for _, object := range objects {
				if object.GetKind() == "Namespace" {
					continue
				}
				require.Equal(t, "kubebrain",
					nestedString(t, object, "metadata", "labels", "app.kubernetes.io/instance"),
					"%s/%s must carry the DBaaS instance ownership label",
					object.GetKind(), object.GetName())
			}
			workload := objectByKindAndName(t, objects, "StatefulSet", "kubebrain")
			require.Equal(t, "kubebrain-dbaas-critical",
				nestedString(t, workload, "spec", "template", "spec", "priorityClassName"))
			require.Equal(t, "kubebrain-peer", nestedString(t, workload, "spec", "serviceName"))
			require.EqualValues(t, 3, nestedInt64(t, workload, "spec", "replicas"))
			require.Equal(t, "RollingUpdate", nestedString(t, workload, "spec", "updateStrategy", "type"))
			require.EqualValues(t, 30, nestedInt64(t, workload, "spec", "template", "spec", "terminationGracePeriodSeconds"))
			require.False(t, nestedBool(t, workload, "spec", "template", "spec", "automountServiceAccountToken"))
			require.True(t, nestedBool(t, workload, "spec", "template", "spec", "securityContext", "runAsNonRoot"))
			require.EqualValues(t, 65532, nestedInt64(t, workload, "spec", "template", "spec", "securityContext", "runAsUser"))
			require.EqualValues(t, 65532, nestedInt64(t, workload, "spec", "template", "spec", "securityContext", "runAsGroup"))
			require.EqualValues(t, 65532, nestedInt64(t, workload, "spec", "template", "spec", "securityContext", "fsGroup"))
			require.Equal(t, "RuntimeDefault", nestedString(t, workload, "spec", "template", "spec", "securityContext", "seccompProfile", "type"))
			podSpec, found, err := unstructured.NestedMap(workload.Object, "spec", "template", "spec")
			require.NoError(t, err)
			require.True(t, found)
			pod := &unstructured.Unstructured{Object: podSpec}

			requiredAntiAffinity, found, err := unstructured.NestedSlice(
				workload.Object, "spec", "template", "spec", "affinity", "podAntiAffinity",
				"requiredDuringSchedulingIgnoredDuringExecution",
			)
			require.NoError(t, err)
			require.True(t, found)
			require.NotEmpty(t, requiredAntiAffinity)
			require.Equal(t, "kubernetes.io/hostname",
				nestedString(t, &unstructured.Unstructured{Object: requiredAntiAffinity[0].(map[string]any)}, "topologyKey"))
			preferredAntiAffinity, found, err := unstructured.NestedSlice(
				workload.Object, "spec", "template", "spec", "affinity", "podAntiAffinity",
				"preferredDuringSchedulingIgnoredDuringExecution",
			)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, preferredAntiAffinity, 1)
			zonePreference := &unstructured.Unstructured{Object: preferredAntiAffinity[0].(map[string]any)}
			require.EqualValues(t, 100, nestedInt64(t, zonePreference, "weight"))
			require.Equal(t, "topology.kubernetes.io/zone",
				nestedString(t, zonePreference, "podAffinityTerm", "topologyKey"))
			require.Equal(t, "kubebrain", nestedString(t, zonePreference,
				"podAffinityTerm", "labelSelector", "matchLabels", "app.kubernetes.io/name"))
			require.Equal(t, "kubebrain", nestedString(t, zonePreference,
				"podAffinityTerm", "labelSelector", "matchLabels", "app.kubernetes.io/instance"))

			containers, found, err := unstructured.NestedSlice(workload.Object, "spec", "template", "spec", "containers")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, containers, 1)
			container := containers[0].(map[string]any)
			containerObject := &unstructured.Unstructured{Object: container}
			require.Equal(t, []string{"/bin/sh", "-c", "curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain && sleep 5"}, nestedStringSlice(t, containerObject,
				"lifecycle", "preStop", "exec", "command"))
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
			require.Equal(t, expectedProductionKubeBrainArgs(tc.scheme), args)
			require.Contains(t, args, "--advertise-host=$(POD_NAME).kubebrain-peer.kubebrain-system.svc.cluster.local")
			require.Contains(t, args, "--advertise-client-urls="+tc.scheme+"://kubebrain-client.kubebrain-system.svc:3379")
			require.Contains(t, args, "--keyspace=kubebrain-system")
			require.Contains(t, args, "--initial-cluster="+expectedInitialCluster(tc.scheme))
			require.Contains(t, args, "--enable-count-index=true")
			require.Contains(t, args, "--count-index-max-keys=5000000")
			require.Contains(t, args, "--enable-storage-metrics=true")
			quotaArgs := 0
			for _, arg := range args {
				if strings.HasPrefix(arg, "--quota-backend-bytes=") {
					quotaArgs++
					require.Equal(t, "--quota-backend-bytes=429496729600", arg)
				}
			}
			require.Equal(t, 1, quotaArgs)
			require.Contains(t, args, "--max-requests-inflight=1024")
			require.Contains(t, args, "--max-request-rate=2000")
			require.Contains(t, args, "--request-rate-burst=4000")
			require.Contains(t, args, "--max-delete-range-keys=1024")
			require.Contains(t, args, "--max-watches=10000")
			require.Equal(t, "/ready", nestedString(t, containerObject, "readinessProbe", "httpGet", "path"))
			require.Equal(t, "/ping", nestedString(t, containerObject, "livenessProbe", "httpGet", "path"))
			require.Equal(t, "/ping", nestedString(t, containerObject, "startupProbe", "httpGet", "path"))
			if tc.file == "kubebrain-tls.yaml" {
				require.Contains(t, args, "--cert-file=/etc/kubebrain/client-tls/tls.crt")
				require.Contains(t, args, "--key-file=/etc/kubebrain/client-tls/tls.key")
				require.Contains(t, args, "--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt")
				require.Contains(t, args, "--peer-cert-file=/etc/kubebrain/peer-tls/tls.crt")
				require.Contains(t, args, "--peer-key-file=/etc/kubebrain/peer-tls/tls.key")
				require.Contains(t, args, "--peer-trusted-ca-file=/etc/kubebrain/peer-tls/ca.crt")
				certificateSecretItems := map[string]string{
					"ca.crt":  "ca.crt",
					"tls.crt": "tls.crt",
					"tls.key": "tls.key",
				}
				assertReadOnlySecretVolume(t, pod, containerObject,
					"client-tls", "kubebrain-client-tls", "/etc/kubebrain/client-tls", 0440,
					certificateSecretItems)
				assertReadOnlySecretVolume(t, pod, containerObject,
					"peer-tls", "kubebrain-peer-tls", "/etc/kubebrain/peer-tls", 0440,
					certificateSecretItems)
			}

			env, found, err := unstructured.NestedSlice(container, "env")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, env, 2)
			envByName := make(map[string]map[string]any, len(env))
			for _, raw := range env {
				entry := raw.(map[string]any)
				envByName[entry["name"].(string)] = entry
			}
			podName := envByName["POD_NAME"]
			require.Equal(t, "POD_NAME", podName["name"])
			require.Equal(t, "metadata.name", nestedString(t, &unstructured.Unstructured{Object: podName}, "valueFrom", "fieldRef", "fieldPath"))
			require.Equal(t, "/var/lib/kubebrain-snapshot", envByName["TMPDIR"]["value"])

			mounts, found, err := unstructured.NestedSlice(container, "volumeMounts")
			require.NoError(t, err)
			require.True(t, found)
			mountByName := make(map[string]map[string]any, len(mounts))
			for _, raw := range mounts {
				mount := raw.(map[string]any)
				mountByName[mount["name"].(string)] = mount
			}
			require.Equal(t, "/var/lib/kubebrain-snapshot", mountByName["snapshot-tmp"]["mountPath"])
			volumes, found, err := unstructured.NestedSlice(workload.Object, "spec", "template", "spec", "volumes")
			require.NoError(t, err)
			require.True(t, found)
			volumeByName := make(map[string]map[string]any, len(volumes))
			for _, raw := range volumes {
				volume := raw.(map[string]any)
				volumeByName[volume["name"].(string)] = volume
			}
			require.Equal(t, "512Gi", nestedString(t,
				&unstructured.Unstructured{Object: volumeByName["snapshot-tmp"]}, "emptyDir", "sizeLimit"))

			serviceAccount := objectByKindAndName(t, objects, "ServiceAccount", "kubebrain")
			require.False(t, nestedBool(t, serviceAccount, "automountServiceAccountToken"))

			pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kubebrain")
			require.EqualValues(t, 2, nestedInt64(t, pdb, "spec", "minAvailable"))
			require.Equal(t, "AlwaysAllow", nestedString(t, pdb, "spec", "unhealthyPodEvictionPolicy"))
			require.Equal(t, "kubebrain", nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/name"))
			require.Equal(t, "kubebrain", nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/instance"))
			require.Equal(t, "kubebrain", nestedString(t, workload, "spec", "selector", "matchLabels", "app.kubernetes.io/instance"))
			require.Equal(t, "kubebrain", nestedString(t, workload, "spec", "template", "metadata", "labels", "app.kubernetes.io/instance"))

			peer := objectByKindAndName(t, objects, "Service", "kubebrain-peer")
			require.Equal(t, "None", nestedString(t, peer, "spec", "clusterIP"))
			require.True(t, nestedBool(t, peer, "spec", "publishNotReadyAddresses"))
			require.Equal(t, "kubebrain", nestedString(t, peer, "spec", "selector", "app.kubernetes.io/instance"))
			client := objectByKindAndName(t, objects, "Service", "kubebrain-client")
			require.Equal(t, "kubebrain", nestedString(t, client, "spec", "selector", "app.kubernetes.io/instance"))
			clusterIP, _, err := unstructured.NestedString(client.Object, "spec", "clusterIP")
			require.NoError(t, err)
			require.NotEqual(t, "None", clusterIP)
		})
	}
}

func expectedProductionKubeBrainArgs(scheme string) []string {
	args := []string{
		"--port=3379",
		"--peer-port=3380",
		"--info-port=8080",
		"--advertise-host=$(POD_NAME).kubebrain-peer.kubebrain-system.svc.cluster.local",
		"--advertise-client-urls=" + scheme + "://kubebrain-client.kubebrain-system.svc:3379",
		"--initial-cluster=" + expectedInitialCluster(scheme),
		"--pd-addrs=kb-pd.tidb-cluster.svc:2379",
		"--keyspace=kubebrain-system",
		"--compatible-with-etcd=true",
		"--enable-count-index=true",
		"--count-index-max-keys=5000000",
		"--enable-storage-metrics=true",
		"--quota-backend-bytes=429496729600",
		"--leader-lease-duration=30s",
		"--leader-renew-deadline=25s",
		"--leader-retry-period=500ms",
		"--max-txn-ops=128",
		"--max-request-bytes=1572864",
		"--max-concurrent-streams=4294967295",
		"--max-requests-inflight=1024",
		"--max-request-rate=2000",
		"--request-rate-burst=4000",
		"--max-delete-range-keys=1024",
		"--max-watches=10000",
		"--grpc-keepalive-min-time=5s",
		"--grpc-keepalive-interval=2h",
		"--grpc-keepalive-timeout=20s",
		"--auth-token=simple",
		"--bcrypt-cost=10",
		"--auth-token-ttl=300",
	}
	if scheme == "https" {
		args = append(args,
			"--grpc-max-connection-age=1h",
			"--grpc-max-connection-age-grace=5m",
			"--tls-min-version=TLS1.2",
			"--cert-file=/etc/kubebrain/client-tls/tls.crt",
			"--key-file=/etc/kubebrain/client-tls/tls.key",
			"--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt",
			"--tls-server-name=kubebrain-client.kubebrain-system.svc",
			"--client-cert-auth=true",
			"--peer-cert-file=/etc/kubebrain/peer-tls/tls.crt",
			"--peer-key-file=/etc/kubebrain/peer-tls/tls.key",
			"--peer-trusted-ca-file=/etc/kubebrain/peer-tls/ca.crt",
			"--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc.cluster.local",
			"--peer-client-cert-auth=true",
		)
	}
	args = append(args, "--v=2")
	return args
}

func TestProductionKubeBrainArgsAreCoveredByRuntimeReleaseGate(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "hack", "production", "validate-instance-ready.sh"))
	require.NoError(t, err)
	coveredArgs := runtimeReleaseGateKubeBrainArgs(string(data))

	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			var missing []string
			for _, arg := range expectedProductionKubeBrainArgs(scheme) {
				name, _, ok := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
				require.True(t, ok, "production KubeBrain arg must be --name=value: %s", arg)
				if !coveredArgs[name] {
					missing = append(missing, name)
				}
			}
			sort.Strings(missing)
			require.Empty(t, missing, "production KubeBrain args must be checked by hack/production/validate-instance-ready.sh")
		})
	}
}

func TestRuntimeReleaseGateDefaultsMatchProductionKubeBrainArgs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "hack", "production", "validate-instance-ready.sh"))
	require.NoError(t, err)
	script := string(data)
	defaults := runtimeReleaseGateEnvDefaults(script)
	exactArgEnvs := runtimeReleaseGateExactArgEnvs(script)
	productionArgs := productionKubeBrainArgValues("http")

	for name, variable := range exactArgEnvs {
		expected, ok := productionArgs[name]
		require.True(t, ok, "runtime release gate exact arg %q must be present in production KubeBrain args", name)
		actual, ok := defaults[variable]
		require.True(t, ok, "runtime release gate exact arg %q must use a defaulted %s", name, variable)
		require.Equal(t, expected, actual, "runtime release gate default for --%s must match production manifest", name)
	}
}

func TestRuntimeReleaseGateTLSOnlyArgsUseOptionalEnv(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "hack", "production", "validate-instance-ready.sh"))
	require.NoError(t, err)
	script := string(data)
	defaults := runtimeReleaseGateEnvDefaults(script)
	exactArgEnvs := runtimeReleaseGateExactArgEnvs(script)
	optionalArgEnvs := runtimeReleaseGateOptionalArgEnvs(script)
	plainArgs := productionKubeBrainArgValues("http")
	tlsArgs := productionKubeBrainArgValues("https")

	var tlsOnlyArgs []string
	for name := range tlsArgs {
		if _, ok := plainArgs[name]; !ok {
			tlsOnlyArgs = append(tlsOnlyArgs, name)
		}
	}
	sort.Strings(tlsOnlyArgs)
	require.NotEmpty(t, tlsOnlyArgs, "TLS production manifest must have TLS-only KubeBrain args")

	for _, name := range tlsOnlyArgs {
		_, exact := exactArgEnvs[name]
		require.False(t, exact, "TLS-only production arg --%s must not be required by the non-TLS release gate baseline", name)
		variable, ok := optionalArgEnvs[name]
		require.True(t, ok, "TLS-only production arg --%s must be guarded by an optional release gate env", name)
		defaultValue, ok := defaults[variable]
		require.True(t, ok, "TLS-only production arg --%s must use a defaulted %s", name, variable)
		require.Empty(t, defaultValue, "TLS-only production arg --%s must default to absent unless TLS baseline env is supplied", name)
	}
}

func runtimeReleaseGateKubeBrainArgs(script string) map[string]bool {
	covered := map[string]bool{}
	for _, match := range regexp.MustCompile(`check_(?:exact|optional)_kubebrain_arg "([^"]+)"`).FindAllStringSubmatch(script, -1) {
		covered[match[1]] = true
	}
	for _, match := range regexp.MustCompile(`--([A-Za-z0-9-]+)=\$\{?EXPECTED_[A-Z0-9_]+`).FindAllStringSubmatch(script, -1) {
		covered[match[1]] = true
	}
	return covered
}

func runtimeReleaseGateEnvDefaults(script string) map[string]string {
	defaults := map[string]string{}
	for _, match := range regexp.MustCompile(`(?m)^([A-Z0-9_]+)="\$\{([A-Z0-9_]+):-(.*)\}"$`).FindAllStringSubmatch(script, -1) {
		if match[1] == match[2] {
			defaults[match[1]] = match[3]
		}
	}
	return defaults
}

func runtimeReleaseGateExactArgEnvs(script string) map[string]string {
	exactArgEnvs := map[string]string{}
	for _, match := range regexp.MustCompile(`check_exact_kubebrain_arg "([^"]+)" "\$([A-Z0-9_]+)"`).FindAllStringSubmatch(script, -1) {
		exactArgEnvs[match[1]] = match[2]
	}
	return exactArgEnvs
}

func runtimeReleaseGateOptionalArgEnvs(script string) map[string]string {
	optionalArgEnvs := map[string]string{}
	for _, match := range regexp.MustCompile(`check_optional_kubebrain_arg "([^"]+)" "\$([A-Z0-9_]+)"`).FindAllStringSubmatch(script, -1) {
		optionalArgEnvs[match[1]] = match[2]
	}
	return optionalArgEnvs
}

func productionKubeBrainArgValues(scheme string) map[string]string {
	values := map[string]string{}
	for _, arg := range expectedProductionKubeBrainArgs(scheme) {
		name, value, ok := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if ok {
			values[name] = value
		}
	}
	return values
}

func TestProductionPriorityClassIsSharedAndNonPreempting(t *testing.T) {
	priorityClass := objectByKindAndName(t, decodeManifest(t, "dbaas-priority-class.yaml"),
		"PriorityClass", "kubebrain-dbaas-critical")
	require.EqualValues(t, 1000000, nestedInt64(t, priorityClass, "value"))
	require.False(t, nestedBool(t, priorityClass, "globalDefault"))
	require.Equal(t, "Never", nestedString(t, priorityClass, "preemptionPolicy"))
	require.NotEmpty(t, nestedString(t, priorityClass, "description"))
}

func TestProductionDataPlaneNetworkPoliciesAreExplicit(t *testing.T) {
	objects := decodeManifest(t, "dbaas-network-policy.yaml")
	require.Len(t, objects, 4)

	for _, tc := range []struct {
		name      string
		namespace string
		policy    string
		rules     string
	}{
		{name: "kubebrain-data-plane-ingress", namespace: "kubebrain-system", policy: "Ingress", rules: "ingress"},
		{name: "kubebrain-data-plane-egress", namespace: "kubebrain-system", policy: "Egress", rules: "egress"},
		{name: "tikv-data-plane-ingress", namespace: "tidb-cluster", policy: "Ingress", rules: "ingress"},
		{name: "tikv-data-plane-egress", namespace: "tidb-cluster", policy: "Egress", rules: "egress"},
	} {
		policy := objectByKindAndName(t, objects, "NetworkPolicy", tc.name)
		require.Equal(t, tc.namespace, policy.GetNamespace())
		policyTypes, found, err := unstructured.NestedStringSlice(policy.Object, "spec", "policyTypes")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{tc.policy}, policyTypes)
		rules, found, err := unstructured.NestedSlice(policy.Object, "spec", tc.rules)
		require.NoError(t, err)
		require.True(t, found)
		require.NotEmpty(t, rules)
	}

	kubebrainIngress := objectByKindAndName(t, objects, "NetworkPolicy", "kubebrain-data-plane-ingress")
	require.Equal(t, "kubebrain", nestedString(t, kubebrainIngress,
		"spec", "podSelector", "matchLabels", "app.kubernetes.io/instance"))
	requireNetworkPolicyRule(t, kubebrainIngress, "ingress", "from",
		"dbaas.kubebrain.io/client-access", "true", []int64{3379})
	requireNetworkPolicyRule(t, kubebrainIngress, "ingress", "from",
		"dbaas.kubebrain.io/monitoring-access", "true", []int64{3378})

	kubebrainEgress := objectByKindAndName(t, objects, "NetworkPolicy", "kubebrain-data-plane-egress")
	requireNetworkPolicyRule(t, kubebrainEgress, "egress", "to",
		"kubernetes.io/metadata.name", "kube-system", []int64{53, 53})
	requireNetworkPolicyRule(t, kubebrainEgress, "egress", "to",
		"dbaas.kubebrain.io/instance", "kubebrain", []int64{2379, 20160})

	storageIngress := objectByKindAndName(t, objects, "NetworkPolicy", "tikv-data-plane-ingress")
	requireStoragePolicySelector(t, storageIngress)
	requireNetworkPolicyRule(t, storageIngress, "ingress", "from",
		"kubernetes.io/metadata.name", "kubebrain-system", []int64{2379, 20160})
	requireNetworkPolicyRule(t, storageIngress, "ingress", "from",
		"kubernetes.io/metadata.name", "tidb-admin", []int64{2379, 20180})
	requireNetworkPolicyRule(t, storageIngress, "ingress", "from",
		"dbaas.kubebrain.io/monitoring-access", "true", []int64{2379, 20180})

	storageEgress := objectByKindAndName(t, objects, "NetworkPolicy", "tikv-data-plane-egress")
	requireStoragePolicySelector(t, storageEgress)
	requireNetworkPolicyRule(t, storageEgress, "egress", "to",
		"kubernetes.io/metadata.name", "kube-system", []int64{53, 53})
}

func requireStoragePolicySelector(t *testing.T, policy *unstructured.Unstructured) {
	t.Helper()
	expressions, found, err := unstructured.NestedSlice(policy.Object,
		"spec", "podSelector", "matchExpressions")
	require.NoError(t, err)
	require.True(t, found)
	want := map[string][]string{
		"app.kubernetes.io/component": {"pd", "tikv"},
		"app.kubernetes.io/instance":  {"kb"},
		"app.kubernetes.io/name":      {"tidb-cluster"},
	}
	require.Len(t, expressions, len(want))
	for _, rawExpression := range expressions {
		expression := rawExpression.(map[string]any)
		key := expression["key"].(string)
		require.Equal(t, "In", expression["operator"])
		values, found, valuesErr := unstructured.NestedStringSlice(expression, "values")
		require.NoError(t, valuesErr)
		require.True(t, found)
		require.Equal(t, want[key], values)
		delete(want, key)
	}
	require.Empty(t, want)
}

func requireNetworkPolicyRule(t *testing.T, policy *unstructured.Unstructured, direction, peerField,
	label, value string, wantPorts []int64,
) {
	t.Helper()
	rules, found, err := unstructured.NestedSlice(policy.Object, "spec", direction)
	require.NoError(t, err)
	require.True(t, found)
	for _, rawRule := range rules {
		rule := rawRule.(map[string]any)
		peers, _, peerErr := unstructured.NestedSlice(rule, peerField)
		require.NoError(t, peerErr)
		for _, rawPeer := range peers {
			peer := rawPeer.(map[string]any)
			labels, _, labelErr := unstructured.NestedStringMap(peer,
				"namespaceSelector", "matchLabels")
			require.NoError(t, labelErr)
			if labels[label] != value {
				continue
			}
			ports, _, portErr := unstructured.NestedSlice(rule, "ports")
			require.NoError(t, portErr)
			gotPorts := make([]int64, 0, len(ports))
			for _, rawPort := range ports {
				port := &unstructured.Unstructured{Object: rawPort.(map[string]any)}
				gotPorts = append(gotPorts, nestedInt64(t, port, "port"))
			}
			require.Equal(t, wantPorts, gotPorts)
			return
		}
	}
	t.Fatalf("%s policy %q has no %s namespace selector %s=%s", direction, policy.GetName(), peerField, label, value)
}

func TestProductionNamespacesDeclareDedicatedInstanceBoundaries(t *testing.T) {
	for _, file := range []string{"kubebrain.yaml", "kubebrain-tls.yaml", "tidb-cluster.yaml"} {
		namespace := objectByKindAndName(t, decodeManifest(t, file), "Namespace",
			map[string]string{
				"kubebrain.yaml":     "kubebrain-system",
				"kubebrain-tls.yaml": "kubebrain-system",
				"tidb-cluster.yaml":  "tidb-cluster",
			}[file])
		require.Equal(t, "kubebrain",
			nestedString(t, namespace, "metadata", "labels", "dbaas.kubebrain.io/instance"))
		require.Equal(t, "true",
			nestedString(t, namespace, "metadata", "labels", "dbaas.kubebrain.io/dedicated"))
	}
}

func TestOperationCRDAndWorkerRBACFencePersistentTasks(t *testing.T) {
	crd := objectByKindAndName(
		t, decodeManifest(t, "kubebrain-operation-crd.yaml"),
		"CustomResourceDefinition", "kubebrainoperations.dbaas.kubebrain.io",
	)
	require.Equal(t, "Namespaced", nestedString(t, crd, "spec", "scope"))
	require.Equal(t, "KubeBrainOperation", nestedString(t, crd, "spec", "names", "kind"))
	versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, versions, 1)
	version := &unstructured.Unstructured{Object: versions[0].(map[string]any)}
	require.True(t, nestedBool(t, version, "storage"))
	operationTypes, found, err := unstructured.NestedStringSlice(
		version.Object, "schema", "openAPIV3Schema", "properties", "spec",
		"properties", "type", "enum")
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, operationTypes, "BackupDeletion")
	require.Contains(t, operationTypes, "NativePITRFullBackup")
	require.Contains(t, operationTypes, "NativePITRFullRestore")
	require.Contains(t, operationTypes, "LegacySnapshotHistoryRemediation")
	tenantType := nestedString(
		t, version, "schema", "openAPIV3Schema", "properties", "spec",
		"properties", "tenant", "type",
	)
	require.Equal(t, "string", tenantType)
	require.EqualValues(t, 63, nestedInt64(
		t, version, "schema", "openAPIV3Schema", "properties", "spec",
		"properties", "tenant", "maxLength",
	))
	require.EqualValues(t, 253, nestedInt64(
		t, version, "schema", "openAPIV3Schema", "properties", "spec",
		"properties", "requestedBy", "maxLength",
	))
	controlFreePattern := `^[^\x00-\x1F\x7F-\x9F]*$`
	require.Equal(t, controlFreePattern, nestedString(
		t, version, "schema", "openAPIV3Schema", "properties", "spec",
		"properties", "requestedBy", "pattern",
	))
	require.Equal(t, controlFreePattern, nestedString(
		t, version, "schema", "openAPIV3Schema", "properties", "status",
		"properties", "owner", "pattern",
	))
	require.Equal(t, controlFreePattern, nestedString(
		t, version, "schema", "openAPIV3Schema", "properties", "status",
		"properties", "message", "pattern",
	))
	controlFree, err := regexp.Compile(controlFreePattern)
	require.NoError(t, err)
	require.True(t, controlFree.MatchString("user@example.com completed"))
	require.False(t, controlFree.MatchString("user\nexample"))
	require.False(t, controlFree.MatchString("user\u0085example"))
	_, found, err = unstructured.NestedMap(version.Object, "subresources", "status")
	require.NoError(t, err)
	require.True(t, found)
	validations, found, err := unstructured.NestedSlice(
		version.Object, "schema", "openAPIV3Schema", "x-kubernetes-validations")
	require.NoError(t, err)
	require.True(t, found)
	require.GreaterOrEqual(t, len(validations), 7)
	crdExpressionsByMessage := map[string]string{}
	for _, raw := range validations {
		validation := raw.(map[string]any)
		crdExpressionsByMessage[validation["message"].(string)] = validation["rule"].(string)
	}
	terminalAuditExpression := crdExpressionsByMessage["terminal operations require audit status identity and timing"]
	require.Contains(t, terminalAuditExpression, `self.status.owner != ""`)
	require.Contains(t, terminalAuditExpression, `self.status.attempt > 0`)
	require.Contains(t, terminalAuditExpression, `self.status.observedGeneration > 0`)
	require.Contains(t, terminalAuditExpression, `self.status.completedAtUnix >= self.status.startedAtUnix`)
	failedReceiptExpression := crdExpressionsByMessage["failed operations cannot carry a receipt SHA-256"]
	require.Contains(t, failedReceiptExpression, `self.status.phase != "Failed"`)
	require.Contains(t, failedReceiptExpression, `self.status.receiptSHA256 == ""`)
	nanoExpression := crdExpressionsByMessage["startedAtUnixNano must match startedAtUnix"]
	require.Contains(t, nanoExpression, `self.status.startedAtUnixNano / 1000000000 == self.status.startedAtUnix`)

	objects := decodeManifest(t, "kubebrain-operation-worker-rbac.yaml")
	account := objectByKindAndName(t, objects, "ServiceAccount", "kubebrain-operation-worker")
	require.False(t, nestedBool(t, account, "automountServiceAccountToken"))
	role := objectByKindAndName(t, objects, "Role", "kubebrain-operation-worker")
	rules, found, err := unstructured.NestedSlice(role.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, rules, 4)
	require.Equal(t, []any{"configmaps"}, rules[0].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"kubebrain-backup-scheduler-inventory", "kubebrain-tikv-repair-operation-inventory"},
		rules[0].(map[string]any)["resourceNames"].([]any))
	require.Equal(t, []any{"get"}, rules[0].(map[string]any)["verbs"].([]any))
	require.Contains(t, rules[1].(map[string]any)["resources"].([]any), "kubebrainoperations")
	require.NotContains(t, rules[1].(map[string]any)["verbs"].([]any), "create")
	require.Contains(t, rules[2].(map[string]any)["resources"].([]any), "kubebrainoperations/status")
	require.Contains(t, rules[2].(map[string]any)["verbs"].([]any), "update")
	require.Equal(t, []any{"leases"}, rules[3].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"create", "get", "update", "delete"}, rules[3].(map[string]any)["verbs"].([]any))
	for _, raw := range rules {
		require.NotContains(t, raw.(map[string]any)["resources"].([]any), "secrets")
	}
}

func TestOperationArchiverIsFailClosedAndHardened(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-archiver-rbac.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-operation-archiver")
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "strategy", "rollingUpdate", "maxUnavailable"))
	require.EqualValues(t, 1, nestedInt64(t, deployment, "spec", "strategy", "rollingUpdate", "maxSurge"))
	require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "automountServiceAccountToken"))
	require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "securityContext", "runAsNonRoot"))
	containers, found, err := unstructured.NestedSlice(
		deployment.Object, "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	command, found, err := unstructured.NestedStringSlice(container.Object, "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-operation-archiver"}, command)
	args, found, err := unstructured.NestedStringSlice(container.Object, "args")
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, args, "--namespace-inventory-configmap=kubebrain-backup-scheduler-inventory")
	require.Contains(t, args, "--object-store-id=$(OBJECT_STORE_ID)")
	require.Contains(t, args, "--bucket=$(S3_BUCKET)")
	require.Contains(t, args, "--reconcile-timeout=15m")
	require.Contains(t, args, "--archive-timeout=2m")
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kubebrain-operation-archiver")
	require.EqualValues(t, 1, nestedInt64(t, pdb, "spec", "maxUnavailable"))
}

func TestOperationExecutorsAreTypeIsolatedFailClosedTemplates(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-executors.yaml")
	expected := map[string]struct {
		script string
		secret string
		claim  string
	}{
		"kubebrain-backup-executor": {
			"run-backup-operation.sh", "kubebrain-backup-executor-env",
			"kubebrain-backup-executor-workspace",
		},
		"kubebrain-native-pitr-full-backup-executor": {
			"run-native-pitr-full-backup-operation.sh", "kubebrain-native-pitr-full-backup-executor-env",
			"kubebrain-native-pitr-full-backup-executor-workspace",
		},
		"kubebrain-backup-deletion-executor": {
			"run-backup-deletion-operation.sh", "kubebrain-backup-deletion-executor-env",
			"kubebrain-backup-deletion-executor-workspace",
		},
		"kubebrain-cold-physical-snapshot-executor": {
			"run-cold-physical-snapshot-operation.sh", "kubebrain-cold-physical-snapshot-executor-env",
			"kubebrain-cold-physical-snapshot-executor-workspace",
		},
		"kubebrain-cold-physical-restore-executor": {
			"run-cold-physical-restore-operation.sh", "kubebrain-cold-physical-restore-executor-env",
			"kubebrain-cold-physical-restore-executor-workspace",
		},
		"kubebrain-legacy-snapshot-remediation-executor": {
			"run-legacy-snapshot-remediation-operation.sh", "kubebrain-legacy-snapshot-remediation-executor-env",
			"kubebrain-legacy-snapshot-remediation-executor-workspace",
		},
		"kubebrain-restore-cutover-executor": {
			"run-restore-cutover-operation.sh", "kubebrain-restore-cutover-executor-env",
			"kubebrain-restore-cutover-executor-workspace",
		},
		"kubebrain-post-restore-audit-executor": {
			"run-post-restore-audit-operation.sh", "kubebrain-post-restore-audit-executor-env",
			"kubebrain-post-restore-audit-executor-workspace",
		},
		"kubebrain-certificate-rotation-executor": {
			"run-certificate-rotation-operation.sh", "kubebrain-certificate-rotation-executor-env",
			"kubebrain-certificate-rotation-executor-workspace",
		},
		"kubebrain-tikv-transaction-repair-executor": {
			"run-tikv-transaction-repair-operation.sh", "kubebrain-tikv-transaction-repair-executor-env",
			"kubebrain-tikv-transaction-repair-executor-workspace",
		},
		"kubebrain-tikv-transaction-recovery-executor": {
			"run-tikv-transaction-recovery-operation.sh", "kubebrain-tikv-transaction-recovery-executor-env",
			"kubebrain-tikv-transaction-recovery-executor-workspace",
		},
		"kubebrain-destroy-executor": {
			"run-destroy-operation.sh", "kubebrain-destroy-executor-env",
			"kubebrain-destroy-executor-workspace",
		},
	}
	// Native PITR restore, target-retirement, and target-provisioning executors are independently
	// tested Recreate single-writer deployments and are not rolling templates.
	require.Len(t, objects, len(expected)*2+6)
	for name, want := range expected {
		account := objectByKindAndName(t, objects, "ServiceAccount", name)
		require.False(t, nestedBool(t, account, "automountServiceAccountToken"))
		deployment := objectByKindAndName(t, objects, "Deployment", name)
		require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
		require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "strategy", "rollingUpdate", "maxUnavailable"))
		require.EqualValues(t, 1, nestedInt64(t, deployment, "spec", "strategy", "rollingUpdate", "maxSurge"))
		require.Equal(t, name,
			nestedString(t, deployment, "spec", "template", "spec", "serviceAccountName"))
		require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "automountServiceAccountToken"))
		require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "securityContext", "runAsNonRoot"))
		spread, found, err := unstructured.NestedSlice(
			deployment.Object, "spec", "template", "spec", "topologySpreadConstraints",
		)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, spread, 2)
		containers, found, err := unstructured.NestedSlice(
			deployment.Object, "spec", "template", "spec", "containers",
		)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, containers, 1)
		container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
		command, found, err := unstructured.NestedStringSlice(container.Object, "command")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"/usr/local/bin/kubebrain-operation-worker"}, command)
		args, found, err := unstructured.NestedStringSlice(container.Object, "args")
		require.NoError(t, err)
		require.True(t, found)
		require.Contains(t, args[0], want.script)
		require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
		require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
		envFrom, found, err := unstructured.NestedSlice(container.Object, "envFrom")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, envFrom, 1)
		require.Equal(t, want.secret, nestedString(
			t, &unstructured.Unstructured{Object: envFrom[0].(map[string]any)},
			"secretRef", "name",
		))
		volumes, found, err := unstructured.NestedSlice(
			deployment.Object, "spec", "template", "spec", "volumes",
		)
		require.NoError(t, err)
		require.True(t, found)
		var workspace, parameterToken, parameterCA, hooks *unstructured.Unstructured
		for _, raw := range volumes {
			volume := &unstructured.Unstructured{Object: raw.(map[string]any)}
			switch nestedString(t, volume, "name") {
			case "workspace":
				workspace = volume
			case "parameter-token":
				parameterToken = volume
			case "parameter-ca":
				parameterCA = volume
			case "hooks":
				hooks = volume
			}
		}
		require.NotNil(t, workspace)
		require.Equal(t, want.claim,
			nestedString(t, workspace, "persistentVolumeClaim", "claimName"))
		if name == "kubebrain-tikv-transaction-repair-executor" || name == "kubebrain-tikv-transaction-recovery-executor" {
			env, found, err := unstructured.NestedSlice(container.Object, "env")
			require.NoError(t, err)
			require.True(t, found)
			envByName := map[string]string{}
			for _, raw := range env {
				entry := &unstructured.Unstructured{Object: raw.(map[string]any)}
				if value, valueFound, valueErr := unstructured.NestedString(entry.Object, "value"); valueErr == nil && valueFound {
					envByName[nestedString(t, entry, "name")] = value
				}
			}
			require.Equal(t, "kubebrain-repair-operations", envByName["OPERATION_NAMESPACE"])
			require.Equal(t, "kubebrain-tikv-repair-operation-inventory", envByName["OPERATION_NAMESPACE_INVENTORY_CONFIGMAP"])
			require.Equal(t, "https://kubebrain-operation-parameter-broker.kubebrain-operations.svc", envByName["OPERATION_PARAMETERS_ENDPOINT"])
		}
		require.NotNil(t, parameterToken)
		require.NotNil(t, parameterCA)
		sources, found, err := unstructured.NestedSlice(
			parameterToken.Object, "projected", "sources",
		)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 0440, nestedInt64(t, parameterToken, "projected", "defaultMode"))
		require.Len(t, sources, 1)
		tokenSource := &unstructured.Unstructured{Object: sources[0].(map[string]any)}
		require.Equal(t, "token", nestedString(t, tokenSource, "serviceAccountToken", "path"))
		require.Equal(t, "kubebrain-operation-parameters",
			nestedString(t, tokenSource, "serviceAccountToken", "audience"))
		require.EqualValues(t, 3600,
			nestedInt64(t, tokenSource, "serviceAccountToken", "expirationSeconds"))
		require.Equal(t, "kubebrain-operation-parameter-broker-ca",
			nestedString(t, parameterCA, "configMap", "name"))
		if name == "kubebrain-certificate-rotation-executor" {
			require.NotNil(t, hooks)
			require.Equal(t, "kubebrain-certificate-rotation-executor-hooks",
				nestedString(t, hooks, "secret", "secretName"))
			require.EqualValues(t, 0555, nestedInt64(t, hooks, "secret", "defaultMode"))
			items, found, err := unstructured.NestedSlice(hooks.Object, "secret", "items")
			require.NoError(t, err)
			require.True(t, found)
			pathsByKey := map[string]string{}
			for _, item := range items {
				itemObject := &unstructured.Unstructured{Object: item.(map[string]any)}
				pathsByKey[nestedString(t, itemObject, "key")] = nestedString(t, itemObject, "path")
			}
			require.Equal(t, map[string]string{
				"publish-overlap": "publish-overlap",
				"publish-final":   "publish-final",
			}, pathsByKey)
		} else {
			require.Nil(t, hooks)
		}
	}
}

func TestNativePITRFullBackupExecutorUsesDedicatedPinnedToolImageAndTLS(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-executors.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-native-pitr-full-backup-executor")
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	require.Equal(t, "kubebrain-native-pitr-full-backup:dev", nestedString(t, container, "image"))
	require.Equal(t, []string{"/usr/local/bin/kubebrain-operation-worker"}, nestedStringSlice(t, container, "command"))
	require.Contains(t, nestedStringSlice(t, container, "args")[0], "run-native-pitr-full-backup-operation.sh")
	envFrom, found, err := unstructured.NestedSlice(container.Object, "envFrom")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "kubebrain-native-pitr-full-backup-executor-env",
		nestedString(t, &unstructured.Unstructured{Object: envFrom[0].(map[string]any)}, "secretRef", "name"))

	volumes, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
	require.NoError(t, err)
	require.True(t, found)
	var tls, encryption *unstructured.Unstructured
	for _, raw := range volumes {
		volume := &unstructured.Unstructured{Object: raw.(map[string]any)}
		if nestedString(t, volume, "name") == "tls" {
			tls = volume
		}
		if nestedString(t, volume, "name") == "encryption" {
			encryption = volume
		}
	}
	require.NotNil(t, tls)
	require.Equal(t, "kubebrain-native-pitr-full-backup-tls", nestedString(t, tls, "secret", "secretName"))
	require.EqualValues(t, 0440, nestedInt64(t, tls, "secret", "defaultMode"))
	require.NotNil(t, encryption)
	require.Equal(t, "kubebrain-native-pitr-full-backup-encryption", nestedString(t, encryption, "secret", "secretName"))
	require.True(t, nestedBool(t, encryption, "secret", "optional"), "plaintext execution must not require an encryption Secret")
	require.EqualValues(t, 0440, nestedInt64(t, encryption, "secret", "defaultMode"))
	items, found, err := unstructured.NestedSlice(encryption.Object, "secret", "items")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, items, 2)
	mounts, found, err := unstructured.NestedSlice(container.Object, "volumeMounts")
	require.NoError(t, err)
	require.True(t, found)
	var encryptionMount *unstructured.Unstructured
	for _, raw := range mounts {
		mount := &unstructured.Unstructured{Object: raw.(map[string]any)}
		if nestedString(t, mount, "name") == "encryption" {
			encryptionMount = mount
		}
	}
	require.NotNil(t, encryptionMount)
	require.True(t, nestedBool(t, encryptionMount, "readOnly"))
}

func TestNativePITRFullRestoreExecutorIsSingleWriterWithDurableWorkspace(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-executors.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-native-pitr-full-restore-executor")
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
	require.Equal(t, "Recreate", nestedString(t, deployment, "spec", "strategy", "type"))
	require.Equal(t, "kubebrain-native-pitr-full-restore-executor", nestedString(t, deployment, "spec", "template", "spec", "serviceAccountName"))
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	require.Equal(t, "kubebrain-native-pitr-full-restore:dev", nestedString(t, container, "image"))
	require.Contains(t, nestedStringSlice(t, container, "args")[0], "run-native-pitr-full-restore-operation.sh")
	containerData, err := json.Marshal(container.Object)
	require.NoError(t, err)
	for _, expected := range []string{"KUBE_CONTEXT", "KUBECTL", "CONTROL", "WRITER_CHECK_INTERVAL_SECONDS", "ADMISSION_CHECK_INTERVAL", "kubebrain-native-pitr-target-provision-control"} {
		require.Contains(t, string(containerData), expected)
	}
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	volumes, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
	require.NoError(t, err)
	require.True(t, found)
	text := fmt.Sprint(volumes)
	require.Contains(t, text, "kubebrain-native-pitr-full-restore-executor-workspace")
	require.Contains(t, text, "kubebrain-native-pitr-full-restore-tls")
	require.Contains(t, text, "kubebrain-native-pitr-full-restore-encryption")
}

func TestNativePITRTargetRetirementExecutorIsSingleWriterAndMinimallyPrivileged(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-executors.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-native-pitr-target-retirement-executor")
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
	require.Equal(t, "Recreate", nestedString(t, deployment, "spec", "strategy", "type"))
	require.Equal(t, "kubebrain-native-pitr-target-retirement-executor", nestedString(t, deployment, "spec", "template", "spec", "serviceAccountName"))
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	container := containers[0].(map[string]any)
	require.Contains(t, container["args"].([]any)[0], "run-native-pitr-target-retirement-operation.sh")

	rbac := decodeManifest(t, "kubebrain-native-pitr-target-retirement-rbac.yaml")
	role := objectByKindAndName(t, rbac, "Role", "kubebrain-native-pitr-target-retirement-executor")
	data, err := json.Marshal(role.Object)
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "tidbclusters")
	require.Contains(t, text, "persistentvolumeclaims")
	require.NotContains(t, text, "persistentvolumes\",\"verbs\":[\"delete")
	require.NotContains(t, text, "statefulsets")
	clusterRole := objectByKindAndName(t, rbac, "ClusterRole", "kubebrain-native-pitr-target-retirement-inspector")
	clusterData, err := json.Marshal(clusterRole.Object)
	require.NoError(t, err)
	require.NotContains(t, string(clusterData), "delete")
	requestAdmission := decodeManifest(t, "kubebrain-native-pitr-target-retirement-requester-admission.yaml")
	require.Len(t, requestAdmission, 4)
	requestRBAC := decodeManifest(t, "kubebrain-native-pitr-target-retirement-requester-rbac.yaml")
	requestRole := objectByKindAndName(t, requestRBAC, "Role", "kubebrain-native-pitr-target-retirement-requester")
	requestBytes, err := json.Marshal(requestRole.Object)
	require.NoError(t, err)
	require.NotContains(t, string(requestBytes), "update")
	require.NotContains(t, string(requestBytes), "delete")
}

func TestNativePITRTargetProvisioningExecutorCreatesButCannotDeleteTargets(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-executors.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-native-pitr-target-provisioning-executor")
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
	require.Equal(t, "Recreate", nestedString(t, deployment, "spec", "strategy", "type"))
	require.Equal(t, "kubebrain-native-pitr-target-provisioning-executor", nestedString(t, deployment, "spec", "template", "spec", "serviceAccountName"))
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	require.Contains(t, containers[0].(map[string]any)["args"].([]any)[0], "run-native-pitr-target-provisioning-operation.sh")
	deploymentData, err := json.Marshal(deployment.Object)
	require.NoError(t, err)
	require.Contains(t, string(deploymentData), "kubebrain-native-pitr-full-restore-tls")
	require.Contains(t, string(deploymentData), "TLS_DIR")

	rbac := decodeManifest(t, "kubebrain-native-pitr-target-provisioning-rbac.yaml")
	require.Len(t, rbac, 6)
	role := objectByKindAndName(t, rbac, "Role", "kubebrain-native-pitr-target-provisioning-executor")
	data, err := json.Marshal(role.Object)
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "tidbclusters")
	require.Contains(t, text, "persistentvolumeclaims")
	require.Contains(t, text, "create")
	for _, forbidden := range []string{"delete", "update", "patch", "statefulsets", "persistentvolumes"} {
		require.NotContains(t, text, forbidden)
	}
	clusterRole := objectByKindAndName(t, rbac, "ClusterRole", "kubebrain-native-pitr-target-provisioning-inspector")
	clusterData, err := json.Marshal(clusterRole.Object)
	require.NoError(t, err)
	require.Contains(t, string(clusterData), "persistentvolumes")
	require.NotContains(t, string(clusterData), "delete")
	allRBAC, err := json.Marshal(rbac)
	require.NoError(t, err)
	require.Contains(t, string(allRBAC), "kubebrain-native-pitr-target-provisioning-writer-inspector")
	require.Contains(t, string(allRBAC), "statefulsets")
	require.NotContains(t, string(allRBAC), "\"update\"")
	require.Len(t, decodeManifest(t, "kubebrain-native-pitr-target-provisioning-requester-admission.yaml"), 4)
	targetAdmission := decodeManifest(t, "kubebrain-native-pitr-target-provisioning-target-admission.yaml")
	require.Len(t, targetAdmission, 2)
	targetAdmissionData, err := json.Marshal(targetAdmission[0].Object)
	require.NoError(t, err)
	require.Contains(t, string(targetAdmissionData), "kubebrain-native-pitr-target-provisioning-executor")
	require.Contains(t, string(targetAdmissionData), "native-pitr-provision-authorization")
	requestRBAC := decodeManifest(t, "kubebrain-native-pitr-target-provisioning-requester-rbac.yaml")
	requestRole := objectByKindAndName(t, requestRBAC, "Role", "kubebrain-native-pitr-target-provisioning-requester")
	requestBytes, err := json.Marshal(requestRole.Object)
	require.NoError(t, err)
	require.NotContains(t, string(requestBytes), "update")
	require.NotContains(t, string(requestBytes), "delete")
	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(dockerfile), "go build -trimpath -o /src/bin/kubebrain-native-pitr-target-empty ./hack/backup/cmd/native-pitr-target-empty")
	require.Contains(t, string(dockerfile), "COPY --from=build /src/bin/kubebrain-native-pitr-target-empty /usr/local/bin/kubebrain-native-pitr-target-empty")
}

func TestNativePITRFullRestoreHasPinnedIsolatedRuntimeImage(t *testing.T) {
	data, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	text := string(data)
	for _, expected := range []string{
		"go build -trimpath -o /src/bin/kubebrain-native-pitr-full-restore ./hack/backup/cmd/native-pitr-full-restore",
		"go build -trimpath -o /src/bin/kubebrain-native-pitr-full-restore-receipt-verify ./hack/backup/cmd/native-pitr-full-restore-receipt-verify",
		"AS native-pitr-full-restore", "COPY --from=br-v751 /br /usr/local/bin/br",
		"COPY --from=build /src/bin/kubebrain-native-pitr-full-restore /usr/local/bin/kubebrain-native-pitr-full-restore",
		"COPY --from=build /src/bin/kubebrain-native-pitr-full-restore-receipt-verify /usr/local/bin/kubebrain-native-pitr-full-restore-receipt-verify",
		"COPY --from=build /src/bin/kubectl /usr/local/bin/kubectl",
		"COPY hack/production/run-native-pitr-full-restore-operation.sh /opt/kubebrain/hack/production/run-native-pitr-full-restore-operation.sh",
	} {
		require.Contains(t, text, expected)
	}
}

func TestNativePITRFullRestoreWriterInspectorIsReadOnly(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-native-pitr-full-restore-writer-rbac.yaml")
	require.Len(t, objects, 2)
	role := objectByKindAndName(t, objects, "Role", "kubebrain-native-pitr-full-restore-writer-inspector")
	data, err := json.Marshal(role.Object)
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "statefulsets")
	require.Contains(t, text, "pods")
	require.Contains(t, text, "kubebrain")
	require.NotContains(t, text, "update")
	require.NotContains(t, text, "patch")
	require.NotContains(t, text, "delete")
	binding := objectByKindAndName(t, objects, "RoleBinding", "kubebrain-native-pitr-full-restore-writer-inspector")
	bindingData, err := json.Marshal(binding.Object)
	require.NoError(t, err)
	require.Contains(t, string(bindingData), "kubebrain-native-pitr-full-restore-executor")
}

func TestNativePITRFullRestoreRequesterAndEncryptionAdmissionAreFailClosed(t *testing.T) {
	requestObjects := decodeManifest(t, "kubebrain-native-pitr-full-restore-requester-admission.yaml")
	require.Len(t, requestObjects, 4)
	requestData, err := os.ReadFile("kubebrain-native-pitr-full-restore-requester-admission.yaml")
	require.NoError(t, err)
	text := string(requestData)
	for _, expected := range []string{"NativePITRFullRestore", "kubebrain-native-pitr-full-restore-requester", "maxAttempts == 2", "parameters.json", "object.immutable == true"} {
		require.Contains(t, text, expected)
	}
	for _, name := range []string{"kubebrain-native-pitr-full-restore-request-operation", "kubebrain-native-pitr-full-restore-request-parameters"} {
		binding := objectByKindAndName(t, requestObjects, "ValidatingAdmissionPolicyBinding", name)
		require.Equal(t, []string{"Deny"}, nestedStringSlice(t, binding, "spec", "validationActions"))
	}
	encryption := decodeManifest(t, "kubebrain-native-pitr-full-restore-encryption-admission.yaml")
	require.Len(t, encryption, 2)
	encryptionData, err := os.ReadFile("kubebrain-native-pitr-full-restore-encryption-admission.yaml")
	require.NoError(t, err)
	require.Contains(t, string(encryptionData), "object.data == oldObject.data")
}

func TestNativePITRFullBackupEncryptionSecretIsImmutable(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-native-pitr-full-backup-encryption-admission.yaml")
	require.Len(t, objects, 2)
	policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-native-pitr-full-backup-encryption")
	require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
	text := fmt.Sprint(policy.Object)
	for _, expected := range []string{
		"kubebrain-native-pitr-full-backup-encryption", "object.immutable == true",
		`size(object.data) == 2`, `"key" in object.data`, `"key-id" in object.data`,
		`oldObject.immutable == true`, `object.data == oldObject.data`,
	} {
		require.Contains(t, text, expected)
	}
	binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", "kubebrain-native-pitr-full-backup-encryption")
	require.Equal(t, []string{"Deny"}, nestedStringSlice(t, binding, "spec", "validationActions"))
}

func TestLegacySnapshotRemediationNativeHelperIsInRuntimeImage(t *testing.T) {
	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	text := string(dockerfile)
	require.Contains(t, text, "go build -trimpath -o /src/bin/kubebrain-legacy-snapshot-remediation ./hack/backup/cmd/legacy-snapshot-remediation")
	require.Contains(t, text, "COPY --from=build /src/bin/kubebrain-legacy-snapshot-remediation /usr/local/bin/kubebrain-legacy-snapshot-remediation")
	require.NotContains(t, text, "COPY --from=build /src/bin/etcdctl")
	require.NotContains(t, text, "COPY --from=build /src/bin/etcdutl")
}

func TestNativePITRFullBackupHasPinnedIsolatedRuntimeImage(t *testing.T) {
	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	text := string(dockerfile)

	require.Contains(t, text,
		"go build -trimpath -o /src/bin/kubebrain-native-pitr-full-backup ./hack/backup/cmd/native-pitr-full-backup")
	require.Contains(t, text,
		"FROM pingcap/br:v7.5.1@sha256:7815531bc337a56845efc3dceb9fe75778d387d76927994df736b6e23a98f768 AS br-v751")
	require.Contains(t, text,
		"FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40 AS native-pitr-full-backup")
	require.Contains(t, text, "gcompat=1.1.0-r4")
	require.Contains(t, text,
		"COPY --from=build /src/bin/kubebrain-native-pitr-full-backup /usr/local/bin/kubebrain-native-pitr-full-backup")
	require.Contains(t, text, "COPY --from=br-v751 /br /usr/local/bin/br")
	require.Contains(t, text, "COPY --from=build /src/bin/kubebrain-operation-worker /usr/local/bin/kubebrain-operation-worker")
	require.Contains(t, text, "COPY --from=build /src/bin/kubebrain-operationctl /usr/local/bin/kubebrain-operationctl")
	require.Contains(t, text, "COPY hack/production/run-native-pitr-full-backup-operation.sh /opt/kubebrain/hack/production/run-native-pitr-full-backup-operation.sh")
	require.Contains(t, text, "USER 65532:65532\nENTRYPOINT [\"/usr/local/bin/kubebrain-native-pitr-full-backup\"]")

	backupTarget := strings.Index(text, " AS native-pitr-full-backup")
	defaultRuntime := strings.LastIndex(text,
		"FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40\n")
	require.NotEqual(t, -1, backupTarget)
	require.Greater(t, defaultRuntime, backupTarget,
		"the default data-plane image must remain the final build target")
	defaultStage := text[defaultRuntime:]
	require.NotContains(t, defaultStage, "COPY --from=br-v751")
	require.NotContains(t, defaultStage, "/src/bin/kubebrain-native-pitr-full-backup")
}

func TestOperationParameterBrokerOwnsAllExecutorParameterSecretPermission(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-parameter-broker.yaml")
	deployment := objectByKindAndName(
		t, objects, "Deployment", "kubebrain-operation-parameter-broker",
	)
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
	require.EqualValues(t, 0, nestedInt64(
		t, deployment, "spec", "strategy", "rollingUpdate", "maxUnavailable",
	))
	require.True(t, nestedBool(
		t, deployment, "spec", "template", "spec", "securityContext", "runAsNonRoot",
	))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "runAsUser"))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "runAsGroup"))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "fsGroup"))
	require.Equal(t, "OnRootMismatch",
		nestedString(t, deployment, "spec", "template", "spec", "securityContext", "fsGroupChangePolicy"))
	podSpec, found, err := unstructured.NestedMap(deployment.Object, "spec", "template", "spec")
	require.NoError(t, err)
	require.True(t, found)
	pod := &unstructured.Unstructured{Object: podSpec}
	containers, found, err := unstructured.NestedSlice(
		deployment.Object, "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	command, found, err := unstructured.NestedStringSlice(container.Object, "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-operation-parameter-broker"}, command)
	args, found, err := unstructured.NestedStringSlice(container.Object, "args")
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, args, "--tls-reload-interval=30s")
	require.Contains(t, args, "--kubernetes-request-timeout=5s")
	require.Contains(t, args, "--additional-readiness-namespace=kubebrain-repair-operations")
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	assertReadOnlyTLSSecretVolume(t, pod, container,
		"tls", "kubebrain-operation-parameter-broker-tls", "/var/run/kubebrain-parameter-tls")

	role := objectByKindAndName(t, objects, "Role", "kubebrain-operation-parameter-broker")
	rules, found, err := unstructured.NestedSlice(role.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, rules, 2)
	require.Equal(t, []any{"kubebrainoperations"}, rules[0].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"get"}, rules[0].(map[string]any)["verbs"].([]any))
	require.Equal(t, []any{"secrets"}, rules[1].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"get"}, rules[1].(map[string]any)["verbs"].([]any))
	repairRole := objectByKindAndName(t, objects, "Role", "kubebrain-operation-parameter-broker-repair-queue")
	require.Equal(t, "kubebrain-repair-operations", repairRole.GetNamespace())
	repairRules, found, err := unstructured.NestedSlice(repairRole.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, repairRules, 2)
	require.Equal(t, []any{"kubebrainoperations"}, repairRules[0].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"get"}, repairRules[0].(map[string]any)["verbs"].([]any))
	require.Equal(t, []any{"secrets"}, repairRules[1].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"get"}, repairRules[1].(map[string]any)["verbs"].([]any))
	repairBinding := objectByKindAndName(t, objects, "RoleBinding", "kubebrain-operation-parameter-broker-repair-queue")
	require.Equal(t, "kubebrain-repair-operations", repairBinding.GetNamespace())
	require.Equal(t, "kubebrain-operation-parameter-broker-repair-queue",
		nestedString(t, repairBinding, "roleRef", "name"))
	repairSubjects, found, err := unstructured.NestedSlice(repairBinding.Object, "subjects")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, repairSubjects, 1)
	repairSubject := &unstructured.Unstructured{Object: repairSubjects[0].(map[string]any)}
	require.Equal(t, "kubebrain-operation-parameter-broker", nestedString(t, repairSubject, "name"))
	require.Equal(t, "kubebrain-operations", nestedString(t, repairSubject, "namespace"))

	tokenRole := objectByKindAndName(
		t, objects, "ClusterRole", "kubebrain-operation-parameter-broker-token-review",
	)
	tokenRules, found, err := unstructured.NestedSlice(tokenRole.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, tokenRules, 1)
	require.Equal(t, []any{"tokenreviews"}, tokenRules[0].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"create"}, tokenRules[0].(map[string]any)["verbs"].([]any))
	networkPolicy := objectByKindAndName(
		t, objects, "NetworkPolicy", "kubebrain-operation-parameter-broker",
	)
	require.Equal(t, "kubebrain-operation-parameter-broker", nestedString(
		t, networkPolicy, "spec", "podSelector", "matchLabels", "app.kubernetes.io/name",
	))
	ingress, found, err := unstructured.NestedSlice(networkPolicy.Object, "spec", "ingress")
	require.NoError(t, err)
	require.True(t, found)
	from := ingress[0].(map[string]any)["from"].([]any)
	selector := from[0].(map[string]any)["podSelector"].(map[string]any)
	expressions := selector["matchExpressions"].([]any)
	values := expressions[0].(map[string]any)["values"].([]any)
	require.ElementsMatch(t, []any{
		"kubebrain-backup-executor", "kubebrain-backup-deletion-executor",
		"kubebrain-native-pitr-full-backup-executor",
		"kubebrain-native-pitr-full-restore-executor",
		"kubebrain-native-pitr-target-retirement-executor",
		"kubebrain-native-pitr-target-provisioning-executor",
		"kubebrain-cold-physical-snapshot-executor",
		"kubebrain-cold-physical-restore-executor",
		"kubebrain-restore-cutover-executor", "kubebrain-post-restore-audit-executor",
		"kubebrain-certificate-rotation-executor", "kubebrain-tikv-transaction-repair-executor",
		"kubebrain-tikv-transaction-recovery-executor", "kubebrain-legacy-snapshot-remediation-executor", "kubebrain-destroy-executor",
	}, values)

	managed := objectByKindAndName(
		t, decodeManifest(t, "kubebrain-operation-managed-namespace-rbac.yaml"),
		"ClusterRole", "kubebrain-operation-worker-managed-namespace",
	)
	managedRules, found, err := unstructured.NestedSlice(managed.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	for _, raw := range managedRules {
		require.NotContains(t, raw.(map[string]any)["resources"].([]any), "secrets")
	}
}

func TestLegacySnapshotRemediationRequesterAdmissionIsFailClosed(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-legacy-snapshot-remediation-requester-admission.yaml")
	require.Len(t, objects, 4)
	operation := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-legacy-snapshot-remediation-request-operation")
	require.Equal(t, "Fail", nestedString(t, operation, "spec", "failurePolicy"))
	operationText := fmt.Sprint(operation.Object)
	require.Contains(t, operationText, "LegacySnapshotHistoryRemediation")
	require.Contains(t, operationText, "kubebrain-legacy-snapshot-remediation-requester")
	require.Contains(t, operationText, "maxAttempts == 1")
	parameters := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-legacy-snapshot-remediation-request-parameters")
	parameterText := fmt.Sprint(parameters.Object)
	require.Contains(t, parameterText, "object.immutable == true")
	require.Contains(t, parameterText, `size(object.data) == 1`)
}

func TestNativePITRFullBackupRequesterAdmissionIsFailClosed(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-native-pitr-full-backup-requester-admission.yaml")
	require.Len(t, objects, 4)
	operation := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-native-pitr-full-backup-request-operation")
	require.Equal(t, "Fail", nestedString(t, operation, "spec", "failurePolicy"))
	operationText := fmt.Sprint(operation.Object)
	for _, expected := range []string{
		"NativePITRFullBackup", "kubebrain-native-pitr-full-backup-requester",
		`parametersSHA256.substring(0, 20)`, "platform:native-pitr-full-backup",
		"maxAttempts == 1", `parametersSecretRef.name == object.metadata.name + "-parameters"`,
		`size(object.metadata.finalizers) == 1`, `dbaas.kubebrain.io/operation-audit`,
	} {
		require.Contains(t, operationText, expected)
	}
	parameters := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-native-pitr-full-backup-request-parameters")
	parameterText := fmt.Sprint(parameters.Object)
	require.Contains(t, parameterText, "object.immutable == true")
	require.Contains(t, parameterText, `size(object.data) == 1`)
	require.Contains(t, parameterText, `"parameters.json" in object.data`)
	for _, name := range []string{
		"kubebrain-native-pitr-full-backup-request-operation",
		"kubebrain-native-pitr-full-backup-request-parameters",
	} {
		binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", name)
		require.Equal(t, []string{"Deny"}, nestedStringSlice(t, binding, "spec", "validationActions"))
	}
}

func TestOperationWorkerAdmissionBindsStatusUpdatesToExecutorType(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-worker-admission.yaml")
	policy := objectByKindAndName(
		t, objects, "ValidatingAdmissionPolicy", "kubebrain-operation-worker-type",
	)
	require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
	validations, found, err := unstructured.NestedSlice(policy.Object, "spec", "validations")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, validations, 1)
	expression := validations[0].(map[string]any)["expression"].(string)
	for _, name := range []string{
		"kubebrain-backup-executor", "kubebrain-backup-deletion-executor",
		"kubebrain-native-pitr-full-backup-executor",
		"kubebrain-restore-cutover-executor", "kubebrain-post-restore-audit-executor",
		"kubebrain-certificate-rotation-executor", "kubebrain-tikv-transaction-repair-executor",
		"kubebrain-tikv-transaction-recovery-executor", "kubebrain-destroy-executor",
	} {
		require.Contains(t, expression, name)
	}
	binding := objectByKindAndName(
		t, objects, "ValidatingAdmissionPolicyBinding", "kubebrain-operation-worker-type",
	)
	actions, found, err := unstructured.NestedStringSlice(
		binding.Object, "spec", "validationActions",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"Deny"}, actions)
}

func TestColdPhysicalSnapshotRequesterAdmissionIsFailClosed(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-cold-physical-snapshot-requester-admission.yaml")
	require.Len(t, objects, 4)
	for _, name := range []string{"kubebrain-cold-snapshot-request-operation", "kubebrain-cold-snapshot-request-parameters"} {
		policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", name)
		require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
		binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", name)
		require.Equal(t, name, nestedString(t, binding, "spec", "policyName"))
		actions, found, err := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"Deny"}, actions)
	}
	operation := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-cold-snapshot-request-operation")
	conditions, found, err := unstructured.NestedSlice(operation.Object, "spec", "matchConditions")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, conditions, 1)
	require.Contains(t, conditions[0].(map[string]any)["expression"].(string), `object.spec.type == "ColdPhysicalSnapshot"`)
	validations, found, err := unstructured.NestedSlice(operation.Object, "spec", "validations")
	require.NoError(t, err)
	require.True(t, found)
	expressions := ""
	for _, raw := range validations {
		expressions += raw.(map[string]any)["expression"].(string)
	}
	require.Contains(t, expressions, `object.spec.type == "ColdPhysicalSnapshot"`)
	require.Contains(t, expressions, `request.userInfo.username == "system:serviceaccount:kubebrain-operations:kubebrain-cold-physical-snapshot-requester"`)
	require.Contains(t, expressions, `object.spec.maxAttempts == 1`)
	require.Contains(t, expressions, `object.spec.parametersSecretRef.name == object.metadata.name + "-parameters"`)
}

func TestColdPhysicalRestoreRequesterAdmissionIsFailClosed(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-cold-physical-restore-requester-admission.yaml")
	require.Len(t, objects, 4)
	operation := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-cold-restore-request-operation")
	require.Equal(t, "Fail", nestedString(t, operation, "spec", "failurePolicy"))
	conditions, found, err := unstructured.NestedSlice(operation.Object, "spec", "matchConditions")
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, conditions[0].(map[string]any)["expression"].(string), `object.spec.type == "ColdPhysicalRestore"`)
	validations, found, err := unstructured.NestedSlice(operation.Object, "spec", "validations")
	require.NoError(t, err)
	require.True(t, found)
	expressions := ""
	for _, raw := range validations {
		expressions += raw.(map[string]any)["expression"].(string)
	}
	require.Contains(t, expressions, `object.spec.maxAttempts == 1`)
	require.Contains(t, expressions, `request.userInfo.username == "system:serviceaccount:kubebrain-operations:kubebrain-cold-physical-restore-requester"`)
	for _, name := range []string{"kubebrain-cold-restore-request-operation", "kubebrain-cold-restore-request-parameters"} {
		binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", name)
		actions, found, err := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"Deny"}, actions)
	}
}

func TestColdPhysicalRestoreTargetAdmissionRestrictsCreateIdentity(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-cold-physical-restore-target-admission.yaml")
	require.Len(t, objects, 2)
	policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-cold-restore-target-create")
	require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
	validations, found, err := unstructured.NestedSlice(policy.Object, "spec", "validations")
	require.NoError(t, err)
	require.True(t, found)
	expressions := ""
	for _, raw := range validations {
		expressions += raw.(map[string]any)["expression"].(string)
	}
	require.Contains(t, expressions, `kubebrain-cold-physical-restore-executor`)
	require.Contains(t, expressions, `object.metadata.name == "kb"`)
	require.Contains(t, expressions, `object.spec.paused == true`)
	require.Contains(t, expressions, `^kb-restore-cold-snapshot-[a-f0-9]{20}-[a-f0-9]{12}$`)
	require.NotContains(t, expressions, `delete`)
}

func TestTiKVRepairAlertReceiverAdmissionPinsRequestShapeAndIdentity(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-tikv-repair-alert-receiver.yaml")
	for _, tc := range []struct {
		name              string
		resource          string
		requiredFragments []string
	}{
		{
			name:     "kubebrain-tikv-repair-alert-operation",
			resource: "kubebrainoperations",
			requiredFragments: []string{
				`^tikv-repair-[a-f0-9]{16,20}$`, `TiKVTransactionRepair`,
				`alertmanager:transaction-path-policy`, `parameters.json`, `maxAttempts == 1`,
			},
		},
		{
			name:     "kubebrain-tikv-repair-alert-parameters",
			resource: "secrets",
			requiredFragments: []string{
				`^tikv-repair-[a-f0-9]{16,20}-parameters$`, `object.immutable == true`,
				`object.type == "Opaque"`, `size(object.data) == 1`, `parameters.json`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", tc.name)
			require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
			rules, found, err := unstructured.NestedSlice(policy.Object, "spec", "matchConstraints", "resourceRules")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, rules, 1)
			require.Equal(t, []any{tc.resource}, rules[0].(map[string]any)["resources"].([]any))
			require.Equal(t, []any{"CREATE"}, rules[0].(map[string]any)["operations"].([]any))

			conditions, found, err := unstructured.NestedSlice(policy.Object, "spec", "matchConditions")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, conditions, 1)
			condition := conditions[0].(map[string]any)["expression"].(string)
			require.Contains(t, condition, `request.namespace == "kubebrain-repair-operations"`)
			require.Contains(t, condition, `system:serviceaccount:kubebrain-repair-operations:kubebrain-tikv-repair-alert-receiver`)

			validations, found, err := unstructured.NestedSlice(policy.Object, "spec", "validations")
			require.NoError(t, err)
			require.True(t, found)
			var expressions strings.Builder
			for _, validation := range validations {
				expressions.WriteString(validation.(map[string]any)["expression"].(string))
				expressions.WriteByte('\n')
			}
			for _, fragment := range tc.requiredFragments {
				require.Contains(t, expressions.String(), fragment)
			}

			binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", tc.name)
			require.Equal(t, tc.name, nestedString(t, binding, "spec", "policyName"))
			actions, found, err := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{"Deny"}, actions)
		})
	}
}

func TestTiKVQuiescedRepairRequesterAdmissionPinsPendingRequestShape(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-tikv-quiesced-repair-requester-admission.yaml")
	for _, tc := range []struct {
		name              string
		resource          string
		requiredFragments []string
	}{
		{
			name:     "kubebrain-tikv-quiesced-repair-request-operation",
			resource: "kubebrainoperations",
			requiredFragments: []string{
				`^tikv-quiesced-repair-[a-f0-9]{20}$`, `TiKVTransactionRepair`,
				`platform:tikv-quiesced-repair`, `parameters.json`, `maxAttempts == 1`,
			},
		},
		{
			name:     "kubebrain-tikv-quiesced-repair-request-parameters",
			resource: "secrets",
			requiredFragments: []string{
				`^tikv-quiesced-repair-[a-f0-9]{20}-parameters$`, `object.immutable == true`,
				`object.type == "Opaque"`, `size(object.data) == 1`, `parameters.json`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", tc.name)
			require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
			rules, found, err := unstructured.NestedSlice(policy.Object, "spec", "matchConstraints", "resourceRules")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, rules, 1)
			require.Equal(t, []any{tc.resource}, rules[0].(map[string]any)["resources"].([]any))
			require.Equal(t, []any{"CREATE"}, rules[0].(map[string]any)["operations"].([]any))
			conditions, found, err := unstructured.NestedSlice(policy.Object, "spec", "matchConditions")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, conditions, 1)
			condition := conditions[0].(map[string]any)["expression"].(string)
			require.Contains(t, condition, `system:serviceaccount:kubebrain-repair-operations:kubebrain-tikv-quiesced-repair-requester`)
			validations, found, err := unstructured.NestedSlice(policy.Object, "spec", "validations")
			require.NoError(t, err)
			require.True(t, found)
			var expressions strings.Builder
			for _, validation := range validations {
				expressions.WriteString(validation.(map[string]any)["expression"].(string))
				expressions.WriteByte('\n')
			}
			for _, fragment := range tc.requiredFragments {
				require.Contains(t, expressions.String(), fragment)
			}
			binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", tc.name)
			require.Equal(t, tc.name, nestedString(t, binding, "spec", "policyName"))
			actions, found, err := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{"Deny"}, actions)
		})
	}
}

func TestTiKVRecoveryRequesterAdmissionPinsPendingRequestShape(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-tikv-transaction-recovery-requester-admission.yaml")
	for _, tc := range []struct {
		name      string
		resource  string
		fragments []string
	}{
		{
			name: "kubebrain-tikv-recovery-request-operation", resource: "kubebrainoperations",
			fragments: []string{`^tikv-recovery-[a-f0-9]{20}$`, `TiKVTransactionRecovery`,
				`platform:tikv-repair-recovery`, `maxAttempts == 1`, `parameters.json`},
		},
		{
			name: "kubebrain-tikv-recovery-request-parameters", resource: "secrets",
			fragments: []string{`^tikv-recovery-[a-f0-9]{20}-parameters$`, `object.immutable == true`,
				`object.type == "Opaque"`, `size(object.data) == 1`, `parameters.json`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", tc.name)
			require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
			conditions, found, err := unstructured.NestedSlice(policy.Object, "spec", "matchConditions")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, conditions, 1)
			require.Contains(t, conditions[0].(map[string]any)["expression"], "kubebrain-tikv-transaction-recovery-requester")
			validations, found, err := unstructured.NestedSlice(policy.Object, "spec", "validations")
			require.NoError(t, err)
			require.True(t, found)
			var text strings.Builder
			for _, validation := range validations {
				text.WriteString(validation.(map[string]any)["expression"].(string))
			}
			for _, fragment := range tc.fragments {
				require.Contains(t, text.String(), fragment)
			}
			binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", tc.name)
			actions, found, err := unstructured.NestedStringSlice(binding.Object, "spec", "validationActions")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{"Deny"}, actions)
		})
	}
}

func TestOperationSubmitterApproverAndAuditAdmissionFenceHighRiskChanges(t *testing.T) {
	for _, tc := range []struct {
		file           string
		name           string
		verbs          []any
		forbiddenVerbs []any
	}{
		{
			file: "kubebrain-operation-submitter-rbac.yaml", name: "kubebrain-operation-submitter",
			verbs: []any{"create", "get", "list", "watch"}, forbiddenVerbs: []any{"update", "patch", "delete"},
		},
		{
			file: "kubebrain-operation-approver-rbac.yaml", name: "kubebrain-operation-approver",
			verbs: []any{"get", "list", "watch", "update"}, forbiddenVerbs: []any{"create", "patch", "delete"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := decodeManifest(t, tc.file)
			account := objectByKindAndName(t, objects, "ServiceAccount", tc.name)
			require.Equal(t, "kubebrain-operations", account.GetNamespace())
			require.False(t, nestedBool(t, account, "automountServiceAccountToken"))
			role := objectByKindAndName(t, objects, "Role", tc.name)
			require.Equal(t, "kubebrain-operations", role.GetNamespace())
			rules, found, err := unstructured.NestedSlice(role.Object, "rules")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, rules, 1)
			rule := rules[0].(map[string]any)
			require.Equal(t, []any{"dbaas.kubebrain.io"}, rule["apiGroups"].([]any))
			require.Equal(t, []any{"kubebrainoperations"}, rule["resources"].([]any))
			require.Equal(t, tc.verbs, rule["verbs"].([]any))
			for _, verb := range tc.forbiddenVerbs {
				require.NotContains(t, rule["verbs"].([]any), verb)
			}
			require.NotContains(t, rule["resources"].([]any), "kubebrainoperations/status")
			require.NotContains(t, rule["resources"].([]any), "secrets")
			require.NotContains(t, rule["resources"].([]any), "leases")

			binding := objectByKindAndName(t, objects, "RoleBinding", tc.name)
			require.Equal(t, "kubebrain-operations", binding.GetNamespace())
			subjects, found, err := unstructured.NestedSlice(binding.Object, "subjects")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, subjects, 1)
			subject := &unstructured.Unstructured{Object: subjects[0].(map[string]any)}
			require.Equal(t, "ServiceAccount", nestedString(t, subject, "kind"))
			require.Equal(t, tc.name, nestedString(t, subject, "name"))
			require.Equal(t, "kubebrain-operations", nestedString(t, subject, "namespace"))
			require.Equal(t, "Role", nestedString(t, binding, "roleRef", "kind"))
			require.Equal(t, tc.name, nestedString(t, binding, "roleRef", "name"))
		})
	}

	objects := decodeManifest(t, "kubebrain-operation-audit-admission.yaml")
	policy := objectByKindAndName(t, objects, "ValidatingAdmissionPolicy", "kubebrain-operation-audit")
	require.Equal(t, "kubebrain-operation-audit",
		nestedString(t, policy, "metadata", "labels", "app.kubernetes.io/name"))
	require.Equal(t, "kubebrain", nestedString(t, policy, "metadata", "labels", "app.kubernetes.io/part-of"))
	require.Equal(t, "Fail", nestedString(t, policy, "spec", "failurePolicy"))
	resourceRules, found, err := unstructured.NestedSlice(
		policy.Object, "spec", "matchConstraints", "resourceRules",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, resourceRules, 1)
	resourceRule := &unstructured.Unstructured{Object: resourceRules[0].(map[string]any)}
	require.Equal(t, []string{"dbaas.kubebrain.io"}, nestedStringSlice(t, resourceRule, "apiGroups"))
	require.Equal(t, []string{"v1alpha1"}, nestedStringSlice(t, resourceRule, "apiVersions"))
	require.Equal(t, []string{"CREATE", "UPDATE"}, nestedStringSlice(t, resourceRule, "operations"))
	require.Equal(t, []string{"kubebrainoperations"}, nestedStringSlice(t, resourceRule, "resources"))
	require.Equal(t, "Namespaced", nestedString(t, resourceRule, "scope"))

	validations, found, err := unstructured.NestedSlice(policy.Object, "spec", "validations")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, validations, 8)
	expressionsByMessage := map[string]string{}
	for _, raw := range validations {
		validation := raw.(map[string]any)
		expressionsByMessage[validation["message"].(string)] = validation["expression"].(string)
	}
	require.Contains(t,
		expressionsByMessage["operations may only use the audit archive finalizer"],
		`"dbaas.kubebrain.io/operation-audit"`)
	require.Contains(t,
		expressionsByMessage["operations may only use the audit archive finalizer"],
		`size(object.metadata.finalizers) == 1`)
	require.Contains(t,
		expressionsByMessage["operations may only use the audit archive finalizer"],
		`request.operation != "UPDATE"`)
	require.Contains(t,
		expressionsByMessage["operations may only use the audit archive finalizer"],
		`object.metadata.finalizers.all(f, f == "dbaas.kubebrain.io/operation-audit")`)
	require.Contains(t,
		expressionsByMessage["operations cannot be created pre-approved"],
		`"dbaas.kubebrain.io/approved-by"`)
	preArchivedExpression := expressionsByMessage["operations cannot be created pre-archived"]
	require.Contains(t, preArchivedExpression, `"dbaas.kubebrain.io/audit-receipt-sha256"`)
	require.Contains(t, preArchivedExpression, `"dbaas.kubebrain.io/audit-artifact-sha256"`)
	require.Contains(t, preArchivedExpression, `"dbaas.kubebrain.io/audit-version-id"`)
	introduceArchiveExpression := expressionsByMessage["operation audit archive evidence can only be introduced by archiver release"]
	require.Contains(t, introduceArchiveExpression,
		`request.userInfo.username == "system:serviceaccount:kubebrain-operations:kubebrain-operation-archiver"`)
	require.Contains(t, introduceArchiveExpression,
		`oldObject.metadata.finalizers.exists(f, f == "dbaas.kubebrain.io/operation-audit")`)
	require.Contains(t, introduceArchiveExpression,
		`!object.metadata.finalizers.exists(f, f == "dbaas.kubebrain.io/operation-audit")`)
	require.Contains(t, introduceArchiveExpression, `object.status.phase in ["Succeeded", "Failed"]`)
	require.Contains(t, introduceArchiveExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"].matches("^[a-f0-9]{64}$")`)
	require.Contains(t, introduceArchiveExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"].matches("^[a-f0-9]{64}$")`)
	require.Contains(t, introduceArchiveExpression,
		`size(object.metadata.annotations["dbaas.kubebrain.io/audit-version-id"]) > 0`)
	approvalExpression := expressionsByMessage["operation approval requires the dedicated approver identity, approval ID, and pending phase"]
	require.Contains(t, approvalExpression,
		`request.userInfo.username == "system:serviceaccount:kubebrain-operations:kubebrain-operation-approver"`)
	require.Contains(t, approvalExpression, `object.status.phase == "Pending"`)
	require.Contains(t, approvalExpression, `approval-id"].matches("^[a-z0-9]`)
	for _, operationType := range []string{"BackupDeletion", "ColdPhysicalSnapshot", "ColdPhysicalRestore", "LegacySnapshotHistoryRemediation", "RestoreCutover", "CertificateRotation", "TiKVTransactionRepair", "TiKVTransactionRecovery", "Destroy"} {
		require.Contains(t, approvalExpression, `"`+operationType+`"`)
	}
	require.NotContains(t, approvalExpression, `"Backup"`)
	require.NotContains(t, approvalExpression, `"PostRestoreAudit"`)
	require.Contains(t,
		expressionsByMessage["operation approval evidence is immutable"],
		`object.metadata.annotations["dbaas.kubebrain.io/approved-by"] == oldObject.metadata.annotations["dbaas.kubebrain.io/approved-by"]`)
	archiveImmutableExpression := expressionsByMessage["operation audit archive evidence is immutable"]
	require.Contains(t, archiveImmutableExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"] == oldObject.metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"]`)
	require.Contains(t, archiveImmutableExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"] == oldObject.metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"]`)
	require.Contains(t, archiveImmutableExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-version-id"] == oldObject.metadata.annotations["dbaas.kubebrain.io/audit-version-id"]`)
	releaseExpression := expressionsByMessage["removing the audit finalizer requires terminal archive evidence"]
	require.Contains(t, releaseExpression,
		`request.userInfo.username == "system:serviceaccount:kubebrain-operations:kubebrain-operation-archiver"`)
	require.Contains(t, releaseExpression, `object.status.phase in ["Succeeded", "Failed"]`)
	require.Contains(t, releaseExpression, `"dbaas.kubebrain.io/audit-receipt-sha256"`)
	require.Contains(t, releaseExpression, `"dbaas.kubebrain.io/audit-artifact-sha256"`)
	require.Contains(t, releaseExpression, `"dbaas.kubebrain.io/audit-version-id"`)
	require.Contains(t, releaseExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"] == oldObject.metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"]`)
	require.Contains(t, releaseExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"] == oldObject.metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"]`)
	require.Contains(t, releaseExpression,
		`object.metadata.annotations["dbaas.kubebrain.io/audit-version-id"] == oldObject.metadata.annotations["dbaas.kubebrain.io/audit-version-id"]`)

	binding := objectByKindAndName(t, objects, "ValidatingAdmissionPolicyBinding", "kubebrain-operation-audit")
	require.Equal(t, "kubebrain-operation-audit",
		nestedString(t, binding, "metadata", "labels", "app.kubernetes.io/name"))
	require.Equal(t, "kubebrain-operation-audit", nestedString(t, binding, "spec", "policyName"))
	actions, found, err := unstructured.NestedStringSlice(
		binding.Object, "spec", "validationActions",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"Deny"}, actions)
}

func TestOperationAPIIsFailClosedAndHardened(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-operation-api.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-operation-api")
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "replicas"))
	require.EqualValues(t, 0, nestedInt64(t, deployment, "spec", "strategy", "rollingUpdate", "maxUnavailable"))
	require.EqualValues(t, 1, nestedInt64(t, deployment, "spec", "strategy", "rollingUpdate", "maxSurge"))
	require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "automountServiceAccountToken"))
	require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "securityContext", "runAsNonRoot"))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "runAsUser"))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "runAsGroup"))
	require.EqualValues(t, 65532, nestedInt64(t, deployment, "spec", "template", "spec", "securityContext", "fsGroup"))
	require.Equal(t, "OnRootMismatch",
		nestedString(t, deployment, "spec", "template", "spec", "securityContext", "fsGroupChangePolicy"))
	podSpec, found, err := unstructured.NestedMap(deployment.Object, "spec", "template", "spec")
	require.NoError(t, err)
	require.True(t, found)
	pod := &unstructured.Unstructured{Object: podSpec}
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	command, found, err := unstructured.NestedStringSlice(container.Object, "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-operation-api"}, command)
	args, found, err := unstructured.NestedStringSlice(container.Object, "args")
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, args, "--oidc-jwks-cache-ttl=5m")
	require.Contains(t, args, "--oidc-jwks-refresh-backoff=5s")
	require.Contains(t, args, "--tls-reload-interval=30s")
	require.Contains(t, args, "--dependency-request-timeout=5s")
	require.Contains(t, args, "--tls-cert-file=/var/run/kubebrain-api-tls/tls.crt")
	require.Contains(t, args, "--tls-key-file=/var/run/kubebrain-api-tls/tls.key")
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	assertReadOnlyTLSSecretVolume(t, pod, container,
		"tls", "kubebrain-operation-api-tls", "/var/run/kubebrain-api-tls")
	require.Equal(t, "/readyz", nestedString(t, container, "readinessProbe", "httpGet", "path"))
	require.Equal(t, "HTTPS", nestedString(t, container, "readinessProbe", "httpGet", "scheme"))
	spreads, found, err := unstructured.NestedSlice(
		deployment.Object, "spec", "template", "spec", "topologySpreadConstraints",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, spreads, 2)
	require.Equal(t, "topology.kubernetes.io/zone",
		nestedString(t, &unstructured.Unstructured{Object: spreads[0].(map[string]any)}, "topologyKey"))
	require.Equal(t, "kubernetes.io/hostname",
		nestedString(t, &unstructured.Unstructured{Object: spreads[1].(map[string]any)}, "topologyKey"))

	pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kubebrain-operation-api")
	require.EqualValues(t, 1, nestedInt64(t, pdb, "spec", "maxUnavailable"))
}

func TestBackupSchedulerIsHAAndLeastPrivilege(t *testing.T) {
	crd := objectByKindAndName(
		t, decodeManifest(t, "kubebrain-backup-policy-crd.yaml"),
		"CustomResourceDefinition", "kubebrainbackuppolicies.dbaas.kubebrain.io",
	)
	require.Equal(t, "Namespaced", nestedString(t, crd, "spec", "scope"))
	require.Equal(t, "KubeBrainBackupPolicy", nestedString(t, crd, "spec", "names", "kind"))
	versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, versions, 1)
	version := &unstructured.Unstructured{Object: versions[0].(map[string]any)}
	required, found, err := unstructured.NestedStringSlice(
		version.Object, "schema", "openAPIV3Schema", "properties", "spec", "required",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, required, "tenant")
	require.Equal(t, "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
		nestedString(t, version, "schema", "openAPIV3Schema", "properties", "spec",
			"properties", "tenant", "pattern"))

	objects := decodeManifest(t, "kubebrain-backup-scheduler.yaml")
	deployment := objectByKindAndName(t, objects, "Deployment", "kubebrain-backup-scheduler")
	require.EqualValues(t, 2, nestedInt64(t, deployment, "spec", "replicas"))
	require.Equal(t, "kubebrain-backup-scheduler",
		nestedString(t, deployment, "spec", "template", "spec", "serviceAccountName"))
	require.True(t, nestedBool(t, deployment, "spec", "template", "spec", "securityContext", "runAsNonRoot"))

	role := objectByKindAndName(t, objects, "Role", "kubebrain-backup-scheduler")
	rules, found, err := unstructured.NestedSlice(role.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, rules, 4)
	require.Equal(t, []any{"configmaps"}, rules[0].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"kubebrain-backup-scheduler-inventory"},
		rules[0].(map[string]any)["resourceNames"].([]any))
	require.Equal(t, []any{"get"}, rules[0].(map[string]any)["verbs"].([]any))
	require.Equal(t, []any{"create", "get"}, rules[2].(map[string]any)["verbs"].([]any))
	require.Equal(t, []any{"create", "get"}, rules[3].(map[string]any)["verbs"].([]any))
	require.NotContains(t, rules[2].(map[string]any)["verbs"].([]any), "update")
	require.NotContains(t, rules[3].(map[string]any)["verbs"].([]any), "update")
	binding := objectByKindAndName(t, objects, "RoleBinding", "kubebrain-backup-scheduler")
	require.Equal(t, "Role",
		nestedString(t, binding, "roleRef", "kind"))
	clusterRole := objectByKindAndName(
		t, objects, "ClusterRole", "kubebrain-backup-scheduler-managed-namespace",
	)
	clusterRules, found, err := unstructured.NestedSlice(clusterRole.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, clusterRules, 3)
	require.Equal(t, rules[1:], clusterRules)
	inventory := objectByKindAndName(
		t, objects, "ConfigMap", "kubebrain-backup-scheduler-inventory",
	)
	require.Equal(t, `["kubebrain-operations"]`,
		nestedString(t, inventory, "data", "namespaces.json"))
	containers, found, err := unstructured.NestedSlice(
		deployment.Object, "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	args, found, err := unstructured.NestedStringSlice(containers[0].(map[string]any), "args")
	require.NoError(t, err)
	require.True(t, found)
	require.NotContains(t, args, "--namespaces=kubebrain-operations")
	require.Contains(t, args,
		"--namespace-inventory-configmap=kubebrain-backup-scheduler-inventory")
	require.Contains(t, args, "--namespace-inventory-namespace=kubebrain-operations")
	require.Contains(t, args, "--namespace-inventory-key=namespaces.json")
	require.Contains(t, args,
		"--requested-by=system:serviceaccount:kubebrain-operations:kubebrain-backup-scheduler")
	require.Contains(t, args, "--reconcile-timeout=2m")
	require.Contains(t, args, "--max-policies=256")
	spreads, found, err := unstructured.NestedSlice(
		deployment.Object, "spec", "template", "spec", "topologySpreadConstraints",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, spreads, 2)
	pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kubebrain-backup-scheduler")
	require.EqualValues(t, 1, nestedInt64(t, pdb, "spec", "maxUnavailable"))
}

func TestTiKVTransactionRepairRBACCoversReadOnlyStorageFence(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-tikv-transaction-repair-rbac.yaml")
	var tidbRole, pvRole, pvBinding *unstructured.Unstructured
	for _, object := range objects {
		name := object.GetName()
		switch {
		case object.GetKind() == "Role" && name == "kubebrain-tikv-transaction-repair" && object.GetNamespace() == "tidb-cluster":
			tidbRole = object
		case object.GetKind() == "ClusterRole" && name == "kubebrain-tikv-transaction-repair-pv-reader":
			pvRole = object
		case object.GetKind() == "ClusterRoleBinding" && name == "kubebrain-tikv-transaction-repair-pv-reader":
			pvBinding = object
		}
	}
	require.NotNil(t, tidbRole)
	require.NotNil(t, pvRole)
	require.NotNil(t, pvBinding)

	rules, found, err := unstructured.NestedSlice(tidbRole.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	findRule := func(resource string) map[string]any {
		t.Helper()
		for _, raw := range rules {
			rule := raw.(map[string]any)
			resources := rule["resources"].([]any)
			for _, candidate := range resources {
				if candidate == resource {
					return rule
				}
			}
		}
		return nil
	}
	execRule := findRule("pods/exec")
	require.NotNil(t, execRule)
	require.Equal(t, []any{"create"}, execRule["verbs"])
	pvcRule := findRule("persistentvolumeclaims")
	require.NotNil(t, pvcRule)
	require.Equal(t, []any{"get"}, pvcRule["verbs"])
	require.ElementsMatch(t, []any{
		"pd-kb-pd-0", "pd-kb-pd-1", "pd-kb-pd-2",
		"tikv-kb-tikv-0", "tikv-kb-tikv-1", "tikv-kb-tikv-2",
	}, pvcRule["resourceNames"])

	pvRules, found, err := unstructured.NestedSlice(pvRole.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, pvRules, 1)
	require.Equal(t, []any{"persistentvolumes"}, pvRules[0].(map[string]any)["resources"])
	require.Equal(t, []any{"get"}, pvRules[0].(map[string]any)["verbs"])
	require.Equal(t, "ClusterRole", nestedString(t, pvBinding, "roleRef", "kind"))
	require.Equal(t, "kubebrain-tikv-transaction-repair-pv-reader", nestedString(t, pvBinding, "roleRef", "name"))
	subjects, found, err := unstructured.NestedSlice(pvBinding.Object, "subjects")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, subjects, 1)
	require.Equal(t, "kubebrain-tikv-transaction-repair-executor", subjects[0].(map[string]any)["name"])
	require.Equal(t, "kubebrain-operations", subjects[0].(map[string]any)["namespace"])
}

func TestProductionMonitoringTracksStatefulSetReadiness(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	monitor := objectByKindAndName(t, objects, "ServiceMonitor", "kubebrain")
	endpoints, found, err := unstructured.NestedSlice(monitor.Object, "spec", "endpoints")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []any{map[string]any{
		"action": "replace", "sourceLabels": []any{"__meta_kubernetes_pod_uid"}, "targetLabel": "uid",
	}}, endpoints[0].(map[string]any)["relabelings"])
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
		`absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:kubebrain_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:kubebrain_replicas:expected < 3 or (max by (namespace, statefulset) (kube_statefulset_status_replicas_ready{namespace="kubebrain-system",statefulset="kubebrain"}) or on() vector(0)) != on() kubebrain_dbaas:kubebrain_replicas:expected`,
		readinessRule["expr"],
	)
	require.NotContains(t, readinessRule["expr"], "kube_deployment_")
	insufficientRule := prometheusRuleByAlert(t, groups, "KubeBrainInsufficientReplicas")
	require.Equal(t,
		`absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:kubebrain_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:kubebrain_replicas:expected < 3 or count(max by (instance) (up{namespace="kubebrain-system",service="kubebrain-peer"} == 1)) != on() kubebrain_dbaas:kubebrain_replicas:expected`,
		insufficientRule["expr"])
	for _, rule := range []map[string]any{readinessRule, insufficientRule} {
		description := rule["annotations"].(map[string]any)["description"].(string)
		require.Contains(t, description, "production minimum of 3 replicas")
		require.Contains(t, description, "exactly match the current expected replica count")
		require.Contains(t, description, "expectation chain is absent")
	}

	transactionPathRule := prometheusRuleByAlert(t, groups,
		"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane")
	require.Equal(t,
		`((max by (namespace, statefulset) (kube_statefulset_status_replicas_ready{namespace="kubebrain-system",statefulset="kubebrain"}) or on() vector(0)) == 0) and on() (kubebrain_dbaas:replica_expectation_sources:count == 3) and on() (kubebrain_dbaas:pd_replicas:expected >= 3) and on() (kubebrain_dbaas:tikv_replicas:expected >= 3) and on() (count(max by (instance) (up{namespace="tidb-cluster",service="kb-pd-metrics"} == 1)) == on() kubebrain_dbaas:pd_replicas:expected) and on() (count(max by (instance) (up{namespace="tidb-cluster",service="kb-tikv-metrics"} == 1)) == on() kubebrain_dbaas:tikv_replicas:expected) and on() (sum(max by (instance) (etcd_server_is_leader{namespace="tidb-cluster",service="kb-pd-metrics"})) == 1) and on() (count(max by (instance, type) (pd_regions_status{namespace="tidb-cluster",service="kb-pd-metrics",type=~"pending-peer-region-count|down-peer-region-count"})) == on() (2 * kubebrain_dbaas:pd_replicas:expected)) and on() (max(tikv_raftstore_leader_missing{namespace="tidb-cluster",service="kb-tikv-metrics"}) == 0) and on() (max(pd_regions_status{namespace="tidb-cluster",service="kb-pd-metrics",type=~"pending-peer-region-count|down-peer-region-count"}) == 0)`,
		transactionPathRule["expr"])
	require.Equal(t, "2m", transactionPathRule["for"])
	require.Equal(t, "critical", transactionPathRule["labels"].(map[string]any)["severity"])
	description := transactionPathRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, description, "end-to-end etcd transaction probe")
	require.Contains(t, description, "complete current PD/TiKV topology")
	require.Contains(t, description, "exactly one leader")
	require.Contains(t, description, "complete Region-peer telemetry")
	incompatibleWitness := prometheusRuleByAlert(t, groups, "KubeBrainIncompatibleTransactionWitness")
	require.Equal(t,
		`sum(increase(leader_election_initialize_incompatible_witness{namespace="kubebrain-system"}[10m])) > 0`,
		incompatibleWitness["expr"])
	require.Equal(t, "critical", incompatibleWitness["labels"].(map[string]any)["severity"])
	require.Contains(t, incompatibleWitness["annotations"].(map[string]any)["description"], "Continue roll-forward")
	invalidAlarm := prometheusRuleByAlert(t, groups, "KubeBrainInvalidCorruptAlarmMetadata")
	require.Equal(t,
		`sum(increase(leader_election_initialize_invalid_alarm_metadata{namespace="kubebrain-system"}[10m])) > 0`,
		invalidAlarm["expr"])
	require.Equal(t, "critical", invalidAlarm["labels"].(map[string]any)["severity"])
	invalidAlarmDescription := invalidAlarm["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, invalidAlarmDescription, "refused leadership")
	require.Contains(t, invalidAlarmDescription, "do not clear or rewrite internal alarm keys")
	require.Contains(t, description, "same-PVC TiKV repair")
	require.Contains(t, description, "no pending/down peer Regions")

	overflowRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexOverflowed")
	require.Equal(t, `max(count_index_overflowed{namespace="kubebrain-system"}) > 0`, overflowRule["expr"])
	require.Equal(t, "1m", overflowRule["for"])
	require.Equal(t, "warning", overflowRule["labels"].(map[string]any)["severity"])

	rebuildRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexRebuildFailures")
	require.Equal(t,
		`sum(increase(count_index_rebuild_err{namespace="kubebrain-system"}[10m])) > 0`,
		rebuildRule["expr"])
	require.Equal(t, "0m", rebuildRule["for"])

	checkpointRule := prometheusRuleByAlert(t, groups, "KubeBrainSerializableCheckpointUnavailable")
	require.Equal(t,
		`count(serializable_checkpoint_available{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1)) or count(serializable_checkpoint_revision{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1)) or count(serializable_checkpoint_remaining_seconds{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1)) or min(serializable_checkpoint_available{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) < 1 or min(serializable_checkpoint_revision{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) <= 0 or min(serializable_checkpoint_remaining_seconds{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) < 60 or max((time() - timestamp(serializable_checkpoint_available{namespace="kubebrain-system"})) and on(namespace, pod, uid) (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 60 or max((time() - timestamp(serializable_checkpoint_revision{namespace="kubebrain-system"})) and on(namespace, pod, uid) (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 60 or max((time() - timestamp(serializable_checkpoint_remaining_seconds{namespace="kubebrain-system"})) and on(namespace, pod, uid) (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 60`,
		checkpointRule["expr"])
	require.Equal(t, "30s", checkpointRule["for"])
	require.Equal(t, "warning", checkpointRule["labels"].(map[string]any)["severity"])
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "PD-isolated serializable")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "less than 60 seconds")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"],
		"stopped publishing any of the available, revision, and remaining-seconds checkpoint gauges")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "Ready Pod UID count during scaling")

	checkpointRefreshRule := prometheusRuleByAlert(t, groups, "KubeBrainSerializableCheckpointRefreshFailures")
	healthFallbackRule := prometheusRuleByAlert(t, groups, "KubeBrainHealthCheckpointFallback")
	require.Equal(t,
		`sum by (check) (increase(health_checkpoint_fallback{namespace="kubebrain-system"}[10m])) > 0`,
		healthFallbackRule["expr"])
	require.Equal(t, "0m", healthFallbackRule["for"])
	require.Equal(t, "warning", healthFallbackRule["labels"].(map[string]any)["severity"])
	healthFallbackDescription := healthFallbackRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, healthFallbackDescription, "bounded-stale degraded service")
	require.Contains(t, healthFallbackDescription, "not proof that PD/TiKV recovered")
	healthFallbackMissingRule := prometheusRuleByAlert(t, groups,
		"KubeBrainHealthCheckpointFallbackMetricsMissing")
	require.Equal(t,
		`count(health_checkpoint_fallback{namespace="kubebrain-system",check=~"alarm|serializable_read|data_corruption"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != 3 * count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))`,
		healthFallbackMissingRule["expr"])
	require.Equal(t, "5m", healthFallbackMissingRule["for"])
	require.Equal(t, "warning", healthFallbackMissingRule["labels"].(map[string]any)["severity"])
	healthFallbackMissingDescription := healthFallbackMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, healthFallbackMissingDescription, "exactly three initialized")
	require.Contains(t, healthFallbackMissingDescription, "current Ready replica count")
	require.Contains(t, healthFallbackMissingDescription, "during scaling")
	require.Contains(t, healthFallbackMissingDescription, "mixed binary versions")
	require.Contains(t, healthFallbackMissingDescription, "pod UID relabeling")
	require.Equal(t,
		`sum(increase(serializable_checkpoint_refresh_err{namespace="kubebrain-system"}[10m])) > 0`,
		checkpointRefreshRule["expr"])
	require.Equal(t, "0m", checkpointRefreshRule["for"])
	require.Equal(t, "warning", checkpointRefreshRule["labels"].(map[string]any)["severity"])

	quotaRules := map[string]struct {
		expr     string
		forValue string
		severity string
	}{
		"KubeBrainQuotaNoSpace": {
			expr: `max(quota_nospace{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 0`, forValue: "0m", severity: "critical",
		},
		"KubeBrainQuotaUsageHigh": {
			expr: `max((quota_logical_usage_bytes{namespace="kubebrain-system"} / quota_backend_bytes{namespace="kubebrain-system"}) * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 0.9`, forValue: "10m", severity: "warning",
		},
		"KubeBrainQuotaMetricsInconsistent": {
			expr: `count(quota_nospace{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1)) or count(quota_backend_bytes{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1)) or count(quota_logical_usage_bytes{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) != count(max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1)) or (max(quota_nospace{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) - min(quota_nospace{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 0) or (max(quota_backend_bytes{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) - min(quota_backend_bytes{namespace="kubebrain-system"} * on(namespace, pod, uid) group_left() (max by (namespace, pod, uid) (kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1))) > 0)`, forValue: "1m", severity: "warning",
		},
		"KubeBrainQuotaRefreshFailures": {
			expr: `sum(increase(quota_refresh_err{namespace="kubebrain-system"}[10m])) > 0`, forValue: "0m", severity: "warning",
		},
	}
	for alert, want := range quotaRules {
		rule := prometheusRuleByAlert(t, groups, alert)
		require.Equal(t, want.expr, rule["expr"])
		require.Equal(t, want.forValue, rule["for"])
		require.Equal(t, want.severity, rule["labels"].(map[string]any)["severity"])
	}
	quotaConsistencyDescription := prometheusRuleByAlert(t, groups,
		"KubeBrainQuotaMetricsInconsistent")["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, quotaConsistencyDescription, "Ready Pod UID count during scaling")
	require.Contains(t, quotaConsistencyDescription, "stale Pod samples are excluded")

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

func TestProductionMonitoringTracksPDAndTiKV(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	for _, component := range []string{"pd", "tikv"} {
		monitor := objectByKindAndName(t, objects, "ServiceMonitor", "kubebrain-"+component)
		namespaces, found, err := unstructured.NestedStringSlice(
			monitor.Object, "spec", "namespaceSelector", "matchNames")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"tidb-cluster"}, namespaces)
		require.Equal(t, "kb",
			nestedString(t, monitor, "spec", "selector", "matchLabels", "app.kubernetes.io/instance"))
		require.Equal(t, component,
			nestedString(t, monitor, "spec", "selector", "matchLabels", "app.kubernetes.io/component"))
		require.Equal(t, "kubebrain",
			nestedString(t, monitor, "spec", "selector", "matchLabels", "app.kubernetes.io/part-of"))
		endpoints, found, err := unstructured.NestedSlice(monitor.Object, "spec", "endpoints")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, endpoints, 1)
		require.Equal(t, "metrics", endpoints[0].(map[string]any)["port"])
	}

	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)
	for alert, expr := range map[string]string{
		"KubeBrainPDInsufficientReplicas":       `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:pd_replicas:expected < 3 or count(max by (instance) (up{namespace="tidb-cluster",service="kb-pd-metrics"} == 1)) != on() kubebrain_dbaas:pd_replicas:expected`,
		"KubeBrainTiKVInsufficientReplicas":     `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:tikv_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:tikv_replicas:expected < 3 or count(max by (instance) (up{namespace="tidb-cluster",service="kb-tikv-metrics"} == 1)) != on() kubebrain_dbaas:tikv_replicas:expected`,
		"KubeBrainPDLeaderUnavailable":          `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or count(max by (instance) (etcd_server_is_leader{namespace="tidb-cluster",service="kb-pd-metrics"})) != on() kubebrain_dbaas:pd_replicas:expected or sum(max by (instance) (etcd_server_is_leader{namespace="tidb-cluster",service="kb-pd-metrics"})) != 1`,
		"KubeBrainTiKVRegionLeaderMissing":      `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:tikv_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or count(max by (instance) (tikv_raftstore_leader_missing{namespace="tidb-cluster",service="kb-tikv-metrics"})) != on() kubebrain_dbaas:tikv_replicas:expected or max(tikv_raftstore_leader_missing{namespace="tidb-cluster",service="kb-tikv-metrics"}) > 0`,
		"KubeBrainPDRegionPeerUnhealthy":        `max by (type) (pd_regions_status{namespace="tidb-cluster",service="kb-pd-metrics",type=~"pending-peer-region-count|down-peer-region-count"}) > 0`,
		"KubeBrainPDRegionHealthMetricsMissing": `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or count(max by (instance, type) (pd_regions_status{namespace="tidb-cluster",service="kb-pd-metrics",type=~"pending-peer-region-count|down-peer-region-count"})) != on() (2 * kubebrain_dbaas:pd_replicas:expected)`,
		"KubeBrainPDWALFsyncLatencyHigh":        `histogram_quantile(0.99, sum by (instance, le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket{namespace="tidb-cluster",service="kb-pd-metrics"}[5m]))) > 1`,
		"KubeBrainTiKVRaftDBWriteLatencyHigh":   `histogram_quantile(0.99, sum by (instance, le) (rate(tikv_raftstore_store_write_raftdb_duration_seconds_bucket{namespace="tidb-cluster",service="kb-tikv-metrics"}[5m]))) > 1`,
		"KubeBrainTiKVKVDBWriteLatencyHigh":     `histogram_quantile(0.99, sum by (instance, le) (rate(tikv_raftstore_store_write_kvdb_duration_seconds_bucket{namespace="tidb-cluster",service="kb-tikv-metrics"}[5m]))) > 1`,
		"KubeBrainStorageLatencyMetricsMissing": `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or absent(kubebrain_dbaas:tikv_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or count(max by (instance) (etcd_disk_wal_fsync_duration_seconds_count{namespace="tidb-cluster",service="kb-pd-metrics"})) != on() kubebrain_dbaas:pd_replicas:expected or count(max by (instance) (tikv_raftstore_store_write_raftdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"})) != on() kubebrain_dbaas:tikv_replicas:expected or count(max by (instance) (tikv_raftstore_store_write_kvdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"})) != on() kubebrain_dbaas:tikv_replicas:expected`,
		"KubeBrainStorageLatencyMetricsStale":   `(max(time() - max by (instance) (timestamp(etcd_disk_wal_fsync_duration_seconds_count{namespace="tidb-cluster",service="kb-pd-metrics"}))) > 60) or (max(time() - max by (instance) (timestamp(tikv_raftstore_store_write_raftdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"}))) > 60) or (max(time() - max by (instance) (timestamp(tikv_raftstore_store_write_kvdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"}))) > 60)`,
		"KubeBrainStorageVolumeMetricsMissing":  `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:storage_replicas:expected) == 1 or absent(kubebrain_dbaas:storage_volumes:expected) == 1 or absent(kubebrain_dbaas:storage_requested_sources:count) == 1 or absent(kubebrain_dbaas:storage_capacity_sources:count) == 1 or absent(kubebrain_dbaas:storage_available_sources:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:storage_volumes:expected < on() kubebrain_dbaas:storage_replicas:expected or kubebrain_dbaas:storage_requested_sources:count != on() kubebrain_dbaas:storage_volumes:expected or kubebrain_dbaas:storage_capacity_sources:count != on() kubebrain_dbaas:storage_replicas:expected or kubebrain_dbaas:storage_available_sources:count != on() kubebrain_dbaas:storage_replicas:expected`,
		"KubeBrainStorageVolumeLow":             `(kubebrain_dbaas:storage_available_bytes:min_by_pvc / kubebrain_dbaas:storage_capacity_bytes:max_by_pvc) < 0.15`,
		"KubeBrainResourceMetricsMissing":       `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:compute_replicas:expected) == 1 or absent(kubebrain_dbaas:cpu_usage_sources:count) == 1 or absent(kubebrain_dbaas:cpu_throttled_period_sources:count) == 1 or absent(kubebrain_dbaas:cpu_period_sources:count) == 1 or absent(kubebrain_dbaas:memory_working_set_sources:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:cpu_usage_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:cpu_throttled_period_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:cpu_period_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:memory_working_set_sources:count != on() kubebrain_dbaas:compute_replicas:expected or (count(max by (namespace, pod, container) (kube_pod_container_resource_limits{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",container="kubebrain",resource="memory",unit="byte"})) + count(max by (namespace, pod, container) (kube_pod_container_resource_limits{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",resource="memory",unit="byte"}))) != on() kubebrain_dbaas:compute_replicas:expected`,
		"KubeBrainDataPlaneMemoryHigh":          `((max by (namespace, pod, container) (container_memory_working_set_bytes{namespace="kubebrain-system",container="kubebrain",image!=""}) / max by (namespace, pod, container) (kube_pod_container_resource_limits{namespace="kubebrain-system",container="kubebrain",resource="memory",unit="byte"})) > 0.9) or ((max by (namespace, pod, container) (container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}) / max by (namespace, pod, container) (kube_pod_container_resource_limits{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",resource="memory",unit="byte"})) > 0.9)`,
		"KubeBrainDataPlaneCPUThrottlingHigh":   `((max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{namespace="kubebrain-system",container="kubebrain",image!=""}[5m])) / max by (namespace, pod, container) (rate(container_cpu_cfs_periods_total{namespace="kubebrain-system",container="kubebrain",image!=""}[5m]))) > 0.25) or ((max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}[5m])) / max by (namespace, pod, container) (rate(container_cpu_cfs_periods_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}[5m]))) > 0.25)`,
		"KubeBrainNetworkMetricsMissing":        `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:compute_replicas:expected) == 1 or absent(kubebrain_dbaas:network_receive_sources:count) == 1 or absent(kubebrain_dbaas:network_transmit_sources:count) == 1 or absent(kubebrain_dbaas:network_receive_error_sources:count) == 1 or absent(kubebrain_dbaas:network_transmit_error_sources:count) == 1 or absent(kubebrain_dbaas:network_receive_drop_sources:count) == 1 or absent(kubebrain_dbaas:network_transmit_drop_sources:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:network_receive_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_transmit_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_receive_error_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_transmit_error_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_receive_drop_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_transmit_drop_sources:count != on() kubebrain_dbaas:compute_replicas:expected`,
		"KubeBrainNetworkErrors":                `(sum by (namespace, pod, interface) (increase(container_network_receive_errors_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[10m]) + increase(container_network_transmit_errors_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[10m])) > 0) or (sum by (namespace, pod, interface) (increase(container_network_receive_errors_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m]) + increase(container_network_transmit_errors_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m])) > 0)`,
		"KubeBrainNetworkPacketDrops":           `(sum by (namespace, pod, interface) (increase(container_network_receive_packets_dropped_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[10m]) + increase(container_network_transmit_packets_dropped_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[10m])) > 0) or (sum by (namespace, pod, interface) (increase(container_network_receive_packets_dropped_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m]) + increase(container_network_transmit_packets_dropped_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m])) > 0)`,
		"KubeBrainLogicalBackupMetricsMissing":  `count(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}) != 1 or count(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"}) != 1 or count(kubebrain_logical_backup_records{instance="kubebrain"}) != 1 or count(kubebrain_logical_backup_leases{instance="kubebrain"}) != 1 or count(kubebrain_logical_backup_snapshot_revision{instance="kubebrain"}) != 1`,
		"KubeBrainLogicalBackupStale":           `(time() - max(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}) > 90000) or (max(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}) > time() + 300)`,
	} {
		alertRule := prometheusRuleByAlert(t, groups, alert)
		require.Equal(t, expr, alertRule["expr"])
		switch alert {
		case "KubeBrainStorageVolumeMetricsMissing", "KubeBrainResourceMetricsMissing", "KubeBrainNetworkMetricsMissing":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "15m", alertRule["for"])
		case "KubeBrainStorageLatencyMetricsMissing":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "5m", alertRule["for"])
		case "KubeBrainStorageLatencyMetricsStale":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "1m", alertRule["for"])
		case "KubeBrainPDWALFsyncLatencyHigh", "KubeBrainTiKVRaftDBWriteLatencyHigh", "KubeBrainTiKVKVDBWriteLatencyHigh":
			require.Equal(t, "critical", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "1m", alertRule["for"])
		case "KubeBrainLogicalBackupMetricsMissing":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "1h", alertRule["for"])
		case "KubeBrainPDRegionHealthMetricsMissing":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "5m", alertRule["for"])
		case "KubeBrainPDRegionPeerUnhealthy":
			require.Equal(t, "critical", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "2m", alertRule["for"])
		case "KubeBrainDataPlaneCPUThrottlingHigh", "KubeBrainNetworkErrors", "KubeBrainNetworkPacketDrops":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			if alert == "KubeBrainDataPlaneCPUThrottlingHigh" {
				require.Equal(t, "15m", alertRule["for"])
			} else {
				require.Equal(t, "0m", alertRule["for"])
			}
		default:
			require.Equal(t, "critical", alertRule["labels"].(map[string]any)["severity"])
		}
	}
	for _, alert := range []string{
		"KubeBrainStorageVolumeMetricsMissing",
		"KubeBrainStorageVolumeLow",
		"KubeBrainResourceMetricsMissing",
		"KubeBrainDataPlaneMemoryHigh",
		"KubeBrainDataPlaneCPUThrottlingHigh",
		"KubeBrainNetworkMetricsMissing",
		"KubeBrainNetworkErrors",
		"KubeBrainNetworkPacketDrops",
	} {
		expr := prometheusRuleByAlert(t, groups, alert)["expr"].(string)
		require.NotContains(t, expr, "[0-2]", "operational alerts must cover scaled StatefulSet ordinals")
	}
	for _, alert := range []string{"KubeBrainPDInsufficientReplicas", "KubeBrainTiKVInsufficientReplicas"} {
		description := prometheusRuleByAlert(t, groups, alert)["annotations"].(map[string]any)["description"].(string)
		require.Contains(t, description, "production minimum of 3 replicas")
		require.Contains(t, description, "exactly match the current expected replica count")
		require.Contains(t, description, "expectation chain is absent")
	}
	regionPeerRule := prometheusRuleByAlert(t, groups, "KubeBrainPDRegionPeerUnhealthy")
	require.Contains(t,
		regionPeerRule["annotations"].(map[string]any)["description"],
		"validate-tikv-region-health.sh",
	)
	regionMetricsRule := prometheusRuleByAlert(t, groups, "KubeBrainPDRegionHealthMetricsMissing")
	require.Contains(t,
		regionMetricsRule["annotations"].(map[string]any)["description"],
		"two series per current PD StatefulSet replica",
	)
	require.Contains(t,
		regionMetricsRule["annotations"].(map[string]any)["description"],
		"expectation chain is absent",
	)
	latencyStaleRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageLatencyMetricsStale")
	require.Contains(t,
		latencyStaleRule["annotations"].(map[string]any)["description"],
		"newest duplicate sample",
	)
	require.Contains(t,
		latencyStaleRule["annotations"].(map[string]any)["description"],
		"must not create a false stale alert",
	)
	backupMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLogicalBackupMetricsMissing")
	require.Contains(t,
		backupMissingRule["annotations"].(map[string]any)["description"],
		"Exactly one timestamp, artifact-bytes, records, leases, and snapshot-revision series is required",
	)
	require.Contains(t,
		backupMissingRule["annotations"].(map[string]any)["description"],
		"atomically published logical backup metrics file",
	)
	backupStaleRule := prometheusRuleByAlert(t, groups, "KubeBrainLogicalBackupStale")
	require.Contains(t,
		backupStaleRule["annotations"].(map[string]any)["description"],
		"over 5 minutes in the future",
	)
	require.Contains(t,
		backupStaleRule["annotations"].(map[string]any)["description"],
		"must not suppress stale-backup detection",
	)
	networkMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainNetworkMetricsMissing")
	require.Contains(t,
		networkMissingRule["annotations"].(map[string]any)["description"],
		"Deduplicated current KubeBrain/PD/TiKV eth0",
	)
	require.Contains(t,
		networkMissingRule["annotations"].(map[string]any)["description"],
		"must not be interpreted as zero network faults",
	)
	resourceMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainResourceMetricsMissing")
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"throttled/total period",
	)
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"must not be interpreted as zero CPU pressure",
	)
	storageLowRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageVolumeLow")
	require.Contains(t,
		storageLowRule["annotations"].(map[string]any)["description"],
		"most conservative deduplicated available/capacity samples",
	)
	require.Contains(t,
		storageLowRule["annotations"].(map[string]any)["description"],
		"received within 60 seconds",
	)
}

func TestProductionMonitoringProvidesInstanceMetering(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)

	var meteringGroup map[string]any
	for _, rawGroup := range groups {
		group := rawGroup.(map[string]any)
		if group["name"] == "kubebrain.metering" {
			meteringGroup = group
			break
		}
	}
	require.NotNil(t, meteringGroup)
	require.Equal(t, "1m", meteringGroup["interval"])

	expected := map[string]string{
		"kubebrain_dbaas:replica_expectation_sources:count":                  `count(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="kubebrain-system",statefulset="kubebrain"})) + count(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="tidb-cluster",statefulset=~"kb-(pd|tikv)"}))`,
		"kubebrain_dbaas:compute_replicas:expected":                          `(sum(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="kubebrain-system",statefulset="kubebrain"})) or on() vector(0)) + (sum(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="tidb-cluster",statefulset=~"kb-(pd|tikv)"})) or on() vector(0))`,
		"kubebrain_dbaas:kubebrain_replicas:expected":                        `sum(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="kubebrain-system",statefulset="kubebrain"})) or on() vector(0)`,
		"kubebrain_dbaas:storage_replicas:expected":                          `sum(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="tidb-cluster",statefulset=~"kb-(pd|tikv)"})) or on() vector(0)`,
		"kubebrain_dbaas:storage_volumes:expected":                           `count(max by (namespace, persistentvolumeclaim) (kube_persistentvolumeclaim_info{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}))`,
		"kubebrain_dbaas:storage_requested_sources:count":                    `count(max by (namespace, persistentvolumeclaim) (kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}))`,
		"kubebrain_dbaas:pd_replicas:expected":                               `sum(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="tidb-cluster",statefulset="kb-pd"})) or on() vector(0)`,
		"kubebrain_dbaas:tikv_replicas:expected":                             `sum(max by (namespace, statefulset) (kube_statefulset_replicas{namespace="tidb-cluster",statefulset="kb-tikv"})) or on() vector(0)`,
		"kubebrain_dbaas:cpu_usage_cores:max_by_container":                   `max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}[5m]) and (time() - timestamp(container_cpu_usage_seconds_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}) <= 60))`,
		"kubebrain_dbaas:memory_working_set_bytes:max_by_container":          `max by (namespace, pod, container) (container_memory_working_set_bytes{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""} and (time() - timestamp(container_memory_working_set_bytes{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}) <= 60))`,
		"kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface":  `max by (namespace, pod, interface) (rate(container_network_receive_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[5m]) and (time() - timestamp(container_network_receive_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface": `max by (namespace, pod, interface) (rate(container_network_transmit_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[5m]) and (time() - timestamp(container_network_transmit_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:cpu_usage_sources:count":                            `count(kubebrain_dbaas:cpu_usage_cores:max_by_container)`,
		"kubebrain_dbaas:cpu_throttled_period_sources:count":                 `count(max by (namespace, pod, container) (container_cpu_cfs_throttled_periods_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",container="kubebrain",image!=""})) + count(max by (namespace, pod, container) (container_cpu_cfs_throttled_periods_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}))`,
		"kubebrain_dbaas:cpu_period_sources:count":                           `count(max by (namespace, pod, container) (container_cpu_cfs_periods_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",container="kubebrain",image!=""})) + count(max by (namespace, pod, container) (container_cpu_cfs_periods_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}))`,
		"kubebrain_dbaas:memory_working_set_sources:count":                   `count(kubebrain_dbaas:memory_working_set_bytes:max_by_container)`,
		"kubebrain_dbaas:network_receive_sources:count":                      `count(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface)`,
		"kubebrain_dbaas:network_transmit_sources:count":                     `count(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface)`,
		"kubebrain_dbaas:network_receive_error_sources:count":                `count(max by (namespace, pod, interface) (container_network_receive_errors_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"})) + count(max by (namespace, pod, interface) (container_network_receive_errors_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}))`,
		"kubebrain_dbaas:network_transmit_error_sources:count":               `count(max by (namespace, pod, interface) (container_network_transmit_errors_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"})) + count(max by (namespace, pod, interface) (container_network_transmit_errors_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}))`,
		"kubebrain_dbaas:network_receive_drop_sources:count":                 `count(max by (namespace, pod, interface) (container_network_receive_packets_dropped_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"})) + count(max by (namespace, pod, interface) (container_network_receive_packets_dropped_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}))`,
		"kubebrain_dbaas:network_transmit_drop_sources:count":                `count(max by (namespace, pod, interface) (container_network_transmit_packets_dropped_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"})) + count(max by (namespace, pod, interface) (container_network_transmit_packets_dropped_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}))`,
		"kubebrain_dbaas:storage_capacity_bytes:max_by_pvc":                  `max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_available_bytes:max_by_pvc":                 `max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_available_bytes:min_by_pvc":                 `min by (namespace, persistentvolumeclaim) (kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_capacity_sources:count":                     `count(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc)`,
		"kubebrain_dbaas:storage_available_sources:count":                    `count(kubebrain_dbaas:storage_available_bytes:max_by_pvc)`,
		"kubebrain_dbaas:logical_backup_artifact_sources:count":              `count(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"})`,
		"kubebrain_dbaas:logical_backup_timestamp_sources:count":             `count(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"})`,
		"kubebrain_dbaas:object_store_write_request_sources:count":           `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="write",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_list_request_sources:count":            `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="list",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_read_request_sources:count":            `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="read",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_delete_request_sources:count":          `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="delete",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_request_period_end_sources:count":            `count(kubebrain_object_store_request_period_end_seconds{dbaas_instance="kubebrain",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_request_data_complete":                       `(kubebrain_dbaas:object_store_write_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_list_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_read_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_delete_request_sources:count == bool 1) * (kubebrain_dbaas:object_request_period_end_sources:count == bool 1)`,
		"kubebrain_dbaas:metering_data_complete":                             `(kubebrain_dbaas:replica_expectation_sources:count == bool 3) * (kubebrain_dbaas:cpu_usage_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:memory_working_set_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:network_receive_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:network_transmit_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:storage_volumes:expected >= bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:storage_requested_sources:count == bool kubebrain_dbaas:storage_volumes:expected) * (kubebrain_dbaas:storage_capacity_sources:count == bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:storage_available_sources:count == bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:logical_backup_artifact_sources:count == bool 1) * (kubebrain_dbaas:logical_backup_timestamp_sources:count == bool 1)`,
		"kubebrain_dbaas:cpu_usage_cores:sum":                                `sum(kubebrain_dbaas:cpu_usage_cores:max_by_container) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:sum":                       `sum(kubebrain_dbaas:memory_working_set_bytes:max_by_container) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes_per_second:sum":               `sum(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes_per_second:sum":              `sum(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_provisioned_bytes:sum":                      `sum(max by (namespace, persistentvolumeclaim) (kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"})) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_used_bytes:sum":                             `clamp_min(sum(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc) - sum(kubebrain_dbaas:storage_available_bytes:max_by_pvc), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_artifact_bytes:last":                 `max(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"}) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_age_seconds:last":                    `clamp_min(time() - max(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:metering_hour_complete":                             `(min_over_time(kubebrain_dbaas:metering_data_complete[1h]) == 1) * (count_over_time(kubebrain_dbaas:metering_data_complete[1h]) >= bool 60)`,
		"kubebrain_dbaas:object_request_hour_complete":                       `(min_over_time(kubebrain_dbaas:object_request_data_complete[1h]) == 1) * (count_over_time(kubebrain_dbaas:object_request_data_complete[1h]) >= bool 60)`,
		"kubebrain_dbaas:object_request_period_end:last":                     `max(kubebrain_object_store_request_period_end_seconds{dbaas_instance="kubebrain",window="1h"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:cpu_usage_core_seconds:hour": `(sum(max by (namespace, pod, container) (increase(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",container="kubebrain",image!=""}[1h]))) + ` +
			`sum(max by (namespace, pod, container) (increase(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}[1h])))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:hour_avg": `avg_over_time(kubebrain_dbaas:memory_working_set_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes:hour": `(sum(max by (namespace, pod, interface) (increase(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[1h]))) + ` +
			`sum(max by (namespace, pod, interface) (increase(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[1h])))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes:hour": `(sum(max by (namespace, pod, interface) (increase(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[1h]))) + ` +
			`sum(max by (namespace, pod, interface) (increase(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[1h])))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:storage_provisioned_bytes:hour_avg": `avg_over_time(kubebrain_dbaas:storage_provisioned_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:storage_used_bytes:hour_avg":        `avg_over_time(kubebrain_dbaas:storage_used_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_write_requests:hour":   `sum(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="write",window="1h"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_list_requests:hour":    `sum(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="list",window="1h"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_read_requests:hour":    `sum(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="read",window="1h"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_delete_requests:hour":  `sum(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="delete",window="1h"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
	}
	rules, ok := meteringGroup["rules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, len(expected))
	for _, rawRule := range rules {
		recordingRule := rawRule.(map[string]any)
		record, ok := recordingRule["record"].(string)
		require.True(t, ok)
		expr, exists := expected[record]
		require.Truef(t, exists, "unexpected recording rule %q", record)
		require.Equal(t, expr, recordingRule["expr"])
		require.Equal(t, map[string]any{"dbaas_instance": "kubebrain"}, recordingRule["labels"])
		delete(expected, record)
	}
	require.Empty(t, expected)

	incompleteRule := prometheusRuleByAlert(t, groups, "KubeBrainMeteringDataIncomplete")
	require.Equal(t, `(kubebrain_dbaas:metering_data_complete or on() vector(0)) != 1`, incompleteRule["expr"])
	require.Equal(t, "15m", incompleteRule["for"])
	require.Equal(t, "critical", incompleteRule["labels"].(map[string]any)["severity"])
	incompleteDescription := incompleteRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, incompleteDescription, "deduplicated StatefulSet replica expectations")
	require.Contains(t, incompleteDescription, "during incomplete scaling")
	require.Contains(t, incompleteDescription, "recording series itself is absent")
	requestIncomplete := prometheusRuleByAlert(
		t, groups, "KubeBrainObjectRequestMeteringDataIncomplete",
	)
	require.Equal(t, `(kubebrain_dbaas:object_request_data_complete or on() vector(0)) != 1`,
		requestIncomplete["expr"])
	require.Equal(t, "15m", requestIncomplete["for"])
	require.Equal(t, "critical", requestIncomplete["labels"].(map[string]any)["severity"])
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"recording series itself is absent")
}

func TestProductionAlertMetricsExist(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)

	emitted := emittedMetricNames(t, "../../pkg")
	for _, external := range []string{
		"etcd_disk_wal_fsync_duration_seconds_bucket",
		"etcd_disk_wal_fsync_duration_seconds_count",
		"etcd_server_is_leader",
		"container_cpu_cfs_periods_total",
		"container_cpu_cfs_throttled_periods_total",
		"container_cpu_usage_seconds_total",
		"container_memory_working_set_bytes",
		"container_network_receive_bytes_total",
		"container_network_receive_errors_total",
		"container_network_receive_packets_dropped_total",
		"container_network_transmit_bytes_total",
		"container_network_transmit_errors_total",
		"container_network_transmit_packets_dropped_total",
		"grpc_server_handled_total",
		"grpc_server_handling_seconds_bucket",
		"kubebrain_logical_backup_artifact_bytes",
		"kubebrain_logical_backup_last_success_timestamp_seconds",
		"kubebrain_logical_backup_leases",
		"kubebrain_logical_backup_records",
		"kubebrain_logical_backup_snapshot_revision",
		"kubebrain_object_store_request_count",
		"kubebrain_object_store_request_period_end_seconds",
		"kube_pod_container_resource_limits",
		"kube_pod_status_ready",
		"kube_persistentvolumeclaim_info",
		"kube_persistentvolumeclaim_resource_requests_storage_bytes",
		"kube_statefulset_replicas",
		"kube_statefulset_status_replicas_ready",
		"kubelet_volume_stats_available_bytes",
		"kubelet_volume_stats_capacity_bytes",
		"pd_regions_status",
		"tikv_raftstore_leader_missing",
		"tikv_raftstore_store_write_kvdb_duration_seconds_bucket",
		"tikv_raftstore_store_write_kvdb_duration_seconds_count",
		"tikv_raftstore_store_write_raftdb_duration_seconds_bucket",
		"tikv_raftstore_store_write_raftdb_duration_seconds_count",
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
	require.Equal(t, "kb", nestedString(t, cluster, "metadata", "labels", "app.kubernetes.io/instance"))
	require.Equal(t, "v8.5.3", nestedString(t, cluster, "spec", "version"))
	require.Equal(t, "Retain", nestedString(t, cluster, "spec", "pvReclaimPolicy"))
	require.True(t, nestedBool(t, cluster, "spec", "enableDynamicConfiguration"))
	require.Equal(t, "RollingUpdate", nestedString(t, cluster, "spec", "configUpdateStrategy"))
	tikvConfig := nestedString(t, cluster, "spec", "tikv", "config")
	require.Contains(t, tikvConfig, "[storage]")
	require.Contains(t, tikvConfig, "max-key-size = 2621440",
		"the managed TiKV key limit must cover etcd's 1.5MiB request envelope plus physical-key overhead")

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
			require.Equal(t, "kubebrain-dbaas-critical",
				nestedString(t, cluster, "spec", component.name, "priorityClassName"))
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

			preferredAntiAffinity, found, err := unstructured.NestedSlice(
				cluster.Object, "spec", component.name, "affinity", "podAntiAffinity",
				"preferredDuringSchedulingIgnoredDuringExecution",
			)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, preferredAntiAffinity, 1)
			preference := &unstructured.Unstructured{Object: preferredAntiAffinity[0].(map[string]any)}
			require.EqualValues(t, 100, nestedInt64(t, preference, "weight"))
			require.Equal(t, "topology.kubernetes.io/zone",
				nestedString(t, preference, "podAffinityTerm", "topologyKey"))
			require.Equal(t, "tidb-cluster",
				nestedString(t, preference, "podAffinityTerm", "labelSelector", "matchLabels", "app.kubernetes.io/name"))
			require.Equal(t, "kb",
				nestedString(t, preference, "podAffinityTerm", "labelSelector", "matchLabels", "app.kubernetes.io/instance"))
			require.Equal(t, component.name,
				nestedString(t, preference, "podAffinityTerm", "labelSelector", "matchLabels", "app.kubernetes.io/component"))

			pdb := objectByKindAndName(t, objects, "PodDisruptionBudget", "kb-"+component.name)
			require.EqualValues(t, 2, nestedInt64(t, pdb, "spec", "minAvailable"))
			require.Equal(t, "AlwaysAllow", nestedString(t, pdb, "spec", "unhealthyPodEvictionPolicy"))
			require.Equal(t, component.name,
				nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/component"))
			require.Equal(t, "kb",
				nestedString(t, pdb, "spec", "selector", "matchLabels", "app.kubernetes.io/instance"))
		})
	}
	require.Equal(t, "10m", nestedString(t, cluster, "spec", "tikv", "evictLeaderTimeout"))
	require.Equal(t, "tcp", nestedString(t, cluster, "spec", "tikv", "readinessProbe", "type"))
	require.EqualValues(t, 10, nestedInt64(t, cluster, "spec", "tikv", "readinessProbe", "initialDelaySeconds"))
	require.EqualValues(t, 5, nestedInt64(t, cluster, "spec", "tikv", "readinessProbe", "periodSeconds"))

	for _, component := range []struct {
		name       string
		port       int64
		targetPort any
	}{
		{name: "pd", port: 2379, targetPort: "client"},
		{name: "tikv", port: 20180, targetPort: int64(20180)},
	} {
		service := objectByKindAndName(t, objects, "Service", "kb-"+component.name+"-metrics")
		require.Equal(t, "None", nestedString(t, service, "spec", "clusterIP"))
		require.Equal(t, "kb",
			nestedString(t, service, "spec", "selector", "app.kubernetes.io/instance"))
		require.Equal(t, component.name,
			nestedString(t, service, "spec", "selector", "app.kubernetes.io/component"))
		ports, found, err := unstructured.NestedSlice(service.Object, "spec", "ports")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, ports, 1)
		port := ports[0].(map[string]any)
		require.EqualValues(t, component.port, port["port"])
		if targetPort, ok := component.targetPort.(int64); ok {
			require.EqualValues(t, targetPort, port["targetPort"])
		} else {
			require.Equal(t, component.targetPort, port["targetPort"])
		}
	}
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

func TestMeteringArchiveCronJobIsFailClosedAndImmutable(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-metering-archive.yaml")
	job := objectByKindAndName(t, objects, "CronJob", "kubebrain-metering-archive")
	require.Equal(t, "17 * * * *", nestedString(t, job, "spec", "schedule"))
	require.Equal(t, "Etc/UTC", nestedString(t, job, "spec", "timeZone"))
	require.Equal(t, "Forbid", nestedString(t, job, "spec", "concurrencyPolicy"))
	require.EqualValues(t, 900, nestedInt64(t, job, "spec", "startingDeadlineSeconds"))
	require.EqualValues(t, 3, nestedInt64(t, job, "spec", "jobTemplate", "spec", "backoffLimit"))
	require.EqualValues(t, 900, nestedInt64(t, job, "spec", "jobTemplate", "spec", "activeDeadlineSeconds"))
	require.False(t, nestedBool(t, job,
		"spec", "jobTemplate", "spec", "template", "spec", "automountServiceAccountToken"))
	podSpec, found, err := unstructured.NestedMap(
		job.Object, "spec", "jobTemplate", "spec", "template", "spec",
	)
	require.NoError(t, err)
	require.True(t, found)
	pod := &unstructured.Unstructured{Object: podSpec}
	require.EqualValues(t, 65532, nestedInt64(t, pod, "securityContext", "fsGroup"))
	require.Equal(t, "OnRootMismatch",
		nestedString(t, pod, "securityContext", "fsGroupChangePolicy"))

	containers, found, err := unstructured.NestedSlice(
		job.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	command, found, err := unstructured.NestedStringSlice(container.Object, "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-metering-archive"}, command)
	args, found, err := unstructured.NestedStringSlice(container.Object, "args")
	require.NoError(t, err)
	require.True(t, found)
	for _, arg := range args {
		require.False(t, strings.HasPrefix(arg, "--prometheus-url=http://"),
			"metering archive must not use plaintext Prometheus: %s", arg)
	}
	for _, expected := range []string{
		"--prometheus-url=https://prometheus-operated.kubebrain-system.svc.cluster.local:9090",
		"--prometheus-ca-file=/var/run/secrets/kubebrain-prometheus/ca.crt",
		"--prometheus-bearer-token-file=/var/run/secrets/kubebrain-prometheus/token",
		"--prometheus-server-name=prometheus-operated.kubebrain-system.svc.cluster.local",
		"--instance=kubebrain",
		"--retention-mode=COMPLIANCE",
		"--retention-duration=61320h",
		"--slot-duration=1h",
		"--finalization-delay=10m",
		"--max-staleness=5m",
	} {
		require.Contains(t, args, expected)
	}
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	mounts, found, err := unstructured.NestedSlice(container.Object, "volumeMounts")
	require.NoError(t, err)
	require.True(t, found)
	foundPrometheusCredentialsMount := false
	for _, raw := range mounts {
		mount := &unstructured.Unstructured{Object: raw.(map[string]any)}
		if nestedString(t, mount, "name") != "prometheus-credentials" {
			continue
		}
		foundPrometheusCredentialsMount = true
		require.Equal(t, "/var/run/secrets/kubebrain-prometheus",
			nestedString(t, mount, "mountPath"))
		require.True(t, nestedBool(t, mount, "readOnly"))
	}
	require.True(t, foundPrometheusCredentialsMount)

	volumes, found, err := unstructured.NestedSlice(
		job.Object, "spec", "jobTemplate", "spec", "template", "spec", "volumes",
	)
	require.NoError(t, err)
	require.True(t, found)
	foundPrometheusCredentialsVolume := false
	for _, raw := range volumes {
		volume := &unstructured.Unstructured{Object: raw.(map[string]any)}
		if nestedString(t, volume, "name") != "prometheus-credentials" {
			continue
		}
		foundPrometheusCredentialsVolume = true
		require.Equal(t, "kubebrain-metering-archive-prometheus",
			nestedString(t, volume, "secret", "secretName"))
		require.EqualValues(t, 0440, nestedInt64(t, volume, "secret", "defaultMode"))
		items, found, err := unstructured.NestedSlice(volume.Object, "secret", "items")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, items, 2)
		require.Equal(t, "ca.crt", nestedString(t,
			&unstructured.Unstructured{Object: items[0].(map[string]any)}, "key"))
		require.Equal(t, "token", nestedString(t,
			&unstructured.Unstructured{Object: items[1].(map[string]any)}, "key"))
	}
	require.True(t, foundPrometheusCredentialsVolume)
	env, found, err := unstructured.NestedSlice(container.Object, "env")
	require.NoError(t, err)
	require.True(t, found)
	for _, raw := range env {
		entry := &unstructured.Unstructured{Object: raw.(map[string]any)}
		require.Equal(t, "kubebrain-metering-archive-object-store",
			nestedString(t, entry, "valueFrom", "secretKeyRef", "name"))
	}

	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(dockerfile),
		"go build -trimpath -o /src/bin/kubebrain-metering-archive ./hack/production/cmd/metering-archive")
	require.Contains(t, string(dockerfile),
		"COPY --from=build /src/bin/kubebrain-metering-archive /usr/local/bin/kubebrain-metering-archive")
}

func TestMeteringRollupCronJobRequiresCompleteImmutableDay(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-metering-rollup.yaml")
	job := objectByKindAndName(t, objects, "CronJob", "kubebrain-metering-rollup")
	require.Equal(t, "47 0 * * *", nestedString(t, job, "spec", "schedule"))
	require.Equal(t, "Etc/UTC", nestedString(t, job, "spec", "timeZone"))
	require.Equal(t, "Forbid", nestedString(t, job, "spec", "concurrencyPolicy"))
	require.EqualValues(t, 1800, nestedInt64(t, job, "spec", "startingDeadlineSeconds"))
	require.False(t, nestedBool(t, job,
		"spec", "jobTemplate", "spec", "template", "spec", "automountServiceAccountToken"))

	containers, found, err := unstructured.NestedSlice(
		job.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	command, found, err := unstructured.NestedStringSlice(container.Object, "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-metering-rollup"}, command)
	args, found, err := unstructured.NestedStringSlice(container.Object, "args")
	require.NoError(t, err)
	require.True(t, found)
	for _, expected := range []string{
		"--instance=kubebrain",
		"--sample-prefix=metering-samples",
		"--rollup-prefix=metering-rollups",
		"--retention-mode=COMPLIANCE",
		"--retention-duration=61320h",
		"--period-duration=24h",
		"--slot-duration=1h",
	} {
		require.Contains(t, args, expected)
	}
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	env, found, err := unstructured.NestedSlice(container.Object, "env")
	require.NoError(t, err)
	require.True(t, found)
	for _, raw := range env {
		entry := &unstructured.Unstructured{Object: raw.(map[string]any)}
		require.Equal(t, "kubebrain-metering-rollup-object-store",
			nestedString(t, entry, "valueFrom", "secretKeyRef", "name"))
	}
	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(dockerfile),
		"go build -trimpath -o /src/bin/kubebrain-metering-rollup ./hack/production/cmd/metering-rollup")
	require.Contains(t, string(dockerfile),
		"COPY --from=build /src/bin/kubebrain-metering-rollup /usr/local/bin/kubebrain-metering-rollup")
}

func TestMeteringChargeCronJobPinsApprovedImmutablePriceVersion(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-metering-charge.yaml")
	policy := objectByKindAndName(t, objects, "ConfigMap", "kubebrain-metering-price-policy")
	require.Equal(t, "global", nestedString(t, policy, "data", "price-scope"))
	require.Equal(t, "kubebrain.metering-price-catalog.v3",
		nestedString(t, policy, "data", "price-catalog-format"))
	require.Equal(t, "replace-with-approved-version", nestedString(t, policy, "data", "price-version"))

	job := objectByKindAndName(t, objects, "CronJob", "kubebrain-metering-charge")
	require.Equal(t, "17 1 * * *", nestedString(t, job, "spec", "schedule"))
	require.Equal(t, "Etc/UTC", nestedString(t, job, "spec", "timeZone"))
	require.Equal(t, "Forbid", nestedString(t, job, "spec", "concurrencyPolicy"))
	require.EqualValues(t, 1800, nestedInt64(t, job, "spec", "startingDeadlineSeconds"))
	require.False(t, nestedBool(t, job,
		"spec", "jobTemplate", "spec", "template", "spec", "automountServiceAccountToken"))

	containers, found, err := unstructured.NestedSlice(
		job.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	command, found, err := unstructured.NestedStringSlice(container.Object, "command")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-metering-charge"}, command)
	args, found, err := unstructured.NestedStringSlice(container.Object, "args")
	require.NoError(t, err)
	require.True(t, found)
	for _, expected := range []string{
		"--instance=kubebrain",
		"--price-scope=$(PRICE_SCOPE)",
		"--price-version=$(PRICE_VERSION)",
		"--price-catalog-format=$(PRICE_CATALOG_FORMAT)",
		"--rollup-prefix=metering-rollups",
		"--storage-rollup-prefix=metering-storage-rollups",
		"--price-prefix=metering-prices",
		"--charge-prefix=metering-charges",
		"--retention-mode=COMPLIANCE",
		"--retention-duration=61320h",
		"--period-duration=24h",
	} {
		require.Contains(t, args, expected)
	}
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	env, found, err := unstructured.NestedSlice(container.Object, "env")
	require.NoError(t, err)
	require.True(t, found)
	for _, raw := range env {
		entry := &unstructured.Unstructured{Object: raw.(map[string]any)}
		name := nestedString(t, entry, "name")
		if name == "PRICE_SCOPE" || name == "PRICE_VERSION" || name == "PRICE_CATALOG_FORMAT" {
			require.Equal(t, "kubebrain-metering-price-policy",
				nestedString(t, entry, "valueFrom", "configMapKeyRef", "name"))
			continue
		}
		require.Equal(t, "kubebrain-metering-charge-object-store",
			nestedString(t, entry, "valueFrom", "secretKeyRef", "name"))
	}
	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(dockerfile),
		"go build -trimpath -o /src/bin/kubebrain-metering-charge ./hack/production/cmd/metering-charge")
	require.Contains(t, string(dockerfile),
		"COPY --from=build /src/bin/kubebrain-metering-charge /usr/local/bin/kubebrain-metering-charge")
	require.Contains(t, string(dockerfile),
		"go build -trimpath -o /src/bin/kubebrain-metering-price-publish ./hack/production/cmd/metering-price-publish")
	require.Contains(t, string(dockerfile),
		"COPY --from=build /src/bin/kubebrain-metering-price-publish /usr/local/bin/kubebrain-metering-price-publish")
}

func TestObjectStorageMeteringUsesSeparatedSourceAndEvidenceCredentials(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-metering-storage.yaml")
	archive := objectByKindAndName(t, objects, "CronJob", "kubebrain-metering-storage-archive")
	rollup := objectByKindAndName(t, objects, "CronJob", "kubebrain-metering-storage-rollup")
	require.Equal(t, "27 * * * *", nestedString(t, archive, "spec", "schedule"))
	require.Equal(t, "57 0 * * *", nestedString(t, rollup, "spec", "schedule"))
	for _, job := range []*unstructured.Unstructured{archive, rollup} {
		require.Equal(t, "Etc/UTC", nestedString(t, job, "spec", "timeZone"))
		require.Equal(t, "Forbid", nestedString(t, job, "spec", "concurrencyPolicy"))
		require.False(t, nestedBool(t, job,
			"spec", "jobTemplate", "spec", "template", "spec", "automountServiceAccountToken"))
		containerValues, found, err := unstructured.NestedSlice(
			job.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers",
		)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, containerValues, 1)
		container := &unstructured.Unstructured{Object: containerValues[0].(map[string]any)}
		require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
		require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	}
	containers, _, _ := unstructured.NestedSlice(
		archive.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers",
	)
	env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
	secrets := map[string]string{}
	for _, raw := range env {
		entry := &unstructured.Unstructured{Object: raw.(map[string]any)}
		name := nestedString(t, entry, "name")
		if strings.HasPrefix(name, "SOURCE_") && name != "SOURCE_PREFIX" {
			secrets[name] = nestedString(t, entry, "valueFrom", "secretKeyRef", "name")
		}
		if strings.HasPrefix(name, "METERING_") {
			secrets[name] = nestedString(t, entry, "valueFrom", "secretKeyRef", "name")
		}
	}
	require.Equal(t, "kubebrain-metering-storage-source", secrets["SOURCE_AWS_ACCESS_KEY_ID"])
	require.Equal(t, "kubebrain-metering-storage-evidence", secrets["METERING_AWS_ACCESS_KEY_ID"])
	require.NotEqual(t, secrets["SOURCE_AWS_ACCESS_KEY_ID"], secrets["METERING_AWS_ACCESS_KEY_ID"])

	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(dockerfile), "kubebrain-metering-storage-archive")
	require.Contains(t, string(dockerfile), "kubebrain-metering-storage-rollup")
}

func TestMeteringInvoicePinsApprovedPlanAndUsesHardenedIdentity(t *testing.T) {
	objects := decodeManifest(t, "kubebrain-metering-invoice.yaml")
	policy := objectByKindAndName(t, objects, "ConfigMap", "kubebrain-metering-invoice-policy")
	require.Equal(t, "replace-with-approved-invoice-plan",
		nestedString(t, policy, "data", "plan-id"))
	job := objectByKindAndName(t, objects, "CronJob", "kubebrain-metering-invoice")
	require.Equal(t, "17 2 2 * *", nestedString(t, job, "spec", "schedule"))
	require.Equal(t, "Etc/UTC", nestedString(t, job, "spec", "timeZone"))
	require.Equal(t, "Forbid", nestedString(t, job, "spec", "concurrencyPolicy"))
	require.False(t, nestedBool(t, job,
		"spec", "jobTemplate", "spec", "template", "spec", "automountServiceAccountToken"))
	containers, found, err := unstructured.NestedSlice(
		job.Object, "spec", "jobTemplate", "spec", "template", "spec", "containers",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, containers, 1)
	container := &unstructured.Unstructured{Object: containers[0].(map[string]any)}
	require.Equal(t, "/usr/local/bin/kubebrain-metering-invoice-finalize",
		nestedStringSlice(t, container, "command")[0])
	args := nestedStringSlice(t, container, "args")
	require.Contains(t, args, "--plan-id=$(INVOICE_PLAN_ID)")
	require.Contains(t, args, "--charge-prefix=metering-charges")
	require.Contains(t, args, "--adjustment-prefix=metering-adjustments")
	require.Contains(t, args, "--invoice-prefix=metering-invoices")
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
	dockerfile, err := os.ReadFile("../../Dockerfile")
	require.NoError(t, err)
	require.Contains(t, string(dockerfile), "kubebrain-metering-settlement-publish")
	require.Contains(t, string(dockerfile), "kubebrain-metering-invoice-finalize")
	require.Contains(t, string(dockerfile), "kubebrain-metering-payment-ledger")
	require.Contains(t, string(dockerfile), "kubebrain-metering-ledger-export")
	require.Contains(t, string(dockerfile), "kubebrain-metering-invoice-number")
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

func nestedStringSlice(t *testing.T, object *unstructured.Unstructured, fields ...string) []string {
	t.Helper()
	value, found, err := unstructured.NestedStringSlice(object.Object, fields...)
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

func assertReadOnlyTLSSecretVolume(
	t *testing.T,
	pod, container *unstructured.Unstructured,
	volumeName, secretName, mountPath string,
) {
	t.Helper()
	assertReadOnlySecretVolume(t, pod, container, volumeName, secretName, mountPath, 0440,
		map[string]string{"tls.crt": "tls.crt", "tls.key": "tls.key"})
}

func assertReadOnlySecretVolume(
	t *testing.T,
	pod, container *unstructured.Unstructured,
	volumeName, secretName, mountPath string,
	defaultMode int64,
	wantPathsByKey map[string]string,
) {
	t.Helper()
	mounts, found, err := unstructured.NestedSlice(container.Object, "volumeMounts")
	require.NoError(t, err)
	require.True(t, found)
	foundMount := false
	for _, raw := range mounts {
		mount := &unstructured.Unstructured{Object: raw.(map[string]any)}
		if nestedString(t, mount, "name") != volumeName {
			continue
		}
		foundMount = true
		require.Equal(t, mountPath, nestedString(t, mount, "mountPath"))
		require.True(t, nestedBool(t, mount, "readOnly"))
	}
	require.True(t, foundMount)

	volumes, found, err := unstructured.NestedSlice(pod.Object, "volumes")
	require.NoError(t, err)
	require.True(t, found)
	foundVolume := false
	for _, raw := range volumes {
		volume := &unstructured.Unstructured{Object: raw.(map[string]any)}
		if nestedString(t, volume, "name") != volumeName {
			continue
		}
		foundVolume = true
		require.Equal(t, secretName, nestedString(t, volume, "secret", "secretName"))
		require.EqualValues(t, defaultMode, nestedInt64(t, volume, "secret", "defaultMode"))
		items, found, err := unstructured.NestedSlice(volume.Object, "secret", "items")
		require.NoError(t, err)
		require.True(t, found)
		pathsByKey := map[string]string{}
		for _, item := range items {
			itemObject := &unstructured.Unstructured{Object: item.(map[string]any)}
			pathsByKey[nestedString(t, itemObject, "key")] = nestedString(t, itemObject, "path")
		}
		require.Equal(t, wantPathsByKey, pathsByKey)
	}
	require.True(t, foundVolume)
}
