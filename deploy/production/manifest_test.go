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
		`absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:statefulset_ready_sources:count) == 1 or absent(kubebrain_dbaas:kubebrain_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:statefulset_ready_sources:count != 3 or kubebrain_dbaas:kubebrain_replicas:expected < 3 or (sum(kubebrain_dbaas:statefulset_ready_replicas:current{namespace="kubebrain-system",statefulset="kubebrain"}) or on() vector(0)) != on() kubebrain_dbaas:kubebrain_replicas:expected`,
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
		require.Contains(t, description, "negative, fractional, non-finite, or greater-than-2^53")
	}
	require.Contains(t, readinessRule["annotations"].(map[string]any)["description"], "a Ready count exceeds desired")
	require.Contains(t,
		readinessRule["annotations"].(map[string]any)["description"],
		"samples within 60 seconds",
	)

	transactionPathRule := prometheusRuleByAlert(t, groups,
		"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane")
	require.Equal(t,
		`((sum(kubebrain_dbaas:statefulset_ready_replicas:current{namespace="kubebrain-system",statefulset="kubebrain"}) or on() vector(0)) == 0) and on() (kubebrain_dbaas:replica_expectation_sources:count == 3) and on() (kubebrain_dbaas:statefulset_ready_sources:count == 3) and on() (kubebrain_dbaas:pd_replicas:expected >= 3) and on() (kubebrain_dbaas:tikv_replicas:expected >= 3) and on() (count(max by (instance) (up{namespace="tidb-cluster",service="kb-pd-metrics"} == 1)) == on() kubebrain_dbaas:pd_replicas:expected) and on() (count(max by (instance) (up{namespace="tidb-cluster",service="kb-tikv-metrics"} == 1)) == on() kubebrain_dbaas:tikv_replicas:expected) and on() (kubebrain_dbaas:storage_control_plane_invalid_values:count == 0) and on() (sum(kubebrain_dbaas:pd_is_leader:max_by_instance) == 1) and on() (count(kubebrain_dbaas:pd_region_status:max_by_instance_type) == on() (2 * kubebrain_dbaas:pd_replicas:expected)) and on() (max(kubebrain_dbaas:tikv_leader_missing:max_by_instance) == 0) and on() (max(kubebrain_dbaas:pd_region_status:max_by_instance_type) == 0)`,
		transactionPathRule["expr"])
	require.Equal(t, "2m", transactionPathRule["for"])
	require.Equal(t, "critical", transactionPathRule["labels"].(map[string]any)["severity"])
	description := transactionPathRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, description, "end-to-end etcd transaction probe")
	require.Contains(t, description, "complete current PD/TiKV topology")
	require.Contains(t, description, "exactly one leader")
	require.Contains(t, description, "complete Region-peer telemetry")
	require.Contains(t, description, "all leader/Region gauges have valid integer values")
	require.Contains(t, description, "Fresh desired/Ready StatefulSet telemetry")
	flappingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaderElectionFlapping")
	require.Equal(t,
		`sum(kubebrain_dbaas:leader_election_lost:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 3`,
		flappingRule["expr"])
	incompatibleWitness := prometheusRuleByAlert(t, groups, "KubeBrainIncompatibleTransactionWitness")
	require.Equal(t,
		`sum(kubebrain_dbaas:leader_incompatible_witness:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		incompatibleWitness["expr"])
	require.Equal(t, "critical", incompatibleWitness["labels"].(map[string]any)["severity"])
	require.Contains(t, incompatibleWitness["annotations"].(map[string]any)["description"], "Continue roll-forward")
	initializationFailures := prometheusRuleByAlert(t, groups, "KubeBrainLeaderInitializationFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:leader_initialize_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		initializationFailures["expr"])
	require.Equal(t, "critical", initializationFailures["labels"].(map[string]any)["severity"])
	require.Contains(t, initializationFailures["annotations"].(map[string]any)["description"], "keep writes fenced")
	servingInitializationFailures := prometheusRuleByAlert(t, groups, "KubeBrainLeaderServingInitializationFailures")
	require.Equal(t,
		`sum by (stage) (kubebrain_dbaas:leader_serving_initialization_err:increase_10m_by_pod_stage) > 0`,
		servingInitializationFailures["expr"])
	require.Equal(t, "critical", servingInitializationFailures["labels"].(map[string]any)["severity"])
	require.Contains(t, servingInitializationFailures["annotations"].(map[string]any)["description"], "intentionally NotReady")
	require.Contains(t, servingInitializationFailures["annotations"].(map[string]any)["description"], "does not filter on Ready Pod identity")
	servingInitializationMissing := prometheusRuleByAlert(t, groups, "KubeBrainLeaderServingInitializationMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:kubebrain_replicas:expected) == 1 or absent(kubebrain_dbaas:leader_serving_initialization_err_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or count(kubebrain_dbaas:leader_serving_initialization_err:current_by_pod_stage) != on() (5 * kubebrain_dbaas:kubebrain_replicas:expected) or count(kubebrain_dbaas:leader_serving_initialization_err:increase_10m_by_pod_stage) != on() (5 * kubebrain_dbaas:kubebrain_replicas:expected) or kubebrain_dbaas:leader_serving_initialization_err_invalid_values:count != 0`,
		servingInitializationMissing["expr"])
	require.Equal(t, "2m", servingInitializationMissing["for"])
	servingInitializationMissingDescription := servingInitializationMissing["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, servingInitializationMissingDescription, "exactly five per current KubeBrain StatefulSet replica")
	require.Contains(t, servingInitializationMissingDescription, "include a leader that is NotReady")
	invalidAlarm := prometheusRuleByAlert(t, groups, "KubeBrainInvalidCorruptAlarmMetadata")
	require.Equal(t,
		`sum(kubebrain_dbaas:leader_invalid_alarm_metadata:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		invalidAlarm["expr"])
	require.Equal(t, "critical", invalidAlarm["labels"].(map[string]any)["severity"])
	invalidAlarmDescription := invalidAlarm["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, invalidAlarmDescription, "refused leadership")
	require.Contains(t, invalidAlarmDescription, "do not clear or rewrite internal alarm keys")
	leaderMetricsMissing := prometheusRuleByAlert(t, groups, "KubeBrainLeaderElectionMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:leader_election_invalid_values:count) == 1 or absent(kubebrain_dbaas:leader_initialize_err_invalid_values:count) == 1 or count(kubebrain_dbaas:leader_election_lost:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_election_lost:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_initialize_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_initialize_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_incompatible_witness:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_incompatible_witness:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_invalid_alarm_metadata:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:leader_invalid_alarm_metadata:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:leader_election_invalid_values:count != 0 or kubebrain_dbaas:leader_initialize_err_invalid_values:count != 0`,
		leaderMetricsMissing["expr"])
	require.Equal(t, "2m", leaderMetricsMissing["for"])
	leaderMetricsMissingDescription := leaderMetricsMissing["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, leaderMetricsMissingDescription, "all four counters to authoritative zero before campaigning")
	require.Contains(t, leaderMetricsMissingDescription, "Missing, stale, or invalid telemetry")
	alarmRefreshRule := prometheusRuleByAlert(t, groups, "KubeBrainAlarmRefreshFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:alarm_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		alarmRefreshRule["expr"])
	require.Equal(t, "0m", alarmRefreshRule["for"])
	require.Contains(t, alarmRefreshRule["annotations"].(map[string]any)["description"], "retains its last exported gauge snapshot")
	alarmRefreshMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainAlarmRefreshMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:alarm_refresh_err_invalid_values:count) == 1 or count(kubebrain_dbaas:alarm_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:alarm_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:alarm_refresh_err_invalid_values:count != 0`,
		alarmRefreshMissingRule["expr"])
	require.Equal(t, "2m", alarmRefreshMissingRule["for"])
	alarmRefreshMissingDescription := alarmRefreshMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, alarmRefreshMissingDescription, "authoritative zero before refreshing shared alarm state")
	require.Contains(t, alarmRefreshMissingDescription, "Missing, stale, or invalid telemetry")
	corruptAlarmRule := prometheusRuleByAlert(t, groups, "KubeBrainCorruptAlarmActive")
	require.Equal(t,
		`max(kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		corruptAlarmRule["expr"])
	require.Equal(t, "critical", corruptAlarmRule["labels"].(map[string]any)["severity"])
	corruptAlarmDescription := corruptAlarmRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, corruptAlarmDescription, "remain fenced with DataLoss")
	require.Contains(t, corruptAlarmDescription, "AlarmDeactivate after validation succeeds")
	corruptAlarmInconsistentRule := prometheusRuleByAlert(t, groups, "KubeBrainCorruptAlarmMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:alarm_corrupt_active_invalid_values:count) == 1 or count(kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:alarm_corrupt_active_invalid_values:count != 0 or (max(kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) - min(kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0)`,
		corruptAlarmInconsistentRule["expr"])
	require.Equal(t, "1m", corruptAlarmInconsistentRule["for"])
	require.Equal(t, "critical", corruptAlarmInconsistentRule["labels"].(map[string]any)["severity"])
	corruptAlarmInconsistentDescription := corruptAlarmInconsistentRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, corruptAlarmInconsistentDescription, "exactly 0 or 1")
	require.Contains(t, corruptAlarmInconsistentDescription, "cannot be interpreted as safely unfenced")
	authRevisionRefreshRule := prometheusRuleByAlert(t, groups, "KubeBrainAuthRevisionRefreshFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:auth_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		authRevisionRefreshRule["expr"])
	require.Equal(t, "warning", authRevisionRefreshRule["labels"].(map[string]any)["severity"])
	authRevisionRefreshDescription := authRevisionRefreshRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, authRevisionRefreshDescription, "telemetry refresh failure")
	require.Contains(t, authRevisionRefreshDescription, "not proof that request authorization failed")
	authRevisionInconsistentRule := prometheusRuleByAlert(t, groups, "KubeBrainAuthRevisionMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:auth_revision_invalid_values:count) == 1 or count(kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:auth_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:auth_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:auth_revision_invalid_values:count != 0 or (max(kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) - min(kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0)`,
		authRevisionInconsistentRule["expr"])
	require.Equal(t, "2m", authRevisionInconsistentRule["for"])
	require.Equal(t, "warning", authRevisionInconsistentRule["labels"].(map[string]any)["severity"])
	authRevisionInconsistentDescription := authRevisionInconsistentRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, authRevisionInconsistentDescription, "replicas must converge to one revision")
	require.Contains(t, authRevisionInconsistentDescription, "does not by itself prove authorization failure")
	mvccCompactRefreshRule := prometheusRuleByAlert(t, groups, "KubeBrainMVCCCompactRevisionRefreshFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:mvcc_compact_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		mvccCompactRefreshRule["expr"])
	require.Equal(t, "warning", mvccCompactRefreshRule["labels"].(map[string]any)["severity"])
	mvccCompactRefreshDescription := mvccCompactRefreshRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, mvccCompactRefreshDescription, "request path independently reads/enforces compact metadata")
	require.Contains(t, mvccCompactRefreshDescription, "retained gauge")
	mvccRevisionInconsistentRule := prometheusRuleByAlert(t, groups, "KubeBrainMVCCRevisionMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:mvcc_revision_invalid_values:count) == 1 or count(kubebrain_dbaas:mvcc_current_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_compact_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_compact_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:mvcc_revision_invalid_values:count != 0 or (max(kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) - min(kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0)`,
		mvccRevisionInconsistentRule["expr"])
	require.Equal(t, "2m", mvccRevisionInconsistentRule["for"])
	require.Equal(t, "critical", mvccRevisionInconsistentRule["labels"].(map[string]any)["severity"])
	mvccRevisionInconsistentDescription := mvccRevisionInconsistentRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, mvccRevisionInconsistentDescription, "no greater than that Pod's current revision")
	require.Contains(t, mvccRevisionInconsistentDescription, "Current revisions may transiently differ on followers")
	liveKeysRefreshRule := prometheusRuleByAlert(t, groups, "KubeBrainMVCCLiveKeysRefreshFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:mvcc_live_keys_refresh_miss:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		liveKeysRefreshRule["expr"])
	require.Equal(t, "0m", liveKeysRefreshRule["for"])
	require.Contains(t, liveKeysRefreshRule["annotations"].(map[string]any)["description"], "refuses an O(keyspace) fallback")
	liveKeysStalledRule := prometheusRuleByAlert(t, groups, "KubeBrainMVCCLiveKeysRefreshStalled")
	require.Equal(t,
		`(time() - kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod) * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current > 120`,
		liveKeysStalledRule["expr"])
	require.Equal(t, "2m", liveKeysStalledRule["for"])
	require.Contains(t, liveKeysStalledRule["annotations"].(map[string]any)["description"], "Scrape timestamps only prove")
	liveKeysInconsistentRule := prometheusRuleByAlert(t, groups, "KubeBrainMVCCLiveKeysMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:mvcc_live_keys_invalid_values:count) == 1 or count(kubebrain_dbaas:mvcc_live_keys:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_live_keys_refresh_miss:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_live_keys_refresh_miss:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:mvcc_live_keys_invalid_values:count != 0`,
		liveKeysInconsistentRule["expr"])
	require.Equal(t, "2m", liveKeysInconsistentRule["for"])
	require.Equal(t, "critical", liveKeysInconsistentRule["labels"].(map[string]any)["severity"])
	liveKeysDescription := liveKeysInconsistentRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, liveKeysDescription, "does not require equality")
	require.Contains(t, liveKeysDescription, "Concurrent mutations and scrape skew")
	watchCapacityRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCapacityHigh")
	require.Equal(t,
		`max(kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 8000 or max(kubebrain_dbaas:mvcc_watch_streams:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 8000`,
		watchCapacityRule["expr"])
	require.Equal(t, "5m", watchCapacityRule["for"])
	require.Contains(t, watchCapacityRule["annotations"].(map[string]any)["description"], "at most 10000 etcd logical watches")
	slowWatchersRule := prometheusRuleByAlert(t, groups, "KubeBrainSlowWatchersPersistent")
	require.Equal(t,
		`max(kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		slowWatchersRule["expr"])
	require.Equal(t, "5m", slowWatchersRule["for"])
	require.Contains(t, slowWatchersRule["annotations"].(map[string]any)["description"], "brief nonzero value is self-healing flow control")
	watchGaugeMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchGaugeMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:mvcc_watch_gauges_invalid_values:count) == 1 or count(kubebrain_dbaas:mvcc_watch_streams:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:mvcc_watch_gauges_invalid_values:count != 0`,
		watchGaugeMissingRule["expr"])
	require.Equal(t, "2m", watchGaugeMissingRule["for"])
	require.Equal(t, "warning", watchGaugeMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, watchGaugeMissingRule["annotations"].(map[string]any)["description"], "authoritative zero while idle")
	watchDeliveryBlockedRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchEventDeliveryBlocked")
	require.Equal(t,
		`max(kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		watchDeliveryBlockedRule["expr"])
	require.Equal(t, "5m", watchDeliveryBlockedRule["for"])
	require.Contains(t, watchDeliveryBlockedRule["annotations"].(map[string]any)["description"], "unbuffered handoff")
	watchDeliveryMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchEventDeliveryMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:mvcc_watch_event_delivery_invalid_values:count) == 1 or count(kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_delivered_watch_events:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:mvcc_delivered_watch_events:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:mvcc_watch_event_delivery_invalid_values:count != 0`,
		watchDeliveryMissingRule["expr"])
	require.Equal(t, "2m", watchDeliveryMissingRule["for"])
	require.Equal(t, "warning", watchDeliveryMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, watchDeliveryMissingRule["annotations"].(map[string]any)["description"], "authoritative-zero pending")
	serverStreamFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainServerStreamFailures")
	require.Equal(t,
		`sum by (api, failure_type) (kubebrain_dbaas:server_stream_failure:increase_10m_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		serverStreamFailureRule["expr"])
	require.Equal(t, "0m", serverStreamFailureRule["for"])
	require.Equal(t, "warning", serverStreamFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, serverStreamFailureRule["annotations"].(map[string]any)["description"], "non-client-cancellation")
	serverStreamMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainServerStreamFailureMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:server_stream_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:server_stream_failure:current_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:server_stream_failure:increase_10m_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:server_stream_failure_invalid_values:count != 0`,
		serverStreamMissingRule["expr"])
	require.Equal(t, "2m", serverStreamMissingRule["for"])
	require.Equal(t, "warning", serverStreamMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, serverStreamMissingRule["annotations"].(map[string]any)["description"], "all four authoritative-zero")
	require.Contains(t, serverStreamMissingRule["annotations"].(map[string]any)["description"], "unknown label values are invalid")
	watchPrevKVBudgetRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchPrevKVBudgetExhausted")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_prev_kv_budget_exhausted:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		watchPrevKVBudgetRule["expr"])
	require.Equal(t, "0m", watchPrevKVBudgetRule["for"])
	require.Equal(t, "warning", watchPrevKVBudgetRule["labels"].(map[string]any)["severity"])
	require.Contains(t, watchPrevKVBudgetRule["annotations"].(map[string]any)["description"], "uncertain nil PrevKV")
	watchPrevKVMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchPrevKVMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:watch_prev_kv_budget_exhausted_invalid_values:count) == 1 or count(kubebrain_dbaas:watch_prev_kv_budget_exhausted:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_prev_kv_budget_exhausted:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:watch_prev_kv_budget_exhausted_invalid_values:count != 0`,
		watchPrevKVMissingRule["expr"])
	require.Equal(t, "2m", watchPrevKVMissingRule["for"])
	require.Equal(t, "warning", watchPrevKVMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, watchPrevKVMissingRule["annotations"].(map[string]any)["description"], "authoritative-zero")
	knownPeersRule := prometheusRuleByAlert(t, groups, "KubeBrainKnownPeersMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:kubebrain_replicas:expected) == 1 or absent(kubebrain_dbaas:known_peers_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:kubebrain_replicas:expected < 3 or count(kubebrain_dbaas:known_peers:max_by_pod_local_remote * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != on() (count(kubebrain_dbaas:ready_pods:current) * kubebrain_dbaas:kubebrain_replicas:expected) or count((count by (namespace, pod, uid) (kubebrain_dbaas:known_peers:max_by_pod_local_remote * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) != on() group_left() kubebrain_dbaas:kubebrain_replicas:expected) > 0 or count(count by (Local) (kubebrain_dbaas:known_peers:max_by_pod_local_remote * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) != count(kubebrain_dbaas:ready_pods:current) or count(count by (Remote) (kubebrain_dbaas:known_peers:max_by_pod_local_remote * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) != on() kubebrain_dbaas:kubebrain_replicas:expected or kubebrain_dbaas:known_peers_invalid_values:count != 0`,
		knownPeersRule["expr"])
	require.Equal(t, "2m", knownPeersRule["for"])
	require.Equal(t, "critical", knownPeersRule["labels"].(map[string]any)["severity"])
	knownPeersDescription := knownPeersRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, knownPeersDescription, "distinct Local IDs")
	require.Contains(t, knownPeersDescription, "union of Remote IDs")
	serverIdentityRule := prometheusRuleByAlert(t, groups, "KubeBrainServerIdentityMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:server_identity_invalid_values:count) == 1 or absent(kubebrain_dbaas:known_peers_invalid_values:count) == 1 or count(kubebrain_dbaas:server_identity:max_by_pod_local * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:server_identity_invalid_values:count != 0 or kubebrain_dbaas:known_peers_invalid_values:count != 0 or count((kubebrain_dbaas:server_identity:max_by_pod_local * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) unless on(namespace, pod, uid, Local) max by (namespace, pod, uid, Local) (kubebrain_dbaas:known_peers:max_by_pod_local_remote * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) > 0 or count(max by (namespace, pod, uid, Local) (kubebrain_dbaas:known_peers:max_by_pod_local_remote * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) unless on(namespace, pod, uid, Local) (kubebrain_dbaas:server_identity:max_by_pod_local * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) > 0`,
		serverIdentityRule["expr"])
	require.Equal(t, "2m", serverIdentityRule["for"])
	require.Equal(t, "critical", serverIdentityRule["labels"].(map[string]any)["severity"])
	serverIdentityDescription := serverIdentityRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, serverIdentityDescription, "exactly equal")
	require.Contains(t, serverIdentityDescription, "in both directions")
	leaderStateRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaderStateUnavailable")
	require.Equal(t,
		`sum(kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 1 or min(kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 1`,
		leaderStateRule["expr"])
	require.Equal(t, "1m", leaderStateRule["for"])
	require.Equal(t, "critical", leaderStateRule["labels"].(map[string]any)["severity"])
	leaderChangesRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaderChangesHigh")
	require.Equal(t,
		`sum(kubebrain_dbaas:server_leader_changes:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 3`,
		leaderChangesRule["expr"])
	require.Equal(t, "0m", leaderChangesRule["for"])
	serverStateRule := prometheusRuleByAlert(t, groups, "KubeBrainServerStateMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:server_state_invalid_values:count) == 1 or count(kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:server_is_learner:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:server_leader_changes:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:server_leader_changes:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:server_state_invalid_values:count != 0`,
		serverStateRule["expr"])
	require.Equal(t, "2m", serverStateRule["for"])
	require.Equal(t, "critical", serverStateRule["labels"].(map[string]any)["severity"])
	serverStateDescription := serverStateRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, serverStateDescription, "leader must know a leader")
	require.Contains(t, serverStateDescription, "cannot be a learner")
	fdUsageHighRule := prometheusRuleByAlert(t, groups, "KubeBrainFileDescriptorUsageHigh")
	require.Equal(t,
		`max((kubebrain_dbaas:fd_used:max_by_pod / on(namespace, pod, uid) kubebrain_dbaas:fd_limit:max_by_pod) * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0.8`,
		fdUsageHighRule["expr"])
	require.Equal(t, "5m", fdUsageHighRule["for"])
	require.Equal(t, "warning", fdUsageHighRule["labels"].(map[string]any)["severity"])
	fdRefreshFailuresRule := prometheusRuleByAlert(t, groups, "KubeBrainFileDescriptorRefreshFailures")
	require.Equal(t,
		`sum by (type) (kubebrain_dbaas:fd_refresh_err:increase_30m_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		fdRefreshFailuresRule["expr"])
	require.Equal(t, "0m", fdRefreshFailuresRule["for"])
	fdRefreshStalledRule := prometheusRuleByAlert(t, groups, "KubeBrainFileDescriptorRefreshStalled")
	require.Equal(t,
		`(time() - kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type) * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current > 1200`,
		fdRefreshStalledRule["expr"])
	require.Equal(t, "2m", fdRefreshStalledRule["for"])
	require.Contains(t, fdRefreshStalledRule["annotations"].(map[string]any)["description"], "Scrape timestamps only show")
	fdInconsistentRule := prometheusRuleByAlert(t, groups, "KubeBrainFileDescriptorMetricsInconsistent")
	require.Equal(t,
		`absent(kubebrain_dbaas:fd_invalid_values:count) == 1 or count(kubebrain_dbaas:fd_used:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:fd_limit:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:fd_refresh_err:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:fd_refresh_err:increase_30m_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:fd_invalid_values:count != 0`,
		fdInconsistentRule["expr"])
	require.Equal(t, "2m", fdInconsistentRule["for"])
	require.Equal(t, "critical", fdInconsistentRule["labels"].(map[string]any)["severity"])
	require.Contains(t, fdInconsistentRule["annotations"].(map[string]any)["description"], "exactly one used and one limit")
	require.Contains(t, description, "same-PVC TiKV repair")
	require.Contains(t, description, "no pending/down peer Regions")

	revisionLagRule := prometheusRuleByAlert(t, groups, "KubeBrainRevisionLagHigh")
	require.Equal(t,
		`max(kubebrain_dbaas:watch_revision_lag:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 10000`,
		revisionLagRule["expr"])
	require.Equal(t, "5m", revisionLagRule["for"])
	require.Equal(t, "warning", revisionLagRule["labels"].(map[string]any)["severity"])
	revisionLagDescription := revisionLagRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, revisionLagDescription, "samples no older than 60 seconds")
	require.Contains(t, revisionLagDescription, "deduplicated by Pod UID")
	require.Contains(t, revisionLagDescription, "stale replaced Pods")

	revisionLagMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainRevisionLagMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:watch_revision_lag_invalid_values:count) == 1 or count(kubebrain_dbaas:watch_revision_lag:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:watch_revision_lag_invalid_values:count != 0`,
		revisionLagMissingRule["expr"])
	require.Equal(t, "2m", revisionLagMissingRule["for"])
	require.Equal(t, "warning", revisionLagMissingRule["labels"].(map[string]any)["severity"])
	revisionLagMissingDescription := revisionLagMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, revisionLagMissingDescription, "refreshes the gauge every 15 seconds")
	require.Contains(t, revisionLagMissingDescription, "authoritative zero while idle")
	require.Contains(t, revisionLagMissingDescription, "cannot be interpreted as an empty watch backlog")
	require.Contains(t, revisionLagMissingDescription, "NaN, infinite, negative, fractional, or above 2^53")

	watchEventDropsRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchEventDrops")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_event_buffer_stale_drop:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		watchEventDropsRule["expr"])
	watchBufferFullRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchBufferFull")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_event_buffer_full:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		watchBufferFullRule["expr"])
	watchBufferMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchEventBufferMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:watch_event_buffer_invalid_values:count) == 1 or count(kubebrain_dbaas:watch_event_buffer_stale_drop:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_event_buffer_stale_drop:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_event_buffer_full:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_event_buffer_full:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:watch_event_buffer_invalid_values:count != 0`,
		watchBufferMissingRule["expr"])
	require.Equal(t, "2m", watchBufferMissingRule["for"])
	require.Equal(t, "critical", watchBufferMissingRule["labels"].(map[string]any)["severity"])
	watchBufferMissingDescription := watchBufferMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, watchBufferMissingDescription, "authoritative zero")
	require.Contains(t, watchBufferMissingDescription, "Missing, stale, or invalid telemetry")
	require.Contains(t, watchBufferMissingDescription, "exact non-negative integers")

	collectorStalledRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCollectorStalled")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_collector_stalled:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		collectorStalledRule["expr"])
	require.Equal(t, "warning", collectorStalledRule["labels"].(map[string]any)["severity"])
	require.Contains(t, collectorStalledRule["annotations"].(map[string]any)["description"], "between revision allocation and event publication")
	collectorSkippedRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCollectorSkippedRevision")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_collector_skipped_revision:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		collectorSkippedRule["expr"])
	require.Equal(t, "critical", collectorSkippedRule["labels"].(map[string]any)["severity"])
	require.Contains(t, collectorSkippedRule["annotations"].(map[string]any)["description"], "durable revision continuity")
	collectorReplayRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCollectorDurableReplay")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_collector_recovery:increase_10m_by_pod_outcome{outcome="replayed"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		collectorReplayRule["expr"])
	require.Equal(t, "warning", collectorReplayRule["labels"].(map[string]any)["severity"])
	require.Contains(t, collectorReplayRule["annotations"].(map[string]any)["description"], "Watch continuity was preserved")
	collectorRecoveryFailedRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCollectorRecoveryFailed")
	require.Equal(t,
		`sum(kubebrain_dbaas:watch_collector_recovery:increase_10m_by_pod_outcome{outcome="failed"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		collectorRecoveryFailedRule["expr"])
	require.Equal(t, "critical", collectorRecoveryFailedRule["labels"].(map[string]any)["severity"])
	require.Contains(t, collectorRecoveryFailedRule["annotations"].(map[string]any)["description"], "did not skip it")
	collectorRecoveryMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCollectorRecoveryMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:watch_collector_recovery_invalid_values:count) == 1 or count(kubebrain_dbaas:watch_collector_recovery:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_collector_recovery:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:watch_collector_recovery_invalid_values:count != 0`,
		collectorRecoveryMissingRule["expr"])
	require.Equal(t, "critical", collectorRecoveryMissingRule["labels"].(map[string]any)["severity"])
	watchBackendIntegrityRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchBackendIntegrityFailures")
	require.Equal(t,
		`sum by (kind) (kubebrain_dbaas:watch_backend_integrity_failure:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		watchBackendIntegrityRule["expr"])
	require.Equal(t, "critical", watchBackendIntegrityRule["labels"].(map[string]any)["severity"])
	require.Contains(t, watchBackendIntegrityRule["annotations"].(map[string]any)["description"], "before publication")
	watchBackendIntegrityMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchBackendIntegrityMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:watch_backend_integrity_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:watch_backend_integrity_failure:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_backend_integrity_failure:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:watch_backend_integrity_failure_invalid_values:count != 0`,
		watchBackendIntegrityMissingRule["expr"])
	require.Equal(t, "2m", watchBackendIntegrityMissingRule["for"])
	require.Equal(t, "critical", watchBackendIntegrityMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, watchBackendIntegrityMissingRule["annotations"].(map[string]any)["description"], "both authoritative-zero kind values")
	collectorMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainWatchCollectorMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:watch_collector_invalid_values:count) == 1 or count(kubebrain_dbaas:watch_collector_stalled:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_collector_stalled:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_collector_skipped_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:watch_collector_skipped_revision:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:watch_collector_invalid_values:count != 0`,
		collectorMissingRule["expr"])
	require.Equal(t, "2m", collectorMissingRule["for"])
	require.Equal(t, "critical", collectorMissingRule["labels"].(map[string]any)["severity"])
	collectorMissingDescription := collectorMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, collectorMissingDescription, "authoritative zero")
	require.Contains(t, collectorMissingDescription, "every allocated revision was published or safely resolved")

	durableRevisionFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainDurableRevisionPersistenceFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:durable_revision_persist_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		durableRevisionFailureRule["expr"])
	require.Equal(t, "critical", durableRevisionFailureRule["labels"].(map[string]any)["severity"])
	durableRevisionFailureDescription := durableRevisionFailureRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, durableRevisionFailureDescription, "older safe snapshot")
	require.Contains(t, durableRevisionFailureDescription, "blocks collector progress")
	durableRevisionMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainDurableRevisionPersistenceMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:durable_revision_persist_err_invalid_values:count) == 1 or count(kubebrain_dbaas:durable_revision_persist_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:durable_revision_persist_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:durable_revision_persist_err_invalid_values:count != 0`,
		durableRevisionMissingRule["expr"])
	require.Equal(t, "2m", durableRevisionMissingRule["for"])
	require.Equal(t, "critical", durableRevisionMissingRule["labels"].(map[string]any)["severity"])
	durableRevisionMissingDescription := durableRevisionMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, durableRevisionMissingDescription, "authoritative zero")
	require.Contains(t, durableRevisionMissingDescription, "cannot prove durable watermark")

	eventLogCorruptionRule := prometheusRuleByAlert(t, groups, "KubeBrainEventLogCorruption")
	require.Equal(t,
		`sum by (kind) (kubebrain_dbaas:event_log_corruption:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		eventLogCorruptionRule["expr"])
	require.Equal(t, "critical", eventLogCorruptionRule["labels"].(map[string]any)["severity"])
	eventLogCorruptionDescription := eventLogCorruptionRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, eventLogCorruptionDescription, "after rechecking cleanup and compaction watermarks")
	require.Contains(t, eventLogCorruptionDescription, "keep writes fenced")
	eventLogAlarmFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainEventLogCorruptAlarmPersistenceFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:event_log_corrupt_alarm_failed:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		eventLogAlarmFailureRule["expr"])
	require.Equal(t, "critical", eventLogAlarmFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, eventLogAlarmFailureRule["annotations"].(map[string]any)["description"], "Treat the instance as corrupt")
	eventLogIntegrityMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainEventLogIntegrityMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:event_log_integrity_invalid_values:count) == 1 or count(kubebrain_dbaas:event_log_corruption:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:event_log_corruption:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:event_log_corrupt_alarm_failed:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:event_log_corrupt_alarm_failed:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:event_log_integrity_invalid_values:count != 0`,
		eventLogIntegrityMissingRule["expr"])
	require.Equal(t, "2m", eventLogIntegrityMissingRule["for"])
	require.Equal(t, "critical", eventLogIntegrityMissingRule["labels"].(map[string]any)["severity"])
	eventLogIntegrityMissingDescription := eventLogIntegrityMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, eventLogIntegrityMissingDescription, "exactly four confirmed-corruption kinds")
	require.Contains(t, eventLogIntegrityMissingDescription, "durable safety fencing")
	readIntegrityFenceRule := prometheusRuleByAlert(t, groups, "KubeBrainReadIntegrityFenceEvents")
	require.Equal(t,
		`sum by (target, outcome) (kubebrain_dbaas:read_integrity_fence:increase_10m_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		readIntegrityFenceRule["expr"])
	require.Equal(t, "critical", readIntegrityFenceRule["labels"].(map[string]any)["severity"])
	require.Contains(t, readIntegrityFenceRule["annotations"].(map[string]any)["description"], "even if AlarmList is empty")
	readIntegrityFenceMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainReadIntegrityFenceMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:read_integrity_fence_invalid_values:count) == 1 or count(kubebrain_dbaas:read_integrity_fence:current_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:read_integrity_fence:increase_10m_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:read_integrity_fence_invalid_values:count != 0`,
		readIntegrityFenceMissingRule["expr"])
	require.Equal(t, "critical", readIntegrityFenceMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, readIntegrityFenceMissingRule["annotations"].(map[string]any)["description"], "four authoritative-zero target/outcome pairs")
	commitWaitBackstopRule := prometheusRuleByAlert(t, groups, "KubeBrainCommitWaitBackstop")
	require.Equal(t,
		`sum(kubebrain_dbaas:commit_wait_failure:increase_10m_by_pod_reason{reason="backstop"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		commitWaitBackstopRule["expr"])
	require.Equal(t, "critical", commitWaitBackstopRule["labels"].(map[string]any)["severity"])
	require.Contains(t, commitWaitBackstopRule["annotations"].(map[string]any)["description"], "outcome-unknown etcd timeout")
	commitWaitContextRule := prometheusRuleByAlert(t, groups, "KubeBrainCommitWaitContextCanceled")
	require.Equal(t,
		`sum(kubebrain_dbaas:commit_wait_failure:increase_10m_by_pod_reason{reason="context_done"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		commitWaitContextRule["expr"])
	require.Equal(t, "warning", commitWaitContextRule["labels"].(map[string]any)["severity"])
	commitWaitMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainCommitWaitFailureMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:commit_wait_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:commit_wait_failure:current_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:commit_wait_failure:increase_10m_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:commit_wait_failure_invalid_values:count != 0`,
		commitWaitMissingRule["expr"])
	require.Equal(t, "critical", commitWaitMissingRule["labels"].(map[string]any)["severity"])

	storageGCUnavailableRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageGCDriverUnavailable")
	require.Equal(t,
		`min(kubebrain_dbaas:storage_gc_enabled:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 1`,
		storageGCUnavailableRule["expr"])
	require.Equal(t, "critical", storageGCUnavailableRule["labels"].(map[string]any)["severity"])
	require.Contains(t, storageGCUnavailableRule["annotations"].(map[string]any)["description"], "no TiDB gc_worker fallback")
	storageGCFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageGCFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:storage_gc_err:increase_30m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		storageGCFailureRule["expr"])
	require.Equal(t, "warning", storageGCFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, storageGCFailureRule["annotations"].(map[string]any)["description"], "progressively degrade read latency")
	storageGCMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageGCMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:storage_gc_invalid_values:count) == 1 or count(kubebrain_dbaas:storage_gc_enabled:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:storage_gc_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:storage_gc_err:increase_30m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:storage_gc_invalid_values:count != 0`,
		storageGCMissingRule["expr"])
	require.Equal(t, "critical", storageGCMissingRule["labels"].(map[string]any)["severity"])
	storageGCMissingDescription := storageGCMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, storageGCMissingDescription, "authoritative-zero")
	require.Contains(t, storageGCMissingDescription, "bare TiKV MVCC garbage collection")
	storageGCStalledRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageGCProgressStalled")
	require.Equal(t,
		`(max(kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) == 0 and max(time() - (kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) > 1200) or (max(kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0 and time() - max(kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1200)`,
		storageGCStalledRule["expr"])
	require.Equal(t, "2m", storageGCStalledRule["for"])
	require.Equal(t, "warning", storageGCStalledRule["labels"].(map[string]any)["severity"])
	storageGCStalledDescription := storageGCStalledRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, storageGCStalledDescription, "initial driver-start grace period")
	require.Contains(t, storageGCStalledDescription, "default driver interval is 10 minutes")
	storageGCProgressMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageGCProgressMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:storage_gc_progress_invalid_values:count) == 1 or count(kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:storage_gc_progress_invalid_values:count != 0`,
		storageGCProgressMissingRule["expr"])
	require.Equal(t, "critical", storageGCProgressMissingRule["labels"].(map[string]any)["severity"])
	storageGCProgressMissingDescription := storageGCProgressMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, storageGCProgressMissingDescription, "authoritative zero on followers")
	require.Contains(t, storageGCProgressMissingDescription, "cannot prove safepoint progress")
	storageCompactionFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageCompactionFailures")
	require.Equal(t,
		`sum by (stage) (kubebrain_dbaas:storage_compaction_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		storageCompactionFailureRule["expr"])
	require.Equal(t, "warning", storageCompactionFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, storageCompactionFailureRule["annotations"].(map[string]any)["description"], "physical MVCC garbage")
	storageCompactionMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageCompactionFailureMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:storage_compaction_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:storage_compaction_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 6 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:storage_compaction_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 6 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:storage_compaction_failure_invalid_values:count != 0`,
		storageCompactionMissingRule["expr"])
	require.Equal(t, "critical", storageCompactionMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, storageCompactionMissingRule["annotations"].(map[string]any)["description"], "all six authoritative-zero stages")

	uncertainTxnRule := prometheusRuleByAlert(t, groups, "KubeBrainUncertainTransactionResolution")
	require.Equal(t,
		`sum by (outcome) (kubebrain_dbaas:uncertain_txn_resolution:increase_10m_by_pod_outcome{outcome=~"retry|committed|not_committed"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		uncertainTxnRule["expr"])
	require.Equal(t, "warning", uncertainTxnRule["labels"].(map[string]any)["severity"])
	uncertainTxnDescription := uncertainTxnRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, uncertainTxnDescription, "durable event-marker conclusion")
	require.Contains(t, uncertainTxnDescription, "without a terminal outcome")
	uncertainTxnCorruptionRule := prometheusRuleByAlert(t, groups, "KubeBrainUncertainTransactionCorruption")
	require.Equal(t,
		`sum by (outcome) (kubebrain_dbaas:uncertain_txn_resolution:increase_10m_by_pod_outcome{outcome=~"witness_corrupt|corrupt_alarm_failed"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		uncertainTxnCorruptionRule["expr"])
	require.Equal(t, "critical", uncertainTxnCorruptionRule["labels"].(map[string]any)["severity"])
	uncertainTxnCorruptionDescription := uncertainTxnCorruptionRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, uncertainTxnCorruptionDescription, "contradict TiKV transaction evidence")
	require.Contains(t, uncertainTxnCorruptionDescription, "treat corrupt_alarm_failed as corrupt")
	uncertainTxnMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainUncertainTransactionMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:uncertain_txn_resolution_invalid_values:count) == 1 or count(kubebrain_dbaas:uncertain_txn_resolution:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 5 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:uncertain_txn_resolution:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 5 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:uncertain_txn_resolution_invalid_values:count != 0`,
		uncertainTxnMissingRule["expr"])
	require.Equal(t, "critical", uncertainTxnMissingRule["labels"].(map[string]any)["severity"])
	uncertainTxnMissingDescription := uncertainTxnMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, uncertainTxnMissingDescription, "exactly five outcome counters")
	require.Contains(t, uncertainTxnMissingDescription, "cannot prove uncertain commit resolution")
	restartWitnessRule := prometheusRuleByAlert(t, groups, "KubeBrainRestartWitnessCorruption")
	require.Equal(t,
		`sum by (outcome) (kubebrain_dbaas:restart_witness_corruption:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		restartWitnessRule["expr"])
	require.Equal(t, "critical", restartWitnessRule["labels"].(map[string]any)["severity"])
	require.Contains(t, restartWitnessRule["annotations"].(map[string]any)["description"], "AlarmList is empty")
	restartWitnessMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainRestartWitnessCorruptionMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:restart_witness_corruption_invalid_values:count) == 1 or count(kubebrain_dbaas:restart_witness_corruption:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:restart_witness_corruption:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:restart_witness_corruption_invalid_values:count != 0`,
		restartWitnessMissingRule["expr"])
	require.Equal(t, "2m", restartWitnessMissingRule["for"])
	require.Equal(t, "critical", restartWitnessMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, restartWitnessMissingRule["annotations"].(map[string]any)["description"], "armed and failed outcomes")
	leaseReconcileRetryRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseUncertainReconcileRetries")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_uncertain_reconcile:increase_10m_by_pod_outcome{outcome="retry"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseReconcileRetryRule["expr"])
	require.Equal(t, "warning", leaseReconcileRetryRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseReconcileRetryRule["annotations"].(map[string]any)["description"], "confirm a later success outcome")
	leaseReconcileErrorRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseUncertainReconcileErrors")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_uncertain_reconcile:increase_10m_by_pod_outcome{outcome="err"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseReconcileErrorRule["expr"])
	require.Equal(t, "critical", leaseReconcileErrorRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseReconcileErrorRule["annotations"].(map[string]any)["description"], "Reconciliation terminates")
	leaseReconcileMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseUncertainReconcileMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_uncertain_reconcile_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_uncertain_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_uncertain_reconcile:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_uncertain_reconcile_invalid_values:count != 0`,
		leaseReconcileMissingRule["expr"])
	require.Equal(t, "critical", leaseReconcileMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseReconcileMissingRule["annotations"].(map[string]any)["description"], "retry, success, and err")
	leaseLifecycleMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseLifecycleMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_lifecycle_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_lifecycle:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_lifecycle:increase_10m_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 4 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_lifecycle_invalid_values:count != 0`,
		leaseLifecycleMissingRule["expr"])
	require.Equal(t, "2m", leaseLifecycleMissingRule["for"])
	require.Equal(t, "warning", leaseLifecycleMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseLifecycleMissingRule["annotations"].(map[string]any)["description"], "grant, revoke, renew, and expire")
	require.Contains(t, leaseLifecycleMissingRule["annotations"].(map[string]any)["description"], "do not infer a local lease balance")
	leaseStartupRestoreFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseStartupRestoreFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_startup_restore_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseStartupRestoreFailureRule["expr"])
	require.Equal(t, "warning", leaseStartupRestoreFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseStartupRestoreFailureRule["annotations"].(map[string]any)["description"], "not a fail-open event")
	leaseStartupRestoreMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseStartupRestoreMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_startup_restore_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_startup_restore_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_startup_restore_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_startup_restore_failure_invalid_values:count != 0`,
		leaseStartupRestoreMissingRule["expr"])
	require.Equal(t, "warning", leaseStartupRestoreMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseStartupRestoreMissingRule["annotations"].(map[string]any)["description"], "authoritative-zero startup restore failure counter")
	leaseCheckpointFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseCheckpointFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation{operation="checkpoint"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseCheckpointFailureRule["expr"])
	require.Equal(t, "warning", leaseCheckpointFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseCheckpointFailureRule["annotations"].(map[string]any)["description"], "lease is retained")
	leaseExpiryDeleteFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseExpiryDeleteFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation{operation="expire_delete"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseExpiryDeleteFailureRule["expr"])
	require.Equal(t, "critical", leaseExpiryDeleteFailureRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseExpiryDeleteFailureRule["annotations"].(map[string]any)["description"], "surviving keys are deliberately retained")
	leaseExpiryCorruptDeferredRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseExpiryCorruptDeferred")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation{operation="expire_corrupt_deferred"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseExpiryCorruptDeferredRule["expr"])
	require.Equal(t, "critical", leaseExpiryCorruptDeferredRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseExpiryCorruptDeferredRule["annotations"].(map[string]any)["description"], "expired data remains publicly visible")
	leaseBackgroundFailureMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseBackgroundFailureMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_background_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_background_failure:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_background_failure_invalid_values:count != 0`,
		leaseBackgroundFailureMissingRule["expr"])
	require.Equal(t, "critical", leaseBackgroundFailureMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseBackgroundFailureMissingRule["annotations"].(map[string]any)["description"], "checkpoint, expire_delete, and expire_corrupt_deferred")
	leaseOrphanBlockingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseOrphanSweepBlockingFailures")
	require.Equal(t,
		`sum by (stage) (kubebrain_dbaas:lease_orphan_sweep_failure:increase_10m_by_pod_stage{stage=~"load|user_read|legacy_attachment_read|legacy_attachment_invalid|key_delete"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseOrphanBlockingRule["expr"])
	require.Equal(t, "critical", leaseOrphanBlockingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseOrphanBlockingRule["annotations"].(map[string]any)["description"], "defunct lease alive")
	leaseOrphanMaintenanceRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseOrphanSweepMaintenanceFailures")
	require.Equal(t,
		`sum by (stage) (kubebrain_dbaas:lease_orphan_sweep_failure:increase_10m_by_pod_stage{stage=~"migration|seal|key_compare|attachment_delete"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseOrphanMaintenanceRule["expr"])
	require.Equal(t, "warning", leaseOrphanMaintenanceRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseOrphanMaintenanceRule["annotations"].(map[string]any)["description"], "never guessed or deleted")
	leaseOrphanMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseOrphanSweepFailureMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_orphan_sweep_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_orphan_sweep_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 9 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_orphan_sweep_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 9 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_orphan_sweep_failure_invalid_values:count != 0`,
		leaseOrphanMissingRule["expr"])
	require.Equal(t, "critical", leaseOrphanMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseOrphanMissingRule["annotations"].(map[string]any)["description"], "all nine authoritative-zero")
	leaseGrantCleanupRetryRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseGrantCleanupRetries")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_grant_cleanup:increase_10m_by_pod_outcome{outcome="retry"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseGrantCleanupRetryRule["expr"])
	require.Equal(t, "warning", leaseGrantCleanupRetryRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseGrantCleanupRetryRule["annotations"].(map[string]any)["description"], "ID remains reserved")
	leaseGrantCleanupMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseGrantCleanupMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_grant_cleanup_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_grant_cleanup:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_grant_cleanup:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_grant_cleanup_invalid_values:count != 0`,
		leaseGrantCleanupMissingRule["expr"])
	require.Equal(t, "critical", leaseGrantCleanupMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseGrantCleanupMissingRule["annotations"].(map[string]any)["description"], "retry, success, and handoff")
	leaseRevokeRetryRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseRevokeReconcileRetries")
	require.Equal(t,
		`sum(kubebrain_dbaas:lease_revoke_reconcile:increase_10m_by_pod_outcome{outcome="retry"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		leaseRevokeRetryRule["expr"])
	require.Equal(t, "warning", leaseRevokeRetryRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseRevokeRetryRule["annotations"].(map[string]any)["description"], "fail closed")
	leaseRevokeMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainLeaseRevokeReconcileMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:lease_revoke_reconcile_invalid_values:count) == 1 or count(kubebrain_dbaas:lease_revoke_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:lease_revoke_reconcile:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:lease_revoke_reconcile_invalid_values:count != 0`,
		leaseRevokeMissingRule["expr"])
	require.Equal(t, "critical", leaseRevokeMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, leaseRevokeMissingRule["annotations"].(map[string]any)["description"], "uncertain-revoke convergence")

	overflowRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexOverflowed")
	require.Equal(t, `max(kubebrain_dbaas:count_index_overflowed:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`, overflowRule["expr"])
	require.Equal(t, "1m", overflowRule["for"])
	require.Equal(t, "warning", overflowRule["labels"].(map[string]any)["severity"])
	overflowDescription := overflowRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, overflowDescription, "samples no older than 60 seconds")
	require.Contains(t, overflowDescription, "Stale replaced Pod samples are excluded")

	countIndexMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:count_index_invalid_values:count) == 1 or count(kubebrain_dbaas:count_index_overflowed:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:count_index_invalid_values:count != 0`,
		countIndexMissingRule["expr"])
	require.Equal(t, "2m", countIndexMissingRule["for"])
	require.Equal(t, "warning", countIndexMissingRule["labels"].(map[string]any)["severity"])
	countIndexMissingDescription := countIndexMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, countIndexMissingDescription, "every 15 seconds")
	require.Contains(t, countIndexMissingDescription, "cannot be interpreted as a healthy non-overflowed index")
	require.Contains(t, countIndexMissingDescription, "mixed binary/configuration rollout")
	require.Contains(t, countIndexMissingDescription, "not exactly 0 or 1")

	rebuildRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexRebuildFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:count_index_rebuild_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		rebuildRule["expr"])
	require.Equal(t, "0m", rebuildRule["for"])
	rebuildMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainCountIndexRebuildMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:count_index_rebuild_err_invalid_values:count) == 1 or count(kubebrain_dbaas:count_index_rebuild_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:count_index_rebuild_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:count_index_rebuild_err_invalid_values:count != 0`,
		rebuildMissingRule["expr"])
	require.Equal(t, "2m", rebuildMissingRule["for"])
	rebuildMissingDescription := rebuildMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, rebuildMissingDescription, "authoritative zero before leadership")
	require.Contains(t, rebuildMissingDescription, "Missing, stale, or invalid telemetry")

	rangeStreamRule := prometheusRuleByAlert(t, groups, "KubeBrainRangeStreamFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:range_stream_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		rangeStreamRule["expr"])
	require.Equal(t, "0m", rangeStreamRule["for"])
	require.Equal(t, "warning", rangeStreamRule["labels"].(map[string]any)["severity"])
	require.Contains(t, rangeStreamRule["annotations"].(map[string]any)["description"], "Caller cancellation is excluded")
	rangeStreamMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainRangeStreamMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:range_stream_failure_invalid_values:count) == 1 or count(kubebrain_dbaas:range_stream_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:range_stream_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:range_stream_failure_invalid_values:count != 0`,
		rangeStreamMissingRule["expr"])
	require.Equal(t, "2m", rangeStreamMissingRule["for"])
	require.Contains(t, rangeStreamMissingRule["annotations"].(map[string]any)["description"], "authoritative-zero")
	require.Contains(t, rangeStreamMissingRule["annotations"].(map[string]any)["description"], "Missing, stale, or invalid telemetry")
	slowReadIndexRule := prometheusRuleByAlert(t, groups, "KubeBrainSlowReadIndexes")
	require.Equal(t,
		`sum(kubebrain_dbaas:read_index:increase_10m_by_pod_outcome{outcome="slow"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		slowReadIndexRule["expr"])
	require.Equal(t, "warning", slowReadIndexRule["labels"].(map[string]any)["severity"])
	readIndexFailureRule := prometheusRuleByAlert(t, groups, "KubeBrainReadIndexFailures")
	require.Equal(t,
		`sum(kubebrain_dbaas:read_index:increase_10m_by_pod_outcome{outcome="failed"} * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		readIndexFailureRule["expr"])
	require.Contains(t, readIndexFailureRule["annotations"].(map[string]any)["description"], "failed closed")
	readIndexMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainReadIndexMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:read_index_invalid_values:count) == 1 or count(kubebrain_dbaas:read_index:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:read_index:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 2 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:read_index_invalid_values:count != 0`,
		readIndexMissingRule["expr"])
	require.Equal(t, "2m", readIndexMissingRule["for"])
	require.Equal(t, "critical", readIndexMissingRule["labels"].(map[string]any)["severity"])
	require.Contains(t, readIndexMissingRule["annotations"].(map[string]any)["description"], "authoritative-zero slow and failed counters")

	checkpointRule := prometheusRuleByAlert(t, groups, "KubeBrainSerializableCheckpointUnavailable")
	require.Equal(t,
		`absent(kubebrain_dbaas:serializable_checkpoint_invalid_values:count) == 1 or count(kubebrain_dbaas:serializable_checkpoint_available:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:serializable_checkpoint_invalid_values:count != 0 or min(kubebrain_dbaas:serializable_checkpoint_available:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 1 or min(kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0 or min(kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 60`,
		checkpointRule["expr"])
	require.Equal(t, "30s", checkpointRule["for"])
	require.Equal(t, "warning", checkpointRule["labels"].(map[string]any)["severity"])
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "PD-isolated serializable")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "less than 60 seconds")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"],
		"stopped publishing any of the available, revision, and remaining-seconds checkpoint gauges")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "KSM Ready samples are no older than 60 seconds")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "Available must be exactly 0 or 1")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "Missing, stale, or invalid checkpoint evidence")
	require.Contains(t, checkpointRule["annotations"].(map[string]any)["description"], "stale replaced Pod identities are excluded")

	checkpointRefreshRule := prometheusRuleByAlert(t, groups, "KubeBrainSerializableCheckpointRefreshFailures")
	healthFallbackRule := prometheusRuleByAlert(t, groups, "KubeBrainHealthCheckpointFallback")
	require.Equal(t,
		`sum by (check) (kubebrain_dbaas:health_checkpoint_fallback:increase_10m_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		healthFallbackRule["expr"])
	require.Equal(t, "0m", healthFallbackRule["for"])
	require.Equal(t, "warning", healthFallbackRule["labels"].(map[string]any)["severity"])
	healthFallbackDescription := healthFallbackRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, healthFallbackDescription, "bounded-stale degraded service")
	require.Contains(t, healthFallbackDescription, "not proof that PD/TiKV recovered")
	require.Contains(t, healthFallbackDescription, "KSM Ready and health counter samples are no older than 60 seconds")
	require.Contains(t, healthFallbackDescription, "deduplicated by Pod UID and check")
	require.Contains(t, healthFallbackDescription, "stopped scrapes are excluded")
	require.Contains(t, healthFallbackDescription, "extrapolated increases must be finite")
	healthFallbackMissingRule := prometheusRuleByAlert(t, groups,
		"KubeBrainHealthCheckpointFallbackMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:health_checkpoint_fallback_invalid_values:count) == 1 or count(kubebrain_dbaas:health_checkpoint_fallback:current_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:health_checkpoint_fallback:increase_10m_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != 3 * count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:health_checkpoint_fallback_invalid_values:count != 0`,
		healthFallbackMissingRule["expr"])
	require.Equal(t, "5m", healthFallbackMissingRule["for"])
	require.Equal(t, "warning", healthFallbackMissingRule["labels"].(map[string]any)["severity"])
	healthFallbackMissingDescription := healthFallbackMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, healthFallbackMissingDescription, "exactly three initialized")
	require.Contains(t, healthFallbackMissingDescription, "samples no older than 60 seconds")
	require.Contains(t, healthFallbackMissingDescription, "stopped application scrapes are excluded")
	require.Contains(t, healthFallbackMissingDescription, "during scaling")
	require.Contains(t, healthFallbackMissingDescription, "mixed binary versions")
	require.Contains(t, healthFallbackMissingDescription, "pod UID relabeling")
	require.Contains(t, healthFallbackMissingDescription, "Missing, stale, or invalid fallback telemetry")
	require.Contains(t, healthFallbackMissingDescription, "extrapolated increases may be fractional")
	require.Equal(t,
		`sum(kubebrain_dbaas:serializable_checkpoint_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`,
		checkpointRefreshRule["expr"])
	require.Equal(t, "0m", checkpointRefreshRule["for"])
	require.Equal(t, "warning", checkpointRefreshRule["labels"].(map[string]any)["severity"])
	checkpointRefreshMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainSerializableCheckpointRefreshMetricsMissing")
	require.Equal(t,
		`absent(kubebrain_dbaas:serializable_checkpoint_refresh_err_invalid_values:count) == 1 or count(kubebrain_dbaas:serializable_checkpoint_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:serializable_checkpoint_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:serializable_checkpoint_refresh_err_invalid_values:count != 0`,
		checkpointRefreshMissingRule["expr"])
	require.Equal(t, "2m", checkpointRefreshMissingRule["for"])
	checkpointRefreshMissingDescription := checkpointRefreshMissingRule["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, checkpointRefreshMissingDescription, "initializes the counter to an authoritative zero")
	require.Contains(t, checkpointRefreshMissingDescription, "Missing, stale, or invalid telemetry")

	quotaRules := map[string]struct {
		expr     string
		forValue string
		severity string
	}{
		"KubeBrainQuotaNoSpace": {
			expr: `max(kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`, forValue: "0m", severity: "critical",
		},
		"KubeBrainQuotaUsageHigh": {
			expr: `max((kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod / kubebrain_dbaas:quota_backend_bytes:max_by_pod) * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0.9`, forValue: "10m", severity: "warning",
		},
		"KubeBrainQuotaMetricsInconsistent": {
			expr: `absent(kubebrain_dbaas:quota_invalid_values:count) == 1 or count(kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:quota_invalid_values:count != 0 or (max(kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) - min(kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0) or (max(kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) - min(kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0)`, forValue: "1m", severity: "warning",
		},
		"KubeBrainQuotaRefreshFailures": {
			expr: `sum(kubebrain_dbaas:quota_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 0`, forValue: "0m", severity: "warning",
		},
		"KubeBrainQuotaRefreshMetricsMissing": {
			expr: `absent(kubebrain_dbaas:quota_refresh_err_invalid_values:count) == 1 or count(kubebrain_dbaas:quota_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or count(kubebrain_dbaas:quota_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != count(kubebrain_dbaas:ready_pods:current) or kubebrain_dbaas:quota_refresh_err_invalid_values:count != 0`, forValue: "2m", severity: "warning",
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
	require.Contains(t, quotaConsistencyDescription, "KSM Ready and quota samples are no older than 60 seconds")
	require.Contains(t, quotaConsistencyDescription, "quota samples are no older than 60 seconds")
	require.Contains(t, quotaConsistencyDescription, "stale replaced Pod identities and stale quota samples are excluded")
	require.Contains(t, quotaConsistencyDescription, "NOSPACE must be exactly 0 or 1")
	require.Contains(t, quotaConsistencyDescription, "positive exact byte integer no greater than 2^53")
	require.Contains(t, quotaConsistencyDescription, "Invalid numerator or denominator telemetry")
	quotaRefreshMissingDescription := prometheusRuleByAlert(t, groups,
		"KubeBrainQuotaRefreshMetricsMissing")["annotations"].(map[string]any)["description"].(string)
	require.Contains(t, quotaRefreshMissingDescription, "initializes the counter to an authoritative zero")
	require.Contains(t, quotaRefreshMissingDescription, "Missing, stale, or invalid telemetry")

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
		"KubeBrainPDLeaderUnavailable":          `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or absent(kubebrain_dbaas:storage_control_plane_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:storage_control_plane_invalid_values:count != 0 or count(kubebrain_dbaas:pd_is_leader:max_by_instance) != on() kubebrain_dbaas:pd_replicas:expected or sum(kubebrain_dbaas:pd_is_leader:max_by_instance) != 1`,
		"KubeBrainTiKVRegionLeaderMissing":      `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:tikv_replicas:expected) == 1 or absent(kubebrain_dbaas:storage_control_plane_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:storage_control_plane_invalid_values:count != 0 or count(kubebrain_dbaas:tikv_leader_missing:max_by_instance) != on() kubebrain_dbaas:tikv_replicas:expected or max(kubebrain_dbaas:tikv_leader_missing:max_by_instance) > 0`,
		"KubeBrainPDRegionPeerUnhealthy":        `max by (type) (kubebrain_dbaas:pd_region_status:max_by_instance_type) > 0`,
		"KubeBrainPDRegionHealthMetricsMissing": `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or absent(kubebrain_dbaas:storage_control_plane_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:storage_control_plane_invalid_values:count != 0 or count(kubebrain_dbaas:pd_region_status:max_by_instance_type) != on() (2 * kubebrain_dbaas:pd_replicas:expected)`,
		"KubeBrainPDWALFsyncLatencyHigh":        `histogram_quantile(0.99, sum by (instance, le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket{namespace="tidb-cluster",service="kb-pd-metrics"}[5m]))) > 1`,
		"KubeBrainTiKVRaftDBWriteLatencyHigh":   `histogram_quantile(0.99, sum by (instance, le) (rate(tikv_raftstore_store_write_raftdb_duration_seconds_bucket{namespace="tidb-cluster",service="kb-tikv-metrics"}[5m]))) > 1`,
		"KubeBrainTiKVKVDBWriteLatencyHigh":     `histogram_quantile(0.99, sum by (instance, le) (rate(tikv_raftstore_store_write_kvdb_duration_seconds_bucket{namespace="tidb-cluster",service="kb-tikv-metrics"}[5m]))) > 1`,
		"KubeBrainStorageLatencyMetricsMissing": `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:pd_replicas:expected) == 1 or absent(kubebrain_dbaas:tikv_replicas:expected) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or count(max by (instance) (etcd_disk_wal_fsync_duration_seconds_count{namespace="tidb-cluster",service="kb-pd-metrics"})) != on() kubebrain_dbaas:pd_replicas:expected or count(max by (instance) (tikv_raftstore_store_write_raftdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"})) != on() kubebrain_dbaas:tikv_replicas:expected or count(max by (instance) (tikv_raftstore_store_write_kvdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"})) != on() kubebrain_dbaas:tikv_replicas:expected`,
		"KubeBrainStorageLatencyMetricsStale":   `(max(time() - max by (instance) (timestamp(etcd_disk_wal_fsync_duration_seconds_count{namespace="tidb-cluster",service="kb-pd-metrics"}))) > 60) or (max(time() - max by (instance) (timestamp(tikv_raftstore_store_write_raftdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"}))) > 60) or (max(time() - max by (instance) (timestamp(tikv_raftstore_store_write_kvdb_duration_seconds_count{namespace="tidb-cluster",service="kb-tikv-metrics"}))) > 60)`,
		"KubeBrainStorageVolumeMetricsMissing":  `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:storage_replicas:expected) == 1 or absent(kubebrain_dbaas:storage_volumes:expected) == 1 or absent(kubebrain_dbaas:storage_requested_sources:count) == 1 or absent(kubebrain_dbaas:storage_volume_identity_mismatches:count) == 1 or absent(kubebrain_dbaas:storage_active_volume_sources:count) == 1 or absent(kubebrain_dbaas:storage_capacity_sources:count) == 1 or absent(kubebrain_dbaas:storage_available_sources:count) == 1 or absent(kubebrain_dbaas:storage_volume_stats_identity_mismatches:count) == 1 or absent(kubebrain_dbaas:storage_invalid_byte_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:storage_volumes:expected < on() kubebrain_dbaas:storage_replicas:expected or kubebrain_dbaas:storage_requested_sources:count != on() kubebrain_dbaas:storage_volumes:expected or kubebrain_dbaas:storage_volume_identity_mismatches:count != 0 or kubebrain_dbaas:storage_active_volume_sources:count != on() kubebrain_dbaas:storage_replicas:expected or kubebrain_dbaas:storage_capacity_sources:count != on() kubebrain_dbaas:storage_replicas:expected or kubebrain_dbaas:storage_available_sources:count != on() kubebrain_dbaas:storage_replicas:expected or kubebrain_dbaas:storage_volume_stats_identity_mismatches:count != 0 or kubebrain_dbaas:storage_invalid_byte_values:count != 0`,
		"KubeBrainStorageVolumeLow":             `(kubebrain_dbaas:storage_available_bytes:min_by_pvc / kubebrain_dbaas:storage_capacity_bytes:max_by_pvc) < 0.15`,
		"KubeBrainResourceMetricsMissing":       `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:compute_replicas:expected) == 1 or absent(kubebrain_dbaas:cpu_usage_sources:count) == 1 or absent(kubebrain_dbaas:cpu_throttled_period_sources:count) == 1 or absent(kubebrain_dbaas:cpu_period_sources:count) == 1 or absent(kubebrain_dbaas:memory_working_set_sources:count) == 1 or absent(kubebrain_dbaas:memory_limit_sources:count) == 1 or absent(kubebrain_dbaas:resource_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:cpu_usage_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:cpu_throttled_period_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:cpu_period_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:memory_working_set_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:memory_limit_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:resource_invalid_values:count != 0`,
		"KubeBrainDataPlaneMemoryHigh":          `(kubebrain_dbaas:memory_working_set_bytes:max_by_container / kubebrain_dbaas:memory_limit_bytes:max_by_container) > 0.9`,
		"KubeBrainDataPlaneCPUThrottlingHigh":   `(kubebrain_dbaas:cpu_throttled_periods_per_second:max_by_container / kubebrain_dbaas:cpu_periods_per_second:max_by_container) > 0.25`,
		"KubeBrainNetworkMetricsMissing":        `absent(kubebrain_dbaas:replica_expectation_sources:count) == 1 or absent(kubebrain_dbaas:compute_replicas:expected) == 1 or absent(kubebrain_dbaas:network_receive_sources:count) == 1 or absent(kubebrain_dbaas:network_transmit_sources:count) == 1 or absent(kubebrain_dbaas:network_receive_error_sources:count) == 1 or absent(kubebrain_dbaas:network_transmit_error_sources:count) == 1 or absent(kubebrain_dbaas:network_receive_drop_sources:count) == 1 or absent(kubebrain_dbaas:network_transmit_drop_sources:count) == 1 or absent(kubebrain_dbaas:network_invalid_values:count) == 1 or kubebrain_dbaas:replica_expectation_sources:count != 3 or kubebrain_dbaas:network_receive_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_transmit_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_receive_error_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_transmit_error_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_receive_drop_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_transmit_drop_sources:count != on() kubebrain_dbaas:compute_replicas:expected or kubebrain_dbaas:network_invalid_values:count != 0`,
		"KubeBrainNetworkErrors":                `(kubebrain_dbaas:network_receive_errors:increase_10m_by_interface + kubebrain_dbaas:network_transmit_errors:increase_10m_by_interface) > 0`,
		"KubeBrainNetworkPacketDrops":           `(kubebrain_dbaas:network_receive_drops:increase_10m_by_interface + kubebrain_dbaas:network_transmit_drops:increase_10m_by_interface) > 0`,
		"KubeBrainLogicalBackupMetricsMissing":  `count(kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current) != 1 or count(kubebrain_dbaas:logical_backup_artifact_bytes:current) != 1 or count(kubebrain_dbaas:logical_backup_records:current) != 1 or count(kubebrain_dbaas:logical_backup_leases:current) != 1 or count(kubebrain_dbaas:logical_backup_snapshot_revision:current) != 1 or kubebrain_dbaas:logical_backup_invalid_values:count != 0`,
		"KubeBrainLogicalBackupStale":           `(time() - max(kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current) > 90000) or (max(kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current) > time() + 300)`,
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
	for _, alert := range []string{"KubeBrainPDLeaderUnavailable", "KubeBrainTiKVRegionLeaderMissing", "KubeBrainPDRegionHealthMetricsMissing"} {
		description := prometheusRuleByAlert(t, groups, alert)["annotations"].(map[string]any)["description"].(string)
		require.Contains(t, description, "leader/Region value is invalid")
		require.Contains(t, description, "exact integers no greater than 2^53")
		require.Contains(t, description, "PD leader must be exactly 0 or 1")
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
		require.Contains(t, description, "negative, fractional, non-finite, or greater-than-2^53")
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
	for _, alert := range []string{
		"KubeBrainPDLeaderUnavailable",
		"KubeBrainTiKVRegionLeaderMissing",
		"KubeBrainPDRegionHealthMetricsMissing",
	} {
		description := prometheusRuleByAlert(t, groups, alert)["annotations"].(map[string]any)["description"].(string)
		require.Contains(t, description, "samples within 60 seconds")
	}
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
	require.Contains(t,
		backupMissingRule["annotations"].(map[string]any)["description"],
		"raw scrape sample must be no older than 60 seconds",
	)
	require.Contains(t,
		backupMissingRule["annotations"].(map[string]any)["description"],
		"Prometheus lookback values cannot be re-timestamped",
	)
	require.Contains(t,
		backupMissingRule["annotations"].(map[string]any)["description"],
		"exact integers no greater than 2^53",
	)
	require.Contains(t,
		backupMissingRule["annotations"].(map[string]any)["description"],
		"Invalid values also block metering completeness",
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
	require.Contains(t,
		networkMissingRule["annotations"].(map[string]any)["description"],
		"NaN, infinite, negative, or above 2^53",
	)
	require.Contains(t,
		networkMissingRule["annotations"].(map[string]any)["description"],
		"Missing, stale, or invalid error/drop families",
	)
	resourceMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainResourceMetricsMissing")
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"throttled/total period",
	)
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"must not be interpreted as zero resource pressure",
	)
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"non-positive total-period denominator",
	)
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"throttled periods above total periods",
	)
	require.Contains(t,
		resourceMissingRule["annotations"].(map[string]any)["description"],
		"memory byte value is not an exact integer within 2^53",
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
	storageMissingRule := prometheusRuleByAlert(t, groups, "KubeBrainStorageVolumeMetricsMissing")
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"same PVC identities",
	)
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"samples within 60 seconds",
	)
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"kubelet capacity/available identities do not exactly equal that active PVC set",
	)
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"must not substitute for missing active volume statistics",
	)
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"exact integer within 2^53",
	)
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"available is negative or exceeds capacity",
	)
	require.Contains(t,
		storageMissingRule["annotations"].(map[string]any)["description"],
		"Invalid byte values also block metering completeness",
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

	quotaInvalidValuesExpr := `count(((kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:quota_nospace:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > on(namespace, pod, uid) (kubebrain_dbaas:quota_backend_bytes:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))`
	checkpointInvalidValuesExpr := `count(((kubebrain_dbaas:serializable_checkpoint_available:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:serializable_checkpoint_available:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:serializable_checkpoint_available:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:serializable_checkpoint_available:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`
	healthFallbackInvalidValuesExpr := `count(((kubebrain_dbaas:health_checkpoint_fallback:current_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:health_checkpoint_fallback:current_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:health_checkpoint_fallback:current_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:health_checkpoint_fallback:current_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:health_checkpoint_fallback:increase_10m_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:health_checkpoint_fallback:increase_10m_by_pod_check * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	checkpointRefreshInvalidValuesExpr := `count(((kubebrain_dbaas:serializable_checkpoint_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:serializable_checkpoint_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:serializable_checkpoint_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:serializable_checkpoint_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:serializable_checkpoint_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:serializable_checkpoint_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	quotaRefreshInvalidValuesExpr := `count(((kubebrain_dbaas:quota_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:quota_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:quota_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:quota_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:quota_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:quota_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	countIndexRebuildInvalidValuesExpr := `count(((kubebrain_dbaas:count_index_rebuild_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:count_index_rebuild_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:count_index_rebuild_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:count_index_rebuild_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:count_index_rebuild_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:count_index_rebuild_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	rangeStreamFailureInvalidValuesExpr := `count(((kubebrain_dbaas:range_stream_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:range_stream_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:range_stream_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:range_stream_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:range_stream_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:range_stream_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	readIndexInvalidValuesExpr := `count(((kubebrain_dbaas:read_index:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:read_index:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:read_index:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:read_index:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:read_index:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:read_index:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	alarmRefreshInvalidValuesExpr := `count(((kubebrain_dbaas:alarm_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:alarm_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:alarm_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:alarm_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:alarm_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:alarm_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	authRevisionInvalidValuesExpr := `count(((kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:auth_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:auth_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:auth_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:auth_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:auth_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:auth_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:auth_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	mvccRevisionInvalidValuesExpr := `count(((kubebrain_dbaas:mvcc_current_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:mvcc_current_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_current_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_current_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:mvcc_compact_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > on(namespace, pod, uid) (kubebrain_dbaas:mvcc_current_revision:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) + count(((kubebrain_dbaas:mvcc_compact_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_compact_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_compact_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_compact_revision_refresh_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:mvcc_compact_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:mvcc_compact_revision_refresh_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	mvccLiveKeysInvalidValuesExpr := `count(((kubebrain_dbaas:mvcc_live_keys:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_live_keys:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_live_keys:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_live_keys:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:mvcc_live_keys_refresh_miss:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_live_keys_refresh_miss:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_live_keys_refresh_miss:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_live_keys_refresh_miss:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:mvcc_live_keys_refresh_miss:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:mvcc_live_keys_refresh_miss:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > time() + 300) or ((kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`
	watchGaugeInvalidValuesExpr := `count(((kubebrain_dbaas:mvcc_watch_streams:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_watch_streams:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_watch_streams:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_watch_streams:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > on(namespace, pod, uid) (kubebrain_dbaas:mvcc_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) or ((kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_slow_watchers:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`
	watchEventDeliveryInvalidValuesExpr := `count(((kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:mvcc_delivered_watch_events:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:mvcc_delivered_watch_events:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:mvcc_delivered_watch_events:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:mvcc_delivered_watch_events:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:mvcc_delivered_watch_events:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:mvcc_delivered_watch_events:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	serverStreamFailureInvalidValuesExpr := `count(((kubebrain_dbaas:server_stream_failure:current_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:server_stream_failure:current_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:server_stream_failure:current_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:server_stream_failure:current_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:server_stream_failure:increase_10m_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:server_stream_failure:increase_10m_by_pod_api_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(kubebrain_dbaas:server_stream_failure:current_by_pod_api_type{api!~"watch|lease-keepalive"} or kubebrain_dbaas:server_stream_failure:current_by_pod_api_type{failure_type!~"receive|send"})`
	watchPrevKVBudgetInvalidValuesExpr := `count(((kubebrain_dbaas:watch_prev_kv_budget_exhausted:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_prev_kv_budget_exhausted:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_prev_kv_budget_exhausted:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_prev_kv_budget_exhausted:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_prev_kv_budget_exhausted:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_prev_kv_budget_exhausted:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	serverStateInvalidValuesExpr := `count(((kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:server_is_learner:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:server_is_learner:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:server_is_learner:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:server_is_learner:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:server_is_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > (kubebrain_dbaas:server_has_leader:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) + count(((kubebrain_dbaas:server_is_leader:max_by_pod + on(namespace, pod, uid) kubebrain_dbaas:server_is_learner:max_by_pod) * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) + count(((kubebrain_dbaas:server_leader_changes:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:server_leader_changes:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:server_leader_changes:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:server_leader_changes:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:server_leader_changes:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:server_leader_changes:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	fdInvalidValuesExpr := `count(((kubebrain_dbaas:fd_used:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:fd_used:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:fd_used:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:fd_used:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:fd_limit:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:fd_limit:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:fd_limit:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:fd_limit:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:fd_used:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > on(namespace, pod, uid) (kubebrain_dbaas:fd_limit:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) + count(((kubebrain_dbaas:fd_refresh_err:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:fd_refresh_err:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:fd_refresh_err:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:fd_refresh_err:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:fd_refresh_err:increase_30m_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:fd_refresh_err:increase_30m_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > time() + 300) or ((kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`
	leaderElectionInvalidValuesExpr := `count(((kubebrain_dbaas:leader_election_lost:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:leader_election_lost:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:leader_election_lost:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:leader_election_lost:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:leader_election_lost:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:leader_election_lost:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:leader_incompatible_witness:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:leader_incompatible_witness:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:leader_incompatible_witness:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:leader_incompatible_witness:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:leader_incompatible_witness:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:leader_incompatible_witness:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:leader_invalid_alarm_metadata:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:leader_invalid_alarm_metadata:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:leader_invalid_alarm_metadata:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:leader_invalid_alarm_metadata:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:leader_invalid_alarm_metadata:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:leader_invalid_alarm_metadata:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaderInitializeInvalidValuesExpr := `count(((kubebrain_dbaas:leader_initialize_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:leader_initialize_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:leader_initialize_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:leader_initialize_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:leader_initialize_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:leader_initialize_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	servingInitializationInvalidValuesExpr := `count((kubebrain_dbaas:leader_serving_initialization_err:current_by_pod_stage < 0) or (kubebrain_dbaas:leader_serving_initialization_err:current_by_pod_stage > 9007199254740992) or (kubebrain_dbaas:leader_serving_initialization_err:current_by_pod_stage != floor(kubebrain_dbaas:leader_serving_initialization_err:current_by_pod_stage))) + count(kubebrain_dbaas:leader_serving_initialization_err:increase_10m_by_pod_stage != clamp(kubebrain_dbaas:leader_serving_initialization_err:increase_10m_by_pod_stage, 0, 9007199254740992))`
	watchEventBufferInvalidValuesExpr := `count(((kubebrain_dbaas:watch_event_buffer_stale_drop:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_event_buffer_stale_drop:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_event_buffer_stale_drop:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_event_buffer_stale_drop:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_event_buffer_stale_drop:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_event_buffer_stale_drop:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:watch_event_buffer_full:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_event_buffer_full:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_event_buffer_full:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_event_buffer_full:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_event_buffer_full:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_event_buffer_full:rate_5m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	watchCollectorInvalidValuesExpr := `count(((kubebrain_dbaas:watch_collector_stalled:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_collector_stalled:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_collector_stalled:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_collector_stalled:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_collector_stalled:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_collector_stalled:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:watch_collector_skipped_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_collector_skipped_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_collector_skipped_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_collector_skipped_revision:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_collector_skipped_revision:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_collector_skipped_revision:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	watchCollectorRecoveryInvalidValuesExpr := `count(((kubebrain_dbaas:watch_collector_recovery:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_collector_recovery:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_collector_recovery:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_collector_recovery:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_collector_recovery:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_collector_recovery:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	watchBackendIntegrityInvalidValuesExpr := `count(((kubebrain_dbaas:watch_backend_integrity_failure:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_backend_integrity_failure:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_backend_integrity_failure:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_backend_integrity_failure:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:watch_backend_integrity_failure:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:watch_backend_integrity_failure:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	durableRevisionPersistInvalidValuesExpr := `count(((kubebrain_dbaas:durable_revision_persist_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:durable_revision_persist_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:durable_revision_persist_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:durable_revision_persist_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:durable_revision_persist_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:durable_revision_persist_err:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	eventLogIntegrityInvalidValuesExpr := `count(((kubebrain_dbaas:event_log_corruption:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:event_log_corruption:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:event_log_corruption:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:event_log_corruption:current_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:event_log_corruption:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:event_log_corruption:increase_10m_by_pod_kind * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992)) + count(((kubebrain_dbaas:event_log_corrupt_alarm_failed:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:event_log_corrupt_alarm_failed:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:event_log_corrupt_alarm_failed:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:event_log_corrupt_alarm_failed:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:event_log_corrupt_alarm_failed:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:event_log_corrupt_alarm_failed:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	readIntegrityFenceInvalidValuesExpr := `count(((kubebrain_dbaas:read_integrity_fence:current_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:read_integrity_fence:current_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:read_integrity_fence:current_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:read_integrity_fence:current_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:read_integrity_fence:increase_10m_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:read_integrity_fence:increase_10m_by_pod_target_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	commitWaitFailureInvalidValuesExpr := `count(((kubebrain_dbaas:commit_wait_failure:current_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:commit_wait_failure:current_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:commit_wait_failure:current_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:commit_wait_failure:current_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:commit_wait_failure:increase_10m_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:commit_wait_failure:increase_10m_by_pod_reason * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	storageGCInvalidValuesExpr := `count(((kubebrain_dbaas:storage_gc_enabled:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:storage_gc_enabled:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:storage_gc_enabled:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:storage_gc_enabled:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count(((kubebrain_dbaas:storage_gc_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:storage_gc_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:storage_gc_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:storage_gc_err:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:storage_gc_err:increase_30m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:storage_gc_err:increase_30m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	storageGCProgressInvalidValuesExpr := `count(((kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) <= 0) or ((kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) or ((kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > time() + 300)) + count(((kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)) or ((kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > time() + 300))`
	storageCompactionFailureInvalidValuesExpr := `count(((kubebrain_dbaas:storage_compaction_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:storage_compaction_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:storage_compaction_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:storage_compaction_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:storage_compaction_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:storage_compaction_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseLifecycleInvalidValuesExpr := `count(((kubebrain_dbaas:lease_lifecycle:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_lifecycle:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_lifecycle:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_lifecycle:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_lifecycle:increase_10m_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_lifecycle:increase_10m_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseStartupRestoreInvalidValuesExpr := `count(((kubebrain_dbaas:lease_startup_restore_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_startup_restore_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_startup_restore_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_startup_restore_failure:current_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_startup_restore_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_startup_restore_failure:increase_10m_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	uncertainTxnInvalidValuesExpr := `count(((kubebrain_dbaas:uncertain_txn_resolution:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:uncertain_txn_resolution:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:uncertain_txn_resolution:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:uncertain_txn_resolution:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:uncertain_txn_resolution:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:uncertain_txn_resolution:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	restartWitnessCorruptionInvalidValuesExpr := `count(((kubebrain_dbaas:restart_witness_corruption:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:restart_witness_corruption:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:restart_witness_corruption:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:restart_witness_corruption:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:restart_witness_corruption:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:restart_witness_corruption:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseUncertainReconcileInvalidValuesExpr := `count(((kubebrain_dbaas:lease_uncertain_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_uncertain_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_uncertain_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_uncertain_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_uncertain_reconcile:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_uncertain_reconcile:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseBackgroundFailureInvalidValuesExpr := `count(((kubebrain_dbaas:lease_background_failure:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_background_failure:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_background_failure:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_background_failure:current_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseOrphanSweepFailureInvalidValuesExpr := `count(((kubebrain_dbaas:lease_orphan_sweep_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_orphan_sweep_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_orphan_sweep_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_orphan_sweep_failure:current_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_orphan_sweep_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_orphan_sweep_failure:increase_10m_by_pod_stage * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseGrantCleanupInvalidValuesExpr := `count(((kubebrain_dbaas:lease_grant_cleanup:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_grant_cleanup:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_grant_cleanup:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_grant_cleanup:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_grant_cleanup:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_grant_cleanup:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	leaseRevokeReconcileInvalidValuesExpr := `count(((kubebrain_dbaas:lease_revoke_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:lease_revoke_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:lease_revoke_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:lease_revoke_reconcile:current_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current))) + count((kubebrain_dbaas:lease_revoke_reconcile:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != clamp(kubebrain_dbaas:lease_revoke_reconcile:increase_10m_by_pod_outcome * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current, 0, 9007199254740992))`
	expected := map[string]string{
		"kubebrain_dbaas:statefulset_replicas:current":                       `max by (namespace, statefulset) (kube_statefulset_replicas{namespace=~"kubebrain-system|tidb-cluster",statefulset=~"kubebrain|kb-(pd|tikv)"} and (time() - timestamp(kube_statefulset_replicas{namespace=~"kubebrain-system|tidb-cluster",statefulset=~"kubebrain|kb-(pd|tikv)"}) <= 60))`,
		"kubebrain_dbaas:statefulset_ready_replicas:current":                 `max by (namespace, statefulset) (kube_statefulset_status_replicas_ready{namespace=~"kubebrain-system|tidb-cluster",statefulset=~"kubebrain|kb-(pd|tikv)"} and (time() - timestamp(kube_statefulset_status_replicas_ready{namespace=~"kubebrain-system|tidb-cluster",statefulset=~"kubebrain|kb-(pd|tikv)"}) <= 60))`,
		"kubebrain_dbaas:statefulset_ready_sources:count":                    `count(((kubebrain_dbaas:statefulset_ready_replicas:current >= 0) and (kubebrain_dbaas:statefulset_ready_replicas:current <= 9007199254740992) and (kubebrain_dbaas:statefulset_ready_replicas:current == floor(kubebrain_dbaas:statefulset_ready_replicas:current))) and on(namespace, statefulset) (kubebrain_dbaas:statefulset_ready_replicas:current <= kubebrain_dbaas:statefulset_replicas:current))`,
		"kubebrain_dbaas:ready_pods:current":                                 `max by (namespace, pod, uid) ((kube_pod_status_ready{namespace="kubebrain-system",condition="true"} == 1) and (time() - timestamp(kube_pod_status_ready{namespace="kubebrain-system",condition="true"}) <= 60))`,
		"kubebrain_dbaas:quota_nospace:max_by_pod":                           `max by (namespace, pod, uid) (quota_nospace{namespace="kubebrain-system"} and (time() - timestamp(quota_nospace{namespace="kubebrain-system"}) <= 60))`,
		"kubebrain_dbaas:quota_backend_bytes:max_by_pod":                     `max by (namespace, pod, uid) (quota_backend_bytes{namespace="kubebrain-system"} and (time() - timestamp(quota_backend_bytes{namespace="kubebrain-system"}) <= 60))`,
		"kubebrain_dbaas:quota_logical_usage_bytes:max_by_pod":               `max by (namespace, pod, uid) (quota_logical_usage_bytes{namespace="kubebrain-system"} and (time() - timestamp(quota_logical_usage_bytes{namespace="kubebrain-system"}) <= 60))`,
		"kubebrain_dbaas:quota_invalid_values:count":                         quotaInvalidValuesExpr,
		"kubebrain_dbaas:health_checkpoint_fallback:current_by_pod_check":    `max by (namespace, pod, uid, check) (health_checkpoint_fallback{namespace="kubebrain-system",check=~"alarm|serializable_read|data_corruption"} and (time() - timestamp(health_checkpoint_fallback{namespace="kubebrain-system",check=~"alarm|serializable_read|data_corruption"}) <= 60))`,
		"kubebrain_dbaas:count_index_overflowed:max_by_pod":                  `max by (namespace, pod, uid) (count_index_overflowed{namespace="kubebrain-system"} and (time() - timestamp(count_index_overflowed{namespace="kubebrain-system"}) <= 60))`,
		"kubebrain_dbaas:count_index_invalid_values:count":                   `count(((kubebrain_dbaas:count_index_overflowed:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:count_index_overflowed:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:count_index_overflowed:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:count_index_overflowed:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`,
		"kubebrain_dbaas:watch_revision_lag:max_by_pod":                      `max by (namespace, pod, uid) (watch_revision_lag{namespace="kubebrain-system"} and (time() - timestamp(watch_revision_lag{namespace="kubebrain-system"}) <= 60))`,
		"kubebrain_dbaas:watch_revision_lag_invalid_values:count":            `count(((kubebrain_dbaas:watch_revision_lag:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:watch_revision_lag:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 9007199254740992) or ((kubebrain_dbaas:watch_revision_lag:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:watch_revision_lag:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`,
		"kubebrain_dbaas:replica_expectation_sources:count":                  `count((kubebrain_dbaas:statefulset_replicas:current >= 0) and (kubebrain_dbaas:statefulset_replicas:current <= 9007199254740992) and (kubebrain_dbaas:statefulset_replicas:current == floor(kubebrain_dbaas:statefulset_replicas:current)))`,
		"kubebrain_dbaas:compute_replicas:expected":                          `sum(kubebrain_dbaas:statefulset_replicas:current) or on() vector(0)`,
		"kubebrain_dbaas:kubebrain_replicas:expected":                        `sum(kubebrain_dbaas:statefulset_replicas:current{namespace="kubebrain-system",statefulset="kubebrain"}) or on() vector(0)`,
		"kubebrain_dbaas:storage_replicas:expected":                          `sum(kubebrain_dbaas:statefulset_replicas:current{namespace="tidb-cluster",statefulset=~"kb-(pd|tikv)"}) or on() vector(0)`,
		"kubebrain_dbaas:storage_volumes:current_by_pvc":                     `max by (namespace, persistentvolumeclaim) (kube_persistentvolumeclaim_info{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kube_persistentvolumeclaim_info{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_requested_bytes:max_by_pvc":                 `max by (namespace, persistentvolumeclaim) (kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kube_persistentvolumeclaim_resource_requests_storage_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_volumes:expected":                           `count(kubebrain_dbaas:storage_volumes:current_by_pvc)`,
		"kubebrain_dbaas:storage_requested_sources:count":                    `count(kubebrain_dbaas:storage_requested_bytes:max_by_pvc)`,
		"kubebrain_dbaas:storage_volume_identity_mismatches:count":           `count(kubebrain_dbaas:storage_volumes:current_by_pvc unless on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_requested_bytes:max_by_pvc) + count(kubebrain_dbaas:storage_requested_bytes:max_by_pvc unless on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_volumes:current_by_pvc)`,
		"kubebrain_dbaas:storage_active_volumes:current_by_pvc":              `max by (namespace, persistentvolumeclaim) (kube_pod_spec_volumes_persistentvolumeclaims_info{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kube_pod_spec_volumes_persistentvolumeclaims_info{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_active_volume_sources:count":                `count(kubebrain_dbaas:storage_active_volumes:current_by_pvc)`,
		"kubebrain_dbaas:pd_replicas:expected":                               `sum(kubebrain_dbaas:statefulset_replicas:current{namespace="tidb-cluster",statefulset="kb-pd"}) or on() vector(0)`,
		"kubebrain_dbaas:tikv_replicas:expected":                             `sum(kubebrain_dbaas:statefulset_replicas:current{namespace="tidb-cluster",statefulset="kb-tikv"}) or on() vector(0)`,
		"kubebrain_dbaas:pd_is_leader:max_by_instance":                       `max by (instance) (etcd_server_is_leader{namespace="tidb-cluster",service="kb-pd-metrics"} and (time() - timestamp(etcd_server_is_leader{namespace="tidb-cluster",service="kb-pd-metrics"}) <= 60))`,
		"kubebrain_dbaas:tikv_leader_missing:max_by_instance":                `max by (instance) (tikv_raftstore_leader_missing{namespace="tidb-cluster",service="kb-tikv-metrics"} and (time() - timestamp(tikv_raftstore_leader_missing{namespace="tidb-cluster",service="kb-tikv-metrics"}) <= 60))`,
		"kubebrain_dbaas:pd_region_status:max_by_instance_type":              `max by (instance, type) (pd_regions_status{namespace="tidb-cluster",service="kb-pd-metrics",type=~"pending-peer-region-count|down-peer-region-count"} and (time() - timestamp(pd_regions_status{namespace="tidb-cluster",service="kb-pd-metrics",type=~"pending-peer-region-count|down-peer-region-count"}) <= 60))`,
		"kubebrain_dbaas:storage_control_plane_invalid_values:count":         `count((kubebrain_dbaas:pd_is_leader:max_by_instance < 0) or (kubebrain_dbaas:pd_is_leader:max_by_instance > 1) or (kubebrain_dbaas:pd_is_leader:max_by_instance != floor(kubebrain_dbaas:pd_is_leader:max_by_instance))) + count((kubebrain_dbaas:tikv_leader_missing:max_by_instance < 0) or (kubebrain_dbaas:tikv_leader_missing:max_by_instance > 9007199254740992) or (kubebrain_dbaas:tikv_leader_missing:max_by_instance != floor(kubebrain_dbaas:tikv_leader_missing:max_by_instance))) + count((kubebrain_dbaas:pd_region_status:max_by_instance_type < 0) or (kubebrain_dbaas:pd_region_status:max_by_instance_type > 9007199254740992) or (kubebrain_dbaas:pd_region_status:max_by_instance_type != floor(kubebrain_dbaas:pd_region_status:max_by_instance_type)))`,
		"kubebrain_dbaas:cpu_usage_cores:max_by_container":                   `max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}[5m]) and (time() - timestamp(container_cpu_usage_seconds_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}) <= 60))`,
		"kubebrain_dbaas:memory_working_set_bytes:max_by_container":          `max by (namespace, pod, container) (container_memory_working_set_bytes{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""} and (time() - timestamp(container_memory_working_set_bytes{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}) <= 60))`,
		"kubebrain_dbaas:memory_limit_bytes:max_by_container":                `max by (namespace, pod, container) (kube_pod_container_resource_limits{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",resource="memory",unit="byte"} and (time() - timestamp(kube_pod_container_resource_limits{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",resource="memory",unit="byte"}) <= 60))`,
		"kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface":  `max by (namespace, pod, interface) (rate(container_network_receive_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[5m]) and (time() - timestamp(container_network_receive_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface": `max by (namespace, pod, interface) (rate(container_network_transmit_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[5m]) and (time() - timestamp(container_network_transmit_bytes_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:cpu_throttled_periods_per_second:max_by_container":  `max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}[5m]) and (time() - timestamp(container_cpu_cfs_throttled_periods_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}) <= 60))`,
		"kubebrain_dbaas:cpu_periods_per_second:max_by_container":            `max by (namespace, pod, container) (rate(container_cpu_cfs_periods_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}[5m]) and (time() - timestamp(container_cpu_cfs_periods_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",container=~"kubebrain|pd|tikv",image!=""}) <= 60))`,
		"kubebrain_dbaas:network_receive_errors:increase_10m_by_interface":   `max by (namespace, pod, interface) (increase(container_network_receive_errors_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m]) and (time() - timestamp(container_network_receive_errors_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:network_transmit_errors:increase_10m_by_interface":  `max by (namespace, pod, interface) (increase(container_network_transmit_errors_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m]) and (time() - timestamp(container_network_transmit_errors_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:network_receive_drops:increase_10m_by_interface":    `max by (namespace, pod, interface) (increase(container_network_receive_packets_dropped_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m]) and (time() - timestamp(container_network_receive_packets_dropped_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:network_transmit_drops:increase_10m_by_interface":   `max by (namespace, pod, interface) (increase(container_network_transmit_packets_dropped_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}[10m]) and (time() - timestamp(container_network_transmit_packets_dropped_total{namespace=~"kubebrain-system|tidb-cluster",pod=~"kubebrain-[0-9]+|kb-(pd|tikv)-[0-9]+",interface="eth0"}) <= 60))`,
		"kubebrain_dbaas:cpu_usage_sources:count":                            `count(kubebrain_dbaas:cpu_usage_cores:max_by_container)`,
		"kubebrain_dbaas:cpu_throttled_period_sources:count":                 `count(kubebrain_dbaas:cpu_throttled_periods_per_second:max_by_container)`,
		"kubebrain_dbaas:cpu_period_sources:count":                           `count(kubebrain_dbaas:cpu_periods_per_second:max_by_container)`,
		"kubebrain_dbaas:memory_working_set_sources:count":                   `count(kubebrain_dbaas:memory_working_set_bytes:max_by_container)`,
		"kubebrain_dbaas:memory_limit_sources:count":                         `count(kubebrain_dbaas:memory_limit_bytes:max_by_container)`,
		"kubebrain_dbaas:resource_invalid_values:count":                      `count(kubebrain_dbaas:cpu_usage_cores:max_by_container != clamp(kubebrain_dbaas:cpu_usage_cores:max_by_container, 0, 9007199254740992)) + count(kubebrain_dbaas:cpu_throttled_periods_per_second:max_by_container != clamp(kubebrain_dbaas:cpu_throttled_periods_per_second:max_by_container, 0, 9007199254740992)) + count((kubebrain_dbaas:cpu_periods_per_second:max_by_container <= 0) or (kubebrain_dbaas:cpu_periods_per_second:max_by_container > 9007199254740992) or (kubebrain_dbaas:cpu_periods_per_second:max_by_container != kubebrain_dbaas:cpu_periods_per_second:max_by_container)) + count(kubebrain_dbaas:cpu_throttled_periods_per_second:max_by_container > on(namespace, pod, container) kubebrain_dbaas:cpu_periods_per_second:max_by_container) + count((kubebrain_dbaas:memory_working_set_bytes:max_by_container < 0) or (kubebrain_dbaas:memory_working_set_bytes:max_by_container > 9007199254740992) or (kubebrain_dbaas:memory_working_set_bytes:max_by_container != floor(kubebrain_dbaas:memory_working_set_bytes:max_by_container))) + count((kubebrain_dbaas:memory_limit_bytes:max_by_container <= 0) or (kubebrain_dbaas:memory_limit_bytes:max_by_container > 9007199254740992) or (kubebrain_dbaas:memory_limit_bytes:max_by_container != floor(kubebrain_dbaas:memory_limit_bytes:max_by_container)))`,
		"kubebrain_dbaas:network_receive_sources:count":                      `count(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface)`,
		"kubebrain_dbaas:network_transmit_sources:count":                     `count(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface)`,
		"kubebrain_dbaas:metering_invalid_compute_values:count":              `count(kubebrain_dbaas:cpu_usage_cores:max_by_container != clamp(kubebrain_dbaas:cpu_usage_cores:max_by_container, 0, 9007199254740992)) + count((kubebrain_dbaas:memory_working_set_bytes:max_by_container < 0) or (kubebrain_dbaas:memory_working_set_bytes:max_by_container > 9007199254740992) or (kubebrain_dbaas:memory_working_set_bytes:max_by_container != floor(kubebrain_dbaas:memory_working_set_bytes:max_by_container))) + count(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface != clamp(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface, 0, 9007199254740992)) + count(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface != clamp(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface, 0, 9007199254740992))`,
		"kubebrain_dbaas:network_receive_error_sources:count":                `count(kubebrain_dbaas:network_receive_errors:increase_10m_by_interface)`,
		"kubebrain_dbaas:network_transmit_error_sources:count":               `count(kubebrain_dbaas:network_transmit_errors:increase_10m_by_interface)`,
		"kubebrain_dbaas:network_receive_drop_sources:count":                 `count(kubebrain_dbaas:network_receive_drops:increase_10m_by_interface)`,
		"kubebrain_dbaas:network_transmit_drop_sources:count":                `count(kubebrain_dbaas:network_transmit_drops:increase_10m_by_interface)`,
		"kubebrain_dbaas:network_invalid_values:count":                       `count(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface != clamp(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface, 0, 9007199254740992)) + count(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface != clamp(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface, 0, 9007199254740992)) + count(kubebrain_dbaas:network_receive_errors:increase_10m_by_interface != clamp(kubebrain_dbaas:network_receive_errors:increase_10m_by_interface, 0, 9007199254740992)) + count(kubebrain_dbaas:network_transmit_errors:increase_10m_by_interface != clamp(kubebrain_dbaas:network_transmit_errors:increase_10m_by_interface, 0, 9007199254740992)) + count(kubebrain_dbaas:network_receive_drops:increase_10m_by_interface != clamp(kubebrain_dbaas:network_receive_drops:increase_10m_by_interface, 0, 9007199254740992)) + count(kubebrain_dbaas:network_transmit_drops:increase_10m_by_interface != clamp(kubebrain_dbaas:network_transmit_drops:increase_10m_by_interface, 0, 9007199254740992))`,
		"kubebrain_dbaas:storage_capacity_bytes:max_by_pvc":                  `max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_available_bytes:max_by_pvc":                 `max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_available_bytes:min_by_pvc":                 `min by (namespace, persistentvolumeclaim) (kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"} and (time() - timestamp(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-9]+"}) <= 60))`,
		"kubebrain_dbaas:storage_volume_stats_identity_mismatches:count":     `count(kubebrain_dbaas:storage_active_volumes:current_by_pvc unless on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_capacity_bytes:max_by_pvc) + count(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc unless on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_active_volumes:current_by_pvc) + count(kubebrain_dbaas:storage_active_volumes:current_by_pvc unless on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_available_bytes:max_by_pvc) + count(kubebrain_dbaas:storage_available_bytes:max_by_pvc unless on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_active_volumes:current_by_pvc)`,
		"kubebrain_dbaas:storage_capacity_sources:count":                     `count(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc)`,
		"kubebrain_dbaas:storage_available_sources:count":                    `count(kubebrain_dbaas:storage_available_bytes:max_by_pvc)`,
		"kubebrain_dbaas:storage_invalid_byte_values:count":                  `count((kubebrain_dbaas:storage_requested_bytes:max_by_pvc <= 0) or (kubebrain_dbaas:storage_requested_bytes:max_by_pvc > 9007199254740992) or (kubebrain_dbaas:storage_requested_bytes:max_by_pvc != floor(kubebrain_dbaas:storage_requested_bytes:max_by_pvc))) + count((kubebrain_dbaas:storage_capacity_bytes:max_by_pvc <= 0) or (kubebrain_dbaas:storage_capacity_bytes:max_by_pvc > 9007199254740992) or (kubebrain_dbaas:storage_capacity_bytes:max_by_pvc != floor(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc))) + count((kubebrain_dbaas:storage_available_bytes:max_by_pvc < 0) or (kubebrain_dbaas:storage_available_bytes:max_by_pvc > 9007199254740992) or (kubebrain_dbaas:storage_available_bytes:max_by_pvc != floor(kubebrain_dbaas:storage_available_bytes:max_by_pvc))) + count((kubebrain_dbaas:storage_available_bytes:min_by_pvc < 0) or (kubebrain_dbaas:storage_available_bytes:min_by_pvc > 9007199254740992) or (kubebrain_dbaas:storage_available_bytes:min_by_pvc != floor(kubebrain_dbaas:storage_available_bytes:min_by_pvc))) + count(kubebrain_dbaas:storage_available_bytes:max_by_pvc > on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_capacity_bytes:max_by_pvc) + count(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc < on(namespace, persistentvolumeclaim) kubebrain_dbaas:storage_requested_bytes:max_by_pvc)`,
		"kubebrain_dbaas:logical_backup_artifact_bytes:current":              `kubebrain_logical_backup_artifact_bytes{instance="kubebrain"} and (time() - timestamp(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"}) <= 60)`,
		"kubebrain_dbaas:logical_backup_records:current":                     `kubebrain_logical_backup_records{instance="kubebrain"} and (time() - timestamp(kubebrain_logical_backup_records{instance="kubebrain"}) <= 60)`,
		"kubebrain_dbaas:logical_backup_leases:current":                      `kubebrain_logical_backup_leases{instance="kubebrain"} and (time() - timestamp(kubebrain_logical_backup_leases{instance="kubebrain"}) <= 60)`,
		"kubebrain_dbaas:logical_backup_snapshot_revision:current":           `kubebrain_logical_backup_snapshot_revision{instance="kubebrain"} and (time() - timestamp(kubebrain_logical_backup_snapshot_revision{instance="kubebrain"}) <= 60)`,
		"kubebrain_dbaas:logical_backup_artifact_sources:count":              `count(kubebrain_dbaas:logical_backup_artifact_bytes:current)`,
		"kubebrain_dbaas:logical_backup_timestamp_sources:count":             `count(kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current)`,
		"kubebrain_dbaas:logical_backup_invalid_values:count":                `count((kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current <= 0) or (kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current > 9007199254740992) or (kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current != floor(kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current)) or (kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current > time() + 300)) + count((kubebrain_dbaas:logical_backup_artifact_bytes:current < 0) or (kubebrain_dbaas:logical_backup_artifact_bytes:current > 9007199254740992) or (kubebrain_dbaas:logical_backup_artifact_bytes:current != floor(kubebrain_dbaas:logical_backup_artifact_bytes:current))) + count((kubebrain_dbaas:logical_backup_records:current < 0) or (kubebrain_dbaas:logical_backup_records:current > 9007199254740992) or (kubebrain_dbaas:logical_backup_records:current != floor(kubebrain_dbaas:logical_backup_records:current))) + count((kubebrain_dbaas:logical_backup_leases:current < 0) or (kubebrain_dbaas:logical_backup_leases:current > 9007199254740992) or (kubebrain_dbaas:logical_backup_leases:current != floor(kubebrain_dbaas:logical_backup_leases:current))) + count((kubebrain_dbaas:logical_backup_snapshot_revision:current <= 0) or (kubebrain_dbaas:logical_backup_snapshot_revision:current > 9007199254740992) or (kubebrain_dbaas:logical_backup_snapshot_revision:current != floor(kubebrain_dbaas:logical_backup_snapshot_revision:current)))`,
		"kubebrain_dbaas:object_store_request_count:current":                 `kubebrain_object_store_request_count{dbaas_instance="kubebrain",window="1h"} and (time() - timestamp(kubebrain_object_store_request_count{dbaas_instance="kubebrain",window="1h"}) <= 60)`,
		"kubebrain_dbaas:object_store_request_period_end_seconds:current":    `kubebrain_object_store_request_period_end_seconds{dbaas_instance="kubebrain",window="1h"} and (time() - timestamp(kubebrain_object_store_request_period_end_seconds{dbaas_instance="kubebrain",window="1h"}) <= 60)`,
		"kubebrain_dbaas:object_store_write_request_sources:count":           `count(kubebrain_dbaas:object_store_request_count:current{request_class="write"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_list_request_sources:count":            `count(kubebrain_dbaas:object_store_request_count:current{request_class="list"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_read_request_sources:count":            `count(kubebrain_dbaas:object_store_request_count:current{request_class="read"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_delete_request_sources:count":          `count(kubebrain_dbaas:object_store_request_count:current{request_class="delete"}) or on() vector(0)`,
		"kubebrain_dbaas:object_request_period_end_sources:count":            `count(kubebrain_dbaas:object_store_request_period_end_seconds:current) or on() vector(0)`,
		"kubebrain_dbaas:object_store_unexpected_request_classes:count":      `count(kubebrain_dbaas:object_store_request_count:current{request_class!~"write|list|read|delete"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_invalid_request_values:count":          `count((kubebrain_dbaas:object_store_request_count:current < 0) or (kubebrain_dbaas:object_store_request_count:current > 9007199254740992) or (kubebrain_dbaas:object_store_request_count:current != floor(kubebrain_dbaas:object_store_request_count:current))) or on() vector(0)`,
		"kubebrain_dbaas:object_store_invalid_period_end_values:count":       `count((kubebrain_dbaas:object_store_request_period_end_seconds:current < 0) or (kubebrain_dbaas:object_store_request_period_end_seconds:current > 9007199254740992) or (kubebrain_dbaas:object_store_request_period_end_seconds:current != floor(kubebrain_dbaas:object_store_request_period_end_seconds:current)) or (kubebrain_dbaas:object_store_request_period_end_seconds:current % 3600 != 0)) or on() vector(0)`,
		"kubebrain_dbaas:object_request_data_complete":                       `(kubebrain_dbaas:object_store_write_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_list_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_read_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_delete_request_sources:count == bool 1) * (kubebrain_dbaas:object_request_period_end_sources:count == bool 1) * (kubebrain_dbaas:object_store_unexpected_request_classes:count == bool 0) * (kubebrain_dbaas:object_store_invalid_request_values:count == bool 0) * (kubebrain_dbaas:object_store_invalid_period_end_values:count == bool 0)`,
		"kubebrain_dbaas:metering_data_complete":                             `(kubebrain_dbaas:replica_expectation_sources:count == bool 3) * (kubebrain_dbaas:cpu_usage_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:memory_working_set_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:network_receive_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:network_transmit_sources:count == bool kubebrain_dbaas:compute_replicas:expected) * (kubebrain_dbaas:metering_invalid_compute_values:count == bool 0) * (kubebrain_dbaas:storage_volumes:expected >= bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:storage_requested_sources:count == bool kubebrain_dbaas:storage_volumes:expected) * (kubebrain_dbaas:storage_volume_identity_mismatches:count == bool 0) * (kubebrain_dbaas:storage_active_volume_sources:count == bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:storage_capacity_sources:count == bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:storage_available_sources:count == bool kubebrain_dbaas:storage_replicas:expected) * (kubebrain_dbaas:storage_volume_stats_identity_mismatches:count == bool 0) * (kubebrain_dbaas:storage_invalid_byte_values:count == bool 0) * (kubebrain_dbaas:logical_backup_artifact_sources:count == bool 1) * (kubebrain_dbaas:logical_backup_timestamp_sources:count == bool 1) * (kubebrain_dbaas:logical_backup_invalid_values:count == bool 0)`,
		"kubebrain_dbaas:cpu_usage_cores:sum":                                `sum(kubebrain_dbaas:cpu_usage_cores:max_by_container) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:sum":                       `sum(kubebrain_dbaas:memory_working_set_bytes:max_by_container) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes_per_second:sum":               `sum(kubebrain_dbaas:network_receive_bytes_per_second:max_by_interface) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes_per_second:sum":              `sum(kubebrain_dbaas:network_transmit_bytes_per_second:max_by_interface) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_provisioned_bytes:sum":                      `sum(kubebrain_dbaas:storage_requested_bytes:max_by_pvc) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_used_bytes:sum":                             `clamp_min(sum(kubebrain_dbaas:storage_capacity_bytes:max_by_pvc) - sum(kubebrain_dbaas:storage_available_bytes:max_by_pvc), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_artifact_bytes:last":                 `max(kubebrain_dbaas:logical_backup_artifact_bytes:current) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_age_seconds:last":                    `clamp_min(time() - max(kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:metering_hour_complete":                             `(min_over_time(kubebrain_dbaas:metering_data_complete[1h]) == 1) * (count_over_time(kubebrain_dbaas:metering_data_complete[1h]) >= bool 60)`,
		"kubebrain_dbaas:object_request_hour_complete":                       `(min_over_time(kubebrain_dbaas:object_request_data_complete[1h]) == 1) * (count_over_time(kubebrain_dbaas:object_request_data_complete[1h]) >= bool 60)`,
		"kubebrain_dbaas:object_request_period_end:last":                     `max(kubebrain_dbaas:object_store_request_period_end_seconds:current) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:cpu_usage_core_seconds:hour": `(sum(max by (namespace, pod, container) (increase(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",container="kubebrain",image!=""}[1h]))) + ` +
			`sum(max by (namespace, pod, container) (increase(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",container=~"pd|tikv",image!=""}[1h])))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:hour_avg": `avg_over_time(kubebrain_dbaas:memory_working_set_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes:hour": `(sum(max by (namespace, pod, interface) (increase(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[1h]))) + ` +
			`sum(max by (namespace, pod, interface) (increase(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[1h])))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes:hour": `(sum(max by (namespace, pod, interface) (increase(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-9]+",interface="eth0"}[1h]))) + ` +
			`sum(max by (namespace, pod, interface) (increase(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-9]+",interface="eth0"}[1h])))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:storage_provisioned_bytes:hour_avg": `avg_over_time(kubebrain_dbaas:storage_provisioned_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:storage_used_bytes:hour_avg":        `avg_over_time(kubebrain_dbaas:storage_used_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_write_requests:hour":   `sum(kubebrain_dbaas:object_store_request_count:current{request_class="write"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_list_requests:hour":    `sum(kubebrain_dbaas:object_store_request_count:current{request_class="list"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_read_requests:hour":    `sum(kubebrain_dbaas:object_store_request_count:current{request_class="read"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:object_store_delete_requests:hour":  `sum(kubebrain_dbaas:object_store_request_count:current{request_class="delete"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
	}
	expected["kubebrain_dbaas:watch_event_buffer_stale_drop:current_by_pod"] =
		`max by (namespace, pod, uid) (watch_event_buffer_stale_drop{namespace="kubebrain-system"} and (time() - timestamp(watch_event_buffer_stale_drop{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_event_buffer_stale_drop:rate_5m_by_pod"] =
		`max by (namespace, pod, uid) (rate(watch_event_buffer_stale_drop{namespace="kubebrain-system"}[5m]) and (time() - timestamp(watch_event_buffer_stale_drop{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_event_buffer_full:current_by_pod"] =
		`max by (namespace, pod, uid) (watch_event_buffer_full{namespace="kubebrain-system"} and (time() - timestamp(watch_event_buffer_full{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_event_buffer_full:rate_5m_by_pod"] =
		`max by (namespace, pod, uid) (rate(watch_event_buffer_full{namespace="kubebrain-system"}[5m]) and (time() - timestamp(watch_event_buffer_full{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_event_buffer_invalid_values:count"] = watchEventBufferInvalidValuesExpr
	expected["kubebrain_dbaas:watch_collector_stalled:current_by_pod"] =
		`max by (namespace, pod, uid) (watch_collector_stalled{namespace="kubebrain-system"} and (time() - timestamp(watch_collector_stalled{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_collector_stalled:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(watch_collector_stalled{namespace="kubebrain-system"}[10m]) and (time() - timestamp(watch_collector_stalled{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_collector_skipped_revision:current_by_pod"] =
		`max by (namespace, pod, uid) (watch_collector_skipped_revision{namespace="kubebrain-system"} and (time() - timestamp(watch_collector_skipped_revision{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_collector_skipped_revision:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(watch_collector_skipped_revision{namespace="kubebrain-system"}[10m]) and (time() - timestamp(watch_collector_skipped_revision{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_collector_invalid_values:count"] = watchCollectorInvalidValuesExpr
	expected["kubebrain_dbaas:watch_collector_recovery:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (watch_collector_recovery{namespace="kubebrain-system",outcome=~"replayed|empty|failed"} and (time() - timestamp(watch_collector_recovery{namespace="kubebrain-system",outcome=~"replayed|empty|failed"}) <= 60))`
	expected["kubebrain_dbaas:watch_collector_recovery:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (increase(watch_collector_recovery{namespace="kubebrain-system",outcome=~"replayed|empty|failed"}[10m]) and (time() - timestamp(watch_collector_recovery{namespace="kubebrain-system",outcome=~"replayed|empty|failed"}) <= 60))`
	expected["kubebrain_dbaas:watch_collector_recovery_invalid_values:count"] = watchCollectorRecoveryInvalidValuesExpr
	expected["kubebrain_dbaas:watch_backend_integrity_failure:current_by_pod_kind"] =
		`max by (namespace, pod, uid, kind) (watch_backend_integrity_failure{namespace="kubebrain-system",kind=~"invalid_result|invalid_revision"} and (time() - timestamp(watch_backend_integrity_failure{namespace="kubebrain-system",kind=~"invalid_result|invalid_revision"}) <= 60))`
	expected["kubebrain_dbaas:watch_backend_integrity_failure:increase_10m_by_pod_kind"] =
		`max by (namespace, pod, uid, kind) (increase(watch_backend_integrity_failure{namespace="kubebrain-system",kind=~"invalid_result|invalid_revision"}[10m]) and (time() - timestamp(watch_backend_integrity_failure{namespace="kubebrain-system",kind=~"invalid_result|invalid_revision"}) <= 60))`
	expected["kubebrain_dbaas:watch_backend_integrity_failure_invalid_values:count"] = watchBackendIntegrityInvalidValuesExpr
	expected["kubebrain_dbaas:durable_revision_persist_err:current_by_pod"] =
		`max by (namespace, pod, uid) (revision_durable_persist_err{namespace="kubebrain-system"} and (time() - timestamp(revision_durable_persist_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:durable_revision_persist_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(revision_durable_persist_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(revision_durable_persist_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:durable_revision_persist_err_invalid_values:count"] = durableRevisionPersistInvalidValuesExpr
	expected["kubebrain_dbaas:event_log_corruption:current_by_pod_kind"] =
		`max by (namespace, pod, uid, kind) (watch_event_log_corruption{namespace="kubebrain-system",kind=~"malformed|witness_mismatch|incomplete|invalid_object"} and (time() - timestamp(watch_event_log_corruption{namespace="kubebrain-system",kind=~"malformed|witness_mismatch|incomplete|invalid_object"}) <= 60))`
	expected["kubebrain_dbaas:event_log_corruption:increase_10m_by_pod_kind"] =
		`max by (namespace, pod, uid, kind) (increase(watch_event_log_corruption{namespace="kubebrain-system",kind=~"malformed|witness_mismatch|incomplete|invalid_object"}[10m]) and (time() - timestamp(watch_event_log_corruption{namespace="kubebrain-system",kind=~"malformed|witness_mismatch|incomplete|invalid_object"}) <= 60))`
	expected["kubebrain_dbaas:event_log_corrupt_alarm_failed:current_by_pod"] =
		`max by (namespace, pod, uid) (watch_event_log_corrupt_alarm_failed{namespace="kubebrain-system"} and (time() - timestamp(watch_event_log_corrupt_alarm_failed{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:event_log_corrupt_alarm_failed:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(watch_event_log_corrupt_alarm_failed{namespace="kubebrain-system"}[10m]) and (time() - timestamp(watch_event_log_corrupt_alarm_failed{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:event_log_integrity_invalid_values:count"] = eventLogIntegrityInvalidValuesExpr
	expected["kubebrain_dbaas:read_integrity_fence:current_by_pod_target_outcome"] =
		`max by (namespace, pod, uid, target, outcome) (read_integrity_fence{namespace="kubebrain-system",target=~"object|revision_index",outcome=~"armed|failed"} and (time() - timestamp(read_integrity_fence{namespace="kubebrain-system",target=~"object|revision_index",outcome=~"armed|failed"}) <= 60))`
	expected["kubebrain_dbaas:read_integrity_fence:increase_10m_by_pod_target_outcome"] =
		`max by (namespace, pod, uid, target, outcome) (increase(read_integrity_fence{namespace="kubebrain-system",target=~"object|revision_index",outcome=~"armed|failed"}[10m]) and (time() - timestamp(read_integrity_fence{namespace="kubebrain-system",target=~"object|revision_index",outcome=~"armed|failed"}) <= 60))`
	expected["kubebrain_dbaas:read_integrity_fence_invalid_values:count"] = readIntegrityFenceInvalidValuesExpr
	expected["kubebrain_dbaas:commit_wait_failure:current_by_pod_reason"] =
		`max by (namespace, pod, uid, reason) (write_commit_wait_failure{namespace="kubebrain-system",reason=~"context_done|backstop"} and (time() - timestamp(write_commit_wait_failure{namespace="kubebrain-system",reason=~"context_done|backstop"}) <= 60))`
	expected["kubebrain_dbaas:commit_wait_failure:increase_10m_by_pod_reason"] =
		`max by (namespace, pod, uid, reason) (increase(write_commit_wait_failure{namespace="kubebrain-system",reason=~"context_done|backstop"}[10m]) and (time() - timestamp(write_commit_wait_failure{namespace="kubebrain-system",reason=~"context_done|backstop"}) <= 60))`
	expected["kubebrain_dbaas:commit_wait_failure_invalid_values:count"] = commitWaitFailureInvalidValuesExpr
	expected["kubebrain_dbaas:storage_gc_enabled:max_by_pod"] =
		`max by (namespace, pod, uid) (storage_gc_enabled{namespace="kubebrain-system"} and (time() - timestamp(storage_gc_enabled{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:storage_gc_err:current_by_pod"] =
		`max by (namespace, pod, uid) (storage_gc_err{namespace="kubebrain-system"} and (time() - timestamp(storage_gc_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:storage_gc_err:increase_30m_by_pod"] =
		`max by (namespace, pod, uid) (increase(storage_gc_err{namespace="kubebrain-system"}[30m]) and (time() - timestamp(storage_gc_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:storage_gc_invalid_values:count"] = storageGCInvalidValuesExpr
	expected["kubebrain_dbaas:storage_gc_driver_started_timestamp_seconds:current_by_pod"] =
		`max by (namespace, pod, uid) (storage_gc_driver_started_timestamp_seconds{namespace="kubebrain-system"} and (time() - timestamp(storage_gc_driver_started_timestamp_seconds{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:storage_gc_last_success_timestamp_seconds:current_by_pod"] =
		`max by (namespace, pod, uid) (storage_gc_last_success_timestamp_seconds{namespace="kubebrain-system"} and (time() - timestamp(storage_gc_last_success_timestamp_seconds{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:storage_gc_progress_invalid_values:count"] = storageGCProgressInvalidValuesExpr
	expected["kubebrain_dbaas:storage_compaction_failure:current_by_pod_stage"] =
		`max by (namespace, pod, uid, stage) (storage_compaction_failure{namespace="kubebrain-system",stage=~"full_scan|incremental|auto|watermark|batch|key_delete"} and (time() - timestamp(storage_compaction_failure{namespace="kubebrain-system",stage=~"full_scan|incremental|auto|watermark|batch|key_delete"}) <= 60))`
	expected["kubebrain_dbaas:storage_compaction_failure:increase_10m_by_pod_stage"] =
		`max by (namespace, pod, uid, stage) (increase(storage_compaction_failure{namespace="kubebrain-system",stage=~"full_scan|incremental|auto|watermark|batch|key_delete"}[10m]) and (time() - timestamp(storage_compaction_failure{namespace="kubebrain-system",stage=~"full_scan|incremental|auto|watermark|batch|key_delete"}) <= 60))`
	expected["kubebrain_dbaas:storage_compaction_failure_invalid_values:count"] = storageCompactionFailureInvalidValuesExpr
	expected["kubebrain_dbaas:lease_lifecycle:current_by_pod_operation"] =
		`max by (namespace, pod, uid, operation) (label_replace((etcd_debugging_lease_granted_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_lease_granted_total{namespace="kubebrain-system"}) <= 60)), "operation", "grant", "namespace", ".*") or label_replace((etcd_debugging_lease_revoked_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_lease_revoked_total{namespace="kubebrain-system"}) <= 60)), "operation", "revoke", "namespace", ".*") or label_replace((etcd_debugging_lease_renewed_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_lease_renewed_total{namespace="kubebrain-system"}) <= 60)), "operation", "renew", "namespace", ".*") or label_replace((etcd_debugging_server_lease_expired_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_server_lease_expired_total{namespace="kubebrain-system"}) <= 60)), "operation", "expire", "namespace", ".*"))`
	expected["kubebrain_dbaas:lease_lifecycle:increase_10m_by_pod_operation"] =
		`max by (namespace, pod, uid, operation) (label_replace((increase(etcd_debugging_lease_granted_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_debugging_lease_granted_total{namespace="kubebrain-system"}) <= 60)), "operation", "grant", "namespace", ".*") or label_replace((increase(etcd_debugging_lease_revoked_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_debugging_lease_revoked_total{namespace="kubebrain-system"}) <= 60)), "operation", "revoke", "namespace", ".*") or label_replace((increase(etcd_debugging_lease_renewed_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_debugging_lease_renewed_total{namespace="kubebrain-system"}) <= 60)), "operation", "renew", "namespace", ".*") or label_replace((increase(etcd_debugging_server_lease_expired_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_debugging_server_lease_expired_total{namespace="kubebrain-system"}) <= 60)), "operation", "expire", "namespace", ".*"))`
	expected["kubebrain_dbaas:lease_lifecycle_invalid_values:count"] = leaseLifecycleInvalidValuesExpr
	expected["kubebrain_dbaas:lease_startup_restore_failure:current_by_pod"] =
		`max by (namespace, pod, uid) (lease_startup_restore_failure{namespace="kubebrain-system"} and (time() - timestamp(lease_startup_restore_failure{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:lease_startup_restore_failure:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(lease_startup_restore_failure{namespace="kubebrain-system"}[10m]) and (time() - timestamp(lease_startup_restore_failure{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:lease_startup_restore_failure_invalid_values:count"] = leaseStartupRestoreInvalidValuesExpr
	expected["kubebrain_dbaas:uncertain_txn_resolution:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (txn_uncertain_resolution{namespace="kubebrain-system",outcome=~"retry|committed|not_committed|witness_corrupt|corrupt_alarm_failed"} and (time() - timestamp(txn_uncertain_resolution{namespace="kubebrain-system",outcome=~"retry|committed|not_committed|witness_corrupt|corrupt_alarm_failed"}) <= 60))`
	expected["kubebrain_dbaas:uncertain_txn_resolution:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (increase(txn_uncertain_resolution{namespace="kubebrain-system",outcome=~"retry|committed|not_committed|witness_corrupt|corrupt_alarm_failed"}[10m]) and (time() - timestamp(txn_uncertain_resolution{namespace="kubebrain-system",outcome=~"retry|committed|not_committed|witness_corrupt|corrupt_alarm_failed"}) <= 60))`
	expected["kubebrain_dbaas:uncertain_txn_resolution_invalid_values:count"] = uncertainTxnInvalidValuesExpr
	expected["kubebrain_dbaas:restart_witness_corruption:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (txn_witness_restart_corruption{namespace="kubebrain-system",outcome=~"armed|failed"} and (time() - timestamp(txn_witness_restart_corruption{namespace="kubebrain-system",outcome=~"armed|failed"}) <= 60))`
	expected["kubebrain_dbaas:restart_witness_corruption:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (increase(txn_witness_restart_corruption{namespace="kubebrain-system",outcome=~"armed|failed"}[10m]) and (time() - timestamp(txn_witness_restart_corruption{namespace="kubebrain-system",outcome=~"armed|failed"}) <= 60))`
	expected["kubebrain_dbaas:restart_witness_corruption_invalid_values:count"] = restartWitnessCorruptionInvalidValuesExpr
	expected["kubebrain_dbaas:lease_uncertain_reconcile:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (label_replace((lease_uncertain_reconcile_retry{namespace="kubebrain-system"} and (time() - timestamp(lease_uncertain_reconcile_retry{namespace="kubebrain-system"}) <= 60)), "outcome", "retry", "namespace", ".*") or label_replace((lease_uncertain_reconcile_success{namespace="kubebrain-system"} and (time() - timestamp(lease_uncertain_reconcile_success{namespace="kubebrain-system"}) <= 60)), "outcome", "success", "namespace", ".*") or label_replace((lease_uncertain_reconcile_err{namespace="kubebrain-system"} and (time() - timestamp(lease_uncertain_reconcile_err{namespace="kubebrain-system"}) <= 60)), "outcome", "err", "namespace", ".*"))`
	expected["kubebrain_dbaas:lease_uncertain_reconcile:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (label_replace((increase(lease_uncertain_reconcile_retry{namespace="kubebrain-system"}[10m]) and (time() - timestamp(lease_uncertain_reconcile_retry{namespace="kubebrain-system"}) <= 60)), "outcome", "retry", "namespace", ".*") or label_replace((increase(lease_uncertain_reconcile_success{namespace="kubebrain-system"}[10m]) and (time() - timestamp(lease_uncertain_reconcile_success{namespace="kubebrain-system"}) <= 60)), "outcome", "success", "namespace", ".*") or label_replace((increase(lease_uncertain_reconcile_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(lease_uncertain_reconcile_err{namespace="kubebrain-system"}) <= 60)), "outcome", "err", "namespace", ".*"))`
	expected["kubebrain_dbaas:lease_uncertain_reconcile_invalid_values:count"] = leaseUncertainReconcileInvalidValuesExpr
	expected["kubebrain_dbaas:lease_background_failure:current_by_pod_operation"] =
		`max by (namespace, pod, uid, operation) (lease_background_failure{namespace="kubebrain-system",operation=~"checkpoint|expire_delete|expire_corrupt_deferred"} and (time() - timestamp(lease_background_failure{namespace="kubebrain-system",operation=~"checkpoint|expire_delete|expire_corrupt_deferred"}) <= 60))`
	expected["kubebrain_dbaas:lease_background_failure:increase_10m_by_pod_operation"] =
		`max by (namespace, pod, uid, operation) (increase(lease_background_failure{namespace="kubebrain-system",operation=~"checkpoint|expire_delete|expire_corrupt_deferred"}[10m]) and (time() - timestamp(lease_background_failure{namespace="kubebrain-system",operation=~"checkpoint|expire_delete|expire_corrupt_deferred"}) <= 60))`
	expected["kubebrain_dbaas:lease_background_failure_invalid_values:count"] = leaseBackgroundFailureInvalidValuesExpr
	expected["kubebrain_dbaas:lease_orphan_sweep_failure:current_by_pod_stage"] =
		`max by (namespace, pod, uid, stage) (lease_orphan_sweep_failure{namespace="kubebrain-system",stage=~"load|migration|seal|user_read|legacy_attachment_read|legacy_attachment_invalid|key_delete|key_compare|attachment_delete"} and (time() - timestamp(lease_orphan_sweep_failure{namespace="kubebrain-system",stage=~"load|migration|seal|user_read|legacy_attachment_read|legacy_attachment_invalid|key_delete|key_compare|attachment_delete"}) <= 60))`
	expected["kubebrain_dbaas:lease_orphan_sweep_failure:increase_10m_by_pod_stage"] =
		`max by (namespace, pod, uid, stage) (increase(lease_orphan_sweep_failure{namespace="kubebrain-system",stage=~"load|migration|seal|user_read|legacy_attachment_read|legacy_attachment_invalid|key_delete|key_compare|attachment_delete"}[10m]) and (time() - timestamp(lease_orphan_sweep_failure{namespace="kubebrain-system",stage=~"load|migration|seal|user_read|legacy_attachment_read|legacy_attachment_invalid|key_delete|key_compare|attachment_delete"}) <= 60))`
	expected["kubebrain_dbaas:lease_orphan_sweep_failure_invalid_values:count"] = leaseOrphanSweepFailureInvalidValuesExpr
	expected["kubebrain_dbaas:lease_grant_cleanup:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (lease_grant_cleanup{namespace="kubebrain-system",outcome=~"retry|success|handoff"} and (time() - timestamp(lease_grant_cleanup{namespace="kubebrain-system",outcome=~"retry|success|handoff"}) <= 60))`
	expected["kubebrain_dbaas:lease_grant_cleanup:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (increase(lease_grant_cleanup{namespace="kubebrain-system",outcome=~"retry|success|handoff"}[10m]) and (time() - timestamp(lease_grant_cleanup{namespace="kubebrain-system",outcome=~"retry|success|handoff"}) <= 60))`
	expected["kubebrain_dbaas:lease_grant_cleanup_invalid_values:count"] = leaseGrantCleanupInvalidValuesExpr
	expected["kubebrain_dbaas:lease_revoke_reconcile:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (lease_revoke_reconcile{namespace="kubebrain-system",outcome=~"retry|success|handoff"} and (time() - timestamp(lease_revoke_reconcile{namespace="kubebrain-system",outcome=~"retry|success|handoff"}) <= 60))`
	expected["kubebrain_dbaas:lease_revoke_reconcile:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (increase(lease_revoke_reconcile{namespace="kubebrain-system",outcome=~"retry|success|handoff"}[10m]) and (time() - timestamp(lease_revoke_reconcile{namespace="kubebrain-system",outcome=~"retry|success|handoff"}) <= 60))`
	expected["kubebrain_dbaas:lease_revoke_reconcile_invalid_values:count"] = leaseRevokeReconcileInvalidValuesExpr
	expected["kubebrain_dbaas:serializable_checkpoint_available:current_by_pod"] =
		`max by (namespace, pod, uid) (serializable_checkpoint_available{namespace="kubebrain-system"} and (time() - timestamp(serializable_checkpoint_available{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:serializable_checkpoint_revision:current_by_pod"] =
		`max by (namespace, pod, uid) (serializable_checkpoint_revision{namespace="kubebrain-system"} and (time() - timestamp(serializable_checkpoint_revision{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:serializable_checkpoint_remaining_seconds:current_by_pod"] =
		`max by (namespace, pod, uid) (serializable_checkpoint_remaining_seconds{namespace="kubebrain-system"} and (time() - timestamp(serializable_checkpoint_remaining_seconds{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:serializable_checkpoint_invalid_values:count"] = checkpointInvalidValuesExpr
	expected["kubebrain_dbaas:health_checkpoint_fallback:increase_10m_by_pod_check"] =
		`max by (namespace, pod, uid, check) (increase(health_checkpoint_fallback{namespace="kubebrain-system",check=~"alarm|serializable_read|data_corruption"}[10m]) and (time() - timestamp(health_checkpoint_fallback{namespace="kubebrain-system",check=~"alarm|serializable_read|data_corruption"}) <= 60))`
	expected["kubebrain_dbaas:health_checkpoint_fallback_invalid_values:count"] = healthFallbackInvalidValuesExpr
	expected["kubebrain_dbaas:serializable_checkpoint_refresh_err:current_by_pod"] =
		`max by (namespace, pod, uid) (serializable_checkpoint_refresh_err{namespace="kubebrain-system"} and (time() - timestamp(serializable_checkpoint_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:serializable_checkpoint_refresh_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(serializable_checkpoint_refresh_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(serializable_checkpoint_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:serializable_checkpoint_refresh_err_invalid_values:count"] = checkpointRefreshInvalidValuesExpr
	expected["kubebrain_dbaas:quota_refresh_err:current_by_pod"] =
		`max by (namespace, pod, uid) (quota_refresh_err{namespace="kubebrain-system"} and (time() - timestamp(quota_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:quota_refresh_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(quota_refresh_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(quota_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:quota_refresh_err_invalid_values:count"] = quotaRefreshInvalidValuesExpr
	expected["kubebrain_dbaas:count_index_rebuild_err:current_by_pod"] =
		`max by (namespace, pod, uid) (count_index_rebuild_err{namespace="kubebrain-system"} and (time() - timestamp(count_index_rebuild_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:count_index_rebuild_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(count_index_rebuild_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(count_index_rebuild_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:count_index_rebuild_err_invalid_values:count"] = countIndexRebuildInvalidValuesExpr
	expected["kubebrain_dbaas:range_stream_failure:current_by_pod"] =
		`max by (namespace, pod, uid) (backend_list_by_stream_failed{namespace="kubebrain-system"} and (time() - timestamp(backend_list_by_stream_failed{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:range_stream_failure:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(backend_list_by_stream_failed{namespace="kubebrain-system"}[10m]) and (time() - timestamp(backend_list_by_stream_failed{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:range_stream_failure_invalid_values:count"] = rangeStreamFailureInvalidValuesExpr
	expected["kubebrain_dbaas:read_index:current_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (label_replace((etcd_server_slow_read_indexes_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_slow_read_indexes_total{namespace="kubebrain-system"}) <= 60)), "outcome", "slow", "namespace", ".*") or label_replace((etcd_server_read_indexes_failed_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_read_indexes_failed_total{namespace="kubebrain-system"}) <= 60)), "outcome", "failed", "namespace", ".*"))`
	expected["kubebrain_dbaas:read_index:increase_10m_by_pod_outcome"] =
		`max by (namespace, pod, uid, outcome) (label_replace((increase(etcd_server_slow_read_indexes_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_server_slow_read_indexes_total{namespace="kubebrain-system"}) <= 60)), "outcome", "slow", "namespace", ".*") or label_replace((increase(etcd_server_read_indexes_failed_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_server_read_indexes_failed_total{namespace="kubebrain-system"}) <= 60)), "outcome", "failed", "namespace", ".*"))`
	expected["kubebrain_dbaas:read_index_invalid_values:count"] = readIndexInvalidValuesExpr
	expected["kubebrain_dbaas:alarm_refresh_err:current_by_pod"] =
		`max by (namespace, pod, uid) (alarm_refresh_err{namespace="kubebrain-system"} and (time() - timestamp(alarm_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:alarm_refresh_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(alarm_refresh_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(alarm_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:alarm_refresh_err_invalid_values:count"] = alarmRefreshInvalidValuesExpr
	expected["kubebrain_dbaas:alarm_corrupt_active:max_by_pod"] =
		`max by (namespace, pod, uid) (alarm_corrupt_active{namespace="kubebrain-system"} and (time() - timestamp(alarm_corrupt_active{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:alarm_corrupt_active_invalid_values:count"] =
		`count(((kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) < 0) or ((kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) > 1) or ((kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current) != floor(kubebrain_dbaas:alarm_corrupt_active:max_by_pod * on(namespace, pod, uid) group_left() kubebrain_dbaas:ready_pods:current)))`
	expected["kubebrain_dbaas:auth_revision:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_auth_revision{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_auth_revision{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:auth_revision_refresh_err:current_by_pod"] =
		`max by (namespace, pod, uid) (auth_revision_refresh_err{namespace="kubebrain-system"} and (time() - timestamp(auth_revision_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:auth_revision_refresh_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(auth_revision_refresh_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(auth_revision_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:auth_revision_invalid_values:count"] = authRevisionInvalidValuesExpr
	expected["kubebrain_dbaas:mvcc_current_revision:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_current_revision{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_current_revision{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_compact_revision:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_compact_revision{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_compact_revision{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_compact_revision_refresh_err:current_by_pod"] =
		`max by (namespace, pod, uid) (mvcc_compact_revision_refresh_err{namespace="kubebrain-system"} and (time() - timestamp(mvcc_compact_revision_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_compact_revision_refresh_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(mvcc_compact_revision_refresh_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(mvcc_compact_revision_refresh_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_revision_invalid_values:count"] = mvccRevisionInvalidValuesExpr
	expected["kubebrain_dbaas:mvcc_live_keys:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_keys_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_keys_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_live_keys_refresh_miss:current_by_pod"] =
		`max by (namespace, pod, uid) (mvcc_keys_total_refresh_miss{namespace="kubebrain-system"} and (time() - timestamp(mvcc_keys_total_refresh_miss{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_live_keys_refresh_miss:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(mvcc_keys_total_refresh_miss{namespace="kubebrain-system"}[10m]) and (time() - timestamp(mvcc_keys_total_refresh_miss{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_live_keys_refresh_last_success_timestamp_seconds:current_by_pod"] =
		`max by (namespace, pod, uid) (mvcc_keys_total_refresh_last_success_timestamp_seconds{namespace="kubebrain-system"} and (time() - timestamp(mvcc_keys_total_refresh_last_success_timestamp_seconds{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_live_keys_invalid_values:count"] = mvccLiveKeysInvalidValuesExpr
	expected["kubebrain_dbaas:mvcc_watch_streams:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_watch_stream_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_watch_stream_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_watchers:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_watcher_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_watcher_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_slow_watchers:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_slow_watcher_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_slow_watcher_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_watch_gauges_invalid_values:count"] = watchGaugeInvalidValuesExpr
	expected["kubebrain_dbaas:mvcc_pending_watch_events:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_pending_events_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_pending_events_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_delivered_watch_events:current_by_pod"] =
		`max by (namespace, pod, uid) (etcd_debugging_mvcc_events_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_debugging_mvcc_events_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_delivered_watch_events:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(etcd_debugging_mvcc_events_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_debugging_mvcc_events_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:mvcc_watch_event_delivery_invalid_values:count"] = watchEventDeliveryInvalidValuesExpr
	expected["kubebrain_dbaas:server_stream_failure:current_by_pod_api_type"] =
		`max by (namespace, pod, uid, api, failure_type) (label_replace(label_replace((etcd_network_server_stream_failures_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_network_server_stream_failures_total{namespace="kubebrain-system"}) <= 60)), "api", "$1", "API", "(.*)"), "failure_type", "$1", "Type", "(.*)"))`
	expected["kubebrain_dbaas:server_stream_failure:increase_10m_by_pod_api_type"] =
		`max by (namespace, pod, uid, api, failure_type) (label_replace(label_replace((increase(etcd_network_server_stream_failures_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_network_server_stream_failures_total{namespace="kubebrain-system"}) <= 60)), "api", "$1", "API", "(.*)"), "failure_type", "$1", "Type", "(.*)"))`
	expected["kubebrain_dbaas:server_stream_failure_invalid_values:count"] = serverStreamFailureInvalidValuesExpr
	expected["kubebrain_dbaas:watch_prev_kv_budget_exhausted:current_by_pod"] =
		`max by (namespace, pod, uid) (watch_prev_kv_budget_exhausted{namespace="kubebrain-system"} and (time() - timestamp(watch_prev_kv_budget_exhausted{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_prev_kv_budget_exhausted:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(watch_prev_kv_budget_exhausted{namespace="kubebrain-system"}[10m]) and (time() - timestamp(watch_prev_kv_budget_exhausted{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:watch_prev_kv_budget_exhausted_invalid_values:count"] = watchPrevKVBudgetInvalidValuesExpr
	expected["kubebrain_dbaas:known_peers:max_by_pod_local_remote"] =
		`max by (namespace, pod, uid, Local, Remote) (etcd_network_known_peers{namespace="kubebrain-system"} and (time() - timestamp(etcd_network_known_peers{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:known_peers_invalid_values:count"] =
		`count(kubebrain_dbaas:known_peers:max_by_pod_local_remote != 1) + count(kubebrain_dbaas:known_peers:max_by_pod_local_remote{Local!~"[1-9a-f][0-9a-f]*"} or kubebrain_dbaas:known_peers:max_by_pod_local_remote{Remote!~"[1-9a-f][0-9a-f]*"})`
	expected["kubebrain_dbaas:server_identity:max_by_pod_local"] =
		`max by (namespace, pod, uid, Local) (label_replace(etcd_server_id{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_id{namespace="kubebrain-system"}) <= 60), "Local", "$1", "server_id", "(.*)"))`
	expected["kubebrain_dbaas:server_identity_invalid_values:count"] =
		`count(kubebrain_dbaas:server_identity:max_by_pod_local != 1) + count(kubebrain_dbaas:server_identity:max_by_pod_local{Local!~"[1-9a-f][0-9a-f]*"})`
	expected["kubebrain_dbaas:server_has_leader:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_server_has_leader{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_has_leader{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:server_is_leader:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_server_is_leader{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_is_leader{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:server_is_learner:max_by_pod"] =
		`max by (namespace, pod, uid) (etcd_server_is_learner{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_is_learner{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:server_leader_changes:current_by_pod"] =
		`max by (namespace, pod, uid) (etcd_server_leader_changes_seen_total{namespace="kubebrain-system"} and (time() - timestamp(etcd_server_leader_changes_seen_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:server_leader_changes:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(etcd_server_leader_changes_seen_total{namespace="kubebrain-system"}[10m]) and (time() - timestamp(etcd_server_leader_changes_seen_total{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:server_state_invalid_values:count"] = serverStateInvalidValuesExpr
	expected["kubebrain_dbaas:fd_used:max_by_pod"] =
		`max by (namespace, pod, uid) (os_fd_used{namespace="kubebrain-system"} and (time() - timestamp(os_fd_used{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:fd_limit:max_by_pod"] =
		`max by (namespace, pod, uid) (os_fd_limit{namespace="kubebrain-system"} and (time() - timestamp(os_fd_limit{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:fd_refresh_err:current_by_pod_type"] =
		`max by (namespace, pod, uid, type) (fd_refresh_err{namespace="kubebrain-system",type=~"used|limit"} and (time() - timestamp(fd_refresh_err{namespace="kubebrain-system",type=~"used|limit"}) <= 60))`
	expected["kubebrain_dbaas:fd_refresh_err:increase_30m_by_pod_type"] =
		`max by (namespace, pod, uid, type) (increase(fd_refresh_err{namespace="kubebrain-system",type=~"used|limit"}[30m]) and (time() - timestamp(fd_refresh_err{namespace="kubebrain-system",type=~"used|limit"}) <= 60))`
	expected["kubebrain_dbaas:fd_refresh_last_success_timestamp_seconds:current_by_pod_type"] =
		`max by (namespace, pod, uid, type) (fd_refresh_last_success_timestamp_seconds{namespace="kubebrain-system",type=~"used|limit"} and (time() - timestamp(fd_refresh_last_success_timestamp_seconds{namespace="kubebrain-system",type=~"used|limit"}) <= 60))`
	expected["kubebrain_dbaas:fd_invalid_values:count"] = fdInvalidValuesExpr
	expected["kubebrain_dbaas:leader_election_lost:current_by_pod"] =
		`sum by (namespace, pod, uid) (max by (namespace, pod, uid, addr) (leader_election_lost{namespace="kubebrain-system"} and (time() - timestamp(leader_election_lost{namespace="kubebrain-system"}) <= 60)))`
	expected["kubebrain_dbaas:leader_election_lost:increase_10m_by_pod"] =
		`sum by (namespace, pod, uid) (max by (namespace, pod, uid, addr) (increase(leader_election_lost{namespace="kubebrain-system"}[10m]) and (time() - timestamp(leader_election_lost{namespace="kubebrain-system"}) <= 60)))`
	expected["kubebrain_dbaas:leader_initialize_err:current_by_pod"] =
		`max by (namespace, pod, uid) (leader_election_initialize_err{namespace="kubebrain-system"} and (time() - timestamp(leader_election_initialize_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:leader_initialize_err:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(leader_election_initialize_err{namespace="kubebrain-system"}[10m]) and (time() - timestamp(leader_election_initialize_err{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:leader_incompatible_witness:current_by_pod"] =
		`max by (namespace, pod, uid) (leader_election_initialize_incompatible_witness{namespace="kubebrain-system"} and (time() - timestamp(leader_election_initialize_incompatible_witness{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:leader_incompatible_witness:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(leader_election_initialize_incompatible_witness{namespace="kubebrain-system"}[10m]) and (time() - timestamp(leader_election_initialize_incompatible_witness{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:leader_invalid_alarm_metadata:current_by_pod"] =
		`max by (namespace, pod, uid) (leader_election_initialize_invalid_alarm_metadata{namespace="kubebrain-system"} and (time() - timestamp(leader_election_initialize_invalid_alarm_metadata{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:leader_invalid_alarm_metadata:increase_10m_by_pod"] =
		`max by (namespace, pod, uid) (increase(leader_election_initialize_invalid_alarm_metadata{namespace="kubebrain-system"}[10m]) and (time() - timestamp(leader_election_initialize_invalid_alarm_metadata{namespace="kubebrain-system"}) <= 60))`
	expected["kubebrain_dbaas:leader_election_invalid_values:count"] = leaderElectionInvalidValuesExpr
	expected["kubebrain_dbaas:leader_initialize_err_invalid_values:count"] = leaderInitializeInvalidValuesExpr
	expected["kubebrain_dbaas:leader_serving_initialization_err:current_by_pod_stage"] =
		`max by (namespace, pod, uid, stage) (leader_serving_initialization_err{namespace="kubebrain-system",stage=~"compact|quota|lease|event_log|checkpoint"} and (time() - timestamp(leader_serving_initialization_err{namespace="kubebrain-system",stage=~"compact|quota|lease|event_log|checkpoint"}) <= 60))`
	expected["kubebrain_dbaas:leader_serving_initialization_err:increase_10m_by_pod_stage"] =
		`max by (namespace, pod, uid, stage) (increase(leader_serving_initialization_err{namespace="kubebrain-system",stage=~"compact|quota|lease|event_log|checkpoint"}[10m]) and (time() - timestamp(leader_serving_initialization_err{namespace="kubebrain-system",stage=~"compact|quota|lease|event_log|checkpoint"}) <= 60))`
	expected["kubebrain_dbaas:leader_serving_initialization_err_invalid_values:count"] = servingInitializationInvalidValuesExpr
	expected["kubebrain_dbaas:logical_backup_last_success_timestamp_seconds:current"] =
		`kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"} and (time() - timestamp(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}) <= 60)`
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
	require.Contains(t, incompleteDescription, "CPU/network rate is non-finite, negative, or above 2^53")
	require.Contains(t, incompleteDescription, "memory byte value is not an exact non-negative integer within 2^53")
	require.Contains(t, incompleteDescription, "invalid usage cannot accumulate an apparently billable slot")
	requestIncomplete := prometheusRuleByAlert(
		t, groups, "KubeBrainObjectRequestMeteringDataIncomplete",
	)
	require.Equal(t, `(kubebrain_dbaas:object_request_data_complete or on() vector(0)) != 1`,
		requestIncomplete["expr"])
	require.Equal(t, "15m", requestIncomplete["for"])
	require.Equal(t, "critical", requestIncomplete["labels"].(map[string]any)["severity"])
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"recording series itself is absent")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"within 60 seconds")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"Prometheus lookback values cannot be re-timestamped")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"missing or unexpected request_class")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"taxonomy drift cannot be hidden by the allowlist")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"negative, fractional, or greater-than-2^53 value")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"exact UTC hour boundary")
	require.Contains(t, requestIncomplete["annotations"].(map[string]any)["description"],
		"invalid billing inputs cannot accumulate an apparently complete slot")
}

func TestProductionAlertMetricsExist(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)

	emitted := emittedMetricNames(t, "../../pkg")
	// Include external metrics and source metrics whose Emit call uses a named
	// constant rather than a string literal (which emittedMetricNames cannot
	// discover with its deliberately small source scanner).
	for _, indirectOrExternal := range []string{
		"etcd_disk_wal_fsync_duration_seconds_bucket",
		"etcd_disk_wal_fsync_duration_seconds_count",
		"etcd_server_read_indexes_failed_total",
		"etcd_server_is_leader",
		"etcd_server_slow_read_indexes_total",
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
		"kube_pod_spec_volumes_persistentvolumeclaims_info",
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
		emitted[indirectOrExternal] = struct{}{}
	}
	for _, rawGroup := range groups {
		group := rawGroup.(map[string]any)
		rules, ok := group["rules"].([]any)
		require.True(t, ok)
		for _, rawRule := range rules {
			candidate := rawRule.(map[string]any)
			if record, ok := candidate["record"].(string); ok {
				emitted[record] = struct{}{}
			}
		}
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
	emitRE := regexp.MustCompile(`Emit(Counter|Gauge|Histogram)\(\s*"([^"]+)"`)
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
