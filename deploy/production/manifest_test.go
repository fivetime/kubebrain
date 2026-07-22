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
			require.Len(t, env, 1)
			podName := env[0].(map[string]any)
			require.Equal(t, "POD_NAME", podName["name"])
			require.Equal(t, "metadata.name", nestedString(t, &unstructured.Unstructured{Object: podName}, "valueFrom", "fieldRef", "fieldPath"))

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
	_, found, err = unstructured.NestedMap(version.Object, "subresources", "status")
	require.NoError(t, err)
	require.True(t, found)
	validations, found, err := unstructured.NestedSlice(
		version.Object, "schema", "openAPIV3Schema", "x-kubernetes-validations")
	require.NoError(t, err)
	require.True(t, found)
	require.GreaterOrEqual(t, len(validations), 7)

	objects := decodeManifest(t, "kubebrain-operation-worker-rbac.yaml")
	account := objectByKindAndName(t, objects, "ServiceAccount", "kubebrain-operation-worker")
	require.False(t, nestedBool(t, account, "automountServiceAccountToken"))
	role := objectByKindAndName(t, objects, "Role", "kubebrain-operation-worker")
	rules, found, err := unstructured.NestedSlice(role.Object, "rules")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, rules, 4)
	require.Equal(t, []any{"configmaps"}, rules[0].(map[string]any)["resources"].([]any))
	require.Equal(t, []any{"kubebrain-backup-scheduler-inventory"},
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
		"kubebrain-backup-deletion-executor": {
			"run-backup-deletion-operation.sh", "kubebrain-backup-deletion-executor-env",
			"kubebrain-backup-deletion-executor-workspace",
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
		"kubebrain-destroy-executor": {
			"run-destroy-operation.sh", "kubebrain-destroy-executor-env",
			"kubebrain-destroy-executor-workspace",
		},
	}
	require.Len(t, objects, len(expected)*2)
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

func TestOperationParameterBrokerOwnsTheOnlyExecutorParameterSecretPermission(t *testing.T) {
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
		"kubebrain-restore-cutover-executor", "kubebrain-post-restore-audit-executor",
		"kubebrain-certificate-rotation-executor", "kubebrain-destroy-executor",
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

	quotaRules := map[string]struct {
		expr     string
		forValue string
		severity string
	}{
		"KubeBrainQuotaNoSpace": {
			expr: `max(quota_nospace{namespace="kubebrain-system"}) > 0`, forValue: "0m", severity: "critical",
		},
		"KubeBrainQuotaUsageHigh": {
			expr: `max(quota_logical_usage_bytes{namespace="kubebrain-system"} / quota_backend_bytes{namespace="kubebrain-system"}) > 0.9`, forValue: "10m", severity: "warning",
		},
		"KubeBrainQuotaMetricsInconsistent": {
			expr: `count(quota_nospace{namespace="kubebrain-system"}) != 3 or count(quota_backend_bytes{namespace="kubebrain-system"}) != 3 or count(quota_logical_usage_bytes{namespace="kubebrain-system"}) != 3 or (max(quota_nospace{namespace="kubebrain-system"}) - min(quota_nospace{namespace="kubebrain-system"}) > 0) or (max(quota_backend_bytes{namespace="kubebrain-system"}) - min(quota_backend_bytes{namespace="kubebrain-system"}) > 0)`, forValue: "1m", severity: "warning",
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
		"KubeBrainPDInsufficientReplicas":      `count(up{namespace="tidb-cluster",service="kb-pd-metrics"} == 1) < 3`,
		"KubeBrainTiKVInsufficientReplicas":    `count(up{namespace="tidb-cluster",service="kb-tikv-metrics"} == 1) < 3`,
		"KubeBrainPDLeaderUnavailable":         `sum(etcd_server_is_leader{namespace="tidb-cluster",service="kb-pd-metrics"}) != 1`,
		"KubeBrainTiKVRegionLeaderMissing":     `max(tikv_raftstore_leader_missing{namespace="tidb-cluster",service="kb-tikv-metrics"}) > 0`,
		"KubeBrainStorageVolumeMetricsMissing": `count(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}) < 6`,
		"KubeBrainStorageVolumeLow": `(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"} / ` +
			`kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}) < 0.15`,
		"KubeBrainResourceMetricsMissing":      `((count(container_memory_working_set_bytes{namespace="kubebrain-system",container="kubebrain",image!=""}) + count(container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""})) < 9) or ((count(kube_pod_container_resource_limits{namespace="kubebrain-system",container="kubebrain",resource="memory",unit="byte"}) + count(kube_pod_container_resource_limits{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",resource="memory",unit="byte"})) < 9)`,
		"KubeBrainDataPlaneMemoryHigh":         `((container_memory_working_set_bytes{namespace="kubebrain-system",container="kubebrain",image!=""} / on(namespace,pod,container) kube_pod_container_resource_limits{namespace="kubebrain-system",container="kubebrain",resource="memory",unit="byte"}) > 0.9) or ((container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""} / on(namespace,pod,container) kube_pod_container_resource_limits{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",resource="memory",unit="byte"}) > 0.9)`,
		"KubeBrainDataPlaneCPUThrottlingHigh":  `((rate(container_cpu_cfs_throttled_periods_total{namespace="kubebrain-system",container="kubebrain",image!=""}[5m]) / rate(container_cpu_cfs_periods_total{namespace="kubebrain-system",container="kubebrain",image!=""}[5m])) > 0.25) or ((rate(container_cpu_cfs_throttled_periods_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[5m]) / rate(container_cpu_cfs_periods_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[5m])) > 0.25)`,
		"KubeBrainNetworkMetricsMissing":       `((count(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}) + count(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"})) < 9) or ((count(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}) + count(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"})) < 9)`,
		"KubeBrainNetworkErrors":               `(sum by (namespace, pod, interface) (increase(container_network_receive_errors_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[10m]) + increase(container_network_transmit_errors_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[10m])) > 0) or (sum by (namespace, pod, interface) (increase(container_network_receive_errors_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[10m]) + increase(container_network_transmit_errors_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[10m])) > 0)`,
		"KubeBrainNetworkPacketDrops":          `(sum by (namespace, pod, interface) (increase(container_network_receive_packets_dropped_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[10m]) + increase(container_network_transmit_packets_dropped_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[10m])) > 0) or (sum by (namespace, pod, interface) (increase(container_network_receive_packets_dropped_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[10m]) + increase(container_network_transmit_packets_dropped_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[10m])) > 0)`,
		"KubeBrainLogicalBackupMetricsMissing": `absent(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}) == 1`,
		"KubeBrainLogicalBackupStale":          `time() - kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"} > 90000`,
	} {
		alertRule := prometheusRuleByAlert(t, groups, alert)
		require.Equal(t, expr, alertRule["expr"])
		switch alert {
		case "KubeBrainStorageVolumeMetricsMissing", "KubeBrainResourceMetricsMissing", "KubeBrainNetworkMetricsMissing":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "15m", alertRule["for"])
		case "KubeBrainLogicalBackupMetricsMissing":
			require.Equal(t, "warning", alertRule["labels"].(map[string]any)["severity"])
			require.Equal(t, "1h", alertRule["for"])
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
		"kubebrain_dbaas:cpu_usage_sources:count":                   `count(rate(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}[5m])) + count(rate(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[5m]))`,
		"kubebrain_dbaas:memory_working_set_sources:count":          `count(container_memory_working_set_bytes{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}) + count(container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""})`,
		"kubebrain_dbaas:network_receive_sources:count":             `count(rate(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + count(rate(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))`,
		"kubebrain_dbaas:network_transmit_sources:count":            `count(rate(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + count(rate(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))`,
		"kubebrain_dbaas:storage_capacity_sources:count":            `count(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"})`,
		"kubebrain_dbaas:storage_available_sources:count":           `count(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"})`,
		"kubebrain_dbaas:logical_backup_artifact_sources:count":     `count(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"})`,
		"kubebrain_dbaas:logical_backup_timestamp_sources:count":    `count(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"})`,
		"kubebrain_dbaas:object_store_write_request_sources:count":  `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="write",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_list_request_sources:count":   `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="list",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_read_request_sources:count":   `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="read",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_store_delete_request_sources:count": `count(kubebrain_object_store_request_count{dbaas_instance="kubebrain",request_class="delete",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_request_period_end_sources:count":   `count(kubebrain_object_store_request_period_end_seconds{dbaas_instance="kubebrain",window="1h"}) or on() vector(0)`,
		"kubebrain_dbaas:object_request_data_complete":              `(kubebrain_dbaas:object_store_write_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_list_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_read_request_sources:count == bool 1) * (kubebrain_dbaas:object_store_delete_request_sources:count == bool 1) * (kubebrain_dbaas:object_request_period_end_sources:count == bool 1)`,
		"kubebrain_dbaas:metering_data_complete":                    `(kubebrain_dbaas:cpu_usage_sources:count == bool 9) * (kubebrain_dbaas:memory_working_set_sources:count == bool 9) * (kubebrain_dbaas:network_receive_sources:count == bool 9) * (kubebrain_dbaas:network_transmit_sources:count == bool 9) * (kubebrain_dbaas:storage_capacity_sources:count == bool 6) * (kubebrain_dbaas:storage_available_sources:count == bool 6) * (kubebrain_dbaas:logical_backup_artifact_sources:count == bool 1) * (kubebrain_dbaas:logical_backup_timestamp_sources:count == bool 1)`,
		"kubebrain_dbaas:cpu_usage_cores:sum":                       `(sum(rate(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}[5m])) + sum(rate(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[5m]))) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:sum":              `(sum(container_memory_working_set_bytes{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}) + sum(container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""})) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes_per_second:sum":      `(sum(rate(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + sum(rate(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes_per_second:sum":     `(sum(rate(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + sum(rate(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_provisioned_bytes:sum":             `sum(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_used_bytes:sum": `clamp_min(sum(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}) - ` +
			`sum(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_artifact_bytes:last": `max(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"}) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_age_seconds:last":    `clamp_min(time() - max(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:metering_hour_complete":             `(min_over_time(kubebrain_dbaas:metering_data_complete[1h]) == 1) * (count_over_time(kubebrain_dbaas:metering_data_complete[1h]) >= bool 60)`,
		"kubebrain_dbaas:object_request_hour_complete":       `(min_over_time(kubebrain_dbaas:object_request_data_complete[1h]) == 1) * (count_over_time(kubebrain_dbaas:object_request_data_complete[1h]) >= bool 60)`,
		"kubebrain_dbaas:object_request_period_end:last":     `max(kubebrain_object_store_request_period_end_seconds{dbaas_instance="kubebrain",window="1h"}) and on() (kubebrain_dbaas:object_request_hour_complete == 1)`,
		"kubebrain_dbaas:cpu_usage_core_seconds:hour": `(sum(increase(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}[1h])) + ` +
			`sum(increase(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[1h]))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:hour_avg": `avg_over_time(kubebrain_dbaas:memory_working_set_bytes:sum[1h]) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes:hour": `(sum(increase(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[1h])) + ` +
			`sum(increase(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[1h]))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes:hour": `(sum(increase(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[1h])) + ` +
			`sum(increase(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[1h]))) and on() (kubebrain_dbaas:metering_hour_complete == 1)`,
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
	require.Equal(t, `kubebrain_dbaas:metering_data_complete != 1`, incompleteRule["expr"])
	require.Equal(t, "15m", incompleteRule["for"])
	require.Equal(t, "critical", incompleteRule["labels"].(map[string]any)["severity"])
	requestIncomplete := prometheusRuleByAlert(
		t, groups, "KubeBrainObjectRequestMeteringDataIncomplete",
	)
	require.Equal(t, `kubebrain_dbaas:object_request_data_complete != 1`,
		requestIncomplete["expr"])
	require.Equal(t, "15m", requestIncomplete["for"])
	require.Equal(t, "critical", requestIncomplete["labels"].(map[string]any)["severity"])
}

func TestProductionAlertMetricsExist(t *testing.T) {
	objects := decodeManifest(t, "monitoring.yaml")
	rule := objectByKindAndName(t, objects, "PrometheusRule", "kubebrain")
	groups, found, err := unstructured.NestedSlice(rule.Object, "spec", "groups")
	require.NoError(t, err)
	require.True(t, found)

	emitted := emittedMetricNames(t, "../../pkg")
	for _, external := range []string{
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
		"kubebrain_object_store_request_count",
		"kubebrain_object_store_request_period_end_seconds",
		"kube_pod_container_resource_limits",
		"kube_statefulset_status_replicas_ready",
		"kubelet_volume_stats_available_bytes",
		"kubelet_volume_stats_capacity_bytes",
		"tikv_raftstore_leader_missing",
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
