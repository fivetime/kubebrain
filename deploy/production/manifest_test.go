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
		var workspace, parameterToken, parameterCA *unstructured.Unstructured
		for _, raw := range volumes {
			volume := &unstructured.Unstructured{Object: raw.(map[string]any)}
			switch nestedString(t, volume, "name") {
			case "workspace":
				workspace = volume
			case "parameter-token":
				parameterToken = volume
			case "parameter-ca":
				parameterCA = volume
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
		require.Len(t, sources, 1)
		require.Equal(t, "kubebrain-operation-parameters", nestedString(
			t, &unstructured.Unstructured{Object: sources[0].(map[string]any)},
			"serviceAccountToken", "audience",
		))
		require.Equal(t, "kubebrain-operation-parameter-broker-ca",
			nestedString(t, parameterCA, "configMap", "name"))
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
	require.True(t, nestedBool(t, container, "securityContext", "readOnlyRootFilesystem"))
	require.False(t, nestedBool(t, container, "securityContext", "allowPrivilegeEscalation"))
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
		"kubebrain_dbaas:cpu_usage_sources:count":                `count(rate(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}[5m])) + count(rate(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[5m]))`,
		"kubebrain_dbaas:memory_working_set_sources:count":       `count(container_memory_working_set_bytes{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}) + count(container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""})`,
		"kubebrain_dbaas:network_receive_sources:count":          `count(rate(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + count(rate(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))`,
		"kubebrain_dbaas:network_transmit_sources:count":         `count(rate(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + count(rate(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))`,
		"kubebrain_dbaas:storage_capacity_sources:count":         `count(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"})`,
		"kubebrain_dbaas:storage_available_sources:count":        `count(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"})`,
		"kubebrain_dbaas:logical_backup_artifact_sources:count":  `count(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"})`,
		"kubebrain_dbaas:logical_backup_timestamp_sources:count": `count(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"})`,
		"kubebrain_dbaas:metering_data_complete":                 `(kubebrain_dbaas:cpu_usage_sources:count == bool 9) * (kubebrain_dbaas:memory_working_set_sources:count == bool 9) * (kubebrain_dbaas:network_receive_sources:count == bool 9) * (kubebrain_dbaas:network_transmit_sources:count == bool 9) * (kubebrain_dbaas:storage_capacity_sources:count == bool 6) * (kubebrain_dbaas:storage_available_sources:count == bool 6) * (kubebrain_dbaas:logical_backup_artifact_sources:count == bool 1) * (kubebrain_dbaas:logical_backup_timestamp_sources:count == bool 1)`,
		"kubebrain_dbaas:cpu_usage_cores:sum":                    `(sum(rate(container_cpu_usage_seconds_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}[5m])) + sum(rate(container_cpu_usage_seconds_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""}[5m]))) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:memory_working_set_bytes:sum":           `(sum(container_memory_working_set_bytes{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",container="kubebrain",image!=""}) + sum(container_memory_working_set_bytes{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",container=~"pd|tikv",image!=""})) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_receive_bytes_per_second:sum":   `(sum(rate(container_network_receive_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + sum(rate(container_network_receive_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:network_transmit_bytes_per_second:sum":  `(sum(rate(container_network_transmit_bytes_total{namespace="kubebrain-system",pod=~"kubebrain-[0-2]",interface="eth0"}[5m])) + sum(rate(container_network_transmit_bytes_total{namespace="tidb-cluster",pod=~"kb-(pd|tikv)-[0-2]",interface="eth0"}[5m]))) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_provisioned_bytes:sum":          `sum(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:storage_used_bytes:sum": `clamp_min(sum(kubelet_volume_stats_capacity_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}) - ` +
			`sum(kubelet_volume_stats_available_bytes{namespace="tidb-cluster",persistentvolumeclaim=~"(pd-kb-pd|tikv-kb-tikv)-[0-2]"}), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_artifact_bytes:last": `max(kubebrain_logical_backup_artifact_bytes{instance="kubebrain"}) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
		"kubebrain_dbaas:logical_backup_age_seconds:last":    `clamp_min(time() - max(kubebrain_logical_backup_last_success_timestamp_seconds{instance="kubebrain"}), 0) and on() (kubebrain_dbaas:metering_data_complete == 1)`,
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
