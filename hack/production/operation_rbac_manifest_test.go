package production_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type rbacManifest struct {
	Kind      string `yaml:"kind"`
	Metadata  rbacMetadata
	Automount *bool       `yaml:"automountServiceAccountToken"`
	Rules     []rbacRule  `yaml:"rules"`
	Subjects  []rbacParty `yaml:"subjects"`
	RoleRef   rbacParty   `yaml:"roleRef"`
}

type rbacMetadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type rbacRule struct {
	APIGroups     []string `yaml:"apiGroups"`
	Resources     []string `yaml:"resources"`
	ResourceNames []string `yaml:"resourceNames"`
	Verbs         []string `yaml:"verbs"`
}

type rbacParty struct {
	Kind      string `yaml:"kind"`
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type auditAdmissionManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		FailurePolicy     string   `yaml:"failurePolicy"`
		PolicyName        string   `yaml:"policyName"`
		ValidationActions []string `yaml:"validationActions"`
		MatchConstraints  struct {
			ResourceRules []struct {
				APIGroups   []string `yaml:"apiGroups"`
				APIVersions []string `yaml:"apiVersions"`
				Operations  []string `yaml:"operations"`
				Resources   []string `yaml:"resources"`
				Scope       string   `yaml:"scope"`
			} `yaml:"resourceRules"`
		} `yaml:"matchConstraints"`
		Validations []struct {
			Expression string `yaml:"expression"`
			Message    string `yaml:"message"`
		} `yaml:"validations"`
	}
}

func TestOperationSubmitterRBACIsNamespacedAndCannotMutateStatus(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-submitter-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 3)

	serviceAccount := documents[0]
	require.Equal(t, "ServiceAccount", serviceAccount.Kind)
	require.Equal(t, "kubebrain-operation-submitter", serviceAccount.Metadata.Name)
	require.Equal(t, "kubebrain-operations", serviceAccount.Metadata.Namespace)
	require.NotNil(t, serviceAccount.Automount)
	require.False(t, *serviceAccount.Automount)

	role := documents[1]
	require.Equal(t, "Role", role.Kind)
	require.Equal(t, "kubebrain-operations", role.Metadata.Namespace)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"create", "get", "list", "watch"},
	}}, role.Rules)

	binding := documents[2]
	require.Equal(t, "RoleBinding", binding.Kind)
	require.Equal(t, "kubebrain-operations", binding.Metadata.Namespace)
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-operation-submitter",
		Namespace: "kubebrain-operations",
	}, binding.Subjects[0])
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-submitter"}, binding.RoleRef)
}

func TestLegacySnapshotRemediationRequesterCanOnlySubmitImmutableEvidence(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-legacy-snapshot-remediation-requester-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 3)
	require.Equal(t, "kubebrain-legacy-snapshot-remediation-requester", documents[0].Metadata.Name)
	require.NotNil(t, documents[0].Automount)
	require.False(t, *documents[0].Automount)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, documents[1].Rules)
	for _, rule := range documents[1].Rules {
		require.NotContains(t, rule.Resources, "kubebrainoperations/status")
		require.NotContains(t, rule.Verbs, "update")
		require.NotContains(t, rule.Verbs, "patch")
		require.NotContains(t, rule.Verbs, "delete")
	}
}

func TestNativePITRFullBackupRequesterCanOnlyCreateBoundOperationAndSecret(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-native-pitr-full-backup-requester-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 3)
	require.Equal(t, "kubebrain-native-pitr-full-backup-requester", documents[0].Metadata.Name)
	require.NotNil(t, documents[0].Automount)
	require.False(t, *documents[0].Automount)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, documents[1].Rules)
	for _, rule := range documents[1].Rules {
		require.NotContains(t, rule.Resources, "kubebrainoperations/status")
		for _, forbidden := range []string{"update", "patch", "delete", "list", "watch"} {
			require.NotContains(t, rule.Verbs, forbidden)
		}
	}
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-native-pitr-full-backup-requester",
		Namespace: "kubebrain-operations",
	}, documents[2].Subjects[0])
}

func TestOperationAPIRBACCanOnlySubmitAndReadOperations(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-api.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 6)

	serviceAccount := documents[0]
	require.Equal(t, "ServiceAccount", serviceAccount.Kind)
	require.Equal(t, "kubebrain-operation-api", serviceAccount.Metadata.Name)
	require.Equal(t, "kubebrain-operations", serviceAccount.Metadata.Namespace)
	require.NotNil(t, serviceAccount.Automount)
	require.True(t, *serviceAccount.Automount)

	role := documents[1]
	require.Equal(t, "Role", role.Kind)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"create", "get"},
	}}, role.Rules)

	binding := documents[2]
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-operation-api",
		Namespace: "kubebrain-operations",
	}, binding.Subjects[0])
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-api"}, binding.RoleRef)
}

func TestOperationApproverRBACCanOnlyApproveExistingOperations(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-approver-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 3)

	serviceAccount := documents[0]
	require.Equal(t, "ServiceAccount", serviceAccount.Kind)
	require.Equal(t, "kubebrain-operation-approver", serviceAccount.Metadata.Name)
	require.Equal(t, "kubebrain-operations", serviceAccount.Metadata.Namespace)
	require.NotNil(t, serviceAccount.Automount)
	require.False(t, *serviceAccount.Automount)

	role := documents[1]
	require.Equal(t, "Role", role.Kind)
	require.Equal(t, "kubebrain-operations", role.Metadata.Namespace)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"get", "list", "watch", "update"},
	}}, role.Rules)

	binding := documents[2]
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-operation-approver",
		Namespace: "kubebrain-operations",
	}, binding.Subjects[0])
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-approver"}, binding.RoleRef)
}

func TestOperationWorkerRBACCanFenceWithLeasesButCannotCreateOperations(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-worker-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 4)

	serviceAccount := documents[1]
	require.Equal(t, "ServiceAccount", serviceAccount.Kind)
	require.Equal(t, "kubebrain-operation-worker", serviceAccount.Metadata.Name)
	require.Equal(t, "kubebrain-operations", serviceAccount.Metadata.Namespace)

	role := documents[2]
	require.Equal(t, "Role", role.Kind)
	require.Equal(t, "kubebrain-operations", role.Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{
			APIGroups:     []string{""},
			Resources:     []string{"configmaps"},
			ResourceNames: []string{"kubebrain-backup-scheduler-inventory", "kubebrain-tikv-repair-operation-inventory"},
			Verbs:         []string{"get"},
		},
		{
			APIGroups: []string{"dbaas.kubebrain.io"},
			Resources: []string{"kubebrainoperations"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{"dbaas.kubebrain.io"},
			Resources: []string{"kubebrainoperations/status"},
			Verbs:     []string{"get", "update", "patch"},
		},
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{"create", "get", "update", "delete"},
		},
	}, role.Rules)

	binding := documents[3]
	require.Equal(t, "RoleBinding", binding.Kind)
	require.Len(t, binding.Subjects, 16)
	for _, name := range []string{
		"kubebrain-backup-executor", "kubebrain-backup-deletion-executor",
		"kubebrain-native-pitr-full-backup-executor",
		"kubebrain-native-pitr-full-restore-executor",
		"kubebrain-native-pitr-target-retirement-executor",
		"kubebrain-native-pitr-target-provisioning-executor",
		"kubebrain-cold-physical-snapshot-executor",
		"kubebrain-cold-physical-restore-executor",
		"kubebrain-legacy-snapshot-remediation-executor",
		"kubebrain-restore-cutover-executor",
		"kubebrain-post-restore-audit-executor",
		"kubebrain-certificate-rotation-executor", "kubebrain-info-certificate-rotation-executor", "kubebrain-destroy-executor",
		"kubebrain-tikv-transaction-repair-executor",
		"kubebrain-tikv-transaction-recovery-executor",
	} {
		require.Contains(t, binding.Subjects, rbacParty{
			Kind: "ServiceAccount", Name: name, Namespace: "kubebrain-operations",
		})
	}
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-worker"}, binding.RoleRef)
}

func TestTiKVTransactionRecoveryRBACCannotDeleteStorage(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-tikv-transaction-recovery-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 6)
	require.Equal(t, "kubebrain-system", documents[0].Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, ResourceNames: []string{"kubebrain"}, Verbs: []string{"get", "watch"}},
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets/scale"}, ResourceNames: []string{"kubebrain"}, Verbs: []string{"get", "patch", "update"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"create"}},
	}, documents[0].Rules)
	require.Equal(t, "tidb-cluster", documents[2].Metadata.Namespace)
	for _, rule := range documents[2].Rules {
		require.NotContains(t, rule.Verbs, "delete")
		require.NotContains(t, rule.Verbs, "patch")
		require.NotContains(t, rule.Verbs, "update")
	}
	require.Equal(t, "ClusterRole", documents[4].Kind)
	require.Equal(t, []rbacRule{{APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: []string{"get"}}}, documents[4].Rules)
	require.Equal(t, rbacParty{Kind: "ClusterRole", Name: "kubebrain-tikv-transaction-recovery-pv-reader"}, documents[5].RoleRef)
}

func TestColdPhysicalSnapshotRBACCanCreateButNeverDeleteRetainedSnapshots(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-cold-physical-snapshot-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 6)
	for _, document := range documents {
		for _, rule := range document.Rules {
			require.NotContains(t, rule.Verbs, "delete")
			require.NotContains(t, rule.Verbs, "deletecollection")
			if rbacContains(rule.Resources, "volumesnapshots") {
				require.Contains(t, rule.Verbs, "create")
			}
			if rbacContains(rule.Resources, "volumesnapshotcontents") {
				require.Equal(t, []string{"get"}, rule.Verbs)
			}
		}
	}
}

func rbacContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func TestColdPhysicalSnapshotRequesterCanOnlySubmitImmutableEvidence(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-cold-physical-snapshot-requester-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 3)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, documents[1].Rules)
}

func TestColdPhysicalRestoreRBACCreatesButNeverDeletesIsolatedResources(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-cold-physical-restore-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 4)
	created := map[string]bool{}
	for _, document := range documents {
		for _, rule := range document.Rules {
			require.NotContains(t, rule.Verbs, "delete")
			require.NotContains(t, rule.Verbs, "deletecollection")
			if rbacContains(rule.Verbs, "create") {
				for _, resource := range rule.Resources {
					created[resource] = true
				}
			}
		}
	}
	for _, resource := range []string{"tidbclusters", "persistentvolumeclaims", "volumesnapshots", "volumesnapshotcontents"} {
		require.True(t, created[resource], resource)
	}
	require.False(t, created["persistentvolumes"])
}

func TestColdPhysicalRestoreRequesterCanOnlySubmitAndReadTargetUIDs(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-cold-physical-restore-requester-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 5)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, documents[1].Rules)
	require.Equal(t, []rbacRule{{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{"kube-system", "tidb-cluster"}, Verbs: []string{"get"}}}, documents[3].Rules)
}

func TestTiKVTransactionRecoveryRequesterCanOnlyReadIdentityAndSubmit(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-tikv-transaction-recovery-requester-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 7)
	require.Equal(t, "ServiceAccount", documents[0].Kind)
	require.Equal(t, "kubebrain-repair-operations", documents[0].Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, documents[1].Rules)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"apps"}, Resources: []string{"statefulsets"},
		ResourceNames: []string{"kubebrain"}, Verbs: []string{"get"},
	}}, documents[3].Rules)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"pingcap.com"}, Resources: []string{"tidbclusters"},
		ResourceNames: []string{"kb"}, Verbs: []string{"get"},
	}}, documents[5].Rules)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, forbidden := range []string{"pods/exec", "statefulsets/scale", "persistentvolumeclaims", "verbs: [delete", "verbs: [patch", "verbs: [update"} {
		require.NotContains(t, string(data), forbidden)
	}
}

func TestTiKVQuiescedRepairRequesterCanOnlyReadFrozenTargetsAndSubmit(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-tikv-quiesced-repair-requester-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 7)
	require.Equal(t, "ServiceAccount", documents[0].Kind)
	require.Equal(t, "kubebrain-repair-operations", documents[0].Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, documents[1].Rules)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"apps"}, Resources: []string{"statefulsets"},
		ResourceNames: []string{"kubebrain"}, Verbs: []string{"get"},
	}}, documents[3].Rules)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"pingcap.com"}, Resources: []string{"tidbclusters"}, ResourceNames: []string{"kb"}, Verbs: []string{"get"}},
		{APIGroups: []string{""}, Resources: []string{"services/proxy"}, ResourceNames: []string{"http:kb-pd:2379"}, Verbs: []string{"get"}},
	}, documents[5].Rules)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, forbidden := range []string{"pods", "pods/exec", "statefulsets/scale", "persistentvolumeclaims", "verbs: [delete", "verbs: [patch", "verbs: [update"} {
		require.NotContains(t, string(data), forbidden)
	}
}

func TestTiKVTransactionRepairRBACIsNamespacedAndCannotDeletePVCs(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-tikv-transaction-repair-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 9)
	require.Equal(t, "Namespace", documents[0].Kind)
	require.Equal(t, "kubebrain-repair-state", documents[0].Metadata.Name)
	require.Equal(t, "kubebrain-repair-state", documents[1].Metadata.Namespace)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{""}, Resources: []string{"configmaps"},
		Verbs: []string{"create", "get", "patch", "delete"},
	}}, documents[1].Rules)
	require.Equal(t, "kubebrain-system", documents[3].Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets"}, ResourceNames: []string{"kubebrain"}, Verbs: []string{"get", "watch"}},
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets/scale"}, ResourceNames: []string{"kubebrain"}, Verbs: []string{"get", "patch", "update"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"create"}},
	}, documents[3].Rules)
	require.Equal(t, "tidb-cluster", documents[5].Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"pingcap.com"}, Resources: []string{"tidbclusters"}, ResourceNames: []string{"kb"}, Verbs: []string{"get"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch", "delete"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"create"}},
		{APIGroups: []string{""}, Resources: []string{"services/proxy"}, ResourceNames: []string{"http:kb-pd:2379"}, Verbs: []string{"get"}},
		{
			APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"},
			ResourceNames: []string{"pd-kb-pd-0", "pd-kb-pd-1", "pd-kb-pd-2", "tikv-kb-tikv-0", "tikv-kb-tikv-1", "tikv-kb-tikv-2"},
			Verbs:         []string{"get"},
		},
	}, documents[5].Rules)
	require.Equal(t, "ClusterRole", documents[7].Kind)
	require.Equal(t, "kubebrain-tikv-transaction-repair-pv-reader", documents[7].Metadata.Name)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: []string{"get"},
	}}, documents[7].Rules)
	require.Equal(t, "ClusterRoleBinding", documents[8].Kind)
	require.Equal(t, rbacParty{Kind: "ClusterRole", Name: "kubebrain-tikv-transaction-repair-pv-reader"}, documents[8].RoleRef)
}

func TestTiKVRepairAlertReceiverUsesAnIsolatedNonDestructiveQueue(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-tikv-repair-alert-receiver.yaml")
	documents := decodeRBACManifest(t, path)

	var receiverRole, parameterReader rbacManifest
	var workerBinding, approverBinding rbacManifest
	for _, document := range documents {
		switch {
		case document.Kind == "Role" && document.Metadata.Name == "kubebrain-tikv-repair-alert-receiver":
			receiverRole = document
		case document.Kind == "Role" && document.Metadata.Name == "kubebrain-tikv-repair-parameter-reader":
			parameterReader = document
		case document.Kind == "RoleBinding" && document.Metadata.Name == "kubebrain-tikv-repair-operation-worker":
			workerBinding = document
		case document.Kind == "RoleBinding" && document.Metadata.Name == "kubebrain-tikv-repair-operation-approver":
			approverBinding = document
		}
	}

	require.Equal(t, "kubebrain-repair-operations", receiverRole.Metadata.Namespace)
	require.Equal(t, []rbacRule{
		{APIGroups: []string{"dbaas.kubebrain.io"}, Resources: []string{"kubebrainoperations"}, Verbs: []string{"create", "get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get"}},
	}, receiverRole.Rules)
	require.Empty(t, parameterReader.Rules, "repair executors must fetch parameters through the broker")
	require.Equal(t, rbacParty{Kind: "ClusterRole", Name: "kubebrain-operation-worker-managed-namespace"}, workerBinding.RoleRef)
	require.ElementsMatch(t, []rbacParty{
		{Kind: "ServiceAccount", Name: "kubebrain-tikv-transaction-repair-executor", Namespace: "kubebrain-operations"},
		{Kind: "ServiceAccount", Name: "kubebrain-tikv-transaction-recovery-executor", Namespace: "kubebrain-operations"},
	}, workerBinding.Subjects)
	require.Equal(t, rbacParty{Kind: "ClusterRole", Name: "kubebrain-operation-approver-managed-namespace"}, approverBinding.RoleRef)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(data), "persistentvolumeclaims")
	require.NotContains(t, string(data), "pods/exec")
	require.NotContains(t, string(data), "verbs: [delete")
	require.NotContains(t, string(data), "kubebrain-tikv-repair-parameter-reader")
}

func TestOperationArchiverRBACCanOnlyReadAndReleaseOperations(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-archiver-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 5)

	serviceAccount := documents[0]
	require.Equal(t, "ServiceAccount", serviceAccount.Kind)
	require.Equal(t, "kubebrain-operation-archiver", serviceAccount.Metadata.Name)
	require.Equal(t, "kubebrain-operations", serviceAccount.Metadata.Namespace)
	require.NotNil(t, serviceAccount.Automount)
	require.True(t, *serviceAccount.Automount)

	role := documents[1]
	require.Equal(t, "Role", role.Kind)
	require.Equal(t, []rbacRule{
		{
			APIGroups:     []string{""},
			Resources:     []string{"configmaps"},
			ResourceNames: []string{"kubebrain-backup-scheduler-inventory"},
			Verbs:         []string{"get"},
		},
		{
			APIGroups: []string{"dbaas.kubebrain.io"},
			Resources: []string{"kubebrainoperations"},
			Verbs:     []string{"get", "list", "update"},
		},
	}, role.Rules)

	binding := documents[2]
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-operation-archiver",
		Namespace: "kubebrain-operations",
	}, binding.Subjects[0])
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-archiver"}, binding.RoleRef)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "readinessProbe:")
	require.Contains(t, string(data), "- ACTION=probe")
	require.Contains(t, string(data), "- TIMEOUT=15s")
	require.Contains(t, string(data), "- /usr/local/bin/kubebrain-logical-object")
}

func TestOperationAuditAdmissionRequiresFinalizerAndReleaseEvidence(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-audit-admission.yaml")
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	decoder := yaml.NewDecoder(file)

	var policy auditAdmissionManifest
	require.NoError(t, decoder.Decode(&policy))
	require.Equal(t, "ValidatingAdmissionPolicy", policy.Kind)
	require.Equal(t, "Fail", policy.Spec.FailurePolicy)
	require.Len(t, policy.Spec.MatchConstraints.ResourceRules, 1)
	rule := policy.Spec.MatchConstraints.ResourceRules[0]
	require.Equal(t, []string{"dbaas.kubebrain.io"}, rule.APIGroups)
	require.Equal(t, []string{"v1alpha1"}, rule.APIVersions)
	require.Equal(t, []string{"CREATE", "UPDATE"}, rule.Operations)
	require.Equal(t, []string{"kubebrainoperations"}, rule.Resources)
	require.Equal(t, "Namespaced", rule.Scope)
	require.Len(t, policy.Spec.Validations, 8)
	require.Contains(t, policy.Spec.Validations[0].Expression, operationaudit.Finalizer)
	require.Contains(t, policy.Spec.Validations[0].Expression, "size(object.metadata.finalizers) == 1")
	require.Contains(t, policy.Spec.Validations[0].Expression, `request.operation != "UPDATE"`)
	require.Contains(t, policy.Spec.Validations[0].Expression, "object.metadata.finalizers.all")
	require.Contains(t, policy.Spec.Validations[1].Expression, operationaudit.ApprovedByAnnotation)
	require.Equal(t, "operations cannot be created pre-archived", policy.Spec.Validations[2].Message)
	require.Contains(t, policy.Spec.Validations[2].Expression, operationaudit.ReceiptSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[2].Expression, operationaudit.ArtifactSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[2].Expression, operationaudit.VersionAnnotation)
	require.Equal(t,
		"operation audit archive evidence can only be introduced by archiver release",
		policy.Spec.Validations[3].Message,
	)
	require.Contains(t, policy.Spec.Validations[3].Expression, "kubebrain-operation-archiver")
	require.Contains(t, policy.Spec.Validations[3].Expression, operationaudit.Finalizer)
	require.Contains(t, policy.Spec.Validations[3].Expression, `object.status.phase in ["Succeeded", "Failed"]`)
	require.Contains(t, policy.Spec.Validations[3].Expression, operationaudit.ReceiptSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[3].Expression, operationaudit.ArtifactSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[3].Expression, operationaudit.VersionAnnotation)
	require.Contains(t, policy.Spec.Validations[4].Expression, "request.userInfo.username")
	require.Contains(t, policy.Spec.Validations[4].Expression, "kubebrain-operation-approver")
	for _, operationType := range []string{"BackupDeletion", "ColdPhysicalSnapshot", "ColdPhysicalRestore", "LegacySnapshotHistoryRemediation", "RestoreCutover", "CertificateRotation", "InfoCertificateRotation", "TiKVTransactionRepair", "TiKVTransactionRecovery", "Destroy"} {
		require.Contains(t, policy.Spec.Validations[4].Expression, operationType)
	}
	require.NotContains(t, policy.Spec.Validations[4].Expression, `"Backup"`)
	require.NotContains(t, policy.Spec.Validations[4].Expression, `"PostRestoreAudit"`)
	require.Contains(t, policy.Spec.Validations[5].Expression, operationaudit.ApprovalIDAnnotation)
	require.Equal(t, "operation audit archive evidence is immutable", policy.Spec.Validations[6].Message)
	require.Contains(t, policy.Spec.Validations[6].Expression, operationaudit.ReceiptSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[6].Expression, operationaudit.ArtifactSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[6].Expression, operationaudit.VersionAnnotation)
	require.Contains(t, policy.Spec.Validations[6].Expression,
		`object.metadata.annotations["`+operationaudit.ReceiptSHAAnnotation+`"] == oldObject.metadata.annotations["`+operationaudit.ReceiptSHAAnnotation+`"]`)
	require.Contains(t, policy.Spec.Validations[6].Expression,
		`object.metadata.annotations["`+operationaudit.ArtifactSHAAnnotation+`"] == oldObject.metadata.annotations["`+operationaudit.ArtifactSHAAnnotation+`"]`)
	require.Contains(t, policy.Spec.Validations[6].Expression,
		`object.metadata.annotations["`+operationaudit.VersionAnnotation+`"] == oldObject.metadata.annotations["`+operationaudit.VersionAnnotation+`"]`)
	require.Contains(t, policy.Spec.Validations[7].Expression, "request.userInfo.username")
	require.Contains(t, policy.Spec.Validations[7].Expression, "kubebrain-operation-archiver")
	require.Contains(t, policy.Spec.Validations[7].Expression, operationaudit.ReceiptSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[7].Expression, operationaudit.ArtifactSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[7].Expression, operationaudit.VersionAnnotation)
	require.Contains(t, policy.Spec.Validations[7].Expression,
		`object.metadata.annotations["`+operationaudit.ReceiptSHAAnnotation+`"] == oldObject.metadata.annotations["`+operationaudit.ReceiptSHAAnnotation+`"]`)
	require.Contains(t, policy.Spec.Validations[7].Expression,
		`object.metadata.annotations["`+operationaudit.ArtifactSHAAnnotation+`"] == oldObject.metadata.annotations["`+operationaudit.ArtifactSHAAnnotation+`"]`)
	require.Contains(t, policy.Spec.Validations[7].Expression,
		`object.metadata.annotations["`+operationaudit.VersionAnnotation+`"] == oldObject.metadata.annotations["`+operationaudit.VersionAnnotation+`"]`)

	var binding auditAdmissionManifest
	require.NoError(t, decoder.Decode(&binding))
	require.Equal(t, "ValidatingAdmissionPolicyBinding", binding.Kind)
	require.Equal(t, "kubebrain-operation-audit", binding.Spec.PolicyName)
	require.Equal(t, []string{"Deny"}, binding.Spec.ValidationActions)
}

func TestManagedNamespaceRBACDefinesUnboundLeastPrivilegeRoles(t *testing.T) {
	path := filepath.Join(
		"..", "..", "deploy", "production", "kubebrain-operation-managed-namespace-rbac.yaml",
	)
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 4)
	for _, document := range documents {
		require.Equal(t, "ClusterRole", document.Kind)
		require.Empty(t, document.Metadata.Namespace)
		require.Empty(t, document.Subjects, "the reusable role must not grant itself to any identity")
	}
	require.Equal(t, "kubebrain-operation-worker-managed-namespace", documents[0].Metadata.Name)
	require.Equal(t, []rbacRule{
		{
			APIGroups: []string{"dbaas.kubebrain.io"},
			Resources: []string{"kubebrainoperations"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{"dbaas.kubebrain.io"},
			Resources: []string{"kubebrainoperations/status"},
			Verbs:     []string{"get", "update", "patch"},
		},
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{"create", "get", "update", "delete"},
		},
	}, documents[0].Rules)
	require.Equal(t, "kubebrain-operation-parameter-broker-managed-namespace",
		documents[1].Metadata.Name)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"get"},
	}, {
		APIGroups: []string{""},
		Resources: []string{"secrets"},
		Verbs:     []string{"get"},
	}}, documents[1].Rules)
	require.Equal(t, "kubebrain-operation-approver-managed-namespace", documents[2].Metadata.Name)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"get", "list", "watch", "update"},
	}}, documents[2].Rules)
	require.Equal(t, "kubebrain-operation-archiver-managed-namespace", documents[3].Metadata.Name)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"get", "list", "update"},
	}}, documents[3].Rules)
}

func decodeRBACManifest(t *testing.T, path string) []rbacManifest {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	decoder := yaml.NewDecoder(file)
	var documents []rbacManifest
	for {
		var document rbacManifest
		err := decoder.Decode(&document)
		if err == io.EOF {
			return documents
		}
		require.NoError(t, err)
		documents = append(documents, document)
	}
}
