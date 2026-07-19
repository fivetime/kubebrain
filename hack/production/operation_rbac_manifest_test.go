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
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
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
		{
			APIGroups: []string{""},
			Resources: []string{"secrets"},
			Verbs:     []string{"get"},
		},
	}, role.Rules)

	binding := documents[3]
	require.Equal(t, "RoleBinding", binding.Kind)
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-operation-worker",
		Namespace: "kubebrain-operations",
	}, binding.Subjects[0])
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-worker"}, binding.RoleRef)
}

func TestOperationArchiverRBACCanOnlyReadAndReleaseOperations(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "production", "kubebrain-operation-archiver-rbac.yaml")
	documents := decodeRBACManifest(t, path)
	require.Len(t, documents, 3)

	serviceAccount := documents[0]
	require.Equal(t, "ServiceAccount", serviceAccount.Kind)
	require.Equal(t, "kubebrain-operation-archiver", serviceAccount.Metadata.Name)
	require.Equal(t, "kubebrain-operations", serviceAccount.Metadata.Namespace)
	require.NotNil(t, serviceAccount.Automount)
	require.True(t, *serviceAccount.Automount)

	role := documents[1]
	require.Equal(t, "Role", role.Kind)
	require.Equal(t, []rbacRule{{
		APIGroups: []string{"dbaas.kubebrain.io"},
		Resources: []string{"kubebrainoperations"},
		Verbs:     []string{"get", "update"},
	}}, role.Rules)

	binding := documents[2]
	require.Equal(t, rbacParty{
		Kind: "ServiceAccount", Name: "kubebrain-operation-archiver",
		Namespace: "kubebrain-operations",
	}, binding.Subjects[0])
	require.Equal(t, rbacParty{Kind: "Role", Name: "kubebrain-operation-archiver"}, binding.RoleRef)
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
	require.Len(t, policy.Spec.Validations, 5)
	require.Contains(t, policy.Spec.Validations[0].Expression, operationaudit.Finalizer)
	require.Contains(t, policy.Spec.Validations[1].Expression, operationaudit.ApprovedByAnnotation)
	require.Contains(t, policy.Spec.Validations[2].Expression, "request.userInfo.username")
	require.Contains(t, policy.Spec.Validations[2].Expression, "kubebrain-operation-approver")
	require.Contains(t, policy.Spec.Validations[3].Expression, operationaudit.ApprovalIDAnnotation)
	require.Contains(t, policy.Spec.Validations[4].Expression, operationaudit.ReceiptSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[4].Expression, operationaudit.ArtifactSHAAnnotation)
	require.Contains(t, policy.Spec.Validations[4].Expression, operationaudit.VersionAnnotation)

	var binding auditAdmissionManifest
	require.NoError(t, decoder.Decode(&binding))
	require.Equal(t, "ValidatingAdmissionPolicyBinding", binding.Kind)
	require.Equal(t, "kubebrain-operation-audit", binding.Spec.PolicyName)
	require.Equal(t, []string{"Deny"}, binding.Spec.ValidationActions)
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
