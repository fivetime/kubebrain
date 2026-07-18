package production_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

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
