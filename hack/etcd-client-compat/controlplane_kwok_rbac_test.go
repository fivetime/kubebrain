package compat

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This checks the manifest's exact permission set, not API authorization or
// controller compatibility. The isolated real-apiserver test must prove those.
func TestControlPlaneKWOKRBAC(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "scale-lab", "config", "controlplane-kwok-rbac.json"))
	require.NoError(t, err)
	type reference struct {
		APIGroup string `json:"apiGroup"`
		Kind     string `json:"kind"`
		Name     string `json:"name"`
	}
	type rule struct {
		APIGroups     []string `json:"apiGroups"`
		Resources     []string `json:"resources"`
		Verbs         []string `json:"verbs"`
		ResourceNames []string `json:"resourceNames"`
	}
	type item struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Rules    []rule      `json:"rules"`
		RoleRef  reference   `json:"roleRef"`
		Subjects []reference `json:"subjects"`
	}
	var manifest struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Items      []item `json:"items"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&manifest))
	require.Equal(t, "v1", manifest.APIVersion)
	require.Equal(t, "List", manifest.Kind)
	require.Len(t, manifest.Items, 6)
	expected := map[string][]rule{
		"/ClusterRole/kubebrain-test-kwok": {
			{APIGroups: []string{""}, Resources: []string{"nodes", "pods"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"nodes/status"}, Verbs: []string{"patch", "update"}, ResourceNames: []string{"reference-node"}},
		},
		"controlplane-smoke/Role/kubebrain-test-kwok": {
			{APIGroups: []string{""}, Resources: []string{"pods/status"}, Verbs: []string{"patch", "update"}},
			{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch", "update"}},
		},
		"kube-node-lease/Role/kubebrain-test-kwok": {
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "patch", "update"}, ResourceNames: []string{"reference-node"}},
		},
	}
	roles, bindings := map[string]bool{}, map[string]bool{}
	for _, obj := range manifest.Items {
		require.Equal(t, "rbac.authorization.k8s.io/v1", obj.APIVersion)
		key := fmt.Sprintf("%s/%s/%s", obj.Metadata.Namespace, obj.Kind, obj.Metadata.Name)
		switch obj.Kind {
		case "ClusterRole", "Role":
			require.Contains(t, expected, key)
			require.False(t, roles[key], "duplicate role")
			roles[key] = true
			require.Equal(t, expected[key], obj.Rules, "no wildcard, create/bind pods, secrets, or role administration grants")
			require.Empty(t, obj.Subjects)
			require.Equal(t, reference{}, obj.RoleRef)
		case "ClusterRoleBinding", "RoleBinding":
			kind := strings.TrimSuffix(obj.Kind, "Binding")
			target := fmt.Sprintf("%s/%s/%s", obj.Metadata.Namespace, kind, obj.Metadata.Name)
			require.Contains(t, expected, target)
			require.False(t, bindings[target], "duplicate binding")
			bindings[target] = true
			require.Equal(t, reference{"rbac.authorization.k8s.io", kind, "kubebrain-test-kwok"}, obj.RoleRef)
			require.Equal(t, []reference{{"rbac.authorization.k8s.io", "User", "kubebrain-test-kwok"}}, obj.Subjects)
			require.Empty(t, obj.Rules)
		default:
			t.Fatalf("unexpected object kind %q", obj.Kind)
		}
	}
	require.Len(t, roles, 3)
	require.Equal(t, roles, bindings)
}
