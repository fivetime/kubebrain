package namespaceinventory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestLoadReturnsSortedStrictNamespaceInventory(t *testing.T) {
	client := inventoryClient(t, `["tenant-b","tenant-a"]`)
	namespaces, err := Load(
		context.Background(), client, "control", "inventory", DefaultKey,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-a", "tenant-b"}, namespaces)
}

func TestLoadFailsClosedForUnsafeInventory(t *testing.T) {
	tooMany := make([]string, MaxNamespaces+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("tenant-%d", i)
	}
	tooManyRaw, err := json.Marshal(tooMany)
	require.NoError(t, err)
	for _, raw := range []string{
		`[]`, `null`, `[""]`, `["tenant-a","tenant-a"]`, `["Tenant_A"]`,
		`{"namespace":"tenant-a"}`, `not-json`, string(tooManyRaw),
		`["tenant-a"] {"trailing":true}`,
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := Load(
				context.Background(), inventoryClient(t, raw),
				"control", "inventory", DefaultKey,
			)
			require.Error(t, err)
		})
	}
}

func TestValidateOneRequiresDNSLabelNamespace(t *testing.T) {
	require.NoError(t, ValidateOne("tenant-a"))
	for _, namespace := range []string{"", "tenant.a", "Tenant-A", "-tenant", "tenant-", strings.Repeat("a", 64)} {
		t.Run(namespace, func(t *testing.T) {
			require.Error(t, ValidateOne(namespace))
		})
	}
}

func TestLoadRejectsInvalidInventoryNamespaceBeforeAPI(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	_, err := Load(context.Background(), client, "control.ns", "inventory", DefaultKey)
	require.ErrorContains(t, err, "inventory namespace")
	require.Empty(t, client.Actions(), "invalid inventory namespace must fail before Kubernetes API reads")
}

func inventoryClient(t *testing.T, raw string) *dynamicfake.FakeDynamicClient {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{ConfigMapResource: "ConfigMapList"},
	)
	_, err := client.Resource(ConfigMapResource).Namespace("control").Create(
		context.Background(),
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "inventory"},
			"data":     map[string]any{DefaultKey: raw},
		}},
		metav1.CreateOptions{},
	)
	require.NoError(t, err)
	return client
}
