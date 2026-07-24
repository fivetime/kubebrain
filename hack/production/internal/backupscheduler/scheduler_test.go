package backupscheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestReconcileIsDeterministicAcrossReplicas(t *testing.T) {
	client := fakeClient()
	ctx := context.Background()
	createTemplate(t, client, validTemplate())
	createPolicy(t, client, false)
	now := time.Unix(1_700_003_000, 0).UTC()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := New(client, "test").WithClock(func() time.Time { return now }).Reconcile(ctx)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	operations, err := client.Resource(operationqueue.Resource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, operations.Items, 1)
	operation := operations.Items[0]
	operationID, _, _ := unstructured.NestedString(operation.Object, "spec", "operationID")
	require.Equal(t, "backup-daily-1700002800", operationID)
	tenant, _, _ := unstructured.NestedString(operation.Object, "spec", "tenant")
	require.Equal(t, "tenant-a", tenant)
	requestedBy, _, _ := unstructured.NestedString(operation.Object, "spec", "requestedBy")
	require.Equal(t, DefaultRequester, requestedBy)
	secretName, _, _ := unstructured.NestedString(
		operation.Object, "spec", "parametersSecretRef", "name",
	)
	require.Equal(t, "params-"+operationID, secretName)

	parameters, err := operationqueue.New(client, "test").Parameters(ctx, operation.GetName())
	require.NoError(t, err)
	var rendered map[string]any
	require.NoError(t, json.Unmarshal(parameters, &rendered))
	require.Equal(t, operationID, rendered["backup_id"])
	require.Equal(t, "/work/"+operationID+".json", rendered["artifact_output"])
	require.Equal(t, "backups/"+operationID+".json", rendered["s3_object_key"])
	require.EqualValues(t, 1_700_089_200, rendered["retain_until_unix"])
	require.EqualValues(t, 1_700_002_800, rendered["scheduled_unix"])
}

func TestReconcileHonorsSuspendAndDoesNotTriggerBeforeFirstSlot(t *testing.T) {
	for _, tc := range []struct {
		name      string
		suspended bool
		now       time.Time
	}{
		{name: "suspended", suspended: true, now: time.Unix(1_700_000_123, 0)},
		{name: "before next slot", now: time.Unix(1_699_999_500, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fakeClient()
			createTemplate(t, client, validTemplate())
			createPolicy(t, client, tc.suspended)
			count, err := New(client, "test").WithClock(func() time.Time { return tc.now }).Reconcile(
				context.Background(),
			)
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func TestReconcileRejectsNonUniqueOutputTemplate(t *testing.T) {
	client := fakeClient()
	template := validTemplate()
	template["s3_object_key"] = "backups/latest.json"
	createTemplate(t, client, template)
	createPolicy(t, client, false)
	_, err := New(client, "test").WithClock(func() time.Time {
		return time.Unix(1_700_003_000, 0)
	}).Reconcile(context.Background())
	require.ErrorContains(t, err, "s3_object_key must contain {operation_id}")
}

func TestReconcileRejectsNullParameterTemplateWithoutCreatingOperation(t *testing.T) {
	client := fakeClient()
	createTemplateRawIn(t, client, "test", []byte("null"))
	createPolicy(t, client, false)
	count, err := New(client, "test").WithClock(func() time.Time {
		return time.Unix(1_700_003_000, 0)
	}).Reconcile(context.Background())
	require.Zero(t, count)
	require.ErrorContains(t, err, "JSON object")
	operations, listErr := client.Resource(operationqueue.Resource).Namespace("test").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Empty(t, operations.Items)
}

func TestReconcileRejectsPolicySchemaBeforeTemplateRead(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
	}{
		{name: "tenant", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(policy.Object, "Tenant_A", "spec", "tenant"))
		}},
		{name: "instance", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(policy.Object, ".invalid", "spec", "instance"))
		}},
		{name: "interval", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(policy.Object, int64(2_592_001), "spec", "intervalSeconds"))
		}},
		{name: "retention", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(policy.Object, int64(3_599), "spec", "retentionSeconds"))
		}},
		{name: "max attempts", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(policy.Object, int64(101), "spec", "maxAttempts"))
		}},
		{name: "template name", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(
				policy.Object, "Invalid_Secret", "spec", "parametersTemplateSecretRef", "name",
			))
		}},
		{name: "template key", mutate: func(t *testing.T, policy *unstructured.Unstructured) {
			require.NoError(t, unstructured.SetNestedField(
				policy.Object, "parameters/json", "spec", "parametersTemplateSecretRef", "key",
			))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fakeClient()
			createPolicy(t, client, false)
			policy, err := client.Resource(PolicyResource).Namespace("test").Get(
				context.Background(), "daily", metav1.GetOptions{},
			)
			require.NoError(t, err)
			tc.mutate(t, policy)
			_, err = client.Resource(PolicyResource).Namespace("test").Update(
				context.Background(), policy, metav1.UpdateOptions{},
			)
			require.NoError(t, err)

			count, err := New(client, "test").WithClock(func() time.Time {
				return time.Unix(1_700_003_000, 0)
			}).Reconcile(context.Background())
			require.Zero(t, count)
			require.ErrorContains(t, err, "policy spec is incomplete")
			operations, listErr := client.Resource(operationqueue.Resource).Namespace("test").
				List(context.Background(), metav1.ListOptions{})
			require.NoError(t, listErr)
			require.Empty(t, operations.Items)
			secrets, listErr := client.Resource(operationqueue.SecretResource).Namespace("test").
				List(context.Background(), metav1.ListOptions{})
			require.NoError(t, listErr)
			require.Empty(t, secrets.Items)
		})
	}
}

func TestReconcileRejectsPreexistingParameterSecretWithWrongOwner(t *testing.T) {
	client := fakeClient()
	ctx := context.Background()
	createTemplate(t, client, validTemplate())
	createPolicy(t, client, false)
	scheduler := New(client, "test").WithClock(func() time.Time {
		return time.Unix(1_700_003_000, 0).UTC()
	})
	policy, err := client.Resource(PolicyResource).Namespace("test").
		Get(ctx, "daily", metav1.GetOptions{})
	require.NoError(t, err)
	const operationID = "backup-daily-1700002800"
	parameters, err := scheduler.renderParameters(
		ctx, "test", "daily-template", "parameters.json",
		operationID, 1_700_002_800, 86_400,
	)
	require.NoError(t, err)
	_, err = client.Resource(operationqueue.SecretResource).Namespace("test").Create(
		ctx,
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]any{
				"name": "params-" + operationID,
				"ownerReferences": []any{map[string]any{
					"apiVersion": policy.GetAPIVersion(),
					"kind":       policy.GetKind(),
					"name":       policy.GetName(),
					"uid":        "replacement-policy-uid",
					"controller": true,
				}},
			},
			"immutable": true,
			"data": map[string]any{
				parametersKey: base64.StdEncoding.EncodeToString(parameters),
			},
		}},
		metav1.CreateOptions{},
	)
	require.NoError(t, err)

	count, err := scheduler.Reconcile(ctx)
	require.Zero(t, count)
	require.ErrorContains(t, err, "different immutable ownership")
	operations, listErr := client.Resource(operationqueue.Resource).Namespace("test").
		List(ctx, metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Empty(t, operations.Items)
}

func TestDecodeParameterTemplatePreservesNumbersAndRejectsTrailingJSON(t *testing.T) {
	parameters, err := decodeParameterTemplate([]byte(`{"batch_size":9007199254740993}`))
	require.NoError(t, err)
	value, ok := parameters["batch_size"].(json.Number)
	require.True(t, ok)
	require.Equal(t, "9007199254740993", value.String())

	_, err = decodeParameterTemplate([]byte(`{"batch_size":1}{"extra":true}`))
	require.ErrorContains(t, err, "trailing JSON")
}

func TestReconcileAcrossNamespacesKeepsQueuesAndSecretsIsolated(t *testing.T) {
	client := fakeClient()
	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		createTemplateIn(t, client, namespace, validTemplate())
		createPolicyIn(t, client, namespace, false)
	}
	now := time.Unix(1_700_003_000, 0).UTC()
	count, err := NewForNamespaces(client, []string{"tenant-a", "tenant-b"}).
		WithClock(func() time.Time { return now }).Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, count)

	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		operations, err := client.Resource(operationqueue.Resource).Namespace(namespace).
			List(context.Background(), metav1.ListOptions{})
		require.NoError(t, err)
		require.Len(t, operations.Items, 1)
		require.Equal(t, namespace, operations.Items[0].GetNamespace())
		secretName, _, _ := unstructured.NestedString(
			operations.Items[0].Object, "spec", "parametersSecretRef", "name",
		)
		_, err = client.Resource(operationqueue.SecretResource).Namespace(namespace).
			Get(context.Background(), secretName, metav1.GetOptions{})
		require.NoError(t, err)
	}
}

func TestStaticNamespaceAllowlistFailsClosedBeforePolicyAccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		namespaces []string
		wantError  string
	}{
		{name: "empty", namespaces: nil, wantError: "namespace allowlist must not be empty"},
		{name: "invalid", namespaces: []string{"tenant.a"}, wantError: "invalid namespace tenant.a"},
		{name: "duplicate", namespaces: []string{"tenant-a", "tenant-a"}, wantError: "duplicate tenant-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fakeClient()
			count, err := NewForNamespaces(client, tc.namespaces).Reconcile(context.Background())
			require.Zero(t, count)
			require.ErrorContains(t, err, tc.wantError)
			require.Empty(t, client.Actions())
		})
	}
}

func TestReconcileAcrossNamespacesIsolatesPolicyFailure(t *testing.T) {
	client := fakeClient()
	createTemplateIn(t, client, "tenant-a", validTemplate())
	createPolicyIn(t, client, "tenant-a", false)
	createPolicyIn(t, client, "tenant-b", false)
	now := time.Unix(1_700_003_000, 0).UTC()

	count, err := NewForNamespaces(client, []string{"tenant-b", "tenant-a"}).
		WithClock(func() time.Time { return now }).Reconcile(context.Background())
	require.Equal(t, 1, count)
	require.ErrorContains(t, err, "tenant-b/daily")
	operations, listErr := client.Resource(operationqueue.Resource).Namespace("tenant-a").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, listErr)
	require.Len(t, operations.Items, 1)
}

func TestReconcileResumesAfterLastAttemptWhenDeadlineExpires(t *testing.T) {
	client := fakeClient()
	for _, name := range []string{"a", "b", "c"} {
		createPolicyNamedIn(t, client, "tenant-a", name, false)
	}
	scheduler := NewForNamespaces(client, []string{"tenant-a"})
	require.NoError(t, scheduler.SetMaxPolicies(2))
	var attempted []string
	blocked := false
	scheduler.reconcile = func(
		ctx context.Context,
		namespace string,
		policy *unstructured.Unstructured,
	) (bool, error) {
		attempted = append(attempted, namespace+"/"+policy.GetName())
		if !blocked {
			blocked = true
			<-ctx.Done()
			return false, ctx.Err()
		}
		return true, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	count, err := scheduler.Reconcile(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, count)
	require.Equal(t, []string{"tenant-a/a"}, attempted)

	count, err = scheduler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, []string{"tenant-a/a", "tenant-a/b", "tenant-a/c"}, attempted)
}

func TestReconcileAppliesGlobalPolicyBatchAcrossNamespaces(t *testing.T) {
	client := fakeClient()
	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		for _, name := range []string{"a", "b"} {
			createPolicyNamedIn(t, client, namespace, name, false)
		}
	}
	scheduler := NewForNamespaces(client, []string{"tenant-a", "tenant-b"})
	require.NoError(t, scheduler.SetMaxPolicies(3))
	var attempted []string
	scheduler.reconcile = func(
		_ context.Context,
		namespace string,
		policy *unstructured.Unstructured,
	) (bool, error) {
		attempted = append(attempted, namespace+"/"+policy.GetName())
		return true, nil
	}

	count, err := scheduler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.Equal(t, []string{"tenant-a/a", "tenant-a/b", "tenant-b/a"}, attempted)
}

func TestSetMaxPoliciesRejectsInvalidLimit(t *testing.T) {
	require.ErrorContains(t, New(nil, "test").SetMaxPolicies(0), "must be positive")
}

func TestDynamicNamespaceInventoryAppliesWithoutSchedulerRestart(t *testing.T) {
	client := fakeClient()
	for _, namespace := range []string{"tenant-a", "tenant-b"} {
		createTemplateIn(t, client, namespace, validTemplate())
		createPolicyIn(t, client, namespace, false)
	}
	createInventory(t, client, `["tenant-a"]`)
	scheduler := NewForInventory(client, "control", "scheduler-inventory", "").
		WithClock(func() time.Time { return time.Unix(1_700_003_000, 0).UTC() })

	count, err := scheduler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, count)
	tenantBOperations, err := client.Resource(operationqueue.Resource).Namespace("tenant-b").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, tenantBOperations.Items)

	updateInventory(t, client, `["tenant-b","tenant-a"]`)
	count, err = scheduler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, count)
	tenantBOperations, err = client.Resource(operationqueue.Resource).Namespace("tenant-b").
		List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, tenantBOperations.Items, 1)
}

func TestDynamicNamespaceInventoryFailsClosedBeforePolicyAccess(t *testing.T) {
	for _, raw := range []string{
		`[]`,
		`[""]`,
		`["tenant-a","tenant-a"]`,
		`["Tenant_A"]`,
		`{"namespace":"tenant-a"}`,
		`not-json`,
	} {
		t.Run(raw, func(t *testing.T) {
			client := fakeClient()
			createTemplateIn(t, client, "tenant-a", validTemplate())
			createPolicyIn(t, client, "tenant-a", false)
			createInventory(t, client, raw)

			count, err := NewForInventory(
				client, "control", "scheduler-inventory", DefaultInventoryKey,
			).Reconcile(context.Background())
			require.Zero(t, count)
			require.Error(t, err)
			operations, listErr := client.Resource(operationqueue.Resource).Namespace("tenant-a").
				List(context.Background(), metav1.ListOptions{})
			require.NoError(t, listErr)
			require.Empty(t, operations.Items)
		})
	}
}

func TestDynamicNamespaceInventoryRequiresConfigMapAndDataKey(t *testing.T) {
	client := fakeClient()
	scheduler := NewForInventory(
		client, "control", "scheduler-inventory", DefaultInventoryKey,
	)
	count, err := scheduler.Reconcile(context.Background())
	require.Zero(t, count)
	require.ErrorContains(t, err, "read namespace inventory")

	createInventory(t, client, `["tenant-a"]`)
	inventory, err := client.Resource(namespaceinventory.ConfigMapResource).Namespace("control").Get(
		context.Background(), "scheduler-inventory", metav1.GetOptions{},
	)
	require.NoError(t, err)
	delete(inventory.Object, "data")
	_, err = client.Resource(namespaceinventory.ConfigMapResource).Namespace("control").Update(
		context.Background(), inventory, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	count, err = scheduler.Reconcile(context.Background())
	require.Zero(t, count)
	require.ErrorContains(t, err, `missing data key "namespaces.json"`)
}

func validTemplate() map[string]any {
	return map[string]any{
		"endpoint": "https://etcd", "prefix": "/registry/", "artifact_output": "/work/{operation_id}.json",
		"batch_size": 1000, "metrics_output": "/work/metrics.prom", "backup_id": "replaced",
		"object_store_id": "store-a", "s3_endpoint": "https://s3", "s3_bucket": "bucket",
		"s3_object_key": "backups/{operation_id}.json", "s3_force_path_style": false,
		"aws_region": "us-east-1", "retention_mode": "COMPLIANCE", "retain_until_unix": 1,
		"min_records": 0, "max_age_seconds": 3600, "receipt_output": "/work/{operation_id}.receipt.json",
	}
}

func createTemplate(t *testing.T, client *dynamicfake.FakeDynamicClient, template map[string]any) {
	t.Helper()
	createTemplateIn(t, client, "test", template)
}

func createTemplateIn(
	t *testing.T,
	client *dynamicfake.FakeDynamicClient,
	namespace string,
	template map[string]any,
) {
	t.Helper()
	raw, err := json.Marshal(template)
	require.NoError(t, err)
	createTemplateRawIn(t, client, namespace, raw)
}

func createTemplateRawIn(
	t *testing.T,
	client *dynamicfake.FakeDynamicClient,
	namespace string,
	raw []byte,
) {
	t.Helper()
	_, err := client.Resource(operationqueue.SecretResource).Namespace(namespace).Create(
		context.Background(),
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": "daily-template"},
			"data":     map[string]any{"parameters.json": base64.StdEncoding.EncodeToString(raw)},
		}},
		metav1.CreateOptions{},
	)
	require.NoError(t, err)
}

func createPolicy(t *testing.T, client *dynamicfake.FakeDynamicClient, suspended bool) {
	t.Helper()
	createPolicyIn(t, client, "test", suspended)
}

func createPolicyIn(
	t *testing.T,
	client *dynamicfake.FakeDynamicClient,
	namespace string,
	suspended bool,
) {
	t.Helper()
	createPolicyNamedIn(t, client, namespace, "daily", suspended)
}

func createPolicyNamedIn(
	t *testing.T,
	client *dynamicfake.FakeDynamicClient,
	namespace, name string,
	suspended bool,
) {
	t.Helper()
	policy := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1", "kind": "KubeBrainBackupPolicy",
		"metadata": map[string]any{"name": name},
		"spec": map[string]any{
			"tenant": "tenant-a", "instance": "instance-a", "intervalSeconds": int64(3600),
			"retentionSeconds": int64(86400), "maxAttempts": int64(3), "suspend": suspended,
			"parametersTemplateSecretRef": map[string]any{
				"name": "daily-template", "key": "parameters.json",
			},
		},
	}}
	policy.SetUID(types.UID(strings.Repeat("a", 32)))
	policy.SetCreationTimestamp(metav1.NewTime(time.Unix(1_699_999_600, 0)))
	_, err := client.Resource(PolicyResource).Namespace(namespace).Create(
		context.Background(), policy, metav1.CreateOptions{},
	)
	require.NoError(t, err)
}

func createInventory(t *testing.T, client *dynamicfake.FakeDynamicClient, raw string) {
	t.Helper()
	_, err := client.Resource(namespaceinventory.ConfigMapResource).Namespace("control").Create(
		context.Background(),
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "scheduler-inventory"},
			"data":     map[string]any{DefaultInventoryKey: raw},
		}},
		metav1.CreateOptions{},
	)
	require.NoError(t, err)
}

func updateInventory(t *testing.T, client *dynamicfake.FakeDynamicClient, raw string) {
	t.Helper()
	inventory, err := client.Resource(namespaceinventory.ConfigMapResource).Namespace("control").Get(
		context.Background(), "scheduler-inventory", metav1.GetOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(
		inventory.Object, raw, "data", DefaultInventoryKey,
	))
	_, err = client.Resource(namespaceinventory.ConfigMapResource).Namespace("control").Update(
		context.Background(), inventory, metav1.UpdateOptions{},
	)
	require.NoError(t, err)
}

func fakeClient() *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			PolicyResource:                       "KubeBrainBackupPolicyList",
			operationqueue.Resource:              "KubeBrainOperationList",
			operationqueue.LeaseResource:         "LeaseList",
			operationqueue.SecretResource:        "SecretList",
			namespaceinventory.ConfigMapResource: "ConfigMapList",
		},
	)
}
