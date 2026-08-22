package operationretention

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const retentionVerifierUsername = "system:serviceaccount:kubebrain-operations:kubebrain-operation-archive-verifier"

func TestRetentionDeleteAdmissionAgainstAPIServer(t *testing.T) {
	if os.Getenv("KUBEBRAIN_RETENTION_APISERVER_TEST") != "1" {
		t.Skip("set KUBEBRAIN_RETENTION_APISERVER_TEST=1 to run against the current kubeconfig context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{},
	).ClientConfig()
	require.NoError(t, err)
	admin, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	namespace := "kb-retention-" + suffix
	policyName := "kb-retention-" + suffix
	selectorKey := "dbaas.kubebrain.io/retention-drill"
	defer cleanupRetentionDrill(context.Background(), admin, namespace, policyName)
	require.NoError(t, createRetentionDrillScope(ctx, admin, namespace, policyName, selectorKey, suffix))

	impersonatedConfig := *config
	impersonatedConfig.Impersonate.UserName = retentionVerifierUsername
	verifier, err := kubernetes.NewForConfig(&impersonatedConfig)
	require.NoError(t, err)
	require.NoError(t, waitForRetentionPolicy(ctx, admin, policyName))

	create := func(name string) *corev1.ConfigMap {
		object, createErr := admin.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"dbaas.kubebrain.io/phase": "Succeeded"},
				Annotations: map[string]string{
					operationReceiptKey:  strings.Repeat("a", 64),
					operationArtifactKey: strings.Repeat("b", 64),
					operationVersionKey:  "version-1",
				},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, createErr)
		return object
	}
	options := func(object *corev1.ConfigMap) metav1.DeleteOptions {
		uid, rv := object.UID, object.ResourceVersion
		return metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}
	}

	require.NoError(t, waitForRetentionEnforcement(ctx, admin, namespace, create, options))

	missing := create("missing-preconditions")
	err = verifier.CoreV1().ConfigMaps(namespace).Delete(ctx, missing.Name, metav1.DeleteOptions{})
	require.True(t, isRetentionAdmissionDenied(err), err)

	wrongUID := create("wrong-uid")
	wrongUIDOptions := options(wrongUID)
	uid := types.UID("replacement-uid")
	wrongUIDOptions.Preconditions.UID = &uid
	err = verifier.CoreV1().ConfigMaps(namespace).Delete(ctx, wrongUID.Name, wrongUIDOptions)
	require.Error(t, err)
	require.False(t, apierrors.IsNotFound(err), err)

	wrongRV := create("wrong-resource-version")
	wrongRVOptions := options(wrongRV)
	rv := wrongRV.ResourceVersion + "-stale"
	wrongRVOptions.Preconditions.ResourceVersion = &rv
	err = verifier.CoreV1().ConfigMaps(namespace).Delete(ctx, wrongRV.Name, wrongRVOptions)
	require.Error(t, err)
	require.False(t, apierrors.IsNotFound(err), err)

	exact := create("exact")
	require.NoError(t, verifier.CoreV1().ConfigMaps(namespace).Delete(ctx, exact.Name, options(exact)))
	_, err = admin.CoreV1().ConfigMaps(namespace).Get(ctx, exact.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), err)
}

func waitForRetentionEnforcement(
	ctx context.Context, client kubernetes.Interface, namespace string,
	create func(string) *corev1.ConfigMap,
	options func(*corev1.ConfigMap) metav1.DeleteOptions,
) error {
	for attempt := 0; ; attempt++ {
		object := create(fmt.Sprintf("wrong-identity-%d", attempt))
		err := client.CoreV1().ConfigMaps(namespace).Delete(ctx, object.Name, options(object))
		if isRetentionAdmissionDenied(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("probe retention admission enforcement: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func isRetentionAdmissionDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), "ValidatingAdmissionPolicy") &&
		strings.Contains(err.Error(), "denied request: retention delete requires exact identity and preconditions")
}

const (
	operationReceiptKey  = "dbaas.kubebrain.io/audit-receipt-sha256"
	operationArtifactKey = "dbaas.kubebrain.io/audit-artifact-sha256"
	operationVersionKey  = "dbaas.kubebrain.io/audit-version-id"
)

func createRetentionDrillScope(ctx context.Context, client kubernetes.Interface, namespace, policyName, selectorKey, selectorValue string) error {
	_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: namespace, Labels: map[string]string{selectorKey: selectorValue},
	}}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	failurePolicy := admissionv1.Fail
	matchPolicy := admissionv1.Equivalent
	_, err = client.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, &admissionv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Spec: admissionv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: &failurePolicy,
			MatchConstraints: &admissionv1.MatchResources{MatchPolicy: &matchPolicy, ResourceRules: []admissionv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionv1.RuleWithOperations{
					Operations: []admissionv1.OperationType{admissionv1.Delete},
					Rule:       admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"configmaps"}, Scope: scope(admissionv1.NamespacedScope)},
				},
			}}},
			Validations: []admissionv1.Validation{{Expression: retentionDrillExpression(), Message: "retention delete requires exact identity and preconditions"}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	action := admissionv1.Deny
	_, err = client.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, &admissionv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Spec: admissionv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName: policyName, ValidationActions: []admissionv1.ValidationAction{action},
			MatchResources: &admissionv1.MatchResources{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{selectorKey: selectorValue}}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	_, err = client.RbacV1().Roles(namespace).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"delete"}}},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	_, err = client.RbacV1().RoleBindings(namespace).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policyName},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "kubebrain-operation-archive-verifier", Namespace: "kubebrain-operations"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: policyName},
	}, metav1.CreateOptions{})
	return err
}

func retentionDrillExpression() string {
	return `request.userInfo.username == "` + retentionVerifierUsername + `" &&
oldObject.metadata.labels["dbaas.kubebrain.io/phase"] in ["Succeeded", "Failed"] &&
oldObject.metadata.annotations["` + operationReceiptKey + `"].matches("^[a-f0-9]{64}$") &&
oldObject.metadata.annotations["` + operationArtifactKey + `"].matches("^[a-f0-9]{64}$") &&
size(oldObject.metadata.annotations["` + operationVersionKey + `"]) > 0 &&
has(request.options.preconditions) && has(request.options.preconditions.uid) &&
request.options.preconditions.uid == oldObject.metadata.uid &&
has(request.options.preconditions.resourceVersion) &&
request.options.preconditions.resourceVersion == oldObject.metadata.resourceVersion`
}

func waitForRetentionPolicy(ctx context.Context, client kubernetes.Interface, name string) error {
	return wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		policy, err := client.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range policy.Status.Conditions {
			if condition.Status == metav1.ConditionFalse {
				return false, fmt.Errorf("policy condition %s: %s", condition.Type, condition.Message)
			}
		}
		return policy.Status.TypeChecking != nil && len(policy.Status.TypeChecking.ExpressionWarnings) == 0, nil
	})
}

func cleanupRetentionDrill(ctx context.Context, client kubernetes.Interface, namespace, policyName string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_ = client.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Delete(ctx, policyName, metav1.DeleteOptions{})
	_ = client.AdmissionregistrationV1().ValidatingAdmissionPolicies().Delete(ctx, policyName, metav1.DeleteOptions{})
	_ = client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
}

func scope(value admissionv1.ScopeType) *admissionv1.ScopeType { return &value }
