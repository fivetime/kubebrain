package backupscheduler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

var PolicyResource = schema.GroupVersionResource{
	Group: "dbaas.kubebrain.io", Version: "v1alpha1", Resource: "kubebrainbackuppolicies",
}

const parametersKey = "parameters.json"
const DefaultRequester = "kubebrain-backup-scheduler"

type Scheduler struct {
	client     dynamic.Interface
	namespaces []string
	requester  string
	now        func() time.Time
}

func New(client dynamic.Interface, namespace string) *Scheduler {
	return NewForNamespaces(client, []string{namespace})
}

func NewForNamespaces(client dynamic.Interface, namespaces []string) *Scheduler {
	return &Scheduler{
		client: client, namespaces: append([]string(nil), namespaces...),
		requester: DefaultRequester, now: func() time.Time { return time.Now().UTC() },
	}
}

func (s *Scheduler) WithRequester(requester string) *Scheduler {
	s.requester = requester
	return s
}

func (s *Scheduler) WithClock(now func() time.Time) *Scheduler {
	s.now = now
	return s
}

func (s *Scheduler) Reconcile(ctx context.Context) (int, error) {
	submitted := 0
	var reconcileErrs []error
	for _, namespace := range s.namespaces {
		list, err := s.client.Resource(PolicyResource).Namespace(namespace).
			List(ctx, metav1.ListOptions{})
		if err != nil {
			reconcileErrs = append(reconcileErrs, fmt.Errorf("%s: list policies: %w", namespace, err))
			continue
		}
		for i := range list.Items {
			created, err := s.reconcilePolicy(ctx, namespace, &list.Items[i])
			if err != nil {
				reconcileErrs = append(reconcileErrs,
					fmt.Errorf("%s/%s: %w", namespace, list.Items[i].GetName(), err))
				continue
			}
			if created {
				submitted++
			}
		}
	}
	return submitted, errors.Join(reconcileErrs...)
}

func (s *Scheduler) reconcilePolicy(
	ctx context.Context,
	namespace string,
	policy *unstructured.Unstructured,
) (bool, error) {
	suspended, _, _ := unstructured.NestedBool(policy.Object, "spec", "suspend")
	if suspended {
		return false, nil
	}
	tenant, _, _ := unstructured.NestedString(policy.Object, "spec", "tenant")
	instance, _, _ := unstructured.NestedString(policy.Object, "spec", "instance")
	interval, _, _ := unstructured.NestedInt64(policy.Object, "spec", "intervalSeconds")
	retention, _, _ := unstructured.NestedInt64(policy.Object, "spec", "retentionSeconds")
	maxAttempts, _, _ := unstructured.NestedInt64(policy.Object, "spec", "maxAttempts")
	templateName, _, _ := unstructured.NestedString(
		policy.Object, "spec", "parametersTemplateSecretRef", "name",
	)
	templateKey, _, _ := unstructured.NestedString(
		policy.Object, "spec", "parametersTemplateSecretRef", "key",
	)
	if len(validation.IsDNS1123Label(tenant)) != 0 || instance == "" ||
		interval < 300 || retention <= 0 || maxAttempts <= 0 ||
		templateName == "" || templateKey == "" {
		return false, errors.New("policy spec is incomplete")
	}
	now := s.now().UTC()
	slot := now.Unix() - now.Unix()%interval
	if slot < policy.GetCreationTimestamp().Unix() {
		return false, nil
	}
	operationID := fmt.Sprintf("backup-%s-%d", policy.GetName(), slot)
	if len(operationID) > 128 {
		return false, errors.New("generated operation ID exceeds 128 characters")
	}
	secretName := "params-" + operationID
	parameters, err := s.renderParameters(
		ctx, namespace, templateName, templateKey, operationID, slot, retention,
	)
	if err != nil {
		return false, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(parameters))
	if err := s.ensureParametersSecret(ctx, namespace, policy, secretName, parameters); err != nil {
		return false, err
	}
	_, err = operationqueue.New(s.client, namespace).Submit(ctx, operationID, operationqueue.Spec{
		OperationID: operationID, Tenant: tenant, RequestedBy: s.requester,
		Instance: instance, Type: "Backup",
		ParametersSHA256: digest, ParametersSecret: secretName, ParametersKey: parametersKey,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Scheduler) renderParameters(
	ctx context.Context,
	namespace, secretName, key, operationID string,
	slot, retention int64,
) ([]byte, error) {
	secret, err := s.client.Resource(operationqueue.SecretResource).Namespace(namespace).
		Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	encoded, found, err := unstructured.NestedString(secret.Object, "data", key)
	if err != nil || !found {
		return nil, errors.New("parameter template Secret does not contain the referenced key")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("parameter template Secret contains invalid base64 data")
	}
	var parameters map[string]any
	if err := json.Unmarshal(raw, &parameters); err != nil {
		return nil, fmt.Errorf("decode parameter template: %w", err)
	}
	parameters["backup_id"] = operationID
	parameters["retain_until_unix"] = slot + retention
	for _, field := range []string{"artifact_output", "receipt_output", "s3_object_key"} {
		value, ok := parameters[field].(string)
		if !ok || !strings.Contains(value, "{operation_id}") {
			return nil, fmt.Errorf("%s must contain {operation_id}", field)
		}
		parameters[field] = strings.ReplaceAll(value, "{operation_id}", operationID)
	}
	parameters["scheduled_unix"] = slot
	rendered, err := json.Marshal(parameters)
	if err != nil {
		return nil, err
	}
	return append(rendered, '\n'), nil
}

func (s *Scheduler) ensureParametersSecret(
	ctx context.Context,
	namespace string,
	policy *unstructured.Unstructured,
	name string,
	parameters []byte,
) error {
	immutable := true
	secret := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]any{
				"app.kubernetes.io/name":       "kubebrain-backup-parameters",
				"app.kubernetes.io/managed-by": "kubebrain-backup-scheduler",
				"dbaas.kubebrain.io/policy":    policy.GetName(),
			},
			"ownerReferences": []any{map[string]any{
				"apiVersion": policy.GetAPIVersion(),
				"kind":       policy.GetKind(),
				"name":       policy.GetName(),
				"uid":        string(policy.GetUID()),
				"controller": true,
			}},
		},
		"immutable": immutable,
		"type":      "Opaque",
		"data": map[string]any{
			parametersKey: base64.StdEncoding.EncodeToString(parameters),
		},
	}}
	secrets := s.client.Resource(operationqueue.SecretResource).Namespace(namespace)
	_, err := secrets.Create(ctx, secret, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing, getErr := secrets.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return getErr
	}
	actual, _, _ := unstructured.NestedString(existing.Object, "data", parametersKey)
	isImmutable, _, _ := unstructured.NestedBool(existing.Object, "immutable")
	if !isImmutable || actual != base64.StdEncoding.EncodeToString(parameters) {
		return errors.New("existing parameter Secret has different immutable content")
	}
	return nil
}

func NextDelay(now time.Time, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	nanos := now.UnixNano()
	step := int64(interval)
	return time.Duration(step - nanos%step)
}
