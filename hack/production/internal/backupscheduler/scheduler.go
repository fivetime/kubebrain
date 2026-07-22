package backupscheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
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
const DefaultInventoryKey = namespaceinventory.DefaultKey
const DefaultMaxPolicies = 256

type policyCandidate struct {
	namespace string
	object    *unstructured.Unstructured
}

type Scheduler struct {
	client             dynamic.Interface
	staticNamespaces   []string
	inventoryNamespace string
	inventoryName      string
	inventoryKey       string
	requester          string
	maxPolicies        int
	now                func() time.Time
	reconcile          func(context.Context, string, *unstructured.Unstructured) (bool, error)
	cursorMu           sync.Mutex
	cursor             string
}

func New(client dynamic.Interface, namespace string) *Scheduler {
	return NewForNamespaces(client, []string{namespace})
}

func NewForNamespaces(client dynamic.Interface, namespaces []string) *Scheduler {
	scheduler := &Scheduler{
		client: client, staticNamespaces: append([]string(nil), namespaces...),
		requester: DefaultRequester, maxPolicies: DefaultMaxPolicies,
		now: func() time.Time { return time.Now().UTC() },
	}
	scheduler.reconcile = scheduler.reconcilePolicy
	return scheduler
}

func NewForInventory(client dynamic.Interface, namespace, name, key string) *Scheduler {
	if key == "" {
		key = DefaultInventoryKey
	}
	scheduler := &Scheduler{
		client: client, inventoryNamespace: namespace, inventoryName: name, inventoryKey: key,
		requester: DefaultRequester, maxPolicies: DefaultMaxPolicies,
		now: func() time.Time { return time.Now().UTC() },
	}
	scheduler.reconcile = scheduler.reconcilePolicy
	return scheduler
}

func (s *Scheduler) WithRequester(requester string) *Scheduler {
	s.requester = requester
	return s
}

func (s *Scheduler) WithClock(now func() time.Time) *Scheduler {
	s.now = now
	return s
}

func (s *Scheduler) SetMaxPolicies(maxPolicies int) error {
	if maxPolicies <= 0 {
		return errors.New("backup scheduler max policies must be positive")
	}
	s.maxPolicies = maxPolicies
	return nil
}

func (s *Scheduler) Reconcile(ctx context.Context) (int, error) {
	namespaces, err := s.namespaces(ctx)
	if err != nil {
		return 0, err
	}
	var candidates []policyCandidate
	var reconcileErrs []error
	for _, namespace := range namespaces {
		if err := ctx.Err(); err != nil {
			reconcileErrs = append(reconcileErrs, err)
			break
		}
		list, err := s.client.Resource(PolicyResource).Namespace(namespace).
			List(ctx, metav1.ListOptions{})
		if err != nil {
			reconcileErrs = append(reconcileErrs, fmt.Errorf("%s: list policies: %w", namespace, err))
			if ctx.Err() != nil {
				break
			}
			continue
		}
		for i := range list.Items {
			candidates = append(candidates, policyCandidate{
				namespace: namespace,
				object:    list.Items[i].DeepCopy(),
			})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidateKey(candidates[i]) < candidateKey(candidates[j])
	})
	candidates = s.nextBatch(candidates)
	submitted := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			reconcileErrs = append(reconcileErrs, err)
			break
		}
		created, err := s.reconcile(ctx, candidate.namespace, candidate.object)
		s.markAttempted(candidate)
		if err != nil {
			reconcileErrs = append(reconcileErrs,
				fmt.Errorf("%s/%s: %w", candidate.namespace, candidate.object.GetName(), err))
			continue
		}
		if created {
			submitted++
		}
	}
	return submitted, errors.Join(reconcileErrs...)
}

func (s *Scheduler) nextBatch(candidates []policyCandidate) []policyCandidate {
	if len(candidates) == 0 {
		return candidates
	}
	s.cursorMu.Lock()
	cursor := s.cursor
	s.cursorMu.Unlock()
	start := 0
	if cursor != "" {
		start = sort.Search(len(candidates), func(i int) bool {
			return candidateKey(candidates[i]) > cursor
		})
		if start == len(candidates) {
			start = 0
		}
	}
	batchSize := min(s.maxPolicies, len(candidates))
	batch := make([]policyCandidate, 0, batchSize)
	for offset := 0; offset < batchSize; offset++ {
		batch = append(batch, candidates[(start+offset)%len(candidates)])
	}
	return batch
}

func (s *Scheduler) markAttempted(candidate policyCandidate) {
	s.cursorMu.Lock()
	s.cursor = candidateKey(candidate)
	s.cursorMu.Unlock()
}

func candidateKey(candidate policyCandidate) string {
	return candidate.namespace + "/" + candidate.object.GetName()
}

func (s *Scheduler) namespaces(ctx context.Context) ([]string, error) {
	if s.inventoryName == "" {
		return append([]string(nil), s.staticNamespaces...), nil
	}
	return namespaceinventory.Load(
		ctx, s.client, s.inventoryNamespace, s.inventoryName, s.inventoryKey,
	)
}

func ValidateNamespaces(namespaces []string) ([]string, error) {
	return namespaceinventory.Validate(namespaces)
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
	parameters, err := decodeParameterTemplate(raw)
	if err != nil {
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

func decodeParameterTemplate(raw []byte) (map[string]any, error) {
	var parameters map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&parameters); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode trailing parameter template data: %w", err)
		}
	} else {
		return nil, errors.New("parameter template contains trailing JSON")
	}
	if parameters == nil {
		return nil, errors.New("parameter template must be a JSON object")
	}
	return parameters, nil
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
