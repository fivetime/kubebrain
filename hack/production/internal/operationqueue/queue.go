package operationqueue

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

var Resource = schema.GroupVersionResource{
	Group: "dbaas.kubebrain.io", Version: "v1alpha1", Resource: "kubebrainoperations",
}

var LeaseResource = schema.GroupVersionResource{
	Group: "coordination.k8s.io", Version: "v1", Resource: "leases",
}

var SecretResource = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}

const microTimeFormat = "2006-01-02T15:04:05.000000Z07:00"

const (
	PhasePending   = "Pending"
	PhaseRunning   = "Running"
	PhaseSucceeded = "Succeeded"
	PhaseFailed    = "Failed"
)

var (
	ErrNoOperation  = errors.New("no claimable operation")
	ErrFenced       = errors.New("operation worker is fenced")
	ErrTerminal     = errors.New("operation is terminal")
	ErrInstanceBusy = errors.New("operation instance is busy")
)

type Spec struct {
	OperationID      string
	Instance         string
	Type             string
	ParametersSHA256 string
	ParametersSecret string
	ParametersKey    string
	MaxAttempts      int64
}

type Claim struct {
	Name             string `json:"name"`
	UID              string `json:"uid"`
	ResourceVersion  string `json:"resource_version"`
	OperationID      string `json:"operation_id"`
	Instance         string `json:"instance"`
	Type             string `json:"type"`
	ParametersSHA256 string `json:"parameters_sha256"`
	ParametersSecret string `json:"parameters_secret,omitempty"`
	ParametersKey    string `json:"parameters_key,omitempty"`
	Owner            string `json:"owner"`
	Attempt          int64  `json:"attempt"`
	LeaseUntilUnix   int64  `json:"lease_until_unix"`
}

type Queue struct {
	resource dynamic.ResourceInterface
	leases   dynamic.ResourceInterface
	secrets  dynamic.ResourceInterface
	now      func() time.Time
}

func New(client dynamic.Interface, namespace string) *Queue {
	return &Queue{
		resource: client.Resource(Resource).Namespace(namespace),
		leases:   client.Resource(LeaseResource).Namespace(namespace),
		secrets:  client.Resource(SecretResource).Namespace(namespace),
		now:      func() time.Time { return time.Now().UTC() },
	}
}

func (q *Queue) WithClock(now func() time.Time) *Queue {
	q.now = now
	return q
}

func (q *Queue) Submit(ctx context.Context, name string, spec Spec) (*unstructured.Unstructured, error) {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return nil, fmt.Errorf("invalid operation name: %s", errs[0])
	}
	if spec.OperationID == "" || spec.Instance == "" || spec.Type == "" ||
		len(spec.ParametersSHA256) != 64 || spec.MaxAttempts <= 0 {
		return nil, errors.New("operation spec is incomplete")
	}
	if (spec.ParametersSecret == "") != (spec.ParametersKey == "") {
		return nil, errors.New("parameter secret name and key must be specified together")
	}
	specObject := map[string]any{
		"operationID":      spec.OperationID,
		"instance":         spec.Instance,
		"type":             spec.Type,
		"parametersSHA256": spec.ParametersSHA256,
		"maxAttempts":      spec.MaxAttempts,
	}
	if spec.ParametersSecret != "" {
		specObject["parametersSecretRef"] = map[string]any{
			"name": spec.ParametersSecret,
			"key":  spec.ParametersKey,
		}
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1",
		"kind":       "KubeBrainOperation",
		"metadata": map[string]any{
			"name": name,
		},
		"spec": specObject,
	}}
	object.SetFinalizers([]string{operationaudit.Finalizer})
	created, err := q.resource.Create(ctx, object, metav1.CreateOptions{})
	if err == nil {
		return created, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	existing, getErr := q.resource.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return nil, getErr
	}
	if !specMatches(existing, spec) {
		return nil, errors.New("existing operation has a different immutable spec")
	}
	return existing, nil
}

func (q *Queue) Claim(ctx context.Context, owner, operationType string, lease time.Duration) (*Claim, error) {
	if owner == "" || lease < time.Second {
		return nil, errors.New("owner and a lease of at least one second are required")
	}
	list, err := q.resource.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	lastStarted := make(map[string]int64)
	for i := range list.Items {
		instance, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "instance")
		started, found, _ := unstructured.NestedInt64(
			list.Items[i].Object, "status", "startedAtUnixNano",
		)
		if !found {
			seconds, _, _ := unstructured.NestedInt64(
				list.Items[i].Object, "status", "startedAtUnix",
			)
			started = seconds * int64(time.Second)
		}
		if started > lastStarted[instance] {
			lastStarted[instance] = started
		}
	}
	sort.Slice(list.Items, func(i, j int) bool {
		leftInstance, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "instance")
		rightInstance, _, _ := unstructured.NestedString(list.Items[j].Object, "spec", "instance")
		if lastStarted[leftInstance] != lastStarted[rightInstance] {
			return lastStarted[leftInstance] < lastStarted[rightInstance]
		}
		left, right := list.Items[i].GetCreationTimestamp(), list.Items[j].GetCreationTimestamp()
		if left.Equal(&right) {
			return list.Items[i].GetName() < list.Items[j].GetName()
		}
		return left.Before(&right)
	})
	nowTime := q.now()
	now := nowTime.Unix()
	var lastConflict error
	for i := range list.Items {
		candidate := &list.Items[i]
		candidateType, _, _ := unstructured.NestedString(candidate.Object, "spec", "type")
		if operationType != "" && candidateType != operationType {
			continue
		}
		if requiresApproval(candidateType) && !isApproved(candidate) {
			continue
		}
		phase, _, _ := unstructured.NestedString(candidate.Object, "status", "phase")
		attempt, _, _ := unstructured.NestedInt64(candidate.Object, "status", "attempt")
		leaseUntil, _, _ := unstructured.NestedInt64(candidate.Object, "status", "leaseUntilUnix")
		maxAttempts, _, _ := unstructured.NestedInt64(candidate.Object, "spec", "maxAttempts")
		if phase != "" && phase != PhasePending && !(phase == PhaseRunning && leaseUntil < now) {
			continue
		}
		if (phase == "" || phase == PhasePending) && attempt >= maxAttempts {
			exhausted := candidate.DeepCopy()
			_ = unstructured.SetNestedField(exhausted.Object, PhaseFailed, "status", "phase")
			_ = unstructured.SetNestedField(exhausted.Object, now, "status", "completedAtUnix")
			_ = unstructured.SetNestedField(exhausted.Object, "maximum attempts exhausted", "status", "message")
			_, updateErr := q.resource.UpdateStatus(ctx, exhausted, metav1.UpdateOptions{})
			if apierrors.IsConflict(updateErr) {
				lastConflict = updateErr
				continue
			}
			if updateErr != nil {
				return nil, updateErr
			}
			continue
		}
		if phase == PhaseRunning && leaseUntil < now && attempt >= maxAttempts {
			exhausted := candidate.DeepCopy()
			_ = unstructured.SetNestedField(exhausted.Object, PhaseFailed, "status", "phase")
			_ = unstructured.SetNestedField(exhausted.Object, int64(0), "status", "leaseUntilUnix")
			_ = unstructured.SetNestedField(exhausted.Object, now, "status", "completedAtUnix")
			_ = unstructured.SetNestedField(exhausted.Object, "maximum attempts exhausted", "status", "message")
			_, updateErr := q.resource.UpdateStatus(ctx, exhausted, metav1.UpdateOptions{})
			if apierrors.IsConflict(updateErr) {
				lastConflict = updateErr
				continue
			}
			if updateErr != nil {
				return nil, updateErr
			}
			continue
		}
		instance, _, _ := unstructured.NestedString(candidate.Object, "spec", "instance")
		holder := leaseHolder(candidate, owner, attempt+1)
		if err := q.acquireInstanceLease(ctx, instance, holder, lease); err != nil {
			if errors.Is(err, ErrInstanceBusy) || apierrors.IsConflict(err) {
				lastConflict = err
				continue
			}
			return nil, err
		}
		updated := candidate.DeepCopy()
		_ = unstructured.SetNestedField(updated.Object, PhaseRunning, "status", "phase")
		_ = unstructured.SetNestedField(updated.Object, owner, "status", "owner")
		_ = unstructured.SetNestedField(updated.Object, attempt+1, "status", "attempt")
		_ = unstructured.SetNestedField(updated.Object, now+int64(lease/time.Second), "status", "leaseUntilUnix")
		_ = unstructured.SetNestedField(updated.Object, updated.GetGeneration(), "status", "observedGeneration")
		_ = unstructured.SetNestedField(updated.Object, now, "status", "startedAtUnix")
		_ = unstructured.SetNestedField(updated.Object, nowTime.UnixNano(), "status", "startedAtUnixNano")
		_ = unstructured.SetNestedField(updated.Object, "", "status", "message")
		claimed, updateErr := q.resource.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			_ = q.releaseInstanceLease(ctx, instance, holder)
			lastConflict = updateErr
			continue
		}
		if updateErr != nil {
			_ = q.releaseInstanceLease(ctx, instance, holder)
			return nil, updateErr
		}
		return claimFrom(claimed)
	}
	if lastConflict != nil {
		return nil, fmt.Errorf("%w: claim conflicts exhausted: %v", ErrNoOperation, lastConflict)
	}
	return nil, ErrNoOperation
}

func requiresApproval(operationType string) bool {
	switch operationType {
	case "RestoreCutover", "CertificateRotation", "Destroy", "BackupDeletion":
		return true
	default:
		return false
	}
}

func isApproved(object *unstructured.Unstructured) bool {
	annotations := object.GetAnnotations()
	approvedBy := annotations[operationaudit.ApprovedByAnnotation]
	approvalID := annotations[operationaudit.ApprovalIDAnnotation]
	return approvedBy == operationaudit.ApproverUsername && len(approvalID) <= 128 &&
		len(validation.IsDNS1123Subdomain(approvalID)) == 0
}

func (q *Queue) Requeue(
	ctx context.Context,
	name, owner string,
	attempt int64,
	message string,
) (*unstructured.Unstructured, error) {
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := q.requireWorker(object, owner, attempt); err != nil {
		return nil, err
	}
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	holder := leaseHolder(object, owner, attempt)
	updated := object.DeepCopy()
	_ = unstructured.SetNestedField(updated.Object, PhasePending, "status", "phase")
	_ = unstructured.SetNestedField(updated.Object, "", "status", "owner")
	_ = unstructured.SetNestedField(updated.Object, int64(0), "status", "leaseUntilUnix")
	_ = unstructured.SetNestedField(updated.Object, message, "status", "message")
	result, err := q.resource.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return nil, ErrFenced
	}
	if err == nil {
		_ = q.releaseInstanceLease(ctx, instance, holder)
	}
	return result, err
}

func (q *Queue) Heartbeat(ctx context.Context, name, owner string, attempt int64, lease time.Duration) (*Claim, error) {
	if lease < time.Second {
		return nil, errors.New("lease must be at least one second")
	}
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := q.requireWorker(object, owner, attempt); err != nil {
		return nil, err
	}
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	holder := leaseHolder(object, owner, attempt)
	if err := q.acquireInstanceLease(ctx, instance, holder, lease); err != nil {
		if errors.Is(err, ErrInstanceBusy) || apierrors.IsConflict(err) {
			return nil, ErrFenced
		}
		return nil, err
	}
	now := q.now().Unix()
	updated := object.DeepCopy()
	_ = unstructured.SetNestedField(updated.Object, now+int64(lease/time.Second), "status", "leaseUntilUnix")
	result, err := q.resource.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return nil, ErrFenced
	}
	if err != nil {
		return nil, err
	}
	return claimFrom(result)
}

func (q *Queue) Finish(
	ctx context.Context,
	name, owner string,
	attempt int64,
	succeeded bool,
	receiptSHA256, message string,
) (*unstructured.Unstructured, error) {
	if succeeded && len(receiptSHA256) != 64 {
		return nil, errors.New("successful operation requires a receipt SHA-256")
	}
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	if phase == PhaseSucceeded || phase == PhaseFailed {
		actualOwner, _, _ := unstructured.NestedString(object.Object, "status", "owner")
		actualAttempt, _, _ := unstructured.NestedInt64(object.Object, "status", "attempt")
		actualReceipt, _, _ := unstructured.NestedString(object.Object, "status", "receiptSHA256")
		actualMessage, _, _ := unstructured.NestedString(object.Object, "status", "message")
		expectedPhase := PhaseFailed
		if succeeded {
			expectedPhase = PhaseSucceeded
		}
		if phase == expectedPhase && actualOwner == owner && actualAttempt == attempt &&
			actualReceipt == receiptSHA256 && actualMessage == message {
			return object, nil
		}
		return nil, ErrTerminal
	}
	if err := q.requireWorker(object, owner, attempt); err != nil {
		return nil, err
	}
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	holder := leaseHolder(object, owner, attempt)
	updated := object.DeepCopy()
	targetPhase := PhaseFailed
	if succeeded {
		targetPhase = PhaseSucceeded
	}
	_ = unstructured.SetNestedField(updated.Object, targetPhase, "status", "phase")
	_ = unstructured.SetNestedField(updated.Object, int64(0), "status", "leaseUntilUnix")
	_ = unstructured.SetNestedField(updated.Object, q.now().Unix(), "status", "completedAtUnix")
	_ = unstructured.SetNestedField(updated.Object, receiptSHA256, "status", "receiptSHA256")
	_ = unstructured.SetNestedField(updated.Object, message, "status", "message")
	result, err := q.resource.UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return nil, ErrFenced
	}
	if err == nil {
		_ = q.releaseInstanceLease(ctx, instance, holder)
	}
	return result, err
}

func (q *Queue) Get(ctx context.Context, name string) (*unstructured.Unstructured, error) {
	return q.resource.Get(ctx, name, metav1.GetOptions{})
}

func (q *Queue) Parameters(ctx context.Context, name string) ([]byte, error) {
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	secretName, _, _ := unstructured.NestedString(
		object.Object, "spec", "parametersSecretRef", "name",
	)
	key, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "key")
	expected, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSHA256")
	if secretName == "" || key == "" {
		return nil, errors.New("operation does not reference managed parameters")
	}
	secret, err := q.secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	immutable, _, _ := unstructured.NestedBool(secret.Object, "immutable")
	encoded, found, err := unstructured.NestedString(secret.Object, "data", key)
	if err != nil || !found || !immutable {
		return nil, errors.New("parameter secret must be immutable and contain the referenced key")
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("parameter secret contains invalid base64 data")
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != expected {
		return nil, errors.New("parameter secret digest does not match operation spec")
	}
	return data, nil
}

func (q *Queue) Approve(
	ctx context.Context,
	name, approvedBy, approvalID string,
) (*unstructured.Unstructured, error) {
	if approvedBy == "" || len(approvalID) > 128 || len(validation.IsDNS1123Subdomain(approvalID)) != 0 {
		return nil, errors.New("approver and a DNS-compatible approval ID are required")
	}
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	operationType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
	if !requiresApproval(operationType) {
		return nil, errors.New("operation type does not require approval")
	}
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	if phase != "" && phase != PhasePending {
		return nil, errors.New("only pending operations can be approved")
	}
	annotations := object.GetAnnotations()
	if annotations[operationaudit.ApprovedByAnnotation] != "" ||
		annotations[operationaudit.ApprovalIDAnnotation] != "" {
		if annotations[operationaudit.ApprovedByAnnotation] == approvedBy &&
			annotations[operationaudit.ApprovalIDAnnotation] == approvalID {
			return object, nil
		}
		return nil, errors.New("operation has different immutable approval evidence")
	}
	updated := object.DeepCopy()
	annotations = updated.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[operationaudit.ApprovedByAnnotation] = approvedBy
	annotations[operationaudit.ApprovalIDAnnotation] = approvalID
	updated.SetAnnotations(annotations)
	return q.resource.Update(ctx, updated, metav1.UpdateOptions{})
}

func (q *Queue) Delete(ctx context.Context, name string, uid types.UID) error {
	policy := metav1.DeletePropagationForeground
	return q.resource.Delete(ctx, name, metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &uid},
		PropagationPolicy: &policy,
	})
}

func (q *Queue) requireWorker(object *unstructured.Unstructured, owner string, attempt int64) error {
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	if phase == PhaseSucceeded || phase == PhaseFailed {
		return ErrTerminal
	}
	actualOwner, _, _ := unstructured.NestedString(object.Object, "status", "owner")
	actualAttempt, _, _ := unstructured.NestedInt64(object.Object, "status", "attempt")
	leaseUntil, _, _ := unstructured.NestedInt64(object.Object, "status", "leaseUntilUnix")
	if phase != PhaseRunning || actualOwner != owner || actualAttempt != attempt ||
		leaseUntil < q.now().Unix() {
		return ErrFenced
	}
	return nil
}

func claimFrom(object *unstructured.Unstructured) (*Claim, error) {
	operationID, _, _ := unstructured.NestedString(object.Object, "spec", "operationID")
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	operationType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
	digest, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSHA256")
	secret, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "name")
	key, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "key")
	owner, _, _ := unstructured.NestedString(object.Object, "status", "owner")
	attempt, _, _ := unstructured.NestedInt64(object.Object, "status", "attempt")
	leaseUntil, _, _ := unstructured.NestedInt64(object.Object, "status", "leaseUntilUnix")
	if operationID == "" || instance == "" || operationType == "" || owner == "" || attempt <= 0 {
		return nil, errors.New("claimed operation is incomplete")
	}
	return &Claim{
		Name: object.GetName(), UID: string(object.GetUID()), ResourceVersion: object.GetResourceVersion(),
		OperationID: operationID, Instance: instance, Type: operationType,
		ParametersSHA256: digest, ParametersSecret: secret, ParametersKey: key,
		Owner: owner, Attempt: attempt, LeaseUntilUnix: leaseUntil,
	}, nil
}

func specMatches(object *unstructured.Unstructured, spec Spec) bool {
	operationID, _, _ := unstructured.NestedString(object.Object, "spec", "operationID")
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	operationType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
	digest, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSHA256")
	maxAttempts, _, _ := unstructured.NestedInt64(object.Object, "spec", "maxAttempts")
	secret, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "name")
	key, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "key")
	return operationID == spec.OperationID && instance == spec.Instance &&
		operationType == spec.Type && digest == spec.ParametersSHA256 &&
		maxAttempts == spec.MaxAttempts && secret == spec.ParametersSecret &&
		key == spec.ParametersKey
}

func (q *Queue) acquireInstanceLease(
	ctx context.Context,
	instance, holder string,
	duration time.Duration,
) error {
	if instance == "" {
		return errors.New("operation instance is empty")
	}
	name := instanceLeaseName(instance)
	now := q.now().UTC()
	nowText := now.Format(microTimeFormat)
	seconds := int64(duration / time.Second)
	lease := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "coordination.k8s.io/v1",
		"kind":       "Lease",
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]any{
				"app.kubernetes.io/name":       "kubebrain-operation-instance-lock",
				"app.kubernetes.io/managed-by": "kubebrain-operation-worker",
			},
		},
		"spec": map[string]any{
			"holderIdentity":       holder,
			"leaseDurationSeconds": seconds,
			"acquireTime":          nowText,
			"renewTime":            nowText,
		},
	}}
	_, err := q.leases.Create(ctx, lease, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing, err := q.leases.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	currentHolder, _, _ := unstructured.NestedString(existing.Object, "spec", "holderIdentity")
	renewText, _, _ := unstructured.NestedString(existing.Object, "spec", "renewTime")
	currentSeconds, _, _ := unstructured.NestedInt64(existing.Object, "spec", "leaseDurationSeconds")
	renewed, parseErr := time.Parse(time.RFC3339Nano, renewText)
	if currentHolder != holder && (parseErr != nil || !renewed.Add(time.Duration(currentSeconds)*time.Second).Before(now)) {
		return ErrInstanceBusy
	}
	updated := existing.DeepCopy()
	if currentHolder != holder {
		_ = unstructured.SetNestedField(updated.Object, nowText, "spec", "acquireTime")
	}
	_ = unstructured.SetNestedField(updated.Object, holder, "spec", "holderIdentity")
	_ = unstructured.SetNestedField(updated.Object, seconds, "spec", "leaseDurationSeconds")
	_ = unstructured.SetNestedField(updated.Object, nowText, "spec", "renewTime")
	_, err = q.leases.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func (q *Queue) releaseInstanceLease(ctx context.Context, instance, holder string) error {
	name := instanceLeaseName(instance)
	existing, err := q.leases.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	currentHolder, _, _ := unstructured.NestedString(existing.Object, "spec", "holderIdentity")
	if currentHolder != holder {
		return ErrFenced
	}
	uid := existing.GetUID()
	return q.leases.Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
}

func instanceLeaseName(instance string) string {
	digest := sha256.Sum256([]byte(instance))
	return fmt.Sprintf("kubebrain-instance-%x", digest[:16])
}

func leaseHolder(object *unstructured.Unstructured, owner string, attempt int64) string {
	ownerDigest := sha256.Sum256([]byte(owner))
	return string(object.GetUID()) + ":" + strconv.FormatInt(attempt, 10) + ":" +
		fmt.Sprintf("%x", ownerDigest[:8])
}
