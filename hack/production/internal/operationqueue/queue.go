package operationqueue

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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

const operationAPIVersion = "dbaas.kubebrain.io/v1alpha1"
const operationKind = "KubeBrainOperation"

var LeaseResource = schema.GroupVersionResource{
	Group: "coordination.k8s.io", Version: "v1", Resource: "leases",
}

var SecretResource = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}

const microTimeFormat = "2006-01-02T15:04:05.000000Z07:00"
const leaseCleanupTimeout = 5 * time.Second

// MaxOperationAttempts mirrors the KubeBrainOperation CRD's spec.maxAttempts maximum.
const MaxOperationAttempts = 100

const (
	maxRequesterLength     = 253
	maxStatusOwnerLength   = 253
	maxStatusMessageLength = 4096
)

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
	ErrInvalidSpec  = errors.New("invalid operation spec")
)

type Spec struct {
	OperationID      string
	Tenant           string
	RequestedBy      string
	Instance         string
	Type             string
	ParametersSHA256 string
	ParametersSecret string
	ParametersKey    string
	MaxAttempts      int64
}

type Claim struct {
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	UID              string `json:"uid"`
	ResourceVersion  string `json:"resource_version"`
	OperationID      string `json:"operation_id"`
	Tenant           string `json:"tenant,omitempty"`
	RequestedBy      string `json:"requested_by,omitempty"`
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
	namespace string
	resource  dynamic.ResourceInterface
	leases    dynamic.ResourceInterface
	secrets   dynamic.ResourceInterface
	now       func() time.Time
}

func New(client dynamic.Interface, namespace string) *Queue {
	return &Queue{
		namespace: namespace,
		resource:  client.Resource(Resource).Namespace(namespace),
		leases:    client.Resource(LeaseResource).Namespace(namespace),
		secrets:   client.Resource(SecretResource).Namespace(namespace),
		now:       func() time.Time { return time.Now().UTC() },
	}
}

func (q *Queue) WithClock(now func() time.Time) *Queue {
	q.now = now
	return q
}

func (q *Queue) validateNamespace() error {
	return validateQueueNamespace(q.namespace)
}

func validateQueueNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("operation queue namespace must not be empty")
	}
	if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
		return errors.New("invalid operation queue namespace " + namespace + ": " + problems[0])
	}
	return nil
}

// ValidateOperationName validates a KubeBrainOperation resource name.
func ValidateOperationName(name string) error {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fmt.Errorf("invalid operation name: %s", errs[0])
	}
	return nil
}

// ValidateWorkerIdentity validates operation status owner and attempt fencing input.
func ValidateWorkerIdentity(owner string, attempt int64) error {
	if owner == "" {
		return errors.New("operation worker owner is required")
	}
	if err := validateStatusOwner(owner); err != nil {
		return err
	}
	if attempt <= 0 {
		return errors.New("operation worker attempt must be positive")
	}
	return nil
}

func validateOperationTypeFilter(operationType string) error {
	if operationType != "" && !isSupportedOperationType(operationType) {
		return fmt.Errorf("unsupported operation type: %s", operationType)
	}
	return nil
}

func (q *Queue) Submit(ctx context.Context, name string, spec Spec) (*unstructured.Unstructured, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := validateOperationSpec(name, spec); err != nil {
		return nil, err
	}
	specObject := map[string]any{
		"operationID":      spec.OperationID,
		"instance":         spec.Instance,
		"type":             spec.Type,
		"parametersSHA256": spec.ParametersSHA256,
		"maxAttempts":      spec.MaxAttempts,
	}
	if spec.Tenant != "" {
		specObject["tenant"] = spec.Tenant
	}
	if spec.RequestedBy != "" {
		specObject["requestedBy"] = spec.RequestedBy
	}
	if spec.ParametersSecret != "" {
		specObject["parametersSecretRef"] = map[string]any{
			"name": spec.ParametersSecret,
			"key":  spec.ParametersKey,
		}
	}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": operationAPIVersion,
		"kind":       operationKind,
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
	reconcileCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	existing, getErr := q.resource.Get(reconcileCtx, name, metav1.GetOptions{})
	if getErr != nil {
		if apierrors.IsNotFound(getErr) && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		return nil, errors.Join(
			err, fmt.Errorf("inspect operation after failed submit: %w", getErr),
		)
	}
	if !operationTypeMetaMatches(existing) {
		return nil, errors.New("existing operation has invalid type metadata")
	}
	if !specMatches(existing, spec) {
		return nil, errors.New("existing operation has a different immutable spec")
	}
	if !containsString(existing.GetFinalizers(), operationaudit.Finalizer) {
		return nil, errors.New("existing operation is missing the audit finalizer")
	}
	return existing, nil
}

func (q *Queue) Claim(ctx context.Context, owner, operationType string, lease time.Duration) (*Claim, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := validateOperationTypeFilter(operationType); err != nil {
		return nil, err
	}
	if owner == "" || lease < time.Second {
		return nil, errors.New("owner and a lease of at least one second are required")
	}
	if err := validateStatusOwner(owner); err != nil {
		return nil, err
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
	var lastInvalid error
	for i := range list.Items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		candidate := &list.Items[i]
		spec := specFromObject(candidate)
		if operationType != "" && spec.Type != operationType {
			continue
		}
		phase, _, _ := unstructured.NestedString(candidate.Object, "status", "phase")
		attempt, _, _ := unstructured.NestedInt64(candidate.Object, "status", "attempt")
		leaseUntil, _, _ := unstructured.NestedInt64(candidate.Object, "status", "leaseUntilUnix")
		if phase == PhaseSucceeded || phase == PhaseFailed {
			continue
		}
		if err := validateClaimCandidate(candidate.GetName(), spec, phase, attempt, leaseUntil); err != nil {
			lastInvalid = err
			continue
		}
		if requiresApproval(spec.Type) && !isApproved(candidate) {
			continue
		}
		if phase != "" && phase != PhasePending && !(phase == PhaseRunning && leaseUntil < now) {
			continue
		}
		if (phase == "" || phase == PhasePending) && attempt >= spec.MaxAttempts {
			exhausted := q.exhaustedFailureStatus(
				candidate, owner, "maximum attempts exhausted", nowTime,
			)
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
		if phase == PhaseRunning && leaseUntil < now && attempt >= spec.MaxAttempts {
			exhausted := q.exhaustedFailureStatus(
				candidate, owner, "maximum attempts exhausted", nowTime,
			)
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
			releaseErr := q.releaseInstanceLeaseForCleanup(ctx, instance, holder)
			if releaseErr != nil {
				return nil, errors.Join(
					updateErr,
					fmt.Errorf("release instance lease after claim conflict: %w", releaseErr),
				)
			}
			lastConflict = updateErr
			continue
		}
		if updateErr != nil {
			releaseErr := q.releaseInstanceLeaseForCleanup(ctx, instance, holder)
			return nil, errors.Join(
				updateErr,
				wrapIfError("release instance lease after failed claim", releaseErr),
			)
		}
		claim, claimErr := claimFrom(claimed)
		if claimErr != nil {
			releaseErr := q.releaseInstanceLeaseForCleanup(ctx, instance, holder)
			return nil, errors.Join(
				claimErr,
				wrapIfError("release instance lease after invalid claim", releaseErr),
			)
		}
		claim.Namespace = q.namespace
		return claim, nil
	}
	if lastConflict != nil {
		return nil, fmt.Errorf("%w: claim conflicts exhausted: %v", ErrNoOperation, lastConflict)
	}
	if lastInvalid != nil {
		return nil, lastInvalid
	}
	return nil, ErrNoOperation
}

func (q *Queue) exhaustedFailureStatus(
	candidate *unstructured.Unstructured,
	owner, message string,
	nowTime time.Time,
) *unstructured.Unstructured {
	exhausted := candidate.DeepCopy()
	now := nowTime.Unix()
	currentOwner, _, _ := unstructured.NestedString(exhausted.Object, "status", "owner")
	startedByCurrentOwner := currentOwner != ""
	if currentOwner == "" {
		_ = unstructured.SetNestedField(exhausted.Object, owner, "status", "owner")
	}
	attempt, _, _ := unstructured.NestedInt64(exhausted.Object, "status", "attempt")
	if attempt <= 0 {
		maxAttempts, _, _ := unstructured.NestedInt64(exhausted.Object, "spec", "maxAttempts")
		if maxAttempts > 0 {
			_ = unstructured.SetNestedField(exhausted.Object, maxAttempts, "status", "attempt")
		}
	}
	observedGeneration, _, _ := unstructured.NestedInt64(
		exhausted.Object, "status", "observedGeneration",
	)
	if observedGeneration <= 0 {
		_ = unstructured.SetNestedField(
			exhausted.Object, exhausted.GetGeneration(), "status", "observedGeneration",
		)
	}
	startedAt, _, _ := unstructured.NestedInt64(exhausted.Object, "status", "startedAtUnix")
	startedAtNano, foundNano, _ := unstructured.NestedInt64(
		exhausted.Object, "status", "startedAtUnixNano",
	)
	if !startedByCurrentOwner || startedAt <= 0 || startedAt > now {
		_ = unstructured.SetNestedField(exhausted.Object, now, "status", "startedAtUnix")
		_ = unstructured.SetNestedField(
			exhausted.Object, nowTime.UnixNano(), "status", "startedAtUnixNano",
		)
	} else if foundNano && startedAtNano > 0 && startedAtNano/int64(time.Second) != startedAt {
		_ = unstructured.SetNestedField(
			exhausted.Object, startedAt*int64(time.Second), "status", "startedAtUnixNano",
		)
	}
	_ = unstructured.SetNestedField(exhausted.Object, PhaseFailed, "status", "phase")
	_ = unstructured.SetNestedField(exhausted.Object, int64(0), "status", "leaseUntilUnix")
	_ = unstructured.SetNestedField(exhausted.Object, now, "status", "completedAtUnix")
	_ = unstructured.SetNestedField(exhausted.Object, "", "status", "receiptSHA256")
	_ = unstructured.SetNestedField(exhausted.Object, message, "status", "message")
	return exhausted
}

type namespaceQueue struct {
	namespace   string
	lastStarted int64
	queue       *Queue
}

// ClaimAcrossNamespaces prioritizes the namespace that has gone longest
// without starting a matching operation, then relies on Queue.Claim for the
// existing per-instance fairness, Lease exclusion, and optimistic fencing.
func ClaimAcrossNamespaces(
	ctx context.Context,
	client dynamic.Interface,
	namespaces []string,
	owner, operationType string,
	lease time.Duration,
) (*Claim, error) {
	if err := validateOperationTypeFilter(operationType); err != nil {
		return nil, err
	}
	for _, namespace := range namespaces {
		if err := validateQueueNamespace(namespace); err != nil {
			return nil, err
		}
	}
	queues := make([]namespaceQueue, 0, len(namespaces))
	var inspectErrs []error
	for _, namespace := range namespaces {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(append(inspectErrs, err)...)
		}
		queue := New(client, namespace)
		lastStarted, err := queue.lastStarted(ctx, operationType)
		if err != nil {
			inspectErrs = append(inspectErrs, fmt.Errorf("%s: inspect queue: %w", namespace, err))
			if ctx.Err() != nil {
				return nil, errors.Join(inspectErrs...)
			}
			continue
		}
		queues = append(queues, namespaceQueue{
			namespace: namespace, lastStarted: lastStarted, queue: queue,
		})
	}
	if len(inspectErrs) != 0 {
		return nil, errors.Join(inspectErrs...)
	}
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].lastStarted != queues[j].lastStarted {
			return queues[i].lastStarted < queues[j].lastStarted
		}
		return queues[i].namespace < queues[j].namespace
	})
	for _, candidate := range queues {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		claim, err := candidate.queue.Claim(ctx, owner, operationType, lease)
		if err == nil {
			return claim, nil
		}
		if !errors.Is(err, ErrNoOperation) {
			return nil, fmt.Errorf("%s: claim: %w", candidate.namespace, err)
		}
	}
	return nil, ErrNoOperation
}

func (q *Queue) lastStarted(ctx context.Context, operationType string) (int64, error) {
	if err := q.validateNamespace(); err != nil {
		return 0, err
	}
	if err := validateOperationTypeFilter(operationType); err != nil {
		return 0, err
	}
	list, err := q.resource.List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	var latest int64
	for i := range list.Items {
		candidateType, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "type")
		if operationType != "" && candidateType != operationType {
			continue
		}
		started, found, _ := unstructured.NestedInt64(
			list.Items[i].Object, "status", "startedAtUnixNano",
		)
		if !found {
			seconds, _, _ := unstructured.NestedInt64(
				list.Items[i].Object, "status", "startedAtUnix",
			)
			started = seconds * int64(time.Second)
		}
		if started > latest {
			latest = started
		}
	}
	return latest, nil
}

func requiresApproval(operationType string) bool {
	switch operationType {
	case "RestoreCutover", "CertificateRotation", "Destroy", "BackupDeletion":
		return true
	default:
		return false
	}
}

func isSupportedOperationType(operationType string) bool {
	switch operationType {
	case "Backup", "BackupDeletion", "RestoreCutover", "PostRestoreAudit", "CertificateRotation", "Destroy":
		return true
	default:
		return false
	}
}

// ValidInstanceName mirrors the KubeBrainOperation CRD's spec.instance schema.
func ValidInstanceName(instance string) bool {
	return validOperationIdentifier(instance)
}

func validOperationIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 || !isOperationIdentifierFirst(value[0]) {
		return false
	}
	for i := 1; i < len(value); i++ {
		if !isOperationIdentifierChar(value[i]) {
			return false
		}
	}
	return true
}

func isOperationIdentifierFirst(value byte) bool {
	return (value >= 'A' && value <= 'Z') ||
		(value >= 'a' && value <= 'z') ||
		(value >= '0' && value <= '9')
}

func isOperationIdentifierChar(value byte) bool {
	return isOperationIdentifierFirst(value) ||
		value == '.' || value == '_' || value == '-'
}

// ValidParameterSecretKey mirrors the KubeBrainOperation CRD's parameter key schema.
func ValidParameterSecretKey(key string) bool {
	if len(key) == 0 || len(key) > 253 {
		return false
	}
	for i := 0; i < len(key); i++ {
		value := key[i]
		if !isOperationIdentifierChar(value) {
			return false
		}
	}
	return true
}

// ValidParameterSecretName mirrors the KubeBrainOperation CRD's parameter
// Secret reference name schema.
func ValidParameterSecretName(name string) bool {
	return len(validation.IsDNS1123Subdomain(name)) == 0
}

// ValidateRequester validates the KubeBrainOperation spec.requestedBy audit text.
func ValidateRequester(requester string) error {
	if err := validateOptionalAuditText(requester, maxRequesterLength); err != nil {
		return fmt.Errorf("invalid operation requester: %w", err)
	}
	return nil
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
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	if err := ValidateWorkerIdentity(owner, attempt); err != nil {
		return nil, err
	}
	if err := validateStatusMessage(message); err != nil {
		return nil, err
	}
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	phase, _, _ := unstructured.NestedString(object.Object, "status", "phase")
	actualOwner, _, _ := unstructured.NestedString(object.Object, "status", "owner")
	actualAttempt, _, _ := unstructured.NestedInt64(object.Object, "status", "attempt")
	actualMessage, _, _ := unstructured.NestedString(object.Object, "status", "message")
	actualLeaseUntil, _, _ := unstructured.NestedInt64(object.Object, "status", "leaseUntilUnix")
	if phase == PhasePending {
		if owner == "" || attempt <= 0 || actualOwner != "" ||
			actualAttempt != attempt || actualMessage != message || actualLeaseUntil != 0 {
			return nil, ErrFenced
		}
		instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
		holder := leaseHolder(object, owner, attempt)
		if releaseErr := q.retryInstanceLeaseCleanup(ctx, instance, holder); releaseErr != nil {
			return object, fmt.Errorf("retry instance lease cleanup after requeue: %w", releaseErr)
		}
		return object, nil
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
	if err != nil {
		latest, committed, reconcileErr := q.reconcileFailedStatusTransition(
			ctx, name, instance, holder, "requeue",
			func(candidate *unstructured.Unstructured) bool {
				candidatePhase, _, _ := unstructured.NestedString(
					candidate.Object, "status", "phase",
				)
				candidateOwner, _, _ := unstructured.NestedString(
					candidate.Object, "status", "owner",
				)
				candidateAttempt, _, _ := unstructured.NestedInt64(
					candidate.Object, "status", "attempt",
				)
				candidateMessage, _, _ := unstructured.NestedString(
					candidate.Object, "status", "message",
				)
				candidateLeaseUntil, _, _ := unstructured.NestedInt64(
					candidate.Object, "status", "leaseUntilUnix",
				)
				return candidatePhase == PhasePending && candidateOwner == "" &&
					candidateAttempt == attempt && candidateMessage == message &&
					candidateLeaseUntil == 0
			},
		)
		if committed {
			if reconcileErr != nil {
				return latest, reconcileErr
			}
			return latest, nil
		}
		if reconcileErr != nil {
			return nil, errors.Join(err, reconcileErr)
		}
		if apierrors.IsConflict(err) {
			return nil, ErrFenced
		}
		return nil, err
	}
	if releaseErr := q.releaseInstanceLeaseForCleanup(ctx, instance, holder); releaseErr != nil {
		return result, fmt.Errorf("release instance lease after requeue: %w", releaseErr)
	}
	return result, nil
}

func (q *Queue) Heartbeat(ctx context.Context, name, owner string, attempt int64, lease time.Duration) (*Claim, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	if err := ValidateWorkerIdentity(owner, attempt); err != nil {
		return nil, err
	}
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
	if err != nil {
		reconciled, reconcileErr := q.reconcileFailedHeartbeat(
			ctx, name, owner, attempt, instance, holder,
			now+int64(lease/time.Second),
		)
		if reconcileErr == nil {
			return reconciled, nil
		}
		if apierrors.IsConflict(err) && errors.Is(reconcileErr, ErrFenced) {
			return nil, ErrFenced
		}
		return nil, errors.Join(err, reconcileErr)
	}
	return claimFrom(result)
}

func (q *Queue) reconcileFailedHeartbeat(
	parent context.Context,
	name, owner string,
	attempt int64,
	instance, holder string,
	leaseUntil int64,
) (*Claim, error) {
	ctx, cancel := leaseCleanupContext(parent)
	defer cancel()
	latest, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("inspect operation after failed heartbeat: %w", err)
	}
	phase, _, _ := unstructured.NestedString(latest.Object, "status", "phase")
	actualOwner, _, _ := unstructured.NestedString(latest.Object, "status", "owner")
	actualAttempt, _, _ := unstructured.NestedInt64(latest.Object, "status", "attempt")
	actualLeaseUntil, _, _ := unstructured.NestedInt64(latest.Object, "status", "leaseUntilUnix")
	if phase == PhaseRunning && actualOwner == owner && actualAttempt == attempt &&
		actualLeaseUntil == leaseUntil {
		return claimFrom(latest)
	}
	if releaseErr := q.releaseInstanceLease(ctx, instance, holder); releaseErr != nil &&
		!errors.Is(releaseErr, ErrFenced) {
		return nil, fmt.Errorf("release instance lease after failed heartbeat: %w", releaseErr)
	}
	return nil, ErrFenced
}

func (q *Queue) Finish(
	ctx context.Context,
	name, owner string,
	attempt int64,
	succeeded bool,
	receiptSHA256, message string,
) (*unstructured.Unstructured, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	if err := ValidateWorkerIdentity(owner, attempt); err != nil {
		return nil, err
	}
	if err := validateStatusMessage(message); err != nil {
		return nil, err
	}
	if !succeeded && receiptSHA256 != "" {
		return nil, errors.New("failed operation cannot carry a receipt SHA-256")
	}
	if succeeded && receiptSHA256 == "" {
		return nil, errors.New("successful operation requires a receipt SHA-256 hex digest")
	}
	if receiptSHA256 != "" && !isSHA256Hex(receiptSHA256) {
		return nil, errors.New("operation receipt SHA-256 hex digest must be empty or lowercase")
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
		actualLeaseUntil, _, _ := unstructured.NestedInt64(
			object.Object, "status", "leaseUntilUnix",
		)
		actualCompletedAt, _, _ := unstructured.NestedInt64(
			object.Object, "status", "completedAtUnix",
		)
		expectedPhase := PhaseFailed
		if succeeded {
			expectedPhase = PhaseSucceeded
		}
		if phase == expectedPhase && actualOwner == owner && actualAttempt == attempt &&
			actualReceipt == receiptSHA256 && actualMessage == message &&
			actualLeaseUntil == 0 && actualCompletedAt > 0 {
			instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
			holder := leaseHolder(object, owner, attempt)
			if releaseErr := q.retryInstanceLeaseCleanup(ctx, instance, holder); releaseErr != nil {
				return object, fmt.Errorf("retry instance lease cleanup after finish: %w", releaseErr)
			}
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
	if err != nil {
		latest, committed, reconcileErr := q.reconcileFailedStatusTransition(
			ctx, name, instance, holder, "finish",
			func(candidate *unstructured.Unstructured) bool {
				candidatePhase, _, _ := unstructured.NestedString(
					candidate.Object, "status", "phase",
				)
				candidateOwner, _, _ := unstructured.NestedString(
					candidate.Object, "status", "owner",
				)
				candidateAttempt, _, _ := unstructured.NestedInt64(
					candidate.Object, "status", "attempt",
				)
				candidateReceipt, _, _ := unstructured.NestedString(
					candidate.Object, "status", "receiptSHA256",
				)
				candidateMessage, _, _ := unstructured.NestedString(
					candidate.Object, "status", "message",
				)
				candidateLeaseUntil, _, _ := unstructured.NestedInt64(
					candidate.Object, "status", "leaseUntilUnix",
				)
				candidateCompletedAt, _, _ := unstructured.NestedInt64(
					candidate.Object, "status", "completedAtUnix",
				)
				return candidatePhase == targetPhase && candidateOwner == owner &&
					candidateAttempt == attempt && candidateReceipt == receiptSHA256 &&
					candidateMessage == message && candidateLeaseUntil == 0 &&
					candidateCompletedAt > 0
			},
		)
		if committed {
			if reconcileErr != nil {
				return latest, reconcileErr
			}
			return latest, nil
		}
		if reconcileErr != nil {
			return nil, errors.Join(err, reconcileErr)
		}
		if apierrors.IsConflict(err) {
			return nil, ErrFenced
		}
		return nil, err
	}
	if releaseErr := q.releaseInstanceLeaseForCleanup(ctx, instance, holder); releaseErr != nil {
		return result, fmt.Errorf("release instance lease after finish: %w", releaseErr)
	}
	return result, nil
}

func (q *Queue) reconcileFailedStatusTransition(
	parent context.Context,
	name, instance, holder, transition string,
	committed func(*unstructured.Unstructured) bool,
) (*unstructured.Unstructured, bool, error) {
	ctx, cancel := leaseCleanupContext(parent)
	defer cancel()
	latest, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, false, fmt.Errorf(
			"inspect operation after failed %s: %w", transition, err,
		)
	}
	isCommitted := committed(latest)
	if releaseErr := q.releaseInstanceLease(ctx, instance, holder); releaseErr != nil &&
		!errors.Is(releaseErr, ErrFenced) {
		return latest, isCommitted, fmt.Errorf(
			"release instance lease after failed %s: %w", transition, releaseErr,
		)
	}
	return latest, isCommitted, nil
}

func (q *Queue) Get(ctx context.Context, name string) (*unstructured.Unstructured, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	return q.resource.Get(ctx, name, metav1.GetOptions{})
}

func (q *Queue) Parameters(ctx context.Context, name string) ([]byte, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return q.parameters(ctx, object)
}

func (q *Queue) ParametersForWorker(
	ctx context.Context, name, operationType, owner string, attempt int64,
) ([]byte, error) {
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	if !isSupportedOperationType(operationType) {
		return nil, fmt.Errorf("unsupported operation type: %s", operationType)
	}
	if err := ValidateWorkerIdentity(owner, attempt); err != nil {
		return nil, err
	}
	object, err := q.resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	actualType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
	if actualType != operationType {
		return nil, errors.New("operation type does not match worker identity")
	}
	if err := q.requireWorker(object, owner, attempt); err != nil {
		return nil, err
	}
	return q.parameters(ctx, object)
}

func (q *Queue) parameters(
	ctx context.Context, object *unstructured.Unstructured,
) ([]byte, error) {
	secretName, _, _ := unstructured.NestedString(
		object.Object, "spec", "parametersSecretRef", "name",
	)
	key, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "key")
	expected, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSHA256")
	if secretName == "" || key == "" {
		return nil, errors.New("operation does not reference managed parameters")
	}
	if errs := validation.IsDNS1123Subdomain(secretName); len(errs) > 0 {
		return nil, fmt.Errorf("invalid parameter secret name: %s", errs[0])
	}
	if !ValidParameterSecretKey(key) {
		return nil, errors.New("invalid parameter secret key")
	}
	if !isSHA256Hex(expected) {
		return nil, errors.New("operation parameters digest is invalid")
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
	if err := q.validateNamespace(); err != nil {
		return nil, err
	}
	if err := ValidateOperationName(name); err != nil {
		return nil, err
	}
	if approvedBy != operationaudit.ApproverUsername {
		return nil, fmt.Errorf(
			"operation approval requires the dedicated approver identity %q",
			operationaudit.ApproverUsername,
		)
	}
	if len(approvalID) > 128 || len(validation.IsDNS1123Subdomain(approvalID)) != 0 {
		return nil, errors.New("a DNS-compatible approval ID is required")
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
	result, err := q.resource.Update(ctx, updated, metav1.UpdateOptions{})
	if err == nil {
		return result, nil
	}
	reconcileCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	latest, getErr := q.resource.Get(reconcileCtx, name, metav1.GetOptions{})
	if getErr != nil {
		return nil, errors.Join(
			err, fmt.Errorf("inspect operation after failed approval: %w", getErr),
		)
	}
	if latest.GetUID() != object.GetUID() {
		return nil, errors.Join(err, errors.New("operation was replaced while approving"))
	}
	latestAnnotations := latest.GetAnnotations()
	if latestAnnotations[operationaudit.ApprovedByAnnotation] == approvedBy &&
		latestAnnotations[operationaudit.ApprovalIDAnnotation] == approvalID {
		return latest, nil
	}
	return nil, err
}

func (q *Queue) Delete(ctx context.Context, name string, uid types.UID) error {
	if err := q.validateNamespace(); err != nil {
		return err
	}
	if err := ValidateOperationName(name); err != nil {
		return err
	}
	if uid == "" {
		return errors.New("operation UID precondition is required")
	}
	policy := metav1.DeletePropagationForeground
	err := q.resource.Delete(ctx, name, metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &uid},
		PropagationPolicy: &policy,
	})
	if err == nil || apierrors.IsNotFound(err) {
		return nil
	}
	reconcileCtx, cancel := leaseCleanupContext(ctx)
	defer cancel()
	current, getErr := q.resource.Get(reconcileCtx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return nil
	}
	if getErr != nil {
		return errors.Join(
			err, fmt.Errorf("inspect operation after failed delete: %w", getErr),
		)
	}
	if current.GetUID() != uid {
		return nil
	}
	return err
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (q *Queue) requireWorker(object *unstructured.Unstructured, owner string, attempt int64) error {
	if err := validateStatusOwner(owner); err != nil {
		return err
	}
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

func validateOperationSpec(name string, spec Spec) error {
	if err := ValidateOperationName(name); err != nil {
		return invalidSpecError("%s", err)
	}
	if spec.OperationID == "" || spec.Instance == "" || spec.Type == "" || spec.MaxAttempts <= 0 {
		return invalidSpecError("operation spec is incomplete")
	}
	if !validOperationIdentifier(spec.OperationID) {
		return invalidSpecError("invalid operation ID")
	}
	if !ValidInstanceName(spec.Instance) {
		return invalidSpecError("invalid operation instance")
	}
	if !isSupportedOperationType(spec.Type) {
		return invalidSpecError("unsupported operation type: %s", spec.Type)
	}
	if spec.MaxAttempts > MaxOperationAttempts {
		return invalidSpecError("operation maxAttempts cannot exceed %d", MaxOperationAttempts)
	}
	if !isSHA256Hex(spec.ParametersSHA256) {
		return invalidSpecError("operation spec requires a lowercase SHA-256 parameters digest")
	}
	if spec.Tenant != "" {
		if errs := validation.IsDNS1123Label(spec.Tenant); len(errs) > 0 {
			return invalidSpecError("invalid operation tenant: %s", errs[0])
		}
	}
	if err := ValidateRequester(spec.RequestedBy); err != nil {
		return invalidSpecError("%s", err)
	}
	if (spec.ParametersSecret == "") != (spec.ParametersKey == "") {
		return invalidSpecError("parameter secret name and key must be specified together")
	}
	if spec.ParametersSecret != "" {
		if !ValidParameterSecretName(spec.ParametersSecret) {
			return invalidSpecError("invalid parameter secret name")
		}
		if !ValidParameterSecretKey(spec.ParametersKey) {
			return invalidSpecError("invalid parameter secret key")
		}
	}
	return nil
}

func validateClaimCandidate(
	name string,
	spec Spec,
	phase string,
	attempt, leaseUntil int64,
) error {
	if err := validateOperationSpec(name, spec); err != nil {
		return fmt.Errorf("listed operation %s is invalid: %w", name, err)
	}
	switch phase {
	case "", PhasePending:
		if attempt < 0 || leaseUntil < 0 {
			return fmt.Errorf("listed operation %s has invalid pending status", name)
		}
	case PhaseRunning:
		if attempt <= 0 || leaseUntil <= 0 {
			return fmt.Errorf("listed operation %s has invalid running status", name)
		}
	case PhaseSucceeded, PhaseFailed:
		return nil
	default:
		return fmt.Errorf("listed operation %s has invalid phase %q", name, phase)
	}
	return nil
}

func specFromObject(object *unstructured.Unstructured) Spec {
	operationID, _, _ := unstructured.NestedString(object.Object, "spec", "operationID")
	tenant, _, _ := unstructured.NestedString(object.Object, "spec", "tenant")
	requestedBy, _, _ := unstructured.NestedString(object.Object, "spec", "requestedBy")
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	operationType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
	digest, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSHA256")
	secret, _, _ := unstructured.NestedString(
		object.Object, "spec", "parametersSecretRef", "name",
	)
	key, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "key")
	maxAttempts, _, _ := unstructured.NestedInt64(object.Object, "spec", "maxAttempts")
	return Spec{
		OperationID: operationID, Tenant: tenant, RequestedBy: requestedBy,
		Instance: instance, Type: operationType, ParametersSHA256: digest,
		ParametersSecret: secret, ParametersKey: key, MaxAttempts: maxAttempts,
	}
}

func claimFrom(object *unstructured.Unstructured) (*Claim, error) {
	spec := specFromObject(object)
	if err := validateOperationSpec(object.GetName(), spec); err != nil {
		return nil, err
	}
	owner, _, _ := unstructured.NestedString(object.Object, "status", "owner")
	attempt, _, _ := unstructured.NestedInt64(object.Object, "status", "attempt")
	leaseUntil, _, _ := unstructured.NestedInt64(object.Object, "status", "leaseUntilUnix")
	if err := ValidateWorkerIdentity(owner, attempt); err != nil {
		return nil, err
	}
	if leaseUntil <= 0 {
		return nil, errors.New("claimed operation lease is missing")
	}
	return &Claim{
		Name: object.GetName(), UID: string(object.GetUID()), ResourceVersion: object.GetResourceVersion(),
		OperationID: spec.OperationID, Tenant: spec.Tenant, RequestedBy: spec.RequestedBy,
		Instance: spec.Instance, Type: spec.Type,
		ParametersSHA256: spec.ParametersSHA256,
		ParametersSecret: spec.ParametersSecret, ParametersKey: spec.ParametersKey,
		Owner: owner, Attempt: attempt, LeaseUntilUnix: leaseUntil,
	}, nil
}

func specMatches(object *unstructured.Unstructured, spec Spec) bool {
	operationID, _, _ := unstructured.NestedString(object.Object, "spec", "operationID")
	tenant, _, _ := unstructured.NestedString(object.Object, "spec", "tenant")
	requestedBy, _, _ := unstructured.NestedString(object.Object, "spec", "requestedBy")
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	operationType, _, _ := unstructured.NestedString(object.Object, "spec", "type")
	digest, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSHA256")
	maxAttempts, _, _ := unstructured.NestedInt64(object.Object, "spec", "maxAttempts")
	secret, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "name")
	key, _, _ := unstructured.NestedString(object.Object, "spec", "parametersSecretRef", "key")
	return operationID == spec.OperationID && tenant == spec.Tenant &&
		requestedBy == spec.RequestedBy && instance == spec.Instance &&
		operationType == spec.Type && digest == spec.ParametersSHA256 &&
		maxAttempts == spec.MaxAttempts && secret == spec.ParametersSecret &&
		key == spec.ParametersKey
}

func operationTypeMetaMatches(object *unstructured.Unstructured) bool {
	return object.GetAPIVersion() == operationAPIVersion && object.GetKind() == operationKind
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(value); i++ {
		switch {
		case value[i] >= '0' && value[i] <= '9':
		case value[i] >= 'a' && value[i] <= 'f':
		default:
			return false
		}
	}
	return true
}

func validateStatusOwner(owner string) error {
	if err := validateOptionalAuditText(owner, maxStatusOwnerLength); err != nil {
		return fmt.Errorf("operation status owner is invalid: %w", err)
	}
	return nil
}

func validateStatusMessage(message string) error {
	if err := validateOptionalAuditText(message, maxStatusMessageLength); err != nil {
		return fmt.Errorf("operation status message is invalid: %w", err)
	}
	return nil
}

func validateOptionalAuditText(value string, maxRunes int) error {
	if !utf8.ValidString(value) {
		return errors.New("must be valid UTF-8")
	}
	if utf8.RuneCountInString(value) > maxRunes {
		return fmt.Errorf("exceeds %d characters", maxRunes)
	}
	if strings.IndexFunc(value, unicode.IsControl) != -1 {
		return errors.New("must not contain control characters")
	}
	return nil
}

func invalidSpecError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(format, args...))
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
		return q.reconcileInstanceLeaseWrite(ctx, name, lease, err)
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
	if err == nil {
		return nil
	}
	return q.reconcileInstanceLeaseWrite(ctx, name, updated, err)
}

func (q *Queue) reconcileInstanceLeaseWrite(
	parent context.Context,
	name string,
	expected *unstructured.Unstructured,
	writeErr error,
) error {
	ctx, cancel := leaseCleanupContext(parent)
	defer cancel()
	actual, err := q.leases.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return errors.Join(
			writeErr,
			fmt.Errorf("inspect instance lease after failed write: %w", err),
		)
	}
	if instanceLeaseStateMatches(actual, expected) {
		return nil
	}
	return writeErr
}

func instanceLeaseStateMatches(actual, expected *unstructured.Unstructured) bool {
	for _, field := range []string{
		"holderIdentity", "acquireTime", "renewTime",
	} {
		actualValue, _, _ := unstructured.NestedString(actual.Object, "spec", field)
		expectedValue, _, _ := unstructured.NestedString(expected.Object, "spec", field)
		if actualValue != expectedValue {
			return false
		}
	}
	actualDuration, _, _ := unstructured.NestedInt64(
		actual.Object, "spec", "leaseDurationSeconds",
	)
	expectedDuration, _, _ := unstructured.NestedInt64(
		expected.Object, "spec", "leaseDurationSeconds",
	)
	return actualDuration == expectedDuration
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

func (q *Queue) releaseInstanceLeaseForCleanup(
	parent context.Context,
	instance, holder string,
) error {
	ctx, cancel := leaseCleanupContext(parent)
	defer cancel()
	return q.releaseInstanceLease(ctx, instance, holder)
}

func (q *Queue) retryInstanceLeaseCleanup(
	parent context.Context,
	instance, holder string,
) error {
	err := q.releaseInstanceLeaseForCleanup(parent, instance, holder)
	if errors.Is(err, ErrFenced) {
		return nil
	}
	return err
}

func leaseCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), leaseCleanupTimeout)
}

func wrapIfError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
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
