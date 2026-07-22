package operationauditrelease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

func Release(
	ctx context.Context,
	client dynamic.Interface,
	namespace, name, artifactPath, receiptPath string,
) (*unstructured.Unstructured, error) {
	return ReleaseWithExpectedReceipt(
		ctx, client, namespace, name, artifactPath, receiptPath, ExpectedArchiveReceipt{},
	)
}

type ExpectedArchiveReceipt struct {
	ObjectStoreID   string
	Bucket          string
	ObjectKey       string
	RetentionMode   string
	RetainUntilUnix int64
}

func ReleaseWithExpectedReceipt(
	ctx context.Context,
	client dynamic.Interface,
	namespace, name, artifactPath, receiptPath string,
	expected ExpectedArchiveReceipt,
) (*unstructured.Unstructured, error) {
	if namespace == "" || name == "" || artifactPath == "" || receiptPath == "" {
		return nil, errors.New("audit release request is incomplete")
	}
	artifactStatus, err := operationaudit.Inspect(artifactPath)
	if err != nil {
		return nil, err
	}
	receipt, receiptSHA, err := operationaudit.InspectArchiveReceipt(receiptPath)
	if err != nil {
		return nil, err
	}
	if !receipt.Matches(artifactStatus) {
		return nil, errors.New("archive receipt does not match the operation audit artifact")
	}
	if err := expected.matches(receipt); err != nil {
		return nil, err
	}
	resource := client.Resource(operationqueue.Resource).Namespace(namespace)
	object, err := resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	current, err := operationauditbuilder.FromOperation(object)
	if err != nil {
		return nil, err
	}
	if current != artifactStatus.Artifact {
		return nil, errors.New("current terminal operation does not match the archived artifact")
	}
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if !contains(object.GetFinalizers(), operationaudit.Finalizer) {
		if annotations[operationaudit.ReceiptSHAAnnotation] == receiptSHA &&
			annotations[operationaudit.ArtifactSHAAnnotation] == artifactStatus.SHA256 &&
			annotations[operationaudit.VersionAnnotation] == receipt.VersionID {
			return object, nil
		}
		return nil, errors.New("operation audit finalizer is missing without matching archive annotations")
	}
	updated := object.DeepCopy()
	updatedAnnotations := make(map[string]string, len(annotations)+3)
	for key, value := range annotations {
		updatedAnnotations[key] = value
	}
	updatedAnnotations[operationaudit.ReceiptSHAAnnotation] = receiptSHA
	updatedAnnotations[operationaudit.ArtifactSHAAnnotation] = artifactStatus.SHA256
	updatedAnnotations[operationaudit.VersionAnnotation] = receipt.VersionID
	updated.SetAnnotations(updatedAnnotations)
	finalizers := make([]string, 0, len(object.GetFinalizers())-1)
	for _, finalizer := range object.GetFinalizers() {
		if finalizer != operationaudit.Finalizer {
			finalizers = append(finalizers, finalizer)
		}
	}
	updated.SetFinalizers(finalizers)
	result, err := resource.Update(ctx, updated, metav1.UpdateOptions{})
	if err == nil {
		return result, nil
	}
	reconcileCtx, cancel := releaseReconciliationContext(ctx)
	defer cancel()
	currentObject, getErr := resource.Get(reconcileCtx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return updated, nil
	}
	if getErr != nil {
		return nil, errors.Join(
			err, fmt.Errorf("inspect operation after failed audit release: %w", getErr),
		)
	}
	if currentObject.GetUID() != object.GetUID() {
		return nil, errors.Join(
			err, errors.New("operation was replaced while releasing audit finalizer"),
		)
	}
	currentAnnotations := currentObject.GetAnnotations()
	if !contains(currentObject.GetFinalizers(), operationaudit.Finalizer) &&
		currentAnnotations[operationaudit.ReceiptSHAAnnotation] == receiptSHA &&
		currentAnnotations[operationaudit.ArtifactSHAAnnotation] == artifactStatus.SHA256 &&
		currentAnnotations[operationaudit.VersionAnnotation] == receipt.VersionID {
		return currentObject, nil
	}
	if apierrors.IsConflict(err) {
		return nil, fmt.Errorf("operation changed while releasing audit finalizer: %w", err)
	}
	return nil, err
}

func (e ExpectedArchiveReceipt) matches(receipt operationaudit.ArchiveReceipt) error {
	if e.ObjectStoreID == "" && e.Bucket == "" && e.ObjectKey == "" &&
		e.RetentionMode == "" && e.RetainUntilUnix == 0 {
		return nil
	}
	if e.ObjectStoreID == "" || e.Bucket == "" || e.ObjectKey == "" ||
		(e.RetentionMode != "COMPLIANCE" && e.RetentionMode != "GOVERNANCE") ||
		e.RetainUntilUnix <= 0 {
		return errors.New("expected archive receipt scope is incomplete")
	}
	if receipt.ObjectStoreID != e.ObjectStoreID || receipt.Bucket != e.Bucket ||
		receipt.ObjectKey != e.ObjectKey {
		return errors.New("archive receipt does not match expected object")
	}
	if receipt.RetentionMode != e.RetentionMode || receipt.RetainUntilUnix != e.RetainUntilUnix {
		return errors.New("archive receipt does not match expected retention")
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func releaseReconciliationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
}
