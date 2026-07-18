package operationauditrelease

import (
	"context"
	"errors"
	"fmt"

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
	annotations[operationaudit.ReceiptSHAAnnotation] = receiptSHA
	annotations[operationaudit.ArtifactSHAAnnotation] = artifactStatus.SHA256
	annotations[operationaudit.VersionAnnotation] = receipt.VersionID
	updated.SetAnnotations(annotations)
	finalizers := make([]string, 0, len(object.GetFinalizers())-1)
	for _, finalizer := range object.GetFinalizers() {
		if finalizer != operationaudit.Finalizer {
			finalizers = append(finalizers, finalizer)
		}
	}
	updated.SetFinalizers(finalizers)
	result, err := resource.Update(ctx, updated, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return nil, fmt.Errorf("operation changed while releasing audit finalizer: %w", err)
	}
	return result, err
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
