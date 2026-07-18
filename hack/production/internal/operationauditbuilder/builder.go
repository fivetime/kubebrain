package operationauditbuilder

import (
	"errors"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func FromOperation(object *unstructured.Unstructured) (operationaudit.Artifact, error) {
	if object == nil {
		return operationaudit.Artifact{}, errors.New("operation is nil")
	}
	stringField := func(fields ...string) string {
		value, _, _ := unstructured.NestedString(object.Object, fields...)
		return value
	}
	intField := func(fields ...string) int64 {
		value, _, _ := unstructured.NestedInt64(object.Object, fields...)
		return value
	}
	artifact := operationaudit.Artifact{
		Format: operationaudit.Format, APIVersion: object.GetAPIVersion(),
		Namespace: object.GetNamespace(), Name: object.GetName(), UID: string(object.GetUID()),
		Generation:  object.GetGeneration(),
		OperationID: stringField("spec", "operationID"), Instance: stringField("spec", "instance"),
		Type: stringField("spec", "type"), ParametersSHA256: stringField("spec", "parametersSHA256"),
		MaxAttempts: intField("spec", "maxAttempts"), Phase: stringField("status", "phase"),
		Owner: stringField("status", "owner"), Attempt: intField("status", "attempt"),
		ObservedGeneration: intField("status", "observedGeneration"),
		StartedAtUnix:      intField("status", "startedAtUnix"),
		StartedAtUnixNano:  intField("status", "startedAtUnixNano"),
		CompletedAtUnix:    intField("status", "completedAtUnix"),
		ReceiptSHA256:      stringField("status", "receiptSHA256"),
		Message:            stringField("status", "message"),
	}
	artifact.ApprovedBy = object.GetAnnotations()[operationaudit.ApprovedByAnnotation]
	artifact.ApprovalID = object.GetAnnotations()[operationaudit.ApprovalIDAnnotation]
	if err := artifact.Validate(); err != nil {
		return operationaudit.Artifact{}, err
	}
	return artifact, nil
}
