package leasefault

import (
	"bytes"
	"context"
	"errors"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// RemoveNetworkPolicy confirms API absence of the recorded policy. It must run
// after joining all fault workers, with an independent bounded recovery context.
// admit verifies live namespace/StatefulSet/Pod identity and exclusive ownership,
// held throughout recovery. Success is NOT Cilium withdrawal: the coordinator
// must verify dataplane recovery before protocol recovery or label restoration.
// Missing receipts, conflicts and ambiguous writes fail closed; no delete retry
// or adoption of an unrecorded same-name object is performed.
func RemoveNetworkPolicy(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, admit func(context.Context) error) error {
	if ctx == nil || client == nil || admit == nil {
		return errors.New("network removal requires context, client and admission")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("network removal requires a bounded recovery deadline")
	}
	raw, err := LoadNetworkReservation(dir, plan)
	if err != nil {
		return err
	}
	var receipt unstructured.Unstructured
	if err := receipt.UnmarshalJSON(raw); err != nil {
		return err
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		retained, err := LoadNetworkReservation(dir, plan)
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, retained) {
			return errors.New("reservation receipt changed during removal")
		}
		if err := admit(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err := check(); err != nil {
		return err
	}
	resource := client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(plan.Namespace)
	current, err := resource.Get(ctx, plan.PolicyName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return check()
	}
	if err != nil {
		return err
	}
	if current == nil || current.GetUID() != receipt.GetUID() {
		return errors.New("refusing removal of replaced policy")
	}
	// Accept only the exact approved active or inactive spec. Normalize a copy,
	// preserving the original server object and its resourceVersion precondition.
	validated := current.DeepCopy()
	label, found, err := unstructured.NestedString(validated.Object, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner")
	if err != nil || !found || (label != plan.Nonce && label != plan.ReservedNonce) {
		return errors.New("unowned policy selector during removal")
	}
	if err := unstructured.SetNestedField(validated.Object, plan.ReservedNonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"); err != nil {
		return err
	}
	observed, err := validated.MarshalJSON()
	if err != nil {
		return err
	}
	if err := validateReservation(plan, observed); err != nil {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	uid, rv := current.GetUID(), current.GetResourceVersion()
	err = resource.Delete(ctx, plan.PolicyName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	_, err = resource.Get(ctx, plan.PolicyName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return check()
	}
	if err != nil {
		return err
	}
	return errors.New("policy still present after removal; recovery unproven")
}
