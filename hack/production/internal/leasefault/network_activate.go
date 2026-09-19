package leasefault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// ActivateNetwork switches only the admitted inactive selector. origin must be
// the same original fault clock used by all acceptance gates, never a retry clock.
// admit checks live identities, owned Pod label and exclusive lifecycle ownership;
// ownership must remain held across the request. Success is API acknowledgement,
// NOT Cilium enforcement or fault acceptance. On any error reconcile after workers
// join: a timed-out PATCH may have taken effect. No application retry is performed.
func ActivateNetwork(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, origin time.Time, admit func(context.Context) error) error {
	if ctx == nil || client == nil || admit == nil || origin.IsZero() || origin.After(time.Now()) {
		return errors.New("invalid network activation context")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(origin.Add(30*time.Second)) {
		return errors.New("activation must use the original fault deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := LoadNetworkReservation(dir, plan)
	if err != nil {
		return err
	}
	var receipt unstructured.Unstructured
	if err := receipt.UnmarshalJSON(raw); err != nil {
		return err
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	resource := client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(plan.Namespace)
	current, err := resource.Get(ctx, plan.PolicyName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if current == nil || current.GetUID() != receipt.GetUID() {
		return errors.New("reservation replaced")
	}
	observed, err := current.MarshalJSON()
	if err != nil {
		return err
	}
	if err := validateReservation(plan, observed); err != nil {
		return err
	}
	patch, err := json.Marshal([]map[string]interface{}{
		{"op": "test", "path": "/metadata/uid", "value": current.GetUID()},
		{"op": "test", "path": "/metadata/resourceVersion", "value": current.GetResourceVersion()},
		{"op": "test", "path": "/spec", "value": current.Object["spec"]},
		{"op": "replace", "path": "/spec/endpointSelector/matchLabels/kubebrain.io~1fault-owner", "value": plan.Nonce},
	})
	if err != nil {
		return err
	}
	// Recheck durable admission and live ownership immediately before mutation.
	retained, err := LoadNetworkReservation(dir, plan)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, retained) {
		return errors.New("reservation receipt changed before activation")
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	active, err := resource.Patch(ctx, plan.PolicyName, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	if active == nil || active.GetUID() != receipt.GetUID() {
		return errors.New("invalid activation response identity")
	}
	label, found, err := unstructured.NestedString(active.Object, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner")
	if err != nil || !found || label != plan.Nonce {
		return errors.New("invalid activation response selector")
	}
	// Validate all other policy fields using the same inactive-spec validator.
	if err := unstructured.SetNestedField(active.Object, plan.ReservedNonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"); err != nil {
		return err
	}
	observed, err = active.MarshalJSON()
	if err != nil {
		return err
	}
	if err := validateReservation(plan, observed); err != nil {
		return err
	}
	return ctx.Err()
}
