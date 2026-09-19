package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	strictjson "sigs.k8s.io/json"
)

// ReserveNetwork persists intent, creates the inactive policy, then durably saves
// its actual response. It never activates, labels a Pod, adopts an existing policy
// or implements a CREATE retry loop. The supplied client's transport retry policy
// must also be reviewed by the caller. Call before any Pod mutation.
// An error after arming requires
// reconciliation even if no receipt exists; repeating this call cannot rearm.
// admit must verify live namespace/StatefulSet/Pod identities, exclusive lifecycle
// ownership and that the reserved nonce selects no endpoints. Keep ownership
// through CREATE and subsequent activation/recovery, not just during admit.
func ReserveNetwork(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, admit func(context.Context) error) error {
	if ctx == nil || client == nil || admit == nil {
		return errors.New("network reservation requires context, client and admission")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("network reservation requires a bounded deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := plan.encoded(); err != nil {
		return err
	}
	var obj map[string]interface{}
	if err := strictjson.UnmarshalCaseSensitivePreserveInts(plan.ApprovedPolicy, &obj); err != nil {
		return err
	}
	policy := &unstructured.Unstructured{Object: obj}
	// Do not submit server identity from an admitted template or preserve status.
	if policy.GetUID() != "" || policy.GetResourceVersion() != "" || policy.GetGenerateName() != "" || obj["status"] != nil {
		return errors.New("network reservation requires a create template")
	}
	if err := unstructured.SetNestedField(obj, plan.ReservedNonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"); err != nil {
		return err
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// This create-once intent also prevents retrying an ambiguous API request.
	if err := ArmNetworkRecovery(dir, plan); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	resource := schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}
	receipt, err := client.Resource(resource).Namespace(plan.Namespace).Create(ctx, policy, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if receipt == nil {
		return errors.New("empty network reservation response")
	}
	raw, err := json.Marshal(receipt.Object)
	if err != nil {
		return err
	}
	// Preserve a successful API response even if cancellation raced with it.
	if err := SaveNetworkReservation(dir, plan, raw); err != nil {
		return err
	}
	return ctx.Err()
}
