package leasefault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// PrepareNetworkLabel labels only the admitted Pod after durable inactive policy
// reservation. admit must hold exclusive lifecycle ownership and verify that
// Cilium has no stale endpoints selected by either nonce. No policy is activated.
func PrepareNetworkLabel(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, statefulSetName string, admit func(context.Context) error) error {
	return changeNetworkLabel(ctx, client, dir, plan, statefulSetName, admit, false)
}

// RestoreNetworkLabel removes only this owner's label. admit must additionally
// verify all workers joined and network/dataplane AND protocol recovery completed.
// Success confirms live API label absence, not Cilium identity convergence; the
// caller must verify that separately before releasing exclusive ownership.
func RestoreNetworkLabel(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, statefulSetName string, admit func(context.Context) error) error {
	return changeNetworkLabel(ctx, client, dir, plan, statefulSetName, admit, true)
}

func changeNetworkLabel(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, sts string, admit func(context.Context) error, restore bool) error {
	return changeNetworkLabelWithOwnership(ctx, client, dir, plan, sts, admit, admit, restore)
}

// Lifecycle callers keep fresh ownership around the API identity reads and run
// their complete dataplane/protocol admission once per label check. Passing that
// complete admission into CheckNetworkIdentity as well would run its remote
// collectors three times per check. Standalone callers retain the same callback
// for both obligations through changeNetworkLabel above.
func changeNetworkLabelWithOwnership(ctx context.Context, client dynamic.Interface, dir string, plan NetworkRecovery, sts string, admit, own func(context.Context) error, restore bool) error {
	if ctx == nil || client == nil || admit == nil || own == nil {
		return errors.New("label mutation requires context, client and admission")
	}
	raw, err := LoadNetworkReservation(dir, plan)
	if err != nil {
		return err
	}
	var receipt unstructured.Unstructured
	if err := receipt.UnmarshalJSON(raw); err != nil {
		return err
	}
	phase, final := NetworkUnlabelled, NetworkLabelOwned
	if restore {
		phase, final = NetworkLabelRecovery, NetworkUnlabelled
	}
	check := func(phase NetworkLabelPhase) error {
		retained, err := LoadNetworkReservation(dir, plan)
		if err != nil {
			return err
		}
		if !bytes.Equal(retained, raw) {
			return errors.New("reservation changed during label mutation")
		}
		if err := CheckNetworkIdentity(ctx, client, plan, sts, phase, own); err != nil {
			return err
		}
		policies, err := client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(plan.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		if policies == nil || policies.GetContinue() != "" {
			return errors.New("incomplete policy observation before label mutation")
		}
		reserved := false
		for i := range policies.Items {
			p := &policies.Items[i]
			if p.GetAPIVersion() != "cilium.io/v2" || p.GetKind() != "CiliumNetworkPolicy" || p.GetNamespace() != plan.Namespace {
				return errors.New("invalid policy list item")
			}
			if p.GetName() == plan.PolicyName {
				if restore || reserved || p.GetUID() != receipt.GetUID() {
					return errors.New("policy remains or reservation replaced")
				}
				data, err := p.MarshalJSON()
				if err != nil {
					return err
				}
				if err := validateReservation(plan, data); err != nil {
					return err
				}
				reserved = true
			} else if containsNonce(p.Object["spec"], plan.Nonce) || containsNonce(p.Object["specs"], plan.Nonce) {
				return errors.New("another policy references the active fault nonce")
			}
		}
		if !restore && !reserved {
			return errors.New("inactive reservation is absent")
		}
		if err := admit(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err := check(phase); err != nil {
		return err
	}
	pods := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(plan.Namespace)
	pod, err := pods.Get(ctx, plan.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod == nil || string(pod.GetUID()) != plan.PodUID || pod.GetNamespace() != plan.Namespace || pod.GetName() != plan.PodName || pod.GetResourceVersion() == "" || pod.GetDeletionTimestamp() != nil {
		return errors.New("Pod changed before label mutation")
	}
	labels, _, err := unstructured.NestedStringMap(pod.Object, "metadata", "labels")
	if err != nil {
		return err
	}
	label, labelled := labels["kubebrain.io/fault-owner"]
	if (labelled && label != plan.Nonce) || (!restore && labelled) {
		return errors.New("label ownership changed before mutation")
	}
	if restore && !labelled {
		return check(final)
	}
	patch := []map[string]interface{}{
		{"op": "test", "path": "/metadata/uid", "value": plan.PodUID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": pod.GetResourceVersion()},
	}
	if restore {
		patch = append(patch, map[string]interface{}{"op": "test", "path": "/metadata/labels/kubebrain.io~1fault-owner", "value": plan.Nonce}, map[string]interface{}{"op": "remove", "path": "/metadata/labels/kubebrain.io~1fault-owner"})
	} else {
		if labels == nil {
			labels = map[string]string{}
		}
		labels["kubebrain.io/fault-owner"] = plan.Nonce
		// The resourceVersion test prevents losing a concurrent label update.
		patch = append(patch, map[string]interface{}{"op": "add", "path": "/metadata/labels", "value": labels})
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if err := check(phase); err != nil {
		return err
	}
	if _, err := pods.Patch(ctx, plan.PodName, types.JSONPatchType, data, metav1.PatchOptions{}); err != nil {
		return err
	}
	return check(final)
}

func containsNonce(value interface{}, nonce string) bool {
	switch value := value.(type) {
	case string:
		return value == nonce
	case []interface{}:
		for _, child := range value {
			if containsNonce(child, nonce) {
				return true
			}
		}
	case map[string]interface{}:
		for _, child := range value {
			if containsNonce(child, nonce) {
				return true
			}
		}
	}
	return false
}
