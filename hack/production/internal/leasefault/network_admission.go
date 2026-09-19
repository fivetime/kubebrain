package leasefault

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// NetworkLabelPhase specifies the only acceptable label states at a lifecycle
// boundary. Recovery permits an absent label because preparation may have failed.
type NetworkLabelPhase int

const (
	NetworkUnlabelled NetworkLabelPhase = iota + 1
	NetworkLabelOwned
	NetworkLabelRecovery
)

// CheckNetworkIdentity performs fresh API reads for a network-operation admission
// callback. statefulSetName must come from independent admission, not a live Pod.
// own must check the externally held exclusive lifecycle ownership; it is NOT
// acquired here. Sequential reads are not a transaction or a distributed lock.
// This function does not verify worker joining, protocol state, Cilium endpoints
// or dataplane withdrawal. Those are additional phase-specific caller obligations.
func CheckNetworkIdentity(ctx context.Context, client dynamic.Interface, plan NetworkRecovery, statefulSetName string, phase NetworkLabelPhase, own func(context.Context) error) error {
	if ctx == nil || client == nil || own == nil || !recoveryIdentity.MatchString(statefulSetName) {
		return errors.New("invalid live network admission")
	}
	if phase != NetworkUnlabelled && phase != NetworkLabelOwned && phase != NetworkLabelRecovery {
		return errors.New("invalid network label phase")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("live admission requires bounded deadline")
	}
	if _, err := plan.encoded(); err != nil {
		return err
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := own(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err := check(); err != nil {
		return err
	}
	identity := func(obj *unstructured.Unstructured, api, kind, ns, name, uid string) error {
		if obj == nil || obj.GetAPIVersion() != api || obj.GetKind() != kind || obj.GetNamespace() != ns || obj.GetName() != name || string(obj.GetUID()) != uid || obj.GetResourceVersion() == "" || obj.Object["metadata"] == nil {
			return fmt.Errorf("live %s identity mismatch", kind)
		}
		deleted, exists, err := unstructured.NestedFieldNoCopy(obj.Object, "metadata", "deletionTimestamp")
		if err != nil || (exists && deleted != nil) {
			return fmt.Errorf("live %s is deleting or malformed", kind)
		}
		return nil
	}
	ns, err := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}).Get(ctx, plan.Namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := identity(ns, "v1", "Namespace", "", plan.Namespace, plan.NamespaceUID); err != nil {
		return err
	}
	sts, err := client.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}).Namespace(plan.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := identity(sts, "apps/v1", "StatefulSet", plan.Namespace, statefulSetName, plan.StatefulSetUID); err != nil {
		return err
	}
	pods := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(plan.Namespace)
	pod, err := pods.Get(ctx, plan.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := identity(pod, "v1", "Pod", plan.Namespace, plan.PodName, plan.PodUID); err != nil {
		return err
	}
	controllers := 0
	for _, ref := range pod.GetOwnerReferences() {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		controllers++
		if ref.APIVersion != "apps/v1" || ref.Kind != "StatefulSet" || ref.Name != statefulSetName || string(ref.UID) != plan.StatefulSetUID {
			return errors.New("Pod controller identity mismatch")
		}
	}
	if controllers != 1 {
		return errors.New("Pod requires exactly one admitted controller")
	}
	labels, _, err := unstructured.NestedStringMap(pod.Object, "metadata", "labels")
	if err != nil {
		return err
	}
	label, labelled := labels["kubebrain.io/fault-owner"]
	if (labelled && label != plan.Nonce) || (phase == NetworkUnlabelled && labelled) || (phase == NetworkLabelOwned && !labelled) {
		return errors.New("Pod fault label does not match lifecycle phase")
	}
	// One complete server-side selection for both nonces: the reserved nonce
	// must match no Pods; the active nonce may match only the admitted Pod.
	selected, err := pods.List(ctx, metav1.ListOptions{LabelSelector: "kubebrain.io/fault-owner in (" + plan.Nonce + "," + plan.ReservedNonce + ")"})
	if err != nil {
		return err
	}
	if selected == nil || selected.GetContinue() != "" {
		return errors.New("incomplete fault selector observation")
	}
	if len(selected.Items) > 1 {
		return errors.New("fault selector matches multiple Pods")
	}
	if labelled != (len(selected.Items) == 1) {
		return errors.New("Pod label changed during admission")
	}
	for i := range selected.Items {
		item := &selected.Items[i]
		if err := identity(item, "v1", "Pod", plan.Namespace, plan.PodName, plan.PodUID); err != nil {
			return err
		}
		value, found, err := unstructured.NestedString(item.Object, "metadata", "labels", "kubebrain.io/fault-owner")
		if err != nil || !found || value != plan.Nonce || item.GetResourceVersion() != pod.GetResourceVersion() {
			return errors.New("fault selector collision or changed Pod")
		}
	}
	return check()
}
