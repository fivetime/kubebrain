package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// VerifyProcessPlatforms is a preclaim read-only check of the admitted Nodes,
// not a platform inferred from a container imageID or mutable Node labels.
// It performs one bounded GET per distinct Node, retains observations, and
// brackets each read with mandatory source/input admission. It does not prove
// continuous Node identity or replace subsequent live Pod process comparisons.
func (p ObservationCommandPlan) VerifyProcessPlatforms(ctx context.Context, client dynamic.Interface, inputs CommandProcessInputs, release CommandRelease, admit func(context.Context) error) error {
	if err := ownerContext(ctx); err != nil {
		return err
	}
	if client == nil || admit == nil {
		return errors.New("Node platform check requires client and admission")
	}
	if err := p.CheckProcessImages(inputs, release); err != nil {
		return err
	}
	wanted := map[string]string{}
	uids := map[string]string{}
	for _, raw := range append([]json.RawMessage{p.Bindings.Network.PodBefore, inputs.Observer}, inputs.Metrics...) {
		var pod corev1.Pod
		if err := json.Unmarshal(raw, &pod); err != nil {
			return err
		}
		wanted[pod.Spec.NodeName] = release.Platforms[string(pod.UID)]
		uids[pod.Spec.NodeName] = release.NodeUIDs[pod.Spec.NodeName]
	}
	names := make([]string, 0, len(wanted))
	for name := range wanted {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		started := time.Now()
		node, observed := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"}).Get(requestCtx, name, metav1.GetOptions{})
		completed := time.Now()
		observed = errors.Join(observed, requestCtx.Err())
		deadline, _ := requestCtx.Deadline()
		if !completed.Before(deadline) {
			observed = errors.Join(observed, context.DeadlineExceeded)
		}
		cancel()
		uid, platform := "", ""
		if node != nil {
			uid = string(node.GetUID())
			osName, _, _ := unstructured.NestedString(node.Object, "status", "nodeInfo", "operatingSystem")
			arch, _, _ := unstructured.NestedString(node.Object, "status", "nodeInfo", "architecture")
			platform = osName + "/" + arch
			if node.GetAPIVersion() != "v1" || node.GetKind() != "Node" || node.GetName() != name || node.GetNamespace() != "" || node.GetDeletionTimestamp() != nil || uid != uids[name] || platform != wanted[name] {
				observed = errors.Join(observed, errors.New("live Node identity or platform differs from admission"))
			}
		} else if observed == nil {
			observed = errors.New("missing live Node")
		}
		data, err := json.Marshal(struct {
			Name, ExpectedUID, UID, ExpectedPlatform, Platform string
			Started, Completed                                 time.Time
		}{name, uids[name], uid, wanted[name], platform, started, completed})
		if err != nil {
			return errors.Join(observed, err)
		}
		retained := retainObserver(p.OwnerDirectory, "experiment", "node-platform", data, observed)
		if err := errors.Join(observed, retained, ctx.Err()); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}
