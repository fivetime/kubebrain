package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// VerifyCommandDeployment checks the live StatefulSet spec before claim. The
// same independently pinned spec digest is consumed by the metric gates. This
// is one bounded GET, not a rollout wait or continuous configuration lock.
// Namespace identity is independently checked by claim acquisition and Own.
func (p ObservationCommandPlan) VerifyCommandDeployment(ctx context.Context, client dynamic.Interface, name string, admit func(context.Context) error) error {
	if err := ownerContext(ctx); err != nil {
		return err
	}
	if client == nil || admit == nil || !recoveryIdentity.MatchString(name) {
		return errors.New("deployment check requires client, name and admission")
	}
	if err := p.Bindings.Validate(); err != nil {
		return err
	}
	ns, uid, wanted := p.Bindings.Network.Namespace, p.Bindings.Network.StatefulSetUID, p.Bindings.Metrics[0].Binding.SpecSHA256
	if err := admit(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	started := time.Now()
	sts, observed := client.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}).Namespace(ns).Get(readCtx, name, metav1.GetOptions{})
	completed := time.Now()
	observed = errors.Join(observed, readCtx.Err())
	deadline, _ := readCtx.Deadline()
	if !completed.Before(deadline) {
		observed = errors.Join(observed, context.DeadlineExceeded)
	}
	cancel()
	actualUID, actualHash := "", ""
	var generation, observedGeneration int64
	if sts == nil {
		observed = errors.Join(observed, errors.New("missing admitted StatefulSet"))
	} else {
		actualUID = string(sts.GetUID())
		generation = sts.GetGeneration()
		var found bool
		var parseErr error
		observedGeneration, found, parseErr = unstructured.NestedInt64(sts.Object, "status", "observedGeneration")
		spec, specFound, specErr := unstructured.NestedMap(sts.Object, "spec")
		if data, err := json.Marshal(spec); err == nil && specFound && specErr == nil {
			actualHash = planinput.SHA256(data)
		}
		if sts.GetAPIVersion() != "apps/v1" || sts.GetKind() != "StatefulSet" || sts.GetNamespace() != ns || sts.GetName() != name || actualUID != uid || sts.GetResourceVersion() == "" || sts.GetDeletionTimestamp() != nil || generation < 1 || !found || parseErr != nil || observedGeneration != generation || actualHash != wanted {
			observed = errors.Join(observed, errors.New("live StatefulSet identity, observed generation or spec differs from admission"))
		}
	}
	// Persist identities and digests, not Pod template environment/secret data.
	data, err := json.Marshal(struct {
		Name, ExpectedUID, UID, ExpectedSpecSHA256, SpecSHA256 string
		Generation, ObservedGeneration                         int64
		Started, Completed                                     time.Time
	}{name, uid, actualUID, wanted, actualHash, generation, observedGeneration, started, completed})
	if err != nil {
		return errors.Join(observed, err)
	}
	retained := retainObserver(p.OwnerDirectory, "experiment", "deployment", data, observed)
	if err := errors.Join(observed, retained, ctx.Err()); err != nil {
		return err
	}
	return errors.Join(admit(ctx), ctx.Err())
}
