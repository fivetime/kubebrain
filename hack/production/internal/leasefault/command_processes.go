package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	strictjson "sigs.k8s.io/json"
)

// CommandProcessInputs contains independently admitted snapshots, not discovery
// results. Metric snapshots follow the exact command target order. The caller's
// successor admission must additionally bind Observer to ObserverMemberID via
// authenticated RPC; a Pod object cannot prove an etcd member ID.
type CommandProcessInputs struct {
	JQ        string            `json:"jq"`
	Predicate string            `json:"predicate"`
	Observer  json.RawMessage   `json:"observer"`
	Metrics   []json.RawMessage `json:"metrics"`
}

// BindProcessAdmission adds fresh process comparisons to existing mandatory
// online admission. It does not replace source/image/TLS/term checks, network
// phase admission or Join. Construction neither calls hooks nor contacts API.
// Use the returned runtime/hooks with ClaimAndRun; retention fields stay unset.
func (p ObservationCommandPlan) BindProcessAdmission(r MeasuredNetworkFaultRuntime, h ObservationHooks, targets []MetricCommandTarget, inputs CommandProcessInputs) (MeasuredNetworkFaultRuntime, ObservationHooks, error) {
	client := r.Network.Lifecycle.Preparation.Client
	if client == nil || h.AdmitOriginal == nil || h.AdmitStack == nil || r.AdmitMetrics == nil || r.Network.AdmitSuccessor == nil || r.Network.Lifecycle.OutcomeAdmit == nil || len(inputs.Metrics) != len(targets) || len(targets) != len(p.Bindings.Metrics) {
		return r, h, errors.New("incomplete command process admission")
	}
	if err := p.Bindings.Validate(); err != nil {
		return r, h, err
	}
	if err := p.CheckCommandControllers(inputs, r.Network.Lifecycle.Preparation.StatefulSetName); err != nil {
		return r, h, err
	}
	if err := processgroup.ValidateExecutable(inputs.JQ); err != nil {
		return r, h, err
	}
	if _, err := planinput.ReadFile(inputs.Predicate, false, 1<<20); err != nil {
		return r, h, err
	}
	decode := func(raw []byte) (*unstructured.Unstructured, error) {
		var pod unstructured.Unstructured
		if len(raw) == 0 || len(raw) > 256<<10 {
			return nil, errors.New("invalid process snapshot size")
		}
		strict, err := strictjson.UnmarshalStrict(raw, &pod.Object, strictjson.DisallowDuplicateFields)
		if err != nil || len(strict) != 0 || pod.GetAPIVersion() != "v1" || pod.GetKind() != "Pod" || pod.GetNamespace() != p.Bindings.Network.Namespace || pod.GetName() == "" || pod.GetUID() == "" {
			return nil, errors.New("process snapshot scope mismatch")
		}
		return &pod, nil
	}
	original, err := decode(p.Bindings.Network.PodBefore)
	if err != nil {
		return r, h, err
	}
	if original.GetName() != p.Bindings.Network.PodName || string(original.GetUID()) != p.Bindings.Network.PodUID {
		return r, h, errors.New("original process snapshot mismatch")
	}
	observer, err := decode(inputs.Observer)
	if err != nil {
		return r, h, err
	}
	if observer.GetName() == original.GetName() || observer.GetUID() == original.GetUID() {
		return r, h, errors.New("successor observer must be independent")
	}
	byUID := map[string]*unstructured.Unstructured{string(original.GetUID()): original, string(observer.GetUID()): observer}
	byName := map[string]string{original.GetName(): string(original.GetUID()), observer.GetName(): string(observer.GetUID())}
	snapshots := make([][]byte, len(inputs.Metrics))
	for i, raw := range inputs.Metrics {
		pod, err := decode(raw)
		if err != nil {
			return r, h, err
		}
		uid := string(pod.GetUID())
		if pod.GetName() != targets[i].PodName || uid != targets[i].PodUID || uid != p.Bindings.Metrics[i].Binding.PodUID {
			return r, h, errors.New("metric process snapshot differs from target")
		}
		if previous, ok := byUID[uid]; ok && !reflect.DeepEqual(previous.Object, pod.Object) {
			return r, h, errors.New("contradictory snapshots for one process")
		}
		if previous, ok := byName[pod.GetName()]; ok && previous != uid {
			return r, h, errors.New("contradictory process name mapping")
		}
		byUID[uid], byName[pod.GetName()] = pod, uid
		snapshots[i] = append([]byte(nil), raw...)
	}
	originalBytes, observerBytes := append([]byte(nil), p.Bindings.Network.PodBefore...), append([]byte(nil), inputs.Observer...)
	directory, jq, predicate := p.OwnerDirectory, inputs.JQ, inputs.Predicate
	check := func(ctx context.Context, stage string, raw []byte, admit func(context.Context) error) error {
		return CheckLivePodProcess(ctx, client, raw, jq, predicate, admit, func(data []byte, observed error) error {
			return retainObserver(directory, "experiment", "process-"+stage, data, observed)
		})
	}
	originalAdmit, stackAdmit := h.AdmitOriginal, h.AdmitStack
	metricAdmit, successorAdmit, outcomeAdmit := r.AdmitMetrics, r.Network.AdmitSuccessor, r.Network.Lifecycle.OutcomeAdmit
	h.AdmitOriginal = func(ctx context.Context) error { return check(ctx, "original", originalBytes, originalAdmit) }
	h.AdmitStack = func(ctx context.Context, stage string) error {
		if stage != "before" && stage != "after" {
			return errors.New("invalid process stack stage")
		}
		return check(ctx, "stack-"+stage, originalBytes, func(ctx context.Context) error { return stackAdmit(ctx, stage) })
	}
	r.AdmitMetrics = func(ctx context.Context) error {
		for i, raw := range snapshots {
			if err := check(ctx, fmt.Sprintf("metric-%d", i), raw, metricAdmit); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	r.Network.AdmitSuccessor = func(ctx context.Context) error { return check(ctx, "successor", observerBytes, successorAdmit) }
	r.Network.Lifecycle.OutcomeAdmit = func(ctx context.Context) error { return check(ctx, "outcome", observerBytes, outcomeAdmit) }
	return r, h, nil
}
