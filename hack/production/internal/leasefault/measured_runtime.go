package leasefault

import (
	"context"
	"errors"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
)

// MeasuredNetworkFaultRuntime adds concrete metric evidence gates to the native
// network runtime. Expected is independently admitted, in worker command order;
// neither worker output nor a successful exit can supply these expectations.
// The caller must still admit commands, source/image, all target Pods and file
// provenance, retain the original clock, and explicitly release recovered ownership.
// Each instance describes one attempt; do not reuse it after Run.
type MeasuredNetworkFaultRuntime struct {
	Network       NetworkFaultRuntime
	Expected      []retirementmetrics.WorkerExpectation
	AdmitMetrics  func(context.Context) error
	RetainMetrics func(context.Context, int, retirementmetrics.WorkerMeasurement) error
}

func (r MeasuredNetworkFaultRuntime) Run(ctx context.Context) (LifecycleResult, error) {
	l, err := r.bind()
	if err != nil {
		return LifecycleResult{}, err
	}
	return RunFaultLifecycle(ctx, l)
}

func (r MeasuredNetworkFaultRuntime) bind() (FaultLifecycle, error) {
	l := r.Network.Lifecycle
	if len(r.Expected) == 0 || len(r.Expected) != len(l.Workers) || r.AdmitMetrics == nil || r.RetainMetrics == nil || l.Metrics.Origin == nil || l.Metrics.Baseline != nil || l.Metrics.Completed != nil || l.Metrics.Inject != nil {
		return FaultLifecycle{}, errors.New("incomplete or conflicting concrete metric runtime")
	}
	// Workers may observe different Pods, but never a different admitted
	// namespace, StatefulSet spec or cluster label. Pod identities and command
	// mappings must additionally be authenticated by AdmitMetrics.
	first := r.Expected[0].Binding
	for _, e := range r.Expected {
		b := e.Binding
		if b.NamespaceUID != l.Preparation.Network.NamespaceUID || b.StatefulSetUID != l.Preparation.Network.StatefulSetUID || !planinput.ValidSHA256(b.SpecSHA256) || b.SpecSHA256 != first.SpecSHA256 || b.Cluster != first.Cluster {
			return FaultLifecycle{}, errors.New("metric expectation differs from admitted experiment scope")
		}
	}
	var err error
	l, err = r.Network.bind()
	if err != nil {
		return FaultLifecycle{}, err
	}
	own := l.Preparation.Own
	admit := func(ctx context.Context) error {
		if err := own(ctx); err != nil {
			return err
		}
		if err := r.AdmitMetrics(ctx); err != nil {
			return err
		}
		return own(ctx)
	}
	gates, err := retirementmetrics.NewWorkerGates(r.Expected, admit, r.RetainMetrics)
	if err != nil {
		return FaultLifecycle{}, err
	}
	l.Metrics.Baseline = gates.Baseline
	l.Metrics.Completed = gates.Completed
	return l, nil
}
