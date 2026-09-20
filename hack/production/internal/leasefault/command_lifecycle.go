package leasefault

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
)

// BindLifecycle connects the plan's concrete commands and identities to the
// existing measured network runtime. The caller supplies acquired ownership,
// admitted connections, live admission/retention and post-child Join. The origin
// is installed here as a create-once durable clock bound to the same plan.
// This is local assembly, not execution, claim acquisition or image admission.
// The returned lifecycle must be passed to RunFaultLifecycle exactly once.
func (p ObservationCommandPlan) BindLifecycle(r MeasuredNetworkFaultRuntime, h ObservationHooks, metricExecutable string, targets []MetricCommandTarget) (FaultLifecycle, error) {
	l := r.Network.Lifecycle
	if l.Observation != nil || len(l.Workers) != 0 || l.Fault.Executable != "" || len(l.Fault.Args) != 0 || len(l.Fault.Env) != 0 || l.Fault.Stderr != nil || l.OriginalEvidence != nil || l.OriginalPending != nil || l.Metrics.Origin != nil || len(r.Expected) != 0 || r.Network.Successor != (SuccessorBinding{}) || len(r.Network.Env) != 0 {
		return FaultLifecycle{}, errors.New("command lifecycle cannot override existing commands or identity bindings")
	}
	if l.Preparation.Directory != p.OwnerDirectory || !reflect.DeepEqual(l.Preparation.Network, p.Bindings.Network) || !reflect.DeepEqual(l.Preparation.Protocol, p.Bindings.Protocol) || r.Network.ScriptDirectory != filepath.Dir(p.StackExecutable) || filepath.Dir(metricExecutable) != r.Network.ScriptDirectory {
		return FaultLifecycle{}, errors.New("command lifecycle differs from admitted preparation or tool directory")
	}
	o, err := p.Observation(h)
	if err != nil {
		return FaultLifecycle{}, err
	}
	workers, err := p.MetricCommands(metricExecutable, targets)
	if err != nil {
		return FaultLifecycle{}, err
	}
	if l.Owner == nil || l.Preparation.Own == nil {
		return FaultLifecycle{}, errors.New("durable origin requires acquired ownership checks")
	}
	owner, own := l.Owner, l.Preparation.Own
	l.Metrics.Origin, err = NewDurableFaultOrigin(p.OwnerDirectory, p.Bindings, func(ctx context.Context) error {
		if err := owner.Check(ctx); err != nil {
			return err
		}
		return own(ctx)
	})
	if err != nil {
		return FaultLifecycle{}, err
	}
	l.Observation = &o
	l.Workers = workers
	r.Network.Lifecycle = l
	r.Network.Successor = p.Bindings.Successor()
	r.Network.Env = append([]string{}, p.Env...)
	r.Expected = append([]retirementmetrics.WorkerExpectation{}, p.Bindings.Metrics...)
	// bind freezes expected minimums and installs the concrete network observers
	// and metric gates without executing caller callbacks or touching the API.
	return r.bind()
}
