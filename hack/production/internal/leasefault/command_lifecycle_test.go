package leasefault

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestCommandLifecycleBinding(t *testing.T) {
	for _, mode := range []string{"bound", "directory", "network", "protocol", "tools", "metric-tools", "observation", "workers", "external-fault", "external-evidence", "external-pending", "expected", "successor", "env", "missing-owner", "missing-metric-admit", "external-origin"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			targets := metricTargets(t, p)
			calls := 0
			admit := func(context.Context) error { calls++; return nil }
			h := ObservationHooks{AdmitOriginal: admit, AdmitStack: func(context.Context, string) error { calls++; return nil }, RetainOriginal: func(context.Context, string, Binding, []byte, error) error { calls++; return nil }, RetainStack: func(context.Context, string, WaitReceipt, error) error { calls++; return nil }}
			r := MeasuredNetworkFaultRuntime{
				Network: NetworkFaultRuntime{
					Lifecycle:       FaultLifecycle{Owner: &FaultOwner{}, Preparation: FaultPreparation{Directory: p.OwnerDirectory, Network: p.Bindings.Network, Protocol: p.Bindings.Protocol, Own: admit}, OutcomeAdmit: admit},
					ScriptDirectory: filepath.Dir(p.StackExecutable), TargetsSHA256: strings.Repeat("a", 64), AdmitNetwork: admit, RetainNetwork: func(string, []byte, error) error { calls++; return nil }, SuccessorConnection: &successorConnection{}, AdmitSuccessor: admit, RetainStatus: func(context.Context, SuccessorSample) error { calls++; return nil }, CaptureSeconds: 1,
				}, AdmitMetrics: admit, RetainMetrics: func(context.Context, int, retirementmetrics.WorkerMeasurement) error { calls++; return nil },
			}
			exe := "/approved/protected-metrics-worker.sh"
			switch mode {
			case "directory":
				r.Network.Lifecycle.Preparation.Directory = "/other"
			case "network":
				r.Network.Lifecycle.Preparation.Network.PodUID = "other"
			case "protocol":
				r.Network.Lifecycle.Preparation.Protocol.LeaseID++
			case "tools":
				r.Network.ScriptDirectory = "/other"
			case "metric-tools":
				exe = "/other/worker.sh"
			case "observation":
				r.Network.Lifecycle.Observation = &OriginalObservation{}
			case "workers":
				r.Network.Lifecycle.Workers = []metricsworker.Command{{}}
			case "external-fault":
				r.Network.Lifecycle.Fault.Executable = "/other"
			case "external-evidence":
				r.Network.Lifecycle.OriginalEvidence = func(context.Context) (Binding, []byte, error) { calls++; return Binding{}, nil, nil }
			case "external-pending":
				r.Network.Lifecycle.OriginalPending = func(context.Context, time.Time) error { calls++; return nil }
			case "expected":
				r.Expected = p.Bindings.Metrics
			case "successor":
				r.Network.Successor = p.Bindings.Successor()
			case "env":
				r.Network.Env = p.Env
			case "missing-owner":
				r.Network.Lifecycle.Owner = nil
			case "missing-metric-admit":
				r.AdmitMetrics = nil
			case "external-origin":
				r.Network.Lifecycle.Metrics.Origin = func(context.Context) (time.Time, error) { return time.Now(), nil }
			}
			l, err := p.BindLifecycle(r, h, exe, targets)
			if mode != "bound" {
				require.Error(t, err)
				require.Zero(t, calls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, p.Bindings.Initial(), l.Observation.Initial)
			require.Equal(t, "1", l.Observation.Before.Command.Args[3])
			require.Equal(t, "0", l.Observation.After.Command.Args[3])
			require.Len(t, l.Workers, 1)
			require.Equal(t, []string{p.Bindings.Network.PodName, "0"}, l.Workers[0].Args)
			require.NotNil(t, l.ObserveFault)
			require.NotNil(t, l.Metrics.Baseline)
			require.NotNil(t, l.Metrics.Completed)
			require.NotNil(t, l.Metrics.Origin)
			require.NotNil(t, l.NetworkRestored)
			require.NotNil(t, l.IdentityRestored)
			require.Nil(t, r.Network.Lifecycle.Observation)
			require.Empty(t, r.Network.Lifecycle.Workers)
			require.Empty(t, r.Expected)
			require.Empty(t, r.Network.Env)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			cancel()
			require.ErrorIs(t, l.Metrics.Baseline(ctx, 0, metricsworker.Ready{}), context.Canceled)
			require.Zero(t, calls, "assembly must not invoke hooks or mutate the cluster")
		})
	}
}
