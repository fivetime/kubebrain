package leasefault

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestMeasuredNetworkFaultRuntimeBinding(t *testing.T) {
	for _, mode := range []string{"bound", "count", "namespace", "statefulset", "spec", "different-spec", "different-cluster", "missing-pod", "missing-minimum", "bad-offset", "baseline", "completed", "inject", "origin", "admit", "retain", "network"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			admit := func(context.Context) error { calls++; return nil }
			minimum := uint64(1)
			e := retirementmetrics.WorkerExpectation{Binding: retirementmetrics.CaptureBinding{NamespaceUID: "ns", StatefulSetUID: "sts", PodUID: "pod", SpecSHA256: strings.Repeat("a", 64), Cluster: "test"}, Key: retirementmetrics.Key{Stage: "peer", Outcome: "confirmed"}, MinimumCount: &minimum, RequireDuration: true}
			r := MeasuredNetworkFaultRuntime{
				Network: NetworkFaultRuntime{
					Lifecycle: FaultLifecycle{
						Owner: &FaultOwner{}, Preparation: FaultPreparation{Own: admit, Network: NetworkRecovery{NamespaceUID: "ns", StatefulSetUID: "sts"}},
						Observation: &OriginalObservation{Initial: Binding{ClusterID: 1, InitialMemberID: 2, InitialTerm: 3}}, OutcomeAdmit: admit,
						Workers: make([]metricsworker.Command, 2), Metrics: metricsworker.Hooks{Origin: func(context.Context) (time.Time, error) { calls++; return time.Now(), nil }},
					},
					ScriptDirectory: "/admitted/scripts", TargetsSHA256: strings.Repeat("a", 64), AdmitNetwork: admit,
					RetainNetwork:       func(string, []byte, error) error { calls++; return nil },
					SuccessorConnection: &successorConnection{}, Successor: SuccessorBinding{ClusterID: 1, OldLeaderID: 2, OldTerm: 3, ObserverMemberID: 4},
					AdmitSuccessor: admit, RetainStatus: func(context.Context, SuccessorSample) error { calls++; return nil }, CaptureSeconds: 1,
				},
				Expected: []retirementmetrics.WorkerExpectation{e, e}, AdmitMetrics: admit,
				RetainMetrics: func(context.Context, int, retirementmetrics.WorkerMeasurement) error { calls++; return nil },
			}
			r.Expected[1].Binding.PodUID = "healthy-pod"
			switch mode {
			case "count":
				r.Expected = r.Expected[:1]
			case "namespace":
				r.Expected[1].Binding.NamespaceUID = "other"
			case "statefulset":
				r.Expected[1].Binding.StatefulSetUID = "other"
			case "spec":
				r.Expected[1].Binding.SpecSHA256 = strings.Repeat("z", 64)
			case "different-spec":
				r.Expected[1].Binding.SpecSHA256 = strings.Repeat("b", 64)
			case "different-cluster":
				r.Expected[1].Binding.Cluster = "other"
			case "missing-pod":
				r.Expected[1].Binding.PodUID = ""
			case "missing-minimum":
				r.Expected[1].MinimumCount = nil
			case "bad-offset":
				r.Expected[1].Offset = 30 * time.Second
			case "baseline":
				r.Network.Lifecycle.Metrics.Baseline = func(context.Context, int, metricsworker.Ready) error { calls++; return nil }
			case "completed":
				r.Network.Lifecycle.Metrics.Completed = func(context.Context, int, metricsworker.Result, time.Time) error { calls++; return nil }
			case "inject":
				r.Network.Lifecycle.Metrics.Inject = func(context.Context, time.Time) error { calls++; return nil }
			case "origin":
				r.Network.Lifecycle.Metrics.Origin = nil
			case "admit":
				r.AdmitMetrics = nil
			case "retain":
				r.RetainMetrics = nil
			case "network":
				r.Network.Successor.OldTerm++
			}
			l, err := r.bind()
			if mode != "bound" {
				require.Error(t, err)
				_, err = r.Run(context.Background())
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, l.Metrics.Baseline)
				require.NotNil(t, l.Metrics.Completed)
				require.NotNil(t, l.Metrics.Origin)
				require.Nil(t, l.Metrics.Inject)
				require.NotNil(t, l.ObserveFault)
				require.Nil(t, r.Network.Lifecycle.Metrics.Baseline)
				require.Nil(t, r.Network.Lifecycle.Metrics.Completed)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				cancel()
				require.ErrorIs(t, l.Metrics.Baseline(ctx, 0, metricsworker.Ready{}), context.Canceled)
				require.ErrorIs(t, l.Metrics.Completed(ctx, 0, metricsworker.Result{}, time.Now()), context.Canceled)
				live, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				require.ErrorContains(t, l.Metrics.Baseline(live, 0, metricsworker.Ready{}), "invalid owner binding", "missing real ownership must fail before permissive caller admission")
			}
			require.Zero(t, calls, "local binding must not execute hooks or touch the cluster")
		})
	}
}
