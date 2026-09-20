package leasefault

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

func TestFaultLifecycleConfigurationBeforeClaim(t *testing.T) {
	for _, mode := range []string{"valid", "missing-worker-tool", "missing-native-tool", "public-log", "missing-recovery", "protocol-mismatch", "cancelled", "unbounded", "mixed", "missing-admission"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			calls := 0
			check := func(context.Context) error { calls++; return nil }
			o, err := p.Observation(ObservationHooks{
				AdmitOriginal:  check,
				AdmitStack:     func(context.Context, string) error { calls++; return nil },
				RetainOriginal: func(context.Context, string, Binding, []byte, error) error { calls++; return nil },
				RetainStack:    func(context.Context, string, WaitReceipt, error) error { calls++; return nil },
			})
			require.NoError(t, err)
			// Real local executables, but no command may actually run in preflight.
			o.Probe.Executable, o.Before.Command.Executable, o.After.Command.Executable = "/bin/bash", "/bin/bash", "/bin/bash"
			client := fake.NewSimpleDynamicClient(runtime.NewScheme())
			conn := &successorConnection{}
			l := FaultLifecycle{
				Preparation: FaultPreparation{Directory: p.OwnerDirectory, StatefulSetName: "brain", Network: p.Bindings.Network, Protocol: p.Bindings.Protocol, Client: client, Connection: conn, Own: check, NoncesSafe: check, ReservedReady: check},
				Observation: &o, ObserveFault: func(context.Context, time.Time) (uint64, error) { calls++; return 0, nil },
				Workers: []metricsworker.Command{{Executable: "/bin/bash", Stderr: p.ProbeLog}},
				Metrics: metricsworker.Hooks{
					Baseline:  func(context.Context, int, metricsworker.Ready) error { calls++; return nil },
					Origin:    func(context.Context) (time.Time, error) { calls++; return time.Now(), nil },
					Completed: func(context.Context, int, metricsworker.Result, time.Time) error { calls++; return nil },
				},
				RecoveryConnection: conn, RecoveryTimeout: time.Minute, Join: check, NetworkRestored: check, IdentityRestored: check, OutcomeAdmit: check,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			switch mode {
			case "missing-worker-tool":
				l.Workers[0].Executable = "/missing/worker"
			case "missing-native-tool":
				o.After.Command.Executable = "/missing/stack"
			case "public-log":
				require.NoError(t, os.Chmod(p.ProbeLog.Name(), 0644))
			case "missing-recovery":
				l.RecoveryConnection = nil
			case "protocol-mismatch":
				o.Initial.LeaseID++
			case "cancelled":
				cancel()
			case "unbounded":
				ctx = context.Background()
			case "mixed":
				l.Fault.Executable = "/bin/bash"
			case "missing-admission":
				l.OutcomeAdmit = nil
			}
			err = ValidateFaultLifecycleConfiguration(ctx, l)
			if mode == "valid" {
				require.NoError(t, err, "local validation must not require a live claim")
				_, err = RunFaultLifecycle(ctx, l)
				require.ErrorContains(t, err, "owner does not match", "execution still requires acquired ownership")
			} else {
				require.Error(t, err)
			}
			require.Zero(t, calls)
			require.Empty(t, client.Actions(), "preflight must not contact Kubernetes")
		})
	}
}
