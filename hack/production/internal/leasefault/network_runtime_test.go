package leasefault

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNetworkFaultRuntimeBinding(t *testing.T) {
	for _, mode := range []string{"bound", "cluster", "old-member", "old-term", "same-observer", "prefilled-clock", "duration", "conflicting-nonces", "conflicting-ready", "conflicting-fault", "conflicting-recovery", "missing-admission", "bad-script", "bad-digest"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			admit := func(context.Context) error { calls++; return nil }
			r := NetworkFaultRuntime{
				Lifecycle:       FaultLifecycle{Owner: &FaultOwner{}, Preparation: FaultPreparation{Own: admit}, Observation: &OriginalObservation{Initial: Binding{ClusterID: 1, InitialMemberID: 2, InitialTerm: 3}}, OutcomeAdmit: admit},
				ScriptDirectory: "/admitted/scripts", TargetsSHA256: strings.Repeat("a", 64),
				AdmitNetwork: admit, RetainNetwork: func(string, []byte, error) error { calls++; return nil },
				SuccessorConnection: &successorConnection{}, Successor: SuccessorBinding{ClusterID: 1, ObserverMemberID: 4, OldLeaderID: 2, OldTerm: 3},
				AdmitSuccessor: admit, RetainStatus: func(context.Context, SuccessorSample) error { calls++; return nil }, CaptureSeconds: 1,
			}
			switch mode {
			case "cluster":
				r.Successor.ClusterID++
			case "old-member":
				r.Successor.OldLeaderID++
			case "old-term":
				r.Successor.OldTerm++
			case "same-observer":
				r.Successor.ObserverMemberID = r.Successor.OldLeaderID
			case "prefilled-clock":
				r.Successor.Origin = time.Now()
			case "duration":
				r.CaptureSeconds = 10
			case "conflicting-ready":
				r.Lifecycle.Preparation.ReservedReady = admit
			case "conflicting-nonces":
				r.Lifecycle.Preparation.NoncesSafe = admit
			case "conflicting-fault":
				r.Lifecycle.ObserveFault = func(context.Context, time.Time) (uint64, error) { calls++; return 4, nil }
			case "conflicting-recovery":
				r.Lifecycle.NetworkRestored = admit
			case "missing-admission":
				r.AdmitSuccessor = nil
			case "bad-script":
				r.ScriptDirectory = "relative"
			case "bad-digest":
				r.TargetsSHA256 = "wrong"
			}
			bound, err := r.bind()
			if mode != "bound" {
				require.Error(t, err)
				_, err = r.Run(context.Background())
				require.Error(t, err)
				require.Zero(t, calls, "reject local mismatches before any callback or mutation")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, bound.Preparation.ReservedReady)
			require.NotNil(t, bound.Preparation.NoncesSafe)
			require.NotNil(t, bound.ObserveFault)
			require.NotNil(t, bound.NetworkRestored)
			require.NotNil(t, bound.IdentityRestored)
			require.Nil(t, r.Lifecycle.ObserveFault, "assembly must not modify caller configuration")
			require.Nil(t, r.Lifecycle.Preparation.ReservedReady)
			// Outcome cannot be checked before the actual fault supplies a clock.
			require.Error(t, bound.OutcomeAdmit(context.Background()))
			origin := time.Now()
			ctx, cancel := context.WithDeadline(context.Background(), origin.Add(time.Second))
			cancel()
			term, err := bound.ObserveFault(ctx, origin)
			require.Zero(t, term)
			require.ErrorIs(t, err, context.Canceled)
			_, err = bound.ObserveFault(ctx, origin)
			require.ErrorContains(t, err, "already invoked")
			require.Zero(t, calls)
		})
	}
}
