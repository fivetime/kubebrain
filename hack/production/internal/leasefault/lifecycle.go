package leasefault

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"google.golang.org/grpc"
)

// FaultLifecycle binds preparation, one original prepared probe, metric workers
// and post-join recovery. Commands and hooks must be independently admitted;
// success from a child is not a substitute for the actual fault/metric gates.
// The prepared script must not mutate protocol/network state or restore it.
// It owns the original probe, expiry wait and fault observations under the
// supplied clock. The parent activates the reserved policy before dispatching
// that clock to the prepared child (metric workers already received it).
type FaultLifecycle struct {
	// Owner must be an acquired, durably receipted claim for this exact scope.
	// The lifecycle never releases it; release is an explicit post-recovery step.
	Owner       *FaultOwner
	Preparation FaultPreparation
	Fault       metricsworker.Command
	Workers     []metricsworker.Command
	// Inject must be nil: the coordinator installs the one prepared-child hook.
	Metrics                                 metricsworker.Hooks
	RecoveryConnection                      grpc.ClientConnInterface
	RecoveryTimeout                         time.Duration
	Join, NetworkRestored, IdentityRestored func(context.Context) error
	// OriginalEvidence must authenticate the original probe log and independent
	// successor binding, not infer either from the response under test. Its origin
	// must equal the clock already dispatched to the prepared child and workers.
	OriginalEvidence func(context.Context) (Binding, []byte, error)
	// OutcomeAdmit checks live isolation and protocol identities before outcome
	// reads; Preparation.Own is also rechecked. Both use the original fault context.
	OutcomeAdmit func(context.Context) error
}

type LifecycleResult struct {
	Metrics                       []metricsworker.Result
	ExecutionError, RecoveryError error
	Outcome                       *OriginalOutcome
	// API acknowledgement only, never Cilium enforcement or acceptance. False
	// after an error does not imply the PATCH had no effect.
	ActivationAcknowledged bool
}

// RunFaultLifecycle never retries an attempt or releases external ownership.
// Recovery runs even after preparation/child failure, only after all managed
// children have joined. The required Join hook additionally accounts for escaped
// descendants and external workers. Recovery has an independent bounded context;
// it cannot turn a failed original fault into acceptance or extend its 30s budget.
func RunFaultLifecycle(ctx context.Context, l FaultLifecycle) (LifecycleResult, error) {
	var result LifecycleResult
	if ctx == nil || l.RecoveryConnection == nil || l.RecoveryTimeout <= 0 || l.RecoveryTimeout > 5*time.Minute || l.Join == nil || l.NetworkRestored == nil || l.IdentityRestored == nil || l.Metrics.Baseline == nil || l.Metrics.Origin == nil || l.Metrics.Completed == nil || l.Metrics.Inject != nil || len(l.Workers) == 0 || len(l.Workers) > 16 {
		return result, errors.New("incomplete fault lifecycle configuration")
	}
	if err := l.Preparation.validate(ctx); err != nil {
		return result, err
	}
	p := l.Preparation
	if l.Owner == nil || l.Owner.directory != p.Directory || l.Owner.binding != (FaultOwnerBinding{Owner: p.Network.Owner, Namespace: p.Network.Namespace, NamespaceUID: p.Network.NamespaceUID, StatefulSetName: p.StatefulSetName, StatefulSetUID: p.Network.StatefulSetUID}) {
		return result, errors.New("fault lifecycle owner does not match preparation")
	}
	if l.OriginalEvidence == nil || l.OutcomeAdmit == nil {
		return result, errors.New("original outcome evidence and admission are required")
	}
	// Reject invalid local tools before any cluster preparation. A private log
	// is mandatory for every child; no implicit credential environment is added.
	commands := append([]metricsworker.Command{l.Fault}, l.Workers...)
	for _, c := range commands {
		if err := processgroup.ValidateExecutable(c.Executable); err != nil {
			return result, err
		}
		if c.Stderr == nil {
			return result, errors.New("lifecycle child requires private evidence log")
		}
		st, err := c.Stderr.Stat()
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return result, errors.New("invalid lifecycle evidence log")
		}
	}
	if err := l.Owner.Check(ctx); err != nil {
		return result, err
	}
	additionalAdmission := l.Preparation.Own
	l.Preparation.Own = func(ctx context.Context) error {
		if err := l.Owner.Check(ctx); err != nil {
			return err
		}
		if err := additionalAdmission(ctx); err != nil {
			return err
		}
		return l.Owner.Check(ctx)
	}
	result.ExecutionError = PrepareFault(ctx, l.Preparation)
	if result.ExecutionError == nil {
		result.ExecutionError = metricsworker.WithPreparedFault(ctx, l.Fault, func(runCtx context.Context, inject func(context.Context, time.Time) error) error {
			hooks := l.Metrics
			hooks.Baseline = func(ctx context.Context, index int, ready metricsworker.Ready) error {
				if err := l.Preparation.Own(ctx); err != nil {
					return err
				}
				return l.Metrics.Baseline(ctx, index, ready)
			}
			hooks.Inject = func(faultCtx context.Context, origin time.Time) error {
				if err := l.Preparation.Own(faultCtx); err != nil {
					return err
				}
				// The single fault clock already runs and has been dispatched to
				// metric workers. No preparation or activation gets a fresh budget.
				if err := l.Preparation.NoncesSafe(faultCtx); err != nil {
					return fmt.Errorf("pre-activation nonce admission: %w", err)
				}
				if err := ActivateNetwork(faultCtx, l.Preparation.Client, l.Preparation.Directory, l.Preparation.Network, origin, func(ctx context.Context) error {
					return CheckNetworkIdentity(ctx, l.Preparation.Client, l.Preparation.Network, l.Preparation.StatefulSetName, NetworkLabelOwned, l.Preparation.Own)
				}); err != nil {
					return fmt.Errorf("activate reserved fault policy: %w", err)
				}
				result.ActivationAcknowledged = true
				if err := inject(faultCtx, origin); err != nil {
					return err
				}
				// The prepared child has exited successfully. These final reads
				// remain fault gates, not part of the later recovery budget.
				binding, log, err := l.OriginalEvidence(faultCtx)
				if err != nil {
					return err
				}
				if !binding.Origin.Equal(origin) {
					return errors.New("original outcome clock changed")
				}
				outcome, err := VerifyOriginalOutcome(faultCtx, l.RecoveryConnection, l.Preparation.Protocol, binding, log, func(ctx context.Context) error {
					if err := l.Preparation.Own(ctx); err != nil {
						return err
					}
					return l.OutcomeAdmit(ctx)
				})
				result.Outcome = &outcome
				return err
			}
			var err error
			result.Metrics, err = metricsworker.Run(runCtx, l.Preparation.Directory, l.Workers, hooks)
			return err
		})
	}
	// WithPreparedFault and Run have returned: their deferred cancel/join paths
	// are complete. Never reuse an expired fault context for restoration.
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.RecoveryTimeout)
	defer cancel()
	p = l.Preparation
	result.RecoveryError = RecoverFault(recoveryCtx, FaultRecovery{
		Directory: p.Directory, StatefulSetName: p.StatefulSetName, Network: p.Network, Protocol: p.Protocol,
		Client: p.Client, Connection: l.RecoveryConnection, Own: p.Own, Join: l.Join,
		NetworkRestored: l.NetworkRestored, IdentityRestored: l.IdentityRestored,
	})
	var executionErr, recoveryErr error
	if result.ExecutionError != nil {
		executionErr = fmt.Errorf("fault execution: %w", result.ExecutionError)
	}
	if result.RecoveryError != nil {
		recoveryErr = fmt.Errorf("fault recovery: %w", result.RecoveryError)
	}
	return result, errors.Join(executionErr, recoveryErr)
}
