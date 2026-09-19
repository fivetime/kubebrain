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
// The prepared script must not repeat protocol/network preparation or restore.
// It owns the original probe, expiry wait and fault gates under the supplied clock.
type FaultLifecycle struct {
	Preparation FaultPreparation
	Fault       metricsworker.Command
	Workers     []metricsworker.Command
	// Inject must be nil: the coordinator installs the one prepared-child hook.
	Metrics                                 metricsworker.Hooks
	RecoveryConnection                      grpc.ClientConnInterface
	RecoveryTimeout                         time.Duration
	Join, NetworkRestored, IdentityRestored func(context.Context) error
}

type LifecycleResult struct {
	Metrics                       []metricsworker.Result
	ExecutionError, RecoveryError error
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
	result.ExecutionError = PrepareFault(ctx, l.Preparation)
	if result.ExecutionError == nil {
		result.ExecutionError = metricsworker.WithPreparedFault(ctx, l.Fault, func(runCtx context.Context, inject func(context.Context, time.Time) error) error {
			hooks := l.Metrics
			hooks.Inject = inject
			var err error
			result.Metrics, err = metricsworker.Run(runCtx, l.Preparation.Directory, l.Workers, hooks)
			return err
		})
	}
	// WithPreparedFault and Run have returned: their deferred cancel/join paths
	// are complete. Never reuse an expired fault context for restoration.
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.RecoveryTimeout)
	defer cancel()
	p := l.Preparation
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
