package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"syscall"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
)

// VerifiedCommandInputs supplies independently admitted executable inputs to
// the composed command path. AdmitTools must recheck pinned tools, credentials,
// source/image provenance and endpoint-to-Pod mappings, without requiring an
// acquired claim or an unchanged initial term during later recovery.
type VerifiedCommandInputs struct {
	Processes                    CommandProcessInputs
	Release                      CommandRelease
	MetricExecutable, JoinScript string
	Targets                      []MetricCommandTarget
	AdmitTools                   func(context.Context) error
	// Required only for the packaged PID-1 Join. RunNativeCommand supplies the
	// independently pinned startup digest, never a digest discovered at cleanup.
	IsolatedJoinSHA256 string
}

// RunVerified composes process/initial-member admission, one durable claim,
// native execution, evidence retention and bounded post-child Join/recovery.
// Online source/identity admission remains mandatory; this is not a JSON CLI.
// The preparation connection is also the initial original-member connection;
// the successor connection is reused for outcome verification and recovery.
// Runtime Join, Owner, RecoveryConnection and retention must be unset. It never releases ownership,
// and must not be retried after an ambiguous acquisition or failed experiment.
func (p ObservationCommandPlan) RunVerified(ctx context.Context, r MeasuredNetworkFaultRuntime, h ObservationHooks, inputs VerifiedCommandInputs) (ClaimedCommandResult, error) {
	var result ClaimedCommandResult
	if err := ownerContext(ctx); err != nil {
		return result, err
	}
	if r.Network.Lifecycle.Preparation.Connection == nil || r.Network.SuccessorConnection == nil || inputs.AdmitTools == nil || r.Network.Lifecycle.Join != nil || r.Network.Lifecycle.RecoveryConnection != nil {
		return result, errors.New("verified command requires preparation/successor connections and tool admission, without preconfigured Join or recovery connection")
	}
	r.Network.Lifecycle.RecoveryConnection = r.Network.SuccessorConnection
	if err := p.CheckCommandEndpoints(r, inputs.Processes); err != nil {
		return result, err
	}
	if err := p.CheckProcessImages(inputs.Processes, inputs.Release); err != nil {
		return result, err
	}
	if _, err := planinput.ReadFile(inputs.JoinScript, false, 1<<20); err != nil {
		return result, err
	}
	if filepath.Base(inputs.JoinScript) == IsolatedJoinScript {
		if err := VerifyJoinInputs(ctx, p.OwnerDirectory, inputs.JoinScript, map[string]string{
			filepath.Join(p.OwnerDirectory, IsolatedJoinIdentity): inputs.IsolatedJoinSHA256,
		}); err != nil {
			return result, err
		}
	}
	// Uniform provenance checks must continue after acquisition, including
	// preparation, observation and recovery. Keep missing stage admission nil
	// so static validation still rejects it rather than masking it with tools.
	guard := func(stage func(context.Context) error) func(context.Context) error {
		if stage == nil {
			return nil
		}
		return func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := inputs.AdmitTools(ctx); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := stage(ctx); err != nil {
				return err
			}
			return errors.Join(inputs.AdmitTools(ctx), ctx.Err())
		}
	}
	r.Network.Lifecycle.Preparation.Own = guard(r.Network.Lifecycle.Preparation.Own)
	r.Network.Lifecycle.OutcomeAdmit = guard(r.Network.Lifecycle.OutcomeAdmit)
	r.Network.AdmitNetwork = guard(r.Network.AdmitNetwork)
	r.Network.AdmitSuccessor = guard(r.Network.AdmitSuccessor)
	r.AdmitMetrics = guard(r.AdmitMetrics)
	h.AdmitOriginal = guard(h.AdmitOriginal)
	if stack := h.AdmitStack; stack != nil {
		h.AdmitStack = func(ctx context.Context, stage string) error {
			return guard(func(ctx context.Context) error { return stack(ctx, stage) })(ctx)
		}
	}
	r, h, err := p.BindProcessAdmission(r, h, inputs.Targets, inputs.Processes)
	if err != nil {
		return result, err
	}
	r.Network.Lifecycle.Join = func(ctx context.Context) error {
		if filepath.Base(inputs.JoinScript) == IsolatedJoinScript {
			if err := inputs.AdmitTools(ctx); err != nil {
				return err
			}
			// RunFaultLifecycle invokes Join only after all managed Wait calls
			// and command-producing workers return. Never reap from a timer.
			children, observed := ReapIsolatedJoinZombies(ctx, p.OwnerDirectory, inputs.IsolatedJoinSHA256)
			for _, child := range children {
				if syscall.WaitStatus(child.Status) != 0 {
					observed = errors.Join(observed, errors.New("unmanaged child exited unsuccessfully"))
				}
			}
			data, encodeErr := json.Marshal(children)
			retained := RetainRecoveryObserver(p.OwnerDirectory, "join-reaped", data, observed)
			if err := errors.Join(observed, encodeErr, retained); err != nil {
				return err
			}
		}
		return RunFaultJoin(ctx, p.OwnerDirectory, inputs.JoinScript, inputs.AdmitTools)
	}
	// Source and endpoint/process checks bracket each Status read. Metrics may
	// involve additional Pods and are also admitted before acquiring ownership.
	initialAdmit := func(ctx context.Context) error {
		if err := inputs.AdmitTools(ctx); err != nil {
			return err
		}
		if err := h.AdmitOriginal(ctx); err != nil {
			return err
		}
		return r.Network.AdmitSuccessor(ctx)
	}
	preclaim := func(ctx context.Context) error {
		if err := initialAdmit(ctx); err != nil {
			return err
		}
		if err := r.AdmitMetrics(ctx); err != nil {
			return err
		}
		if err := p.VerifyCommandDeployment(ctx, r.Network.Lifecycle.Preparation.Client, r.Network.Lifecycle.Preparation.StatefulSetName, inputs.AdmitTools); err != nil {
			return err
		}
		if err := p.VerifyProcessPlatforms(ctx, r.Network.Lifecycle.Preparation.Client, inputs.Processes, inputs.Release, inputs.AdmitTools); err != nil {
			return err
		}
		return p.VerifyInitialMembers(ctx, r.Network.Lifecycle.Preparation.Connection, r.Network.SuccessorConnection, initialAdmit)
	}
	return p.ClaimAndRun(ctx, r, h, inputs.MetricExecutable, inputs.Targets, preclaim)
}
