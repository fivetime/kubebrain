package leasefault

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
)

// CommandAdmission supplies independently implemented online gates. None may
// be replaced by a successful local plan check. Tools must authenticate the
// exact approved plan's CI/source, credentials, transitive tools and environment
// throughout execution and recovery. Stage gates must check fresh live scope.
type CommandAdmission struct {
	Tools, Own, Original, Network, Successor, Metrics, Outcome func(context.Context) error
	Stack                                                      func(context.Context, string) error
}

func (a CommandAdmission) validate() error {
	if a.Tools == nil || a.Own == nil || a.Original == nil || a.Network == nil || a.Successor == nil || a.Metrics == nil || a.Outcome == nil || a.Stack == nil {
		return errors.New("command execution requires every independent admission gate")
	}
	return nil
}

// RunNativeCommand loads one independently approved immutable plan and wires
// its actual connections and artifacts into RunVerified. It can mutate a
// dedicated test cluster. Call exactly once for a fresh owner attempt; errors
// (including a nil returned Owner) do not prove that acquisition had no effect.
//
// It never releases the claim, deletes evidence or retries. RunVerified joins
// managed children before returning; closing parent descriptors below does not
// prove escaped descendants are gone. Failed Join/recovery still requires the
// independent recovery workflow and fresh proof before explicit claim release.
// This is the execution adapter, not a CLI with concrete online admission.
func RunNativeCommand(ctx context.Context, path, digest string, admission CommandAdmission) (result ClaimedCommandResult, err error) {
	if err = ownerContext(ctx); err != nil {
		return result, err
	}
	if err = admission.validate(); err != nil {
		return result, err
	}
	p, err := LoadNativeCommandPlan(path, digest)
	if err != nil {
		return result, err
	}
	// Retain the independent digest, not a digest discovered during execution.
	// Reject plan replacement at every tool gate, including recovery. A byte
	// match only establishes consistency; Tools remains mandatory provenance.
	checkPlan := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := planinput.ReadFile(path, true, 4<<20)
		if err != nil {
			return err
		}
		if planinput.SHA256(data) != digest {
			return errors.New("executing command plan changed")
		}
		return ctx.Err()
	}
	tools := func(ctx context.Context) error {
		if err := checkPlan(ctx); err != nil {
			return err
		}
		if err := p.VerifyFiles(ctx); err != nil {
			return err
		}
		if err := admission.Tools(ctx); err != nil {
			return err
		}
		if err := checkPlan(ctx); err != nil {
			return err
		}
		return p.VerifyFiles(ctx)
	}
	if err = tools(ctx); err != nil {
		return result, err
	}
	finish, err := beginCommandJournal(p, digest)
	if err != nil {
		return result, err
	}
	// Registered before connection/artifact cleanup so their errors are also
	// present in the final record. Retention failure remains a returned error.
	defer func() { err = errors.Join(err, finish(result, err)) }()
	o, err := p.ObservationPlan()
	if err != nil {
		return result, err
	}
	connections, err := o.OpenConnections(p.Kubeconfig, p.KubeContext, p.APIServer, p.ObserverEndpoint, p.ObserverServerName, p.Files)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, connections.Close()) }()
	artifacts, err := o.PrepareArtifacts(p.MetricExecutable, p.MetricTargets())
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, artifacts.Close()) }()
	recovery, err := time.ParseDuration(p.RecoveryTimeout)
	if err != nil {
		return result, err
	} // already validated; no implicit default
	r := MeasuredNetworkFaultRuntime{Network: NetworkFaultRuntime{
		Lifecycle:       FaultLifecycle{Preparation: FaultPreparation{Directory: p.OwnerDirectory, StatefulSetName: p.StatefulSetName, Network: p.Bindings.Network, Protocol: p.Bindings.Protocol, Own: admission.Own}, RecoveryTimeout: recovery, OutcomeAdmit: admission.Outcome},
		ScriptDirectory: filepath.Dir(p.StackExecutable), TargetsSHA256: p.TargetsSHA256, CaptureSeconds: p.CaptureSeconds, AdmitNetwork: admission.Network, AdmitSuccessor: admission.Successor,
	}, AdmitMetrics: admission.Metrics}
	r, err = connections.Bind(r)
	if err != nil {
		return result, err
	}
	h := ObservationHooks{AdmitOriginal: admission.Original, AdmitStack: admission.Stack}
	inputs := VerifiedCommandInputs{Processes: p.Processes, Release: p.Release, MetricExecutable: p.MetricExecutable, JoinScript: p.JoinScript, Targets: artifacts.Targets, AdmitTools: tools}
	inputs.IsolatedJoinSHA256 = p.Files[filepath.Join(p.OwnerDirectory, IsolatedJoinIdentity)]
	return artifacts.Plan.RunVerified(ctx, r, h, inputs)
}
