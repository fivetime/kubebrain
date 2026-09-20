package leasefault

import (
	"context"
	"errors"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"google.golang.org/grpc"
)

// VerifiedCommandInputs supplies independently admitted executable inputs to
// the composed command path. AdmitTools must recheck pinned tools, credentials,
// source/image provenance and endpoint-to-Pod mappings, without requiring an
// acquired claim or an unchanged initial term during later recovery.
type VerifiedCommandInputs struct {
	Processes                    CommandProcessInputs
	MetricExecutable, JoinScript string
	Targets                      []MetricCommandTarget
	OriginalConnection           grpc.ClientConnInterface
	AdmitTools                   func(context.Context) error
}

// RunVerified composes process/initial-member admission, one durable claim,
// native execution, evidence retention and bounded post-child Join/recovery.
// Online source/identity admission remains mandatory; this is not a JSON CLI.
// Runtime Join, Owner and retention must be unset. It never releases ownership,
// and must not be retried after an ambiguous acquisition or failed experiment.
func (p ObservationCommandPlan) RunVerified(ctx context.Context, r MeasuredNetworkFaultRuntime, h ObservationHooks, inputs VerifiedCommandInputs) (ClaimedCommandResult, error) {
	var result ClaimedCommandResult
	if err := ownerContext(ctx); err != nil {
		return result, err
	}
	if inputs.OriginalConnection == nil || inputs.AdmitTools == nil || r.Network.Lifecycle.Join != nil {
		return result, errors.New("verified command requires endpoint/tool admission and no preconfigured Join")
	}
	if _, err := planinput.ReadFile(inputs.JoinScript, false, 1<<20); err != nil {
		return result, err
	}
	r, h, err := p.BindProcessAdmission(r, h, inputs.Targets, inputs.Processes)
	if err != nil {
		return result, err
	}
	r.Network.Lifecycle.Join = func(ctx context.Context) error {
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
		return p.VerifyInitialMembers(ctx, inputs.OriginalConnection, r.Network.SuccessorConnection, initialAdmit)
	}
	return p.ClaimAndRun(ctx, r, h, inputs.MetricExecutable, inputs.Targets, preclaim)
}
