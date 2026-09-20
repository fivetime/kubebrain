package leasefault

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
)

// BindEvidence installs durable retention for the concrete command lifecycle.
// Admission and Join remain caller obligations. Records are observations, never
// proof of provenance, acceptance or recovery. Existing retention is rejected.
// Bind only after creating the private owner directory; its identity is pinned
// across calls. Cancelled observations are still retained, then return ctx.Err.
func (p ObservationCommandPlan) BindEvidence(r MeasuredNetworkFaultRuntime, h ObservationHooks) (MeasuredNetworkFaultRuntime, ObservationHooks, error) {
	if r.RetainMetrics != nil || r.Network.RetainNetwork != nil || r.Network.RetainStatus != nil || h.RetainOriginal != nil || h.RetainStack != nil {
		return r, h, errors.New("command evidence cannot replace existing retention")
	}
	if err := p.Bindings.Validate(); err != nil {
		return r, h, err
	}
	root, err := recoveryRoot(p.OwnerDirectory)
	if err != nil {
		return r, h, err
	}
	identity, statErr := root.Stat(".")
	closeErr := root.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return r, h, err
	}
	bindings, err := json.Marshal(p.Bindings)
	if err != nil {
		return r, h, err
	}
	digest := sha256.Sum256(bindings)
	owner, directory, count := p.Bindings.Network.Owner, p.OwnerDirectory, len(p.Bindings.Metrics)
	check := func() error {
		current, err := os.Lstat(directory)
		if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
			return errors.New("command evidence owner directory changed")
		}
		return nil
	}
	retain := func(ctx context.Context, stage string, payload any, observed error) error {
		if ctx == nil {
			return errors.New("command evidence requires context")
		}
		if err := check(); err != nil {
			return err
		}
		data, err := json.Marshal(struct {
			Owner          string `json:"owner"`
			BindingsSHA256 string `json:"bindings_sha256"`
			Payload        any    `json:"payload"`
		}{owner, hex.EncodeToString(digest[:]), payload})
		if err != nil {
			return err
		}
		err = retainObserver(directory, "experiment", stage, data, observed)
		return errors.Join(err, check(), ctx.Err())
	}
	h.RetainOriginal = func(ctx context.Context, stage string, binding Binding, output []byte, observed error) error {
		if stage != "pending" && stage != "response" {
			return errors.New("invalid original evidence stage")
		}
		return retain(ctx, "original-"+stage, struct {
			Binding Binding
			Output  []byte
		}{binding, output}, observed)
	}
	h.RetainStack = func(ctx context.Context, stage string, receipt WaitReceipt, observed error) error {
		if stage != "before" && stage != "after" {
			return errors.New("invalid stack evidence stage")
		}
		return retain(ctx, "stack-"+stage, receipt, observed)
	}
	r.Network.RetainNetwork = func(stage string, output []byte, observed error) error {
		// This callback has no context in NetworkObserver. Its caller enforces
		// the fault/recovery deadline separately after durable retention.
		return retain(context.Background(), "network-"+stage, output, observed)
	}
	r.Network.RetainStatus = func(ctx context.Context, sample SuccessorSample) error {
		observed := sample.Error
		sample.Error = nil // error interfaces otherwise marshal as an empty object.
		return retain(ctx, "successor", sample, observed)
	}
	r.RetainMetrics = func(ctx context.Context, index int, measurement retirementmetrics.WorkerMeasurement) error {
		if index < 0 || index >= count {
			return errors.New("invalid metric evidence index")
		}
		return retain(ctx, "metrics", struct {
			Index       int
			Measurement retirementmetrics.WorkerMeasurement
		}{index, measurement}, nil)
	}
	return r, h, nil
}
