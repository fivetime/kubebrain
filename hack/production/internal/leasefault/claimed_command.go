package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
)

// ClaimedCommandResult returns ownership even when execution/recovery fails.
// A nil Owner does NOT prove CREATE had no effect: acquisition may be ambiguous.
// Neither success nor failure automatically releases the cluster claim.
type ClaimedCommandResult struct {
	Owner     *FaultOwner
	Lifecycle LifecycleResult
}

// ClaimAndRun performs local preflight, independent online preclaim admission,
// one durable claim acquisition, then the actual native lifecycle and recovery.
// admit must authenticate source/image/tools/credentials and all live targets;
// it must not acquire ownership or mutate the cluster. The supplied runtime must
// have no Owner. Its live hooks remain mandatory throughout execution/recovery.
// Retention callbacks must be unset: this entry installs the plan's concrete
// durable evidence writers rather than accepting no-op retention from callers.
// Callers must prepare private receipt directories and logs beforehand, retain
// the returned result, and explicitly release only after fresh recovery proof.
func (p ObservationCommandPlan) ClaimAndRun(ctx context.Context, r MeasuredNetworkFaultRuntime, h ObservationHooks, metricExecutable string, targets []MetricCommandTarget, admit func(context.Context) error) (ClaimedCommandResult, error) {
	var result ClaimedCommandResult
	if r.Network.Lifecycle.Owner != nil || admit == nil {
		return result, errors.New("claiming command requires preclaim admission and no existing owner")
	}
	r, h, err := p.BindEvidence(r, h)
	if err != nil {
		return result, err
	}
	prep := r.Network.Lifecycle.Preparation
	binding := FaultOwnerBinding{Owner: p.Bindings.Network.Owner, Namespace: p.Bindings.Network.Namespace, NamespaceUID: p.Bindings.Network.NamespaceUID, StatefulSetName: prep.StatefulSetName, StatefulSetUID: p.Bindings.Network.StatefulSetUID}
	// Hooks capture this pointer; it remains unusable until acquisition supplies
	// the actual server UID and durably stored receipt. No hooks run in preflight.
	owner := &FaultOwner{client: prep.Client, directory: p.OwnerDirectory, binding: binding}
	r.Network.Lifecycle.Owner = owner
	l, err := p.BindLifecycle(r, h, metricExecutable, targets)
	if err != nil {
		return result, err
	}
	if err := ValidateFaultLifecycleConfiguration(ctx, l); err != nil {
		return result, err
	}
	root, err := recoveryRoot(p.OwnerDirectory)
	if err != nil {
		return result, err
	}
	defer root.Close()
	identity, err := root.Stat(".")
	if err != nil {
		return result, err
	}
	checkUnused := func() error {
		for _, name := range []string{"deployment-claimed", "HOLD", "final-exit-code", faultOriginFile, ownerIntentFile, ownerReceiptFile} {
			if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
				return errors.New("claiming command requires an unused owner attempt")
			}
		}
		return nil
	}
	if err := checkUnused(); err != nil {
		return result, err
	}
	if err := admit(ctx); err != nil {
		return result, err
	}
	checkDirectory := func() error {
		current, err := os.Lstat(p.OwnerDirectory)
		if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
			return errors.New("claiming command owner directory changed")
		}
		return ctx.Err()
	}
	if err := checkDirectory(); err != nil {
		return result, err
	}
	if err := checkUnused(); err != nil {
		return result, err
	}
	acquired, err := AcquireFaultOwner(ctx, prep.Client, p.OwnerDirectory, binding)
	if err != nil {
		return result, err // Never retry/adopt an ambiguous CREATE.
	}
	*owner = *acquired
	result.Owner = owner
	if err := checkDirectory(); err != nil {
		return result, err
	}
	if err := root.Mkdir("deployment-claimed", 0700); err != nil {
		return result, err
	}
	// Run repeats local validation and fresh ownership checks before preparing
	// anything; its existing post-child recovery runs on its independent budget.
	result.Lifecycle, err = RunFaultLifecycle(ctx, l)
	// Archive even an expired/failed run, without treating archiving as a new
	// fault budget or successful recovery. Keep both errors and the claim.
	current, statErr := os.Lstat(p.OwnerDirectory)
	if statErr != nil || !current.IsDir() || !os.SameFile(identity, current) {
		return result, errors.Join(err, errors.New("lifecycle result owner directory changed"))
	}
	return result, errors.Join(err, retainClaimedLifecycle(p.OwnerDirectory, owner, result.Lifecycle, err))
}

// This is a lifecycle return record, not COMPLETE or permission to release.
// Preserve partial protocol results and explicit error strings (JSON marshaling
// an error interface would otherwise silently produce {}).
func retainClaimedLifecycle(directory string, owner *FaultOwner, result LifecycleResult, runErr error) error {
	message := func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	data, err := json.Marshal(struct {
		Owner                  FaultOwnerBinding      `json:"owner"`
		ClaimUID               string                 `json:"claim_uid"`
		RecoveryAttempted      bool                   `json:"recovery_attempted"`
		ExecutionError         string                 `json:"execution_error"`
		RecoveryError          string                 `json:"recovery_error"`
		ActivationAcknowledged bool                   `json:"activation_acknowledged"`
		Metrics                []metricsworker.Result `json:"metrics"`
		Outcome                *OriginalOutcome       `json:"outcome"`
	}{owner.binding, owner.uid, result.RecoveryAttempted, message(result.ExecutionError), message(result.RecoveryError), result.ActivationAcknowledged, result.Metrics, result.Outcome})
	if err != nil {
		return err
	}
	return retainObserver(directory, "experiment", "lifecycle", data, runErr)
}
