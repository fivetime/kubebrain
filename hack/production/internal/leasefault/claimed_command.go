package leasefault

import (
	"context"
	"errors"
	"os"
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
// Callers must prepare private receipt directories and logs beforehand, retain
// the returned result, and explicitly release only after fresh recovery proof.
func (p ObservationCommandPlan) ClaimAndRun(ctx context.Context, r MeasuredNetworkFaultRuntime, h ObservationHooks, metricExecutable string, targets []MetricCommandTarget, admit func(context.Context) error) (ClaimedCommandResult, error) {
	var result ClaimedCommandResult
	if r.Network.Lifecycle.Owner != nil || admit == nil {
		return result, errors.New("claiming command requires preclaim admission and no existing owner")
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
	return result, err
}
