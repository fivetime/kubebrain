package leasefault

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
)

// FaultObservation composes the read-only gates after parent-owned activation.
// Active is normally NetworkObserver.Active. Drops must capture, authenticate,
// classify and retain BOTH PD and TiKV policy drops in this original interval.
// CheckIsolation must freshly admit ownership, source/environment, observer
// connection, Pod process and the exact active policy on every call. It must
// not merely return a cached successful Active observation. All hooks must obey
// ctx; none may mutate policy, retry the original renewal or start recovery.
// Observe fits FaultLifecycle.ObserveFault; the caller must bind Successor's
// cluster/old leader/old term to the same original probe admission.
type FaultObservation struct {
	Connection     grpc.ClientConnInterface
	Successor      SuccessorBinding // Origin must be zero until Observe.
	Active         func(context.Context, time.Time) error
	Drops          func(context.Context, time.Time) error
	CheckIsolation func(context.Context) error
	RetainStatus   func(context.Context, SuccessorSample) error
}

func (o FaultObservation) Observe(ctx context.Context, origin time.Time) (uint64, error) {
	if ctx == nil || origin.IsZero() || origin.After(time.Now()) || o.Connection == nil || o.Active == nil || o.Drops == nil || o.CheckIsolation == nil || o.RetainStatus == nil || !o.Successor.Origin.IsZero() || o.Successor.ClusterID == 0 || o.Successor.ObserverMemberID == 0 || o.Successor.OldLeaderID == 0 || o.Successor.OldTerm == 0 || o.Successor.ObserverMemberID == o.Successor.OldLeaderID {
		return 0, errors.New("incomplete fault observation binding")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(origin.Add(30*time.Second)) {
		return 0, errors.New("fault observation requires original deadline")
	}
	check := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		err := o.CheckIsolation(ctx)
		if !time.Now().Before(deadline) {
			return errors.Join(err, context.DeadlineExceeded)
		}
		return errors.Join(err, ctx.Err())
	}
	// Neither policy realization alone nor a new leader alone establishes the
	// intended fault. Fail before any Status RPC if either network gate fails.
	for _, gate := range []func(context.Context, time.Time) error{o.Active, o.Drops} {
		if err := check(ctx); err != nil {
			return 0, err
		}
		if err := gate(ctx, origin); err != nil {
			return 0, err
		}
		if err := check(ctx); err != nil {
			return 0, err
		}
	}
	b := o.Successor
	b.Origin = origin
	status, err := ObserveSuccessor(ctx, o.Connection, b, check, o.RetainStatus)
	if err != nil {
		return 0, err
	}
	// Realization is sampled again after the independent transition. This does
	// not prove uninterrupted enforcement between samples or replace drops.
	if err := o.Active(ctx, origin); err != nil {
		return 0, err
	}
	if err := check(ctx); err != nil {
		return 0, err
	}
	return status.Header.RaftTerm, nil
}
