package leasefault

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
)

// NetworkFaultRuntime assembles the concrete repository network observers with
// the native probe lifecycle. It does not acquire/release ownership, discover
// identities, admit images or build commands. Those are independent inputs.
// Lifecycle must leave NoncesSafe, ReservedReady, ObserveFault, NetworkRestored and
// IdentityRestored empty; this runtime supplies them, never silently overrides.
type NetworkFaultRuntime struct {
	Lifecycle                      FaultLifecycle
	ScriptDirectory, TargetsSHA256 string
	Env                            []string
	AdmitNetwork                   func(context.Context) error
	RetainNetwork                  func(string, []byte, error) error
	SuccessorConnection            grpc.ClientConnInterface
	Successor                      SuccessorBinding
	AdmitSuccessor                 func(context.Context) error
	RetainStatus                   func(context.Context, SuccessorSample) error
	CaptureSeconds                 int
	// Set only by RunVerified: bracket the complete ownership/network
	// admission, not each of its internal read-only checks independently.
	admitTools func(context.Context) error
}

func (r NetworkFaultRuntime) Run(ctx context.Context) (LifecycleResult, error) {
	l, err := r.bind()
	if err != nil {
		return LifecycleResult{}, err
	}
	return RunFaultLifecycle(ctx, l)
}

func (r NetworkFaultRuntime) bind() (FaultLifecycle, error) {
	l := r.Lifecycle
	if l.Observation == nil || l.Owner == nil || l.Preparation.Own == nil || l.OutcomeAdmit == nil || l.Preparation.NoncesSafe != nil || l.Preparation.ReservedReady != nil || l.ObserveFault != nil || l.NetworkRestored != nil || l.IdentityRestored != nil || r.AdmitNetwork == nil || r.RetainNetwork == nil || r.SuccessorConnection == nil || r.AdmitSuccessor == nil || r.RetainStatus == nil || r.CaptureSeconds < 1 || r.CaptureSeconds > 9 {
		return FaultLifecycle{}, errors.New("incomplete or conflicting concrete network runtime")
	}
	initial := l.Observation.Initial
	digest, err := hex.DecodeString(r.TargetsSHA256)
	if !filepath.IsAbs(r.ScriptDirectory) || filepath.Clean(r.ScriptDirectory) != r.ScriptDirectory || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != r.TargetsSHA256 {
		return FaultLifecycle{}, errors.New("invalid concrete network script/input binding")
	}
	b := r.Successor
	if !b.Origin.IsZero() || b.ClusterID == 0 || b.ClusterID != initial.ClusterID || b.OldLeaderID == 0 || b.OldLeaderID != initial.InitialMemberID || b.OldTerm == 0 || b.OldTerm != initial.InitialTerm || b.ObserverMemberID == 0 || b.ObserverMemberID == b.OldLeaderID {
		return FaultLifecycle{}, errors.New("independent successor does not bind the original probe")
	}
	p := l.Preparation
	liveAdmission := func(ctx context.Context) error {
		if err := l.Owner.Check(ctx); err != nil {
			return err
		}
		if err := p.Own(ctx); err != nil {
			return err
		}
		if err := r.AdmitNetwork(ctx); err != nil {
			return err
		}
		if err := l.Owner.Check(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	admit := func(ctx context.Context) error {
		if r.admitTools != nil {
			if err := r.admitTools(ctx); err != nil {
				return err
			}
		}
		if err := liveAdmission(ctx); err != nil {
			return err
		}
		if r.admitTools != nil {
			return errors.Join(r.admitTools(ctx), ctx.Err())
		}
		return ctx.Err()
	}
	n := NetworkObserver{Directory: p.Directory, StatefulSetName: p.StatefulSetName, ScriptDirectory: r.ScriptDirectory, TargetsSHA256: r.TargetsSHA256, Network: p.Network, Client: p.Client, Env: append([]string{}, r.Env...), Admit: admit, Retain: r.RetainNetwork}
	l.Preparation.Own = admit
	l.Preparation.identityCheck = func(ctx context.Context, phase NetworkLabelPhase) (err error) {
		if r.admitTools != nil {
			if err := r.admitTools(ctx); err != nil {
				return err
			}
			defer func() { err = errors.Join(err, r.admitTools(ctx), ctx.Err()) }()
		}
		// Keep both live admissions within the identity read; do not multiply
		// the complete source bracket by those internal callbacks.
		return CheckNetworkIdentity(ctx, p.Client, p.Network, p.StatefulSetName, phase, liveAdmission)
	}
	l.Preparation.protocolReservationCheck = func(ctx context.Context) (err error) {
		if r.admitTools != nil {
			if err := r.admitTools(ctx); err != nil {
				return err
			}
			defer func() { err = errors.Join(err, r.admitTools(ctx), ctx.Err()) }()
		}
		identity := func(ctx context.Context, phase NetworkLabelPhase) error {
			return CheckNetworkIdentity(ctx, p.Client, p.Network, p.StatefulSetName, phase, liveAdmission)
		}
		return p.checkProtocolReservation(ctx, identity, liveAdmission)
	}
	// The nonce scan is wholly read-only. Keep every live ownership/identity
	// check, but bracket the whole scan rather than each nested API admission.
	nonces := n
	nonces.Admit = liveAdmission
	nonces.nonceTools = r.admitTools
	l.Preparation.NoncesSafe = nonces.NoncesSafe
	prepared := n
	prepared.Admit = liveAdmission
	prepared.preparedTools = r.admitTools
	l.Preparation.ReservedReady = prepared.Prepared
	l.NetworkRestored = n.Restored
	l.IdentityRestored = n.Unlabelled
	var origin time.Time
	l.ObserveFault = func(ctx context.Context, at time.Time) (uint64, error) {
		if !origin.IsZero() {
			return 0, errors.New("concrete fault observation already invoked")
		}
		origin = at
		observe := FaultObservation{
			Connection: r.SuccessorConnection, Successor: b, Active: n.Active,
			Drops: func(ctx context.Context, at time.Time) error { return n.Drops(ctx, at, r.CaptureSeconds) },
			CheckIsolation: func(ctx context.Context) error {
				if err := n.CheckActive(ctx, at); err != nil {
					return err
				}
				return r.AdmitSuccessor(ctx)
			},
			RetainStatus: r.RetainStatus,
		}
		return observe.Observe(ctx, at)
	}
	outcomeAdmit := l.OutcomeAdmit
	l.OutcomeAdmit = func(ctx context.Context) error {
		if err := n.CheckActive(ctx, origin); err != nil {
			return err
		}
		if err := outcomeAdmit(ctx); err != nil {
			return err
		}
		return n.CheckActive(ctx, origin)
	}
	return l, nil
}
