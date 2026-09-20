package leasefault

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// RecoveryReleaseProof requires fresh independent observers, never cached
// success markers. Admit authenticates tools, inputs, live processes and the
// healthy RPC endpoint. Join accounts for all owned descendants. The two network
// hooks establish dataplane withdrawal and final unlabelled identity convergence.
type RecoveryReleaseProof struct {
	Network                                        NetworkRecovery
	Protocol                                       ProtocolRecovery
	Connection                                     grpc.ClientConnInterface
	Admit, Join, NetworkRestored, IdentityRestored func(context.Context) error
}

// ReleaseRecovered explicitly releases this exact acquired claim only after
// fresh post-recovery proof. It does not restore protocol/network state, infer
// absence from missing journals, or turn a failed fault into acceptance. Its only
// API mutation is the claim's UID/RV-preconditioned DELETE; Join may stop owned
// workers. A final evidence error after DELETE is ambiguous and must not retry.
func (o *FaultOwner) ReleaseRecovered(ctx context.Context, p RecoveryReleaseProof) error {
	if err := ownerContext(ctx); err != nil {
		return err
	}
	if o == nil || p.Connection == nil || p.Admit == nil || p.Join == nil || p.NetworkRestored == nil || p.IdentityRestored == nil {
		return errors.New("release requires complete fresh recovery proof")
	}
	if err := ValidateRecoveryPlans(p.Network, p.Protocol); err != nil {
		return err
	}
	b := o.binding
	if p.Network.Owner != b.Owner || p.Network.Namespace != b.Namespace || p.Network.NamespaceUID != b.NamespaceUID || p.Network.StatefulSetUID != b.StatefulSetUID {
		return errors.New("release proof differs from acquired claim")
	}
	admit := func(ctx context.Context) error {
		if err := o.Check(ctx); err != nil {
			return err
		}
		if err := p.Admit(ctx); err != nil {
			return err
		}
		return o.Check(ctx)
	}
	absence := func(ctx context.Context) error {
		if err := CheckNetworkIdentity(ctx, o.client, p.Network, b.StatefulSetName, NetworkUnlabelled, admit); err != nil {
			return err
		}
		_, err := o.client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(b.Namespace).Get(ctx, p.Network.PolicyName, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			if err != nil {
				return err
			}
			return errors.New("fault policy still exists before claim release")
		}
		return admit(ctx)
	}
	proof := func(ctx context.Context) error {
		for _, stage := range []struct {
			name  string
			check func(context.Context) error
		}{
			{"join", p.Join}, {"absence-before", absence}, {"network", p.NetworkRestored}, {"identity", p.IdentityRestored},
			{"protocol", func(ctx context.Context) error { return VerifyProtocolRecovery(ctx, p.Protocol, p.Connection) }}, {"absence-after", absence},
		} {
			if err := admit(ctx); err != nil {
				return err
			}
			observed := stage.check(ctx)
			observed = errors.Join(observed, ctx.Err())
			retained := RetainRecoveryObserver(o.directory, "release-"+stage.name, nil, observed)
			if err := errors.Join(observed, retained); err != nil {
				return err
			}
			if err := admit(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	err := o.Release(ctx, proof)
	return errors.Join(err, RetainRecoveryObserver(o.directory, "release-return", []byte(o.uid), err))
}
