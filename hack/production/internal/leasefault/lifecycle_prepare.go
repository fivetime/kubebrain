package leasefault

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// FaultPreparation prepares one exclusively owned dedicated test instance.
// Own must retain external exclusive ownership through later fault and recovery.
// NoncesSafe must reject stale/foreign endpoints selected by either nonce, while
// allowing the admitted target as its label converges. ReservedReady must prove
// target identity convergence and that the inactive policy is not isolating it.
// All hooks must honor ctx and inspect fresh independently bound evidence.
type FaultPreparation struct {
	Directory, StatefulSetName     string
	Network                        NetworkRecovery
	Protocol                       ProtocolRecovery
	Client                         dynamic.Interface
	Connection                     grpc.ClientConnInterface
	Own, NoncesSafe, ReservedReady func(context.Context) error
}

// PrepareFault serializes durable network reservation, label and protocol setup.
// It never activates the policy, starts the fault clock or retries a mutation.
// Errors require post-join reconciliation, including ambiguous CREATE without a
// receipt; callers must not interpret an error as proof nothing was changed.
func PrepareFault(ctx context.Context, p FaultPreparation) error {
	if err := p.validate(ctx); err != nil {
		return err
	}
	return prepareFault(ctx, p)
}

func (p FaultPreparation) validate(ctx context.Context) error {
	if ctx == nil || p.Client == nil || p.Connection == nil || p.Own == nil || p.NoncesSafe == nil || p.ReservedReady == nil || p.StatefulSetName == "" {
		return errors.New("incomplete fault preparation")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("preparation requires bounded context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.Protocol.valid() || p.Network.Owner != p.Protocol.Owner || p.Network.NamespaceUID != p.Protocol.NamespaceUID || p.Network.StatefulSetUID != p.Protocol.StatefulSetUID {
		return errors.New("invalid or inconsistent preparation identities")
	}
	_, err := p.Network.encoded()
	return err
}

func prepareFault(ctx context.Context, p FaultPreparation) error {
	safe := func(ctx context.Context) error {
		if err := p.Own(ctx); err != nil {
			return err
		}
		if err := p.NoncesSafe(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	reserve := func(ctx context.Context) error {
		return CheckNetworkIdentity(ctx, p.Client, p.Network, p.StatefulSetName, NetworkUnlabelled, safe)
	}
	if err := ReserveNetwork(ctx, p.Client, p.Directory, p.Network, reserve); err != nil {
		return fmt.Errorf("reserve network: %w", err)
	}
	if err := PrepareNetworkLabel(ctx, p.Client, p.Directory, p.Network, p.StatefulSetName, safe); err != nil {
		return fmt.Errorf("prepare label: %w", err)
	}
	reservation := func(ctx context.Context) error {
		if err := CheckNetworkIdentity(ctx, p.Client, p.Network, p.StatefulSetName, NetworkLabelOwned, p.Own); err != nil {
			return err
		}
		raw, err := LoadNetworkReservation(p.Directory, p.Network)
		if err != nil {
			return err
		}
		var receipt unstructured.Unstructured
		if err := receipt.UnmarshalJSON(raw); err != nil {
			return err
		}
		live, err := p.Client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(p.Network.Namespace).Get(ctx, p.Network.PolicyName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if live == nil || live.GetUID() != receipt.GetUID() {
			return errors.New("inactive reservation replaced")
		}
		raw, err = live.MarshalJSON()
		if err != nil {
			return err
		}
		if err := validateReservation(p.Network, raw); err != nil {
			return err
		}
		return p.Own(ctx)
	}
	ready := func(ctx context.Context) error {
		if err := safe(ctx); err != nil {
			return err
		}
		if err := reservation(ctx); err != nil {
			return err
		}
		if err := p.ReservedReady(ctx); err != nil {
			return err
		}
		return safe(ctx)
	}
	if err := prepareProtocol(ctx, p.Directory, p.Protocol, p.Connection, ready, reservation); err != nil {
		return fmt.Errorf("prepare protocol: %w", err)
	}
	return ready(ctx)
}
