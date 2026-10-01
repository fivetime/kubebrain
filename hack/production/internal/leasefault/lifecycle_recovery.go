package leasefault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"k8s.io/client-go/dynamic"
)

// FaultRecovery binds the network and protocol journals to one recovery owner.
// Hooks must use ctx, inspect fresh evidence, and fail if evidence is missing.
// Own checks externally held exclusive ownership (it does not acquire a lock).
// Join must stop/join all fault and metric workers, including their descendants.
// NetworkRestored independently verifies Cilium withdrawal and network access.
// IdentityRestored verifies final Cilium identity convergence after label removal.
// No hook may start a new experiment or silently substitute a new fault clock.
type FaultRecovery struct {
	Directory, StatefulSetName                   string
	Network                                      NetworkRecovery
	Protocol                                     ProtocolRecovery
	Client                                       dynamic.Interface
	Connection                                   grpc.ClientConnInterface
	Own, Join, NetworkRestored, IdentityRestored func(context.Context) error
}

// ValidateRecoveryPlans validates independent network/protocol bindings without
// opening records, accessing APIs or authorizing any mutation.
func ValidateRecoveryPlans(network NetworkRecovery, protocol ProtocolRecovery) error {
	if !protocol.valid() || network.Owner != protocol.Owner || network.NamespaceUID != protocol.NamespaceUID || network.StatefulSetUID != protocol.StatefulSetUID {
		return errors.New("invalid or inconsistent recovery plans")
	}
	_, err := network.encoded()
	return err
}

// RecoverFault serializes the recovery half of the fault coordinator. Use an
// independent recovery context after fault cancellation, bounded to five minutes.
// It never reports fault acceptance, releases ownership, deletes journals, retries
// ambiguous mutations, or skips a failed stage. Cluster-specific hook implementations
// and the preparation/fault-gate half of the complete coordinator remain required.
func RecoverFault(ctx context.Context, r FaultRecovery) error {
	if ctx == nil || r.Client == nil || r.Connection == nil || r.Own == nil || r.Join == nil || r.NetworkRestored == nil || r.IdentityRestored == nil {
		return errors.New("incomplete recovery configuration")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("recovery requires independent bounded context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Joining precedes all journal/API/RPC work, including malformed-plan errors.
	if err := r.Join(ctx); err != nil {
		return fmt.Errorf("join fault workers: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Network.Owner != r.Protocol.Owner || r.Network.NamespaceUID != r.Protocol.NamespaceUID || r.Network.StatefulSetUID != r.Protocol.StatefulSetUID {
		return errors.New("network and protocol recovery owners differ")
	}
	_, protocolErr := LoadProtocolRecovery(r.Directory, r.Protocol)
	protocolMissing := errors.Is(protocolErr, os.ErrNotExist)
	if protocolErr != nil && !protocolMissing {
		return protocolErr
	}
	checkProtocolIntent := func() error {
		_, err := LoadProtocolRecovery(r.Directory, r.Protocol)
		if protocolMissing {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err == nil {
				return errors.New("protocol intent appeared during read-only recovery")
			}
		}
		return err
	}
	if _, err := LoadNetworkReservation(r.Directory, r.Network); err != nil {
		return err
	}
	admit := func(ctx context.Context) error {
		return CheckNetworkIdentity(ctx, r.Client, r.Network, r.StatefulSetName, NetworkLabelRecovery, r.Own)
	}
	if err := RemoveNetworkPolicy(ctx, r.Client, r.Directory, r.Network, admit); err != nil {
		return fmt.Errorf("remove fault policy: %w", err)
	}
	networkReady := func(ctx context.Context) error {
		if err := admit(ctx); err != nil {
			return err
		}
		if err := r.NetworkRestored(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err := networkReady(ctx); err != nil {
		return fmt.Errorf("verify network withdrawal: %w", err)
	}
	if !protocolMissing {
		if err := RestoreProtocol(ctx, r.Directory, r.Protocol, r.Connection, networkReady); err != nil {
			return fmt.Errorf("restore protocol: %w", err)
		}
	}
	protocolReady := func(ctx context.Context) error {
		if err := networkReady(ctx); err != nil {
			return err
		}
		if err := checkProtocolIntent(); err != nil {
			return err
		}
		return VerifyProtocolRecovery(ctx, r.Protocol, r.Connection)
	}
	// Missing intent never authorizes protocol writes or proves preparation did
	// not run. Only independent absence checks can permit label cleanup; any live
	// fixture/uncertain state remains a reconciliation error with journals intact.
	if err := protocolReady(ctx); err != nil {
		return fmt.Errorf("verify protocol before label recovery: %w", err)
	}
	if err := changeNetworkLabelWithOwnership(ctx, r.Client, r.Directory, r.Network, r.StatefulSetName, protocolReady, r.Own, true); err != nil {
		return fmt.Errorf("restore Pod label: %w", err)
	}
	if err := r.IdentityRestored(ctx); err != nil {
		return fmt.Errorf("verify identity convergence: %w", err)
	}
	if err := protocolReady(ctx); err != nil {
		return err
	}
	return CheckNetworkIdentity(ctx, r.Client, r.Network, r.StatefulSetName, NetworkUnlabelled, r.Own)
}
