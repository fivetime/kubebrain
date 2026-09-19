package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"net/http"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// PeerRetirementConfig is an explicit, experimental opt-in, with no automatic
// default. Operators must provision distinct holder keys
// and approve the exact backend scope. Lease continuity and production fault
// acceptance remain required before enabling this in a deployment.
type PeerRetirementConfig struct {
	// ControlCredentialSource reloads credentials for each control HTTP request.
	// Requires SuccessorHolders. TLS remains mandatory for startup validation;
	// this source does not configure the independent gRPC proxy TLS transport.
	ControlCredentialSource transportidentity.ClientCredentialSource
	// ProxyCredentialSource reloads material at each gRPC connection handshake.
	// Requires SuccessorHolders; finite connection lifetime forces reloading.
	ProxyCredentialSource transportidentity.ClientCredentialSource
	Scope                 string
	HolderPins            map[string][]string
	PeerURLs              []string
	// SuccessorHolders optionally enables read-only discovery for forwarding.
	// Keys are exact canonical peer base URLs; values are configured holders.
	SuccessorHolders                        map[string]string
	TLS                                     *tls.Config `json:"-"`
	ReadBudget, OperationBudget, SendBudget time.Duration
	Concurrency                             int
	RequestsPerSecond                       float64
}

type peerRetirementProtocol struct {
	handler   *peerRetirementHandler
	sender    *peerRetirementSender
	discovery *peerSuccessorDiscovery
}

func preparePeerRetirement(lock resourcelock.Interface, config PeerRetirementConfig) (*peerRetirementProtocol, error) {
	invalid := errors.New("invalid peer retirement protocol configuration")
	if lock == nil || len(config.HolderPins) < 2 {
		return nil, invalid
	}
	auth, err := newPeerRetirementAuthorizer(config.Scope, config.HolderPins)
	if err != nil {
		return nil, invalid
	}
	handler, err := newStoragePeerRetirementHandler(auth, lock, config.ReadBudget, config.OperationBudget, config.Concurrency, config.RequestsPerSecond)
	if err != nil {
		return nil, invalid
	}
	sender, err := newPeerRetirementSender(config.Scope, lock.Identity(), config.PeerURLs, config.TLS, config.SendBudget)
	if err != nil {
		return nil, invalid
	}
	// The sender owns a detached parsed certificate. Refuse an ordinary client
	// key or a peer key assigned to a different holder before starting workers.
	transport := sender.client.Transport.(*http.Transport)
	leaf := transport.TLSClientConfig.Certificates[0].Leaf
	if _, ok := auth.holders[lock.Identity()][sha256.Sum256(leaf.RawSubjectPublicKeyInfo)]; !ok {
		return nil, invalid
	}
	protocol := &peerRetirementProtocol{handler: handler, sender: sender}
	if config.SuccessorHolders != nil {
		protocol.discovery, err = newPeerSuccessorDiscovery(sender, auth, config.SuccessorHolders)
		if err != nil {
			return nil, invalid
		}
	}
	if config.ControlCredentialSource != nil {
		if protocol.discovery == nil || protocol.discovery.useCredentialSource(config.ControlCredentialSource) != nil {
			return nil, invalid
		}
	}
	if config.ProxyCredentialSource != nil {
		if protocol.discovery == nil {
			return nil, invalid
		}
		protocol.discovery.proxySource = config.ProxyCredentialSource
	}
	return protocol, nil
}

// NewServerWithPeerRetirement validates the experimental protocol before any
// campaign/background goroutine starts. The ordinary NewServer remains disabled.
// Callers must use a verified-mTLS peer listener with bounded header/handshake
// and connection admission; this API does not configure or start the listener.
// Client/info handlers never expose the retirement route.
func NewServerWithPeerRetirement(ctx context.Context, b backend.Backend, metricCli metrics.Metrics, config Config, retirement PeerRetirementConfig) (Server, error) {
	if ctx == nil || b == nil {
		return nil, errors.New("invalid retirement server dependencies")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := config.getLeaderConfig().Validate(); err != nil {
		return nil, errors.New("invalid retirement leader durations")
	}
	protocol, err := preparePeerRetirement(b.GetResourceLock(), retirement)
	if err != nil {
		return nil, err
	}
	if protocol.discovery != nil && config.EnableEtcdProxy {
		if config.ProxyAllowInsecure {
			return nil, errors.New("successor forwarding requires strict peer TLS")
		}
		proxyCredentials, err := newPeerRetirementSender(retirement.Scope, b.GetResourceLock().Identity(), retirement.PeerURLs, config.ProxyTLS, retirement.SendBudget)
		if err != nil {
			return nil, errors.New("invalid successor forwarding TLS")
		}
		tlsConfig := proxyCredentials.client.Transport.(*http.Transport).TLSClientConfig
		leaf := tlsConfig.Certificates[0].Leaf
		if _, ok := protocol.handler.auth.holders[b.GetResourceLock().Identity()][sha256.Sum256(leaf.RawSubjectPublicKeyInfo)]; !ok {
			return nil, errors.New("invalid successor forwarding identity")
		}
		config.ProxyTLS = tlsConfig
	}
	protocol.sender.metricCli = metricCli
	return newServer(ctx, b, metricCli, config, protocol), nil
}

func (p *peerRetirementProtocol) onTermRetired(ctx context.Context, condition election.OwnershipCondition, available bool) {
	p.sender.onTermRetired(ctx, condition, available)
}
