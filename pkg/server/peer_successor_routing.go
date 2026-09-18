package server

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"google.golang.org/grpc/credentials"
)

// Only the explicit dynamic proxy mode implements this provider. The embedded
// routing view retains election semantics and static endpoint validation.
type successorCredentialRoutingView struct {
	*successorRoutingView
	discovery *peerSuccessorDiscovery
}

func (v *successorCredentialRoutingView) ProxyCredentialsForEndpoint(endpoint string) (credentials.TransportCredentials, error) {
	d := v.discovery
	if d == nil || d.proxySource == nil || d.sender == nil || d.auth == nil {
		return nil, errPeerSuccessorUnavailable
	}
	u, holder, err := d.resolveProxyEndpoint(endpoint)
	if err != nil {
		return nil, errPeerSuccessorUnavailable
	}
	return &reloadedPeerGRPC{source: d.proxySource, auth: d.auth, local: d.sender.holder, remote: holder, hostname: u.Hostname(), budget: d.sender.budget}, nil
}

// successorRoutingView is passed ONLY to the proxy connector. All other users
// retain the original election, including Campaign and backend write fencing.
// Hints expire and are invalidated by local leadership or a new observed record.
type successorRoutingView struct {
	leader.LeaderElection
	discover       func(context.Context) (string, error)
	peerTLS        func(string) (*tls.Config, error)
	mu             sync.Mutex
	hint, observed string
	epoch, term    uint64
	until          time.Time
}

// ProxyTLSForEndpoint is consumed only by the proxy connector. Unknown routes
// fail closed rather than falling back to a CA-only or plaintext connection.
func (v *successorRoutingView) ProxyTLSForEndpoint(endpoint string) (*tls.Config, error) {
	if v.peerTLS == nil {
		return nil, errPeerSuccessorUnavailable
	}
	return v.peerTLS(endpoint)
}

func (v *successorRoutingView) GetLeaderInfo() string {
	observed := v.LeaderElection.GetLeaderInfo()
	epoch, fresh := v.EpochAndLeadingFresh()
	term := v.CurrentLeadershipTerm()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.hint != "" && !fresh && !v.IsLeader() && observed == v.observed && epoch == v.epoch && term == v.term && time.Now().Before(v.until) {
		return v.hint
	}
	v.hint = ""
	return observed
}

func (v *successorRoutingView) RefreshLeaderInfo(ctx context.Context) error {
	// Reserve time for authenticated discovery even if local storage stalls.
	budget := 100 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/2)
	}
	readCtx, cancel := context.WithTimeout(ctx, budget)
	err := v.LeaderElection.RefreshLeaderInfo(readCtx)
	cancel()
	v.mu.Lock()
	v.hint = ""
	v.mu.Unlock()
	if err == nil || ctx.Err() != nil || v.IsLeader() {
		return err
	}
	epoch, fresh := v.EpochAndLeadingFresh()
	if fresh {
		return err
	}
	observed := v.LeaderElection.GetLeaderInfo()
	term := v.CurrentLeadershipTerm()
	hint, discoveryErr := v.discover(ctx)
	if discoveryErr != nil || hint == "" || ctx.Err() != nil {
		return err
	}
	currentEpoch, currentFresh := v.EpochAndLeadingFresh()
	if currentFresh || v.IsLeader() || currentEpoch != epoch || v.CurrentLeadershipTerm() != term || v.LeaderElection.GetLeaderInfo() != observed {
		return err
	}
	v.mu.Lock()
	v.hint, v.observed, v.epoch, v.term, v.until = hint, observed, epoch, term, time.Now().Add(2*time.Second)
	v.mu.Unlock()
	return nil
}
