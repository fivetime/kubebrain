package server

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	peerSuccessorPath     = "/internal/successor/v1"
	successorScopeHeader  = "X-Kubebrain-Successor-Scope"
	successorHolderHeader = "X-Kubebrain-Successor-Holder"
)

var errPeerSuccessorUnavailable = errors.New("peer successor unavailable")

// A successful probe supplies only a forwarding candidate, never election
// ownership. The candidate may demote immediately after replying: normal peer
// admission and backend fencing must still validate every forwarded operation.
// Registration and connector use require explicit SuccessorHolders opt-in.
type peerSuccessorDiscovery struct {
	proxySource transportidentity.ClientCredentialSource
	sender      *peerRetirementSender
	auth        *peerRetirementAuthorizer
	targets     []peerSuccessorTarget
	slot        chan struct{}
	rate        *rate.Limiter
}

type peerSuccessorTarget struct{ endpoint, holder string }

// proxyTLS binds every new/reconnected gRPC transport to the exact configured
// endpoint's holder, including the initial authoritative route. Normal TLS
// chain and hostname verification still run before this additional pin check.
func (d *peerSuccessorDiscovery) proxyTLS(base *tls.Config, endpoint string) (*tls.Config, error) {
	if d == nil || base == nil || base.InsecureSkipVerify || base.VerifyConnection != nil || base.VerifyPeerCertificate != nil {
		return nil, errPeerSuccessorUnavailable
	}
	_, holder, err := d.resolveProxyEndpoint(endpoint)
	if err != nil {
		return nil, errPeerSuccessorUnavailable
	}
	config := base.Clone()
	config.VerifyConnection = func(state tls.ConnectionState) error {
		return d.auth.authorizeVerifiedCertificate(&state, d.auth.instance, holder, time.Now())
	}
	return config, nil
}

// The CLI election identity is host:port while authenticated discovery returns
// an HTTPS URL. Accept only the exact Host spelling of an operator-pinned URL,
// never arbitrary normalization, a redirect, or an explicit plaintext scheme.
func (d *peerSuccessorDiscovery) resolveProxyEndpoint(endpoint string) (*url.URL, string, error) {
	if d == nil || d.sender == nil || d.auth == nil {
		return nil, "", errPeerSuccessorUnavailable
	}
	for _, target := range d.targets {
		u, err := url.Parse(target.endpoint)
		if err == nil && u.Scheme == "https" && u.Hostname() != "" && (endpoint == target.endpoint || endpoint == u.Host) && len(d.auth.holders[target.holder]) > 0 {
			return u, target.holder, nil
		}
	}
	if endpoint == d.sender.holder && len(d.auth.holders[endpoint]) > 0 {
		address := endpoint
		if host, port, err := net.SplitHostPort(endpoint); err == nil && host != "" && port != "" {
			address = "https://" + endpoint
		}
		u, err := url.Parse(address)
		if err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" {
			return u, endpoint, nil
		}
	}
	return nil, "", errPeerSuccessorUnavailable
}

// endpointHolders is operator configuration, not a redirect or response body.
// Require an exact mapping for every endpoint already validated by the sender.
func newPeerSuccessorDiscovery(sender *peerRetirementSender, auth *peerRetirementAuthorizer, endpointHolders map[string]string) (*peerSuccessorDiscovery, error) {
	if sender == nil || sender.client == nil || auth == nil || sender.instance != auth.instance || len(endpointHolders) != len(sender.endpoints) || len(sender.endpoints) == 0 {
		return nil, errPeerSuccessorUnavailable
	}
	d := &peerSuccessorDiscovery{sender: sender, auth: auth, slot: make(chan struct{}, 1), rate: rate.NewLimiter(4, 1)}
	for _, endpoint := range sender.endpoints {
		base := strings.TrimSuffix(endpoint, peerRetirementPath)
		holder, ok := endpointHolders[base]
		if !ok || holder == sender.holder || len(auth.holders[holder]) == 0 {
			return nil, errPeerSuccessorUnavailable
		}
		d.targets = append(d.targets, peerSuccessorTarget{base, holder})
	}
	return d, nil
}

// discover has one shared deadline, one attempt per fixed endpoint, one flight
// and a bounded call rate. It never reads a response body or accepts a new URL.
func (d *peerSuccessorDiscovery) discover(parent context.Context) (string, error) {
	if d == nil || parent == nil || parent.Err() != nil {
		return "", errPeerSuccessorUnavailable
	}
	select {
	case d.slot <- struct{}{}:
		defer func() { <-d.slot }()
	default:
		return "", errPeerSuccessorUnavailable
	}
	if !d.rate.Allow() {
		return "", errPeerSuccessorUnavailable
	}
	ctx, cancel := context.WithTimeout(parent, d.sender.budget)
	defer cancel()
	for i, target := range d.targets {
		if ctx.Err() != nil {
			break
		}
		// Reserve a fair share for every remaining configured candidate. A
		// blackholed first peer must not consume each search's entire budget
		// forever, starving reachable successors later in the fixed list.
		deadline, _ := ctx.Deadline()
		attemptBudget := time.Until(deadline) / time.Duration(len(d.targets)-i)
		attempt, finish := context.WithTimeout(ctx, attemptBudget)
		r, err := http.NewRequestWithContext(attempt, http.MethodPost, target.endpoint+peerSuccessorPath, nil)
		if err != nil {
			finish()
			return "", errPeerSuccessorUnavailable
		}
		r.Header.Set(retirementInstanceHeader, d.sender.instance)
		r.Header.Set(retirementHolderHeader, d.sender.holder)
		response, err := d.sender.client.Do(r)
		if err != nil {
			finish()
			continue
		}
		_ = response.Body.Close()
		timely := attempt.Err() == nil
		finish()
		if response.StatusCode != http.StatusNoContent || !timely || ctx.Err() != nil {
			continue
		}
		if len(response.Header.Values(successorScopeHeader)) != 1 || response.Header.Get(successorScopeHeader) != d.auth.instance || len(response.Header.Values(successorHolderHeader)) != 1 || response.Header.Get(successorHolderHeader) != target.holder {
			continue
		}
		// CA/hostname verification alone cannot distinguish two holders whose
		// certificates share a service SAN. Pin the configured receiver as well.
		if d.auth.authorize(response.TLS, d.auth.instance, target.holder, time.Now()) != nil {
			continue
		}
		return target.endpoint, nil
	}
	return "", errPeerSuccessorUnavailable
}

// ready MUST be a nonblocking, storage-free local readiness/freshness snapshot,
// not follower proxy readiness. limits is already bound to the local storage
// scope by newStoragePeerRetirementHandler. No release callback is invoked.
type peerSuccessorHandler struct {
	limits *peerRetirementHandler
	holder string
	ready  func() bool
}

func (h *peerSuccessorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Connection", "close")
	w.Header().Set("Cache-Control", "no-store")
	if h == nil || h.limits == nil || h.ready == nil || h.limits.auth == nil || len(h.limits.auth.holders[h.holder]) == 0 || h.limits.rate == nil || h.limits.slots == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	limits := h.limits
	controller := http.NewResponseController(w)
	deadline := time.Now().Add(limits.readBudget)
	if controller.SetReadDeadline(deadline) != nil || controller.SetWriteDeadline(deadline) != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if _, err := authorizePeerRetirementRequest(r, limits.auth); err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	select {
	case limits.slots <- struct{}{}:
		defer func() { <-limits.slots }()
	default:
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	if !limits.rate.Allow() {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	if r.Context().Err() != nil || !h.ready() || time.Now().After(deadline) || r.Context().Err() != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set(successorScopeHeader, limits.auth.instance)
	w.Header().Set(successorHolderHeader, h.holder)
	w.WriteHeader(http.StatusNoContent)
}
