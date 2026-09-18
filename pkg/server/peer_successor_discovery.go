package server

import (
	"context"
	"errors"
	"net/http"
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
// This component is not yet registered or wired into the proxy connector.
type peerSuccessorDiscovery struct {
	sender  *peerRetirementSender
	auth    *peerRetirementAuthorizer
	targets []peerSuccessorTarget
	slot    chan struct{}
	rate    *rate.Limiter
}

type peerSuccessorTarget struct{ endpoint, holder string }

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
	for _, target := range d.targets {
		if ctx.Err() != nil {
			break
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, target.endpoint+peerSuccessorPath, nil)
		if err != nil {
			return "", errPeerSuccessorUnavailable
		}
		r.Header.Set(retirementInstanceHeader, d.sender.instance)
		r.Header.Set(retirementHolderHeader, d.sender.holder)
		response, err := d.sender.client.Do(r)
		if err != nil {
			continue
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent || ctx.Err() != nil {
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
