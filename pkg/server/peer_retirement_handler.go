package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"golang.org/x/time/rate"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// peerRetirementHandler is registered only by the explicit opt-in server. The release
// callback MUST be bound to the authorizer's instance/keyspace by construction,
// perform only the exact conditional release, and honor context cancellation.
// It must never reactivate the retired holder, even after an uncertain commit.
// A callback ignoring cancellation retains its slot: no detached goroutine may
// turn a timeout into unbounded background storage work.
type peerRetirementHandler struct {
	auth                        *peerRetirementAuthorizer
	release                     func(context.Context, election.OwnershipCondition) error
	readBudget, operationBudget time.Duration
	slots                       chan struct{}
	rate                        *rate.Limiter
}

// newStoragePeerRetirementHandler binds authorization to the local lock's
// cluster/keyspace/election namespace before any endpoint can be registered.
// auth.instance must be the operator-approved RetirementScope, not request data.
func newStoragePeerRetirementHandler(auth *peerRetirementAuthorizer, lock resourcelock.Interface,
	readBudget, operationBudget time.Duration, concurrency int, requestsPerSecond float64,
) (*peerRetirementHandler, error) {
	releaser, ok := lock.(election.RetiredOwnershipReleaser)
	if !ok || auth == nil || auth.instance == "" || releaser.RetirementScope() == "" || auth.instance != releaser.RetirementScope() {
		return nil, errors.New("peer retirement instance does not match storage scope")
	}
	scope := auth.instance
	return newPeerRetirementHandler(auth, func(ctx context.Context, condition election.OwnershipCondition) error {
		return releaser.ReleaseRetiredOwnership(ctx, scope, condition)
	}, readBudget, operationBudget, concurrency, requestsPerSecond)
}

func newPeerRetirementHandler(auth *peerRetirementAuthorizer, release func(context.Context, election.OwnershipCondition) error,
	readBudget, operationBudget time.Duration, concurrency int, requestsPerSecond float64,
) (*peerRetirementHandler, error) {
	if auth == nil || release == nil || readBudget <= 0 || readBudget > time.Minute || operationBudget <= 0 || operationBudget > time.Minute || concurrency < 1 || concurrency > 64 || !(requestsPerSecond > 0 && requestsPerSecond <= 1000) {
		return nil, errors.New("invalid peer retirement handler configuration")
	}
	return &peerRetirementHandler{auth: auth, release: release, readBudget: readBudget, operationBudget: operationBudget,
		slots: make(chan struct{}, concurrency), rate: rate.NewLimiter(rate.Limit(requestsPerSecond), concurrency)}, nil
}

func (h *peerRetirementHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This rare control exchange does not reuse HTTP/1 connections. In
	// particular, rejection must not drain an attacker-controlled slow body.
	w.Header().Set("Connection", "close")
	w.Header().Set("Cache-Control", "no-store")
	if h == nil || h.auth == nil || h.release == nil || h.rate == nil || h.slots == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	start := time.Now()
	controller := http.NewResponseController(w)
	// Context cancellation cannot interrupt a stalled socket body Read.
	// Fail closed if wrapping middleware hides deadline support.
	if controller.SetReadDeadline(start.Add(h.readBudget)) != nil || controller.SetWriteDeadline(start.Add(h.readBudget+h.operationBudget+time.Second)) != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if _, err := authorizePeerRetirementRequest(r, h.auth); err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	if !h.rate.Allow() {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	condition, err := readPeerRetirementCondition(r, h.auth)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.operationBudget)
	defer cancel()
	if ctx.Err() != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	err = h.release(ctx, condition)
	if err != nil || ctx.Err() != nil {
		// Do not reflect backend errors or distinguish a stale condition from
		// an uncertain commit. Neither response authorizes reactivation.
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
