package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/stretchr/testify/require"
)

type successorRoutingElection struct {
	leader.LeaderElection
	address        string
	epoch, term    uint64
	fresh, leading bool
	refresh        func(context.Context) error
}

func (e *successorRoutingElection) GetLeaderInfo() string                { return e.address }
func (e *successorRoutingElection) EpochAndLeadingFresh() (uint64, bool) { return e.epoch, e.fresh }
func (e *successorRoutingElection) IsLeader() bool                       { return e.leading }
func (e *successorRoutingElection) CurrentLeadershipTerm() uint64        { return e.term }
func (e *successorRoutingElection) RefreshLeaderInfo(ctx context.Context) error {
	return e.refresh(ctx)
}

func TestSuccessorRoutingViewInvalidation(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	for _, mode := range []string{"hint", "new record", "new epoch", "new term", "local leader", "fresh", "expired", "storage restored"} {
		t.Run(mode, func(t *testing.T) {
			base := &successorRoutingElection{address: "old", epoch: 3, term: 7, refresh: func(context.Context) error { return storageErr }}
			view := &successorRoutingView{LeaderElection: base, discover: func(context.Context) (string, error) { return "next", nil }}
			require.NoError(t, view.RefreshLeaderInfo(context.Background()))
			require.Equal(t, "next", view.GetLeaderInfo())
			require.Equal(t, "old", base.GetLeaderInfo(), "hint must not mutate the authoritative election")
			switch mode {
			case "new record":
				base.address = "third"
			case "new epoch":
				base.epoch++
			case "new term":
				base.term++
			case "local leader":
				base.leading = true
			case "fresh":
				base.fresh = true
			case "expired":
				view.until = time.Now().Add(-time.Second)
			case "storage restored":
				base.refresh = func(context.Context) error { return nil }
				require.NoError(t, view.RefreshLeaderInfo(context.Background()))
			}
			if mode == "hint" {
				require.Equal(t, "next", view.GetLeaderInfo())
			} else {
				require.Equal(t, base.address, view.GetLeaderInfo())
			}
		})
	}
}

func TestSuccessorRoutingViewDiscoveryFences(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	for _, mode := range []string{"leader before", "fresh before", "leader during", "epoch during", "record during", "term during", "canceled", "failed", "empty"} {
		t.Run(mode, func(t *testing.T) {
			base := &successorRoutingElection{address: "old", epoch: 3, term: 7, refresh: func(context.Context) error { return storageErr }}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "leader before" {
				base.leading = true
			}
			if mode == "fresh before" {
				base.fresh = true
			}
			calls := 0
			view := &successorRoutingView{LeaderElection: base, discover: func(context.Context) (string, error) {
				calls++
				switch mode {
				case "leader during":
					base.leading = true
				case "epoch during":
					base.epoch++
				case "record during":
					base.address = "third"
				case "term during":
					base.term++
				case "canceled":
					cancel()
				case "failed":
					return "", errors.New("probe failed")
				case "empty":
					return "", nil
				}
				return "next", nil
			}}
			require.ErrorIs(t, view.RefreshLeaderInfo(ctx), storageErr)
			require.Equal(t, base.address, view.GetLeaderInfo())
			if mode == "leader before" || mode == "fresh before" {
				require.Zero(t, calls)
			}
		})
	}
}

func TestSuccessorRoutingViewReservesDiscoveryBudget(t *testing.T) {
	base := &successorRoutingElection{address: "old", refresh: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	view := &successorRoutingView{LeaderElection: base, discover: func(ctx context.Context) (string, error) {
		require.NoError(t, ctx.Err(), "storage timeout must leave discovery time")
		return "next", nil
	}}
	require.NoError(t, view.RefreshLeaderInfo(ctx))
	require.Equal(t, "next", view.GetLeaderInfo())
}
