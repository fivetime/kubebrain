package leasefault

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestFaultObservationGateOrdering(t *testing.T) {
	for _, mode := range []string{"success", "active-fails", "drops-fail", "drops-cancel", "isolation-lost", "retain-fails", "final-active-fails", "final-active-cancel", "extended-budget", "prefilled-clock", "missing-drops", "invalid-member"} {
		t.Run(mode, func(t *testing.T) {
			origin := time.Now()
			budget := time.Second
			if mode == "extended-budget" {
				budget = 31 * time.Second
			}
			ctx, cancel := context.WithDeadline(context.Background(), origin.Add(budget))
			defer cancel()
			var events []string
			active, drops, retained := 0, 0, 0
			checkClock := func(got context.Context, at time.Time) {
				require.Equal(t, origin, at)
				require.Same(t, ctx, got)
			}
			conn := &successorConnection{read: func(got context.Context, _ int) (*pb.StatusResponse, error) {
				events = append(events, "status")
				require.Equal(t, 1, active)
				require.Equal(t, 1, drops)
				deadline, ok := got.Deadline()
				require.True(t, ok)
				require.False(t, deadline.After(origin.Add(budget)))
				return &pb.StatusResponse{Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 3, RaftTerm: 5}, Leader: 3}, nil
			}}
			o := FaultObservation{
				Connection: conn,
				Successor:  SuccessorBinding{ClusterID: 1, ObserverMemberID: 3, OldLeaderID: 2, OldTerm: 4},
				Active: func(got context.Context, at time.Time) error {
					checkClock(got, at)
					active++
					events = append(events, "active")
					if mode == "active-fails" || (mode == "final-active-fails" && active == 2) {
						return errors.New("policy not realized")
					}
					if mode == "final-active-cancel" && active == 2 {
						cancel()
					}
					return nil
				},
				Drops: func(got context.Context, at time.Time) error {
					checkClock(got, at)
					drops++
					events = append(events, "drops")
					if mode == "drops-fail" {
						return errors.New("missing TiKV drops")
					}
					if mode == "drops-cancel" {
						cancel()
					}
					return nil
				},
				CheckIsolation: func(got context.Context) error {
					require.Same(t, ctx, got)
					if mode == "isolation-lost" && drops > 0 {
						return errors.New("policy replaced after capture")
					}
					return nil
				},
				RetainStatus: func(got context.Context, _ SuccessorSample) error {
					require.Same(t, ctx, got)
					retained++
					events = append(events, "retain")
					if mode == "retain-fails" {
						return errors.New("evidence persistence failed")
					}
					return nil
				},
			}
			switch mode {
			case "prefilled-clock":
				o.Successor.Origin = origin
			case "missing-drops":
				o.Drops = nil
			case "invalid-member":
				o.Successor.ObserverMemberID = o.Successor.OldLeaderID
			}
			term, err := o.Observe(ctx, origin)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, uint64(5), term)
				require.Equal(t, []string{"active", "drops", "status", "retain", "active"}, events)
			} else {
				require.Error(t, err)
				require.Zero(t, term)
			}
			require.Equal(t, conn.calls, retained)
			switch mode {
			case "success", "retain-fails", "final-active-fails", "final-active-cancel":
				require.Equal(t, 1, conn.calls)
			default:
				require.Zero(t, conn.calls)
			}
			if mode == "extended-budget" || mode == "prefilled-clock" || mode == "missing-drops" || mode == "invalid-member" {
				require.Empty(t, events)
			}
			require.True(t, o.Successor.Origin.IsZero() || mode == "prefilled-clock", "do not persist a per-run clock in reusable configuration")
		})
	}
}
