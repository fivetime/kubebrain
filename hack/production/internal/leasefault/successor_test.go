package leasefault

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type successorConnection struct {
	grpc.ClientConnInterface
	calls int
	read  func(context.Context, int) (*pb.StatusResponse, error)
}

func (c *successorConnection) Invoke(ctx context.Context, method string, _ any, reply any, _ ...grpc.CallOption) error {
	if method != "/etcdserverpb.Maintenance/Status" {
		return errors.New("unexpected RPC")
	}
	c.calls++
	response, err := c.read(ctx, c.calls)
	if err == nil && response != nil {
		proto.Merge(reply.(*pb.StatusResponse), response)
	}
	return err
}

func TestObserveSuccessor(t *testing.T) {
	for _, mode := range []string{"success", "pending", "transient", "stale", "wrong-cluster", "wrong-observer", "retention-fail", "retention-cancel", "retention-mutates", "admission-lost", "permission-denied", "extended-budget"} {
		t.Run(mode, func(t *testing.T) {
			b := SuccessorBinding{ClusterID: math.MaxUint64, ObserverMemberID: 22, OldLeaderID: 11, OldTerm: 9007199254740993, Origin: time.Now()}
			duration := 2 * time.Second
			if mode == "stale" {
				duration = 50 * time.Millisecond
			}
			if mode == "extended-budget" {
				duration = 31 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			conn := &successorConnection{read: func(ctx context.Context, n int) (*pb.StatusResponse, error) {
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
				r := &pb.StatusResponse{Header: &pb.ResponseHeader{ClusterId: b.ClusterID, MemberId: b.ObserverMemberID, RaftTerm: b.OldTerm + 1, Revision: 1}, Leader: 33}
				switch mode {
				case "pending":
					if n == 1 {
						r.Leader = b.OldLeaderID
					}
				case "transient":
					if n == 1 {
						return nil, status.Error(codes.Unavailable, "read unavailable")
					}
				case "stale":
					r.Header.RaftTerm = b.OldTerm
				case "wrong-cluster":
					r.Header.ClusterId--
				case "wrong-observer":
					r.Header.MemberId++
				case "permission-denied":
					return nil, status.Error(codes.PermissionDenied, "denied")
				}
				return r, nil
			}}
			retained := 0
			result, err := ObserveSuccessor(ctx, conn, b, func(context.Context) error {
				if mode == "admission-lost" && retained > 0 {
					return errors.New("claim lost")
				}
				return nil
			}, func(_ context.Context, s SuccessorSample) error {
				retained++
				require.False(t, s.Completed.Before(s.Started))
				if mode == "retention-mutates" {
					s.Status.Header.ClusterId = 0
					s.Status.Leader = b.OldLeaderID
				}
				if mode == "retention-fail" {
					return errors.New("disk failure")
				}
				if mode == "retention-cancel" {
					cancel()
				}
				return nil
			})
			if mode == "success" || mode == "pending" || mode == "transient" || mode == "retention-mutates" {
				require.NoError(t, err)
				require.Equal(t, uint64(33), result.Leader)
				require.Equal(t, b.ClusterID, result.Header.ClusterId)
				require.Equal(t, b.OldTerm+1, result.Header.RaftTerm)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
			require.Equal(t, conn.calls, retained)
			if mode == "pending" || mode == "transient" {
				require.Equal(t, 2, conn.calls)
			} else if mode == "extended-budget" {
				require.Zero(t, conn.calls)
			} else {
				require.Equal(t, 1, conn.calls)
			}
		})
	}
}

func TestObserveSuccessorKubeBrainRPC(t *testing.T) {
	conn := recoveryGRPCFixtureAtTerm(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	initial, err := pb.NewMaintenanceClient(conn).Status(ctx, &pb.StatusRequest{})
	require.NoError(t, err)
	old := initial.Leader + 1
	if old == 0 {
		old = 1
	}
	retained := 0
	result, err := ObserveSuccessor(ctx, conn, SuccessorBinding{ClusterID: initial.Header.ClusterId, ObserverMemberID: initial.Header.MemberId, OldLeaderID: old, OldTerm: 2, Origin: time.Now()}, func(context.Context) error { return nil }, func(context.Context, SuccessorSample) error { retained++; return nil })
	require.NoError(t, err)
	require.Equal(t, initial.Leader, result.Leader)
	require.Equal(t, uint64(3), result.Header.RaftTerm)
	require.Equal(t, 1, retained)
}
