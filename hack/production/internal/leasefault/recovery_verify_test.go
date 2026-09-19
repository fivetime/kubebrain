package leasefault

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type recoveryWireServer struct {
	pb.UnimplementedMaintenanceServer
	pb.UnimplementedLeaseServer
	pb.UnimplementedKVServer
	plan    ProtocolRecovery
	granted int64
}

func (s *recoveryWireServer) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{ClusterId: s.plan.ClusterID, MemberId: s.plan.AlarmMemberID, RaftTerm: 9, Revision: 4}
}

func (s *recoveryWireServer) Alarm(_ context.Context, request *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	if request.Action != pb.AlarmRequest_GET || request.Alarm != pb.AlarmType_NONE || request.MemberID != 0 {
		return nil, errors.New("unexpected alarm request")
	}
	return &pb.AlarmResponse{Header: s.header()}, nil
}

func (s *recoveryWireServer) LeaseTimeToLive(_ context.Context, request *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	if request.ID != s.plan.LeaseID || !request.Keys {
		return nil, errors.New("unexpected lease request")
	}
	return &pb.LeaseTimeToLiveResponse{Header: s.header(), ID: s.plan.LeaseID, TTL: -1, GrantedTTL: s.granted}, nil
}

func (s *recoveryWireServer) Range(_ context.Context, request *pb.RangeRequest) (*pb.RangeResponse, error) {
	if string(request.Key) != s.plan.Key || request.Serializable || len(request.RangeEnd) != 0 {
		return nil, errors.New("unexpected key request")
	}
	return &pb.RangeResponse{Header: s.header()}, nil
}

func TestVerifyProtocolRecoveryGRPCWire(t *testing.T) {
	for _, granted := range []int64{0, 10} {
		t.Run(fmt.Sprint(granted), func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			fixture := &recoveryWireServer{plan: recoveryPlan(), granted: granted}
			pb.RegisterMaintenanceServer(server, fixture)
			pb.RegisterLeaseServer(server, fixture)
			pb.RegisterKVServer(server, fixture)
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(listener) }()
			defer func() { server.Stop(); _ = listener.Close(); <-done }()
			// Insecure credentials are confined to this in-memory test listener.
			conn, err := grpc.NewClient("passthrough:///recovery-fixture", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err = VerifyProtocolRecovery(ctx, fixture.plan, conn)
			if granted == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

type recoveryConnection struct {
	t     *testing.T
	plan  ProtocolRecovery
	calls int
	mode  string
}

func (c *recoveryConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	c.t.Fatal("recovery verifier must not open a stream")
	return nil, errors.New("unexpected stream")
}

func (c *recoveryConnection) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	c.calls++
	require.NoError(c.t, ctx.Err())
	header := &pb.ResponseHeader{ClusterId: c.plan.ClusterID, MemberId: c.plan.AlarmMemberID, Revision: 4, RaftTerm: 9}
	if c.mode == "wrong-cluster" {
		header.ClusterId--
	}
	if c.mode == "missing-header" {
		header = nil
	}
	if c.mode == "rpc-error" {
		return errors.New("synthetic RPC failure")
	}
	if c.mode == "deadline" {
		<-ctx.Done()
		return ctx.Err()
	}
	switch c.calls {
	case 1:
		require.Equal(c.t, "/etcdserverpb.Maintenance/Alarm", method)
		require.Equal(c.t, &pb.AlarmRequest{Action: pb.AlarmRequest_GET, Alarm: pb.AlarmType_NONE}, args)
		r := reply.(*pb.AlarmResponse)
		r.Header = header
		if c.mode == "alarm-remains" {
			r.Alarms = []*pb.AlarmMember{{MemberID: c.plan.AlarmMemberID, Alarm: pb.AlarmType_CORRUPT}}
		}
	case 2:
		require.Equal(c.t, "/etcdserverpb.Lease/LeaseTimeToLive", method)
		require.Equal(c.t, &pb.LeaseTimeToLiveRequest{ID: c.plan.LeaseID, Keys: true}, args)
		r := reply.(*pb.LeaseTimeToLiveResponse)
		r.Header, r.ID, r.TTL = header, c.plan.LeaseID, -1
		if c.mode == "expired-not-revoked" {
			r.GrantedTTL = 10
		}
		if c.mode == "wrong-lease" {
			r.ID++
		}
		if c.mode == "lease-key-remains" {
			r.Keys = [][]byte{[]byte(c.plan.Key)}
		}
		if c.mode == "live-lease" {
			r.TTL = 3
		}
		if c.mode == "ttl-cluster" {
			r.Header.ClusterId--
		}
	case 3:
		require.Equal(c.t, "/etcdserverpb.KV/Range", method)
		// Default Range is linearizable, exact-key, not a prefix/count-only query.
		require.Equal(c.t, &pb.RangeRequest{Key: []byte(c.plan.Key)}, args)
		r := reply.(*pb.RangeResponse)
		r.Header = header
		if c.mode == "key-remains" {
			r.Kvs = []*mvccpb.KeyValue{{Key: []byte(c.plan.Key)}}
		}
		if c.mode == "count-remains" {
			r.Count = 1
		}
		if c.mode == "more" {
			r.More = true
		}
		if c.mode == "key-cluster" {
			r.Header.ClusterId--
		}
	default:
		c.t.Fatal("unexpected retry or extra RPC")
	}
	return nil
}

func TestVerifyProtocolRecoveryReadOnlyCalls(t *testing.T) {
	for _, mode := range []string{"success", "wrong-cluster", "missing-header", "rpc-error", "deadline", "alarm-remains", "expired-not-revoked", "wrong-lease", "lease-key-remains", "live-lease", "ttl-cluster", "key-remains", "count-remains", "more", "key-cluster"} {
		t.Run(mode, func(t *testing.T) {
			plan := recoveryPlan()
			conn := &recoveryConnection{t: t, plan: plan, mode: mode}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
			}
			defer cancel()
			err := VerifyProtocolRecovery(ctx, plan, conn)
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 3, conn.calls)
			} else {
				require.Error(t, err)
			}
			if mode == "deadline" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
		})
	}
}

func TestVerifyProtocolRecoveryRequiresBudget(t *testing.T) {
	plan := recoveryPlan()
	conn := &recoveryConnection{t: t, plan: plan}
	require.Error(t, VerifyProtocolRecovery(context.Background(), plan, conn))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	require.Error(t, VerifyProtocolRecovery(ctx, plan, conn))
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	cancel()
	require.ErrorIs(t, VerifyProtocolRecovery(ctx, plan, conn), context.Canceled)
	require.Zero(t, conn.calls)
}
