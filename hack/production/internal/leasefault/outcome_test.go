package leasefault

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type outcomeConnection struct {
	grpc.ClientConnInterface
	t     *testing.T
	plan  ProtocolRecovery
	lease *pb.LeaseTimeToLiveResponse
	key   *pb.RangeResponse
	calls int
	wait  bool
}

func (c *outcomeConnection) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	c.calls++
	if c.wait {
		<-ctx.Done()
		return ctx.Err()
	}
	switch method {
	case "/etcdserverpb.Lease/LeaseTimeToLive":
		require.Equal(c.t, &pb.LeaseTimeToLiveRequest{ID: c.plan.LeaseID, Keys: true}, args)
		proto.Merge(reply.(*pb.LeaseTimeToLiveResponse), c.lease)
	case "/etcdserverpb.KV/Range":
		require.Equal(c.t, &pb.RangeRequest{Key: []byte(c.plan.Key)}, args)
		proto.Merge(reply.(*pb.RangeResponse), c.key)
	default:
		c.t.Fatalf("unexpected outcome RPC %s", method)
	}
	return nil
}

func TestVerifyOriginalOutcome(t *testing.T) {
	for _, mode := range []string{"zero", "positive", "positive-expired-retained", "zero-retained", "positive-missing", "wrong-cluster", "stale-term", "wrong-lease", "extra-keys", "wrong-value", "wrong-key-lease", "wrong-count", "more", "extended-budget", "no-deadline", "expired-budget", "rpc-timeout", "lost-final-admission", "invalid-log", "future-response"} {
		t.Run(mode, func(t *testing.T) {
			b, events := fixture()
			b.Origin = time.Now().Add(-time.Second)
			events[0]["at"] = b.Origin.Add(-2 * time.Second)
			events[1]["at"] = b.Origin.Add(-time.Second)
			events[2]["at"] = b.Origin.Add(500 * time.Millisecond)
			plan := recoveryPlan()
			plan.ClusterID = b.ClusterID
			plan.AlarmMemberID = b.InitialMemberID
			plan.LeaseID = b.LeaseID
			header := func() *pb.ResponseHeader {
				return &pb.ResponseHeader{ClusterId: b.ClusterID, MemberId: b.InitialMemberID + 1, RaftTerm: b.SuccessorTerm, Revision: 5}
			}
			c := &outcomeConnection{t: t, plan: plan, lease: &pb.LeaseTimeToLiveResponse{Header: header(), ID: plan.LeaseID, TTL: -1}, key: &pb.RangeResponse{Header: header()}}
			positive := mode != "zero" && mode != "zero-retained"
			if positive {
				events[2]["ttl"] = int64(10)
				c.lease.TTL = 9
				c.lease.GrantedTTL = 10
				c.lease.Keys = [][]byte{[]byte(plan.Key)}
				c.key.Count = 1
				c.key.Kvs = []*mvccpb.KeyValue{{Key: []byte(plan.Key), Value: []byte("fixture"), Lease: plan.LeaseID}}
			}
			deadline := time.Now().Add(5 * time.Second)
			switch mode {
			case "positive-expired-retained":
				c.lease.TTL = -2
			case "zero-retained":
				c.lease.GrantedTTL = 10
			case "positive-missing":
				c.lease.GrantedTTL = 0
			case "wrong-cluster":
				c.lease.Header.ClusterId--
			case "stale-term":
				c.lease.Header.RaftTerm = b.InitialTerm
			case "wrong-lease":
				c.lease.ID++
			case "extra-keys":
				c.lease.Keys = append(c.lease.Keys, []byte("/foreign"))
			case "wrong-value":
				c.key.Kvs[0].Value = []byte("changed")
			case "wrong-key-lease":
				c.key.Kvs[0].Lease++
			case "wrong-count":
				c.key.Count = 2
			case "more":
				c.key.More = true
			case "extended-budget":
				deadline = b.Origin.Add(30*time.Second + time.Nanosecond)
			case "expired-budget":
				deadline = time.Now().Add(-time.Second)
			case "rpc-timeout":
				deadline = time.Now().Add(30 * time.Millisecond)
				c.wait = true
			case "invalid-log":
				events[1]["lease_id"] = int64(1)
			case "future-response":
				events[2]["at"] = time.Now().Add(time.Second)
			}
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			if mode == "no-deadline" {
				ctx = context.Background()
			}
			admissions := 0
			out, err := VerifyOriginalOutcome(ctx, c, plan, b, encode(t, events), func(context.Context) error {
				admissions++
				if mode == "lost-final-admission" && admissions == 3 {
					return errors.New("isolation no longer confirmed")
				}
				return nil
			})
			if mode == "zero" || mode == "positive" || mode == "positive-expired-retained" {
				require.NoError(t, err)
				require.Equal(t, 2, c.calls)
				require.True(t, proto.Equal(c.lease, out.Lease))
				require.True(t, proto.Equal(c.key, out.Key))
			} else {
				require.Error(t, err)
			}
			if mode == "extended-budget" || mode == "no-deadline" || mode == "expired-budget" || mode == "invalid-log" || mode == "future-response" {
				require.Zero(t, c.calls)
			}
			if mode == "rpc-timeout" || mode == "expired-budget" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if mode == "lost-final-admission" {
				require.ErrorContains(t, err, "isolation no longer confirmed")
				require.NotNil(t, out.Key)
			}
		})
	}
}
