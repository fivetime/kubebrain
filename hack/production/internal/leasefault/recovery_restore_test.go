package leasefault

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
)

type restoreConnection struct {
	recoveryConnection
	alarm, lease bool
	writes       []string
}

func (c *restoreConnection) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	c.calls++
	if err := ctx.Err(); err != nil {
		return err
	}
	h := &pb.ResponseHeader{ClusterId: c.plan.ClusterID, MemberId: c.plan.AlarmMemberID, RaftTerm: 9, Revision: 4}
	switch method {
	case "/etcdserverpb.Maintenance/Alarm":
		req := args.(*pb.AlarmRequest)
		if req.Action == pb.AlarmRequest_DEACTIVATE {
			require.Equal(c.t, c.plan.AlarmMemberID, req.MemberID)
			require.Equal(c.t, pb.AlarmType_CORRUPT, req.Alarm)
			c.writes = append(c.writes, "alarm")
			if c.mode == "alarm-error" {
				return errors.New("ambiguous alarm failure")
			}
			c.alarm = false
		} else {
			require.Equal(c.t, pb.AlarmRequest_GET, req.Action)
		}
		r := reply.(*pb.AlarmResponse)
		r.Header = h
		if c.alarm {
			r.Alarms = []*pb.AlarmMember{{MemberID: c.plan.AlarmMemberID, Alarm: pb.AlarmType_CORRUPT}}
		}
		if c.mode == "unrelated-alarm" {
			r.Alarms = append(r.Alarms, &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE})
		}
	case "/etcdserverpb.Lease/LeaseTimeToLive":
		require.Equal(c.t, &pb.LeaseTimeToLiveRequest{ID: c.plan.LeaseID, Keys: true}, args)
		r := reply.(*pb.LeaseTimeToLiveResponse)
		r.Header, r.ID, r.TTL = h, c.plan.LeaseID, -1
		if c.lease {
			r.TTL = -9 // Expired retained leases may continue below -1.
			r.GrantedTTL = 10
			r.Keys = [][]byte{[]byte(c.plan.Key)}
		}
		if c.mode == "unrelated-key" {
			r.Keys = append(r.Keys, []byte("/business/key"))
		}
	case "/etcdserverpb.KV/Range":
		require.Equal(c.t, &pb.RangeRequest{Key: []byte(c.plan.Key)}, args)
		r := reply.(*pb.RangeResponse)
		r.Header = h
		if c.lease {
			r.Count = 1
			r.Kvs = []*mvccpb.KeyValue{{Key: []byte(c.plan.Key), Lease: c.plan.LeaseID}}
			if c.mode == "key-new-owner" {
				r.Kvs[0].Lease++
			}
		}
	case "/etcdserverpb.Lease/LeaseRevoke":
		require.Equal(c.t, &pb.LeaseRevokeRequest{ID: c.plan.LeaseID}, args)
		c.writes = append(c.writes, "lease")
		if c.mode == "revoke-error" {
			return errors.New("ambiguous revoke failure")
		}
		c.lease = false
		if c.mode == "expired-concurrently" {
			return rpctypes.ErrGRPCLeaseNotFound
		}
		reply.(*pb.LeaseRevokeResponse).Header = h
	default:
		c.t.Fatalf("unexpected recovery RPC %s", method)
	}
	return nil
}

func TestRestoreProtocolBoundedOwnedWrites(t *testing.T) {
	for _, mode := range []string{"success", "expired-concurrently", "already-restored", "no-intent", "denied", "deny-before-write", "unrelated-alarm", "unrelated-key", "key-new-owner", "alarm-error", "revoke-error"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := recoveryPlan()
			if mode != "no-intent" {
				require.NoError(t, ArmProtocolRecovery(dir, plan))
			}
			conn := &restoreConnection{recoveryConnection: recoveryConnection{t: t, plan: plan, mode: mode}, alarm: mode != "already-restored", lease: mode != "already-restored"}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			admissions := 0
			err := RestoreProtocol(ctx, dir, plan, conn, func(context.Context) error {
				admissions++
				if mode == "denied" || (mode == "deny-before-write" && admissions == 2) {
					return errors.New("admission denied")
				}
				return nil
			})
			if mode == "success" || mode == "expired-concurrently" || mode == "already-restored" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			switch mode {
			case "success", "expired-concurrently", "revoke-error":
				require.Equal(t, []string{"alarm", "lease"}, conn.writes)
			case "alarm-error":
				require.Equal(t, []string{"alarm"}, conn.writes)
			default:
				require.Empty(t, conn.writes)
			}
			if mode == "success" {
				require.Equal(t, 8, conn.calls)
				require.Equal(t, 4, admissions)
			}
			if mode == "no-intent" || mode == "denied" {
				require.Zero(t, conn.calls)
			}
		})
	}
}
