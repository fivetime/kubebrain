package leasefault

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// VerifyProtocolRecovery performs only Alarm(GET), LeaseTimeToLive and an exact
// linearizable Range on an independently authenticated healthy-member connection.
// The caller must first join fault/metrics children, verify network restoration,
// validate live namespace/STS/member identity and finish its recovery mutations.
// A successful return proves only these three protocol observations, not network
// recovery, identity admission, absence of future writes, or fault acceptance.
// No dialing, retry, mutation or acceptance-deadline extension occurs here.
func VerifyProtocolRecovery(ctx context.Context, plan ProtocolRecovery, conn grpc.ClientConnInterface) error {
	if ctx == nil || conn == nil || !plan.valid() {
		return errors.New("protocol recovery verification requires context, identity and connection")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("protocol recovery verification requires a bounded recovery deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	validHeader := func(h *pb.ResponseHeader) bool {
		return h != nil && h.ClusterId == plan.ClusterID && h.MemberId != 0 && h.RaftTerm != 0 && h.Revision >= 0
	}
	alarm, err := pb.NewMaintenanceClient(conn).Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET, Alarm: pb.AlarmType_NONE})
	if err != nil {
		return fmt.Errorf("verify recovery alarms: %w", err)
	}
	if alarm == nil || !validHeader(alarm.Header) || len(alarm.Alarms) != 0 {
		return errors.New("recovery alarms remain or response identity is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ttl, err := pb.NewLeaseClient(conn).LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: plan.LeaseID, Keys: true})
	if err != nil {
		return fmt.Errorf("verify recovery lease: %w", err)
	}
	if ttl == nil || !validHeader(ttl.Header) || ttl.ID != plan.LeaseID || ttl.TTL != -1 || ttl.GrantedTTL != 0 || len(ttl.Keys) != 0 {
		return errors.New("recovery lease remains or response identity is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := pb.NewKVClient(conn).Range(ctx, &pb.RangeRequest{Key: []byte(plan.Key)})
	if err != nil {
		return fmt.Errorf("verify recovery key: %w", err)
	}
	if key == nil || !validHeader(key.Header) || key.Count != 0 || key.More || len(key.Kvs) != 0 {
		return errors.New("recovery key remains or response identity is invalid")
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}
