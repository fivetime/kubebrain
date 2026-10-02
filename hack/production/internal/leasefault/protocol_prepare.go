package leasefault

import (
	"context"
	"errors"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// PrepareProtocol creates the original 10-second lease fixture and CORRUPT alarm.
// conn must authenticate the admitted cluster; admit must enforce live identities
// and exclusive ownership of this dedicated test instance, including allocation
// of the unused lease ID. It must also verify the inactive network reservation.
// No write is retried. Any error after arming requires post-join reconciliation.
// This does not wait for lease expiry, start a probe, or begin the fault clock.
func PrepareProtocol(ctx context.Context, dir string, plan ProtocolRecovery, conn grpc.ClientConnInterface, admit func(context.Context) error) error {
	return prepareProtocol(ctx, dir, plan, conn, admit, admit)
}

// The lifecycle supplies complete dataplane admission at preparation boundaries
// and fresh ownership/API reservation admission before each protocol mutation.
// No network mutation occurs within this fixture sequence. Re-running remote
// collectors between Grant and Txn would consume the unchanged ten-second TTL.
func prepareProtocol(ctx context.Context, dir string, plan ProtocolRecovery, conn grpc.ClientConnInterface, admit, mutationAdmit func(context.Context) error) error {
	if ctx == nil || conn == nil || admit == nil || mutationAdmit == nil || !plan.valid() {
		return errors.New("invalid protocol preparation")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("preparation requires bounded deadline")
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err := check(); err != nil {
		return err
	}
	// Preserve the original isolated-fixture prerequisite, not merely absence
	// of the selected lease ID. Existing alarms/keys/leases prevent preparation.
	if err := VerifyProtocolRecovery(ctx, plan, conn); err != nil {
		return err
	}
	validHeader := func(h *pb.ResponseHeader) bool {
		return h != nil && h.ClusterId == plan.ClusterID && h.MemberId != 0 && h.RaftTerm != 0 && h.Revision >= 0
	}
	lease, kv, maintenance := pb.NewLeaseClient(conn), pb.NewKVClient(conn), pb.NewMaintenanceClient(conn)
	leases, err := lease.LeaseLeases(ctx, &pb.LeaseLeasesRequest{})
	if err != nil {
		return err
	}
	if leases == nil || !validHeader(leases.Header) || len(leases.Leases) != 0 {
		return errors.New("preparation requires an empty lease set")
	}
	if err := check(); err != nil {
		return err
	}
	if err := ArmProtocolRecovery(dir, plan); err != nil {
		return err
	}
	beforeWrite := func() error {
		if _, err := LoadProtocolRecovery(dir, plan); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.Join(mutationAdmit(ctx), ctx.Err())
	}
	if err := beforeWrite(); err != nil {
		return err
	}
	grant, err := lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{ID: plan.LeaseID, TTL: 10})
	if err != nil {
		return err
	}
	if grant == nil || !validHeader(grant.Header) || grant.ID != plan.LeaseID || grant.TTL != 10 || grant.Error != "" {
		return errors.New("invalid fixture lease grant")
	}
	if err := beforeWrite(); err != nil {
		return err
	}
	txn, err := kv.Txn(ctx, &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte(plan.Key), Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: 0}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(plan.Key), Value: []byte("fixture"), Lease: plan.LeaseID}}}},
	})
	if err != nil {
		return err
	}
	if txn == nil || !validHeader(txn.Header) || txn.Header.Revision <= 0 || !txn.Succeeded || len(txn.Responses) != 1 || txn.Responses[0] == nil || txn.Responses[0].GetResponsePut() == nil {
		return errors.New("fixture key creation was not confirmed")
	}
	// etcd transaction operation headers carry the revision, not the outer
	// cluster/member/term identity. Bind identity above and revision here.
	put := txn.Responses[0].GetResponsePut()
	if put.Header == nil || put.Header.Revision != txn.Header.Revision || put.PrevKv != nil {
		return errors.New("invalid fixture transaction operation response")
	}
	if err := beforeWrite(); err != nil {
		return err
	}
	alarm, err := maintenance.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, MemberID: plan.AlarmMemberID, Alarm: pb.AlarmType_CORRUPT})
	if err != nil {
		return err
	}
	if alarm == nil || !validHeader(alarm.Header) || len(alarm.Alarms) != 1 || alarm.Alarms[0] == nil || alarm.Alarms[0].MemberID != plan.AlarmMemberID || alarm.Alarms[0].Alarm != pb.AlarmType_CORRUPT {
		return errors.New("fixture alarm activation was not confirmed")
	}
	// The admission before Alarm may outlast the fixed ten-second TTL. A
	// successful Alarm response cannot resurrect an already expired fixture.
	// Read only: never renew, recreate or retry the lease to hide this race.
	ttl, err := lease.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: plan.LeaseID, Keys: true})
	if err != nil {
		return err
	}
	if ttl == nil || !validHeader(ttl.Header) || ttl.ID != plan.LeaseID || ttl.GrantedTTL != 10 || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != plan.Key {
		return errors.New("fixture lease was not retained after alarm activation")
	}
	return ctx.Err()
}
