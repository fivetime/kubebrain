package leasefault

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
)

// RestoreProtocol reconciles ONLY the recorded CORRUPT alarm and fixture lease.
// It never deletes a key directly or restores network policies/Pod labels.
// The external recovery owner must hold exclusive lifecycle ownership and have
// joined all fault/metrics processes and restored network access beforehand.
// admit must independently verify that ownership and live namespace/STS/member
// identity; it is called before reads and again before each write. It must obey
// ctx and remain valid through the request (a callback is not a distributed lock).
// conn must be an authenticated healthy-member connection owned by recovery.
//
// All writes are preceded by the durable intent check. Ambiguous RPC failures
// are returned, never automatically retried or converted to success. A partial
// recovery error requires reconciliation with the same intent, not a new test.
func RestoreProtocol(ctx context.Context, dir string, expected ProtocolRecovery, conn grpc.ClientConnInterface, admit func(context.Context) error) error {
	if ctx == nil || conn == nil || admit == nil {
		return errors.New("protocol restore requires context, connection and admission")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("protocol restore requires bounded recovery deadline")
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		if _, err := LoadProtocolRecovery(dir, expected); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return fmt.Errorf("protocol recovery admission: %w", err)
		}
		return ctx.Err()
	}
	if err := check(); err != nil {
		return err
	}
	validHeader := func(h *pb.ResponseHeader) bool {
		return h != nil && h.ClusterId == expected.ClusterID && h.MemberId != 0 && h.RaftTerm != 0 && h.Revision >= 0
	}
	maintenance, lease, kv := pb.NewMaintenanceClient(conn), pb.NewLeaseClient(conn), pb.NewKVClient(conn)
	alarms, err := maintenance.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET, Alarm: pb.AlarmType_NONE})
	if err != nil {
		return err
	}
	if alarms == nil || !validHeader(alarms.Header) {
		return errors.New("invalid recovery alarm identity")
	}
	for _, alarm := range alarms.Alarms {
		if alarm == nil || alarm.MemberID != expected.AlarmMemberID || alarm.Alarm != pb.AlarmType_CORRUPT {
			return errors.New("unrelated alarm prevents automatic protocol restore")
		}
	}
	ttl, err := lease.LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: expected.LeaseID, Keys: true})
	if err != nil {
		return err
	}
	if ttl == nil || !validHeader(ttl.Header) || ttl.ID != expected.LeaseID || ttl.GrantedTTL < 0 {
		return errors.New("invalid recovery lease identity")
	}
	for _, key := range ttl.Keys {
		if !bytes.Equal(key, []byte(expected.Key)) {
			return errors.New("unrelated key prevents lease revocation")
		}
	}
	key, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte(expected.Key)})
	if err != nil {
		return err
	}
	if key == nil || !validHeader(key.Header) || key.More || len(key.Kvs) > 1 || key.Count != int64(len(key.Kvs)) {
		return errors.New("invalid recovery key response")
	}
	for _, item := range key.Kvs {
		if item == nil || !bytes.Equal(item.Key, []byte(expected.Key)) || item.Lease != expected.LeaseID {
			return errors.New("fixture key is not owned by the recorded lease")
		}
	}
	// A revoked lease cannot still own the fixture key. Do not repair this by
	// deleting the key: that could erase a new writer's value.
	if ttl.GrantedTTL == 0 && (ttl.TTL != -1 || len(ttl.Keys) != 0 || len(key.Kvs) != 0) {
		return errors.New("inconsistent revoked lease state")
	}
	if len(alarms.Alarms) != 0 {
		if err := check(); err != nil {
			return err
		}
		response, err := maintenance.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, MemberID: expected.AlarmMemberID, Alarm: pb.AlarmType_CORRUPT})
		if err != nil {
			return fmt.Errorf("deactivate fixture alarm: %w", err)
		}
		if response == nil || !validHeader(response.Header) {
			return errors.New("invalid alarm deactivation response")
		}
	}
	if ttl.GrantedTTL > 0 {
		if err := check(); err != nil {
			return err
		}
		response, err := lease.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: expected.LeaseID})
		if err != nil && rpctypes.Error(err) != rpctypes.ErrLeaseNotFound {
			return fmt.Errorf("revoke fixture lease: %w", err)
		}
		// Alarm removal may let the normal expiry worker revoke first. Only
		// the exact not-found error proceeds to independent final reads.
		if err == nil && (response == nil || !validHeader(response.Header)) {
			return errors.New("invalid lease revocation response")
		}
	}
	if err := check(); err != nil {
		return err
	}
	return VerifyProtocolRecovery(ctx, expected, conn)
}
