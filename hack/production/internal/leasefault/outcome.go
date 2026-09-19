package leasefault

import (
	"bytes"
	"context"
	"errors"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// OriginalOutcome retains the observations, including partial/failing RPC
// responses. Callers must archive them and the error; this is not fault acceptance.
type OriginalOutcome struct {
	Response Response
	Lease    *pb.LeaseTimeToLiveResponse
	Key      *pb.RangeResponse
}

// VerifyOriginalOutcome validates the original log and its post-response lease/key
// gate inside the SAME original 30-second fault budget. A zero response TTL requires
// absent lease/key; a positive response TTL requires the retained owned fixture.
// Natural expiry after a positive response is permitted while grant/key persist,
// matching the original gate. No mutation, retry, recovery or new budget occurs.
// conn must be an independently authenticated healthy-member connection; admit
// must confirm live ownership/identities and ongoing isolation. The caller must
// independently join the original probe with exit 0, verify drops/successor/stack
// and finish ALL other gates in the same deadline. These reads alone do not do so.
func VerifyOriginalOutcome(ctx context.Context, conn grpc.ClientConnInterface, plan ProtocolRecovery, binding Binding, log []byte, admit func(context.Context) error) (OriginalOutcome, error) {
	var result OriginalOutcome
	if ctx == nil || conn == nil || admit == nil || !plan.valid() || plan.ClusterID != binding.ClusterID || plan.LeaseID != binding.LeaseID || plan.AlarmMemberID != binding.InitialMemberID {
		return result, errors.New("invalid original outcome admission")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(binding.Origin.Add(30*time.Second)) || binding.Origin.After(time.Now()) {
		return result, errors.New("outcome requires remaining original fault budget")
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		if err := admit(ctx); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !time.Now().Before(deadline) {
		return result, context.DeadlineExceeded
	}
	response, err := ValidateOriginalResponse(log, binding)
	if err != nil {
		return result, err
	}
	if response.At.After(time.Now()) {
		return result, errors.New("original response timestamp is in the future")
	}
	result.Response = response
	validHeader := func(h *pb.ResponseHeader) bool {
		return h != nil && h.ClusterId == plan.ClusterID && h.MemberId != 0 && h.RaftTerm >= binding.SuccessorTerm && h.Revision >= 0
	}
	if err := check(); err != nil {
		return result, err
	}
	result.Lease, err = pb.NewLeaseClient(conn).LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: plan.LeaseID, Keys: true})
	if err != nil {
		return result, err
	}
	ttl := result.Lease
	if ttl == nil || !validHeader(ttl.Header) || ttl.ID != plan.LeaseID {
		return result, errors.New("invalid post-response lease identity")
	}
	if response.TTL == 0 {
		if ttl.TTL != -1 || ttl.GrantedTTL != 0 || len(ttl.Keys) != 0 {
			return result, errors.New("zero response TTL left a retained lease")
		}
	} else {
		if ttl.GrantedTTL <= 0 || len(ttl.Keys) != 1 || !bytes.Equal(ttl.Keys[0], []byte(plan.Key)) {
			return result, errors.New("positive response TTL lost its owned lease fixture")
		}
	}
	if err := check(); err != nil {
		return result, err
	}
	result.Key, err = pb.NewKVClient(conn).Range(ctx, &pb.RangeRequest{Key: []byte(plan.Key)})
	if err != nil {
		return result, err
	}
	key := result.Key
	if key == nil || !validHeader(key.Header) || key.More {
		return result, errors.New("invalid post-response key observation")
	}
	if response.TTL == 0 {
		if key.Count != 0 || len(key.Kvs) != 0 {
			return result, errors.New("zero response TTL left a fixture key")
		}
	} else {
		if key.Count != 1 || len(key.Kvs) != 1 || key.Kvs[0] == nil || !bytes.Equal(key.Kvs[0].Key, []byte(plan.Key)) || !bytes.Equal(key.Kvs[0].Value, []byte("fixture")) || key.Kvs[0].Lease != plan.LeaseID {
			return result, errors.New("positive response TTL fixture key mismatch")
		}
	}
	return result, check()
}
