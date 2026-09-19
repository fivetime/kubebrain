package leasefault

import (
	"context"
	"errors"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type SuccessorBinding struct {
	ClusterID, ObserverMemberID, OldLeaderID, OldTerm uint64
	Origin                                            time.Time
}

type SuccessorSample struct {
	Started, Completed time.Time
	Status             *pb.StatusResponse
	Error              error
}

// ObserveSuccessor polls independent healthy-member Status reads, not the
// original renewal stream. conn must already be authenticated/admitted with
// transport retries disabled. Read samples are retained before deciding success;
// all admission, requests and retention share the original <=30s deadline.
// Each read has at most 5s, with 200ms between pending/transient observations.
// No fault mutation, readiness inference, response retry or recovery occurs.
func ObserveSuccessor(ctx context.Context, conn grpc.ClientConnInterface, b SuccessorBinding, admit func(context.Context) error, retain func(context.Context, SuccessorSample) error) (*pb.StatusResponse, error) {
	if ctx == nil || conn == nil || admit == nil || retain == nil || b.ClusterID == 0 || b.ObserverMemberID == 0 || b.OldLeaderID == 0 || b.OldTerm == 0 || b.ObserverMemberID == b.OldLeaderID || b.Origin.IsZero() || b.Origin.After(time.Now()) {
		return nil, errors.New("invalid independent successor observation")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(b.Origin.Add(30*time.Second)) {
		return nil, errors.New("successor observation needs original fault budget")
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
	for {
		if err := check(); err != nil {
			return nil, err
		}
		started := time.Now()
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		requestDeadline, _ := requestCtx.Deadline()
		response, rpcErr := pb.NewMaintenanceClient(conn).Status(requestCtx, &pb.StatusRequest{})
		completed := time.Now()
		if !completed.Before(requestDeadline) {
			rpcErr = errors.Join(rpcErr, context.DeadlineExceeded)
		}
		cancel()
		var retained *pb.StatusResponse
		if response != nil {
			retained = proto.Clone(response).(*pb.StatusResponse)
		}
		if err := retain(ctx, SuccessorSample{Started: started, Completed: completed, Status: retained, Error: rpcErr}); err != nil {
			return nil, errors.Join(err, rpcErr, ctx.Err())
		}
		if err := check(); err != nil {
			return nil, errors.Join(err, rpcErr)
		}
		if rpcErr == nil {
			if response == nil || response.Header == nil || response.Header.ClusterId != b.ClusterID || response.Header.MemberId != b.ObserverMemberID || response.Header.RaftTerm == 0 || response.Header.Revision < 0 {
				return nil, errors.New("successor status identity mismatch")
			}
			if response.Leader != 0 && response.Leader != b.OldLeaderID && response.Header.RaftTerm > b.OldTerm {
				return proto.Clone(response).(*pb.StatusResponse), nil
			}
		} else if status.Code(rpcErr) != codes.Unavailable && status.Code(rpcErr) != codes.DeadlineExceeded && !errors.Is(rpcErr, context.DeadlineExceeded) {
			return nil, rpcErr
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
