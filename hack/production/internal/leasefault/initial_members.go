package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// VerifyInitialMembers is a preclaim/preparation read-only gate, not a callback
// for use after isolation or election. Connections must already be admitted,
// direct mTLS endpoints with transport retries disabled; admit must bind them to
// the original/observer Pod processes and independently approved source/image.
// Both Status reads must identify the expected members, initial leader and term.
// Sequential reads do not lock the term; original probe admission still rechecks
// it before activation. No retries, lease changes, ownership or new fault clock.
func (p ObservationCommandPlan) VerifyInitialMembers(ctx context.Context, original, observer grpc.ClientConnInterface, admit func(context.Context) error) error {
	if err := ownerContext(ctx); err != nil {
		return err
	}
	if original == nil || observer == nil || admit == nil {
		return errors.New("initial member admission requires both endpoints")
	}
	if err := p.Bindings.Validate(); err != nil {
		return err
	}
	root, err := recoveryRoot(p.OwnerDirectory)
	if err != nil {
		return err
	}
	identity, statErr := root.Stat(".")
	if err := errors.Join(statErr, root.Close()); err != nil {
		return err
	}
	checkDirectory := func() error {
		current, err := os.Lstat(p.OwnerDirectory)
		if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
			return errors.New("initial member evidence directory changed")
		}
		return nil
	}
	b := p.Bindings
	for _, target := range []struct {
		role   string
		member uint64
		conn   grpc.ClientConnInterface
	}{
		{"original", b.Protocol.AlarmMemberID, original}, {"observer", b.ObserverMemberID, observer},
	} {
		if err := errors.Join(ctx.Err(), checkDirectory()); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return err
		}
		if err := errors.Join(ctx.Err(), checkDirectory()); err != nil {
			return err
		}
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		deadline, _ := requestCtx.Deadline()
		started := time.Now()
		response, observed := pb.NewMaintenanceClient(target.conn).Status(requestCtx, &pb.StatusRequest{})
		completed := time.Now()
		observed = errors.Join(observed, requestCtx.Err())
		if !completed.Before(deadline) {
			observed = errors.Join(observed, context.DeadlineExceeded)
		}
		cancel()
		if observed == nil && (response == nil || response.Header == nil || response.Header.ClusterId != b.Protocol.ClusterID || response.Header.MemberId != target.member || response.Header.RaftTerm != b.InitialTerm || response.Leader != b.Protocol.AlarmMemberID || response.Header.Revision < 0 || len(response.Errors) != 0) {
			observed = errors.New("initial Status member, leader, term or health mismatch")
		}
		data, marshalErr := json.Marshal(struct {
			Owner              string `json:"owner"`
			ExpectedMember     uint64 `json:"expected_member,string"`
			ExpectedCluster    uint64 `json:"expected_cluster,string"`
			ExpectedLeader     uint64 `json:"expected_leader,string"`
			ExpectedTerm       uint64 `json:"expected_term,string"`
			Started, Completed time.Time
			Status             *pb.StatusResponse
		}{b.Network.Owner, target.member, b.Protocol.ClusterID, b.Protocol.AlarmMemberID, b.InitialTerm, started, completed, response})
		if marshalErr != nil {
			return errors.Join(observed, marshalErr)
		}
		if err := checkDirectory(); err != nil {
			return errors.Join(observed, err)
		}
		retained := retainObserver(p.OwnerDirectory, "experiment", "initial-"+target.role, data, observed)
		if err := errors.Join(observed, retained, ctx.Err()); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return err
		}
	}
	return errors.Join(ctx.Err(), checkDirectory())
}
