package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type watchEmptyKeyAuthOutcome struct {
	PointCreated     bool
	FromKeyDenied    bool
	FromKeyReason    string
	HeadersCanonical bool
}

// This test is destructive: both endpoints must be disposable, empty, and
// auth-disabled. It fixes the upstream admission ordering independently from
// the auth-disabled data-selection differential.
func TestWatchEmptyKeyAuthDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_EMPTY_KEY_AUTH_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_EMPTY_KEY_AUTH_ENDPOINT to a disposable reference etcd")
	}
	want := watchEmptyKeyAuthOutcome{
		PointCreated: true, FromKeyDenied: true,
		FromKeyReason: rpctypes.ErrGRPCPermissionDenied.Error(), HeadersCanonical: true,
	}
	referenceOutcome := runWatchEmptyKeyAuthScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	kubebrain := os.Getenv("KUBEBRAIN_EMPTY_KEY_AUTH_ENDPOINT")
	if kubebrain == "" {
		t.Skip("set KUBEBRAIN_EMPTY_KEY_AUTH_ENDPOINT to a disposable KubeBrain instance")
	}
	require.Equal(t, referenceOutcome, runWatchEmptyKeyAuthScenario(t, kubebrain))
}

func runWatchEmptyKeyAuthScenario(t *testing.T, endpoint string) watchEmptyKeyAuthOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	bootstrap, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bootstrap.Close()) })

	_, err = bootstrap.RoleAdd(ctx, "root")
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "root", "root-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = bootstrap.RoleAdd(ctx, "nul-reader")
	require.NoError(t, err)
	_, err = bootstrap.RoleGrantPermission(ctx, "nul-reader", string([]byte{0}), "", clientv3.PermissionType(clientv3.PermRead))
	require.NoError(t, err)
	_, err = bootstrap.UserAdd(ctx, "alice", "alice-secret")
	require.NoError(t, err)
	_, err = bootstrap.UserGrantRole(ctx, "alice", "nul-reader")
	require.NoError(t, err)
	_, err = bootstrap.AuthEnable(ctx)
	require.NoError(t, err)

	root := authClient(t, endpoint, "root", "root-secret")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = root.AuthDisable(cleanupCtx)
	})
	alice := authClient(t, endpoint, "alice", "alice-secret")
	stream, err := etcdserverpb.NewWatchClient(alice.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseSend() })
	create := func(id int64, rangeEnd []byte) *etcdserverpb.WatchResponse {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{WatchId: id, RangeEnd: rangeEnd},
		}}))
		response, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		return response
	}
	point := create(901, nil)
	fromKey := create(902, []byte{0})
	return watchEmptyKeyAuthOutcome{
		PointCreated:  point.Created && !point.Canceled && point.WatchId == 901,
		FromKeyDenied: fromKey.Created && fromKey.Canceled && fromKey.WatchId == -1,
		FromKeyReason: fromKey.CancelReason,
		HeadersCanonical: point.GetHeader().GetClusterId() != 0 && point.GetHeader().GetMemberId() != 0 &&
			point.GetHeader().GetRaftTerm() > 0 && fromKey.GetHeader().GetClusterId() == point.GetHeader().GetClusterId() &&
			fromKey.GetHeader().GetMemberId() == point.GetHeader().GetMemberId() && fromKey.GetHeader().GetRaftTerm() > 0,
	}
}
