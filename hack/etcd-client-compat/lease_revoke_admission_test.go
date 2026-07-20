package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLeaseRevokeUsesReservedAdmissionDuringClientOverload(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_PRIORITY_REVOKE_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_PRIORITY_REVOKE_ENDPOINT to a dedicated replica with max-requests-inflight=1")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	key := testPrefix(t) + "/priority-revoke"
	_, err = cli.Put(ctx, key, "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	conn := cli.ActiveConnection()
	watchCtx, watchCancel := context.WithCancel(ctx)
	watch, err := etcdserverpb.NewWatchClient(conn).Watch(watchCtx)
	require.NoError(t, err)
	require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte(key)},
		},
	}))
	created, err := watch.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)

	rangeCtx, rangeCancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = etcdserverpb.NewKVClient(conn).Range(rangeCtx, &etcdserverpb.RangeRequest{Key: []byte(key)})
	rangeCancel()
	require.Equal(t, codes.ResourceExhausted, status.Code(err),
		"the watch must occupy the only ordinary inflight slot")

	revokeCtx, revokeCancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = etcdserverpb.NewLeaseClient(conn).LeaseRevoke(
		revokeCtx,
		&etcdserverpb.LeaseRevokeRequest{ID: int64(lease.ID)},
	)
	revokeCancel()
	require.NoError(t, err, "LeaseRevoke must use the bounded priority reserve")

	watchCancel()
	require.Eventually(t, func() bool {
		getCtx, getCancel := context.WithTimeout(context.Background(), time.Second)
		defer getCancel()
		response, getErr := cli.Get(getCtx, key)
		return getErr == nil && len(response.Kvs) == 0
	}, 5*time.Second, 50*time.Millisecond)
}
