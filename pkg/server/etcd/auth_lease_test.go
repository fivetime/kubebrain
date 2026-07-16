package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func TestAuthLeaseRequiresCallerAndProtectsBoundKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	plain := context.Background()

	lease, err := server.LeaseGrant(plain, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.Put(plain, &etcdserverpb.PutRequest{Key: []byte("/denied/leased"), Value: []byte("secret"), Lease: lease.ID})
	require.NoError(t, err)
	ctx := setupAuthKVUser(t, server)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: lease.ID})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID})
	require.NoError(t, err, "TTL without attached keys does not reveal protected key names")
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID, Keys: true})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	stored, err := server.backend.Get(plain, &etcdserverpb.RangeRequest{Key: []byte("/denied/leased")})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1, "denied lease revoke must not delete protected keys")

	_, err = server.LeaseGrant(plain, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	allowed, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: allowed.ID})
	require.NoError(t, err)
	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied, "one inaccessible attached key denies the all-leases listing like etcd")
}
