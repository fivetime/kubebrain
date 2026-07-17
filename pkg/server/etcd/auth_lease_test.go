package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
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

func TestAuthLeaseKeepAliveRequiresWritePermissionOnEveryRequest(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	plain := context.Background()

	deniedLease, err := server.LeaseGrant(plain, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.Put(plain, &etcdserverpb.PutRequest{
		Key: []byte("/denied/leased"), Value: []byte("secret"), Lease: deniedLease.ID,
	})
	require.NoError(t, err)
	allowedLease, err := server.LeaseGrant(plain, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.Put(plain, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/leased"), Value: []byte("value"), Lease: allowedLease.ID,
	})
	require.NoError(t, err)
	aliceCtx := setupAuthKVUser(t, server)

	denied := &fakeLeaseKeepAliveServer{
		ctx: aliceCtx, requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: deniedLease.ID}},
	}
	require.ErrorIs(t, server.LeaseKeepAlive(denied), rpctypes.ErrPermissionDenied)
	require.Empty(t, denied.sent)

	var revokeErr error
	allowed := &fakeLeaseKeepAliveServer{
		ctx: aliceCtx,
		requests: []*etcdserverpb.LeaseKeepAliveRequest{
			{ID: allowedLease.ID}, {ID: allowedLease.ID},
		},
		onSend: func() {
			revokeErr = server.auth.roleRevokePermission(plain, "allowed", []byte("/allowed/"), []byte("/allowed0"))
		},
	}
	require.ErrorIs(t, server.LeaseKeepAlive(allowed), rpctypes.ErrPermissionDenied)
	require.NoError(t, revokeErr)
	require.Len(t, allowed.sent, 1, "the second request on the same stream must observe revoked permission")

	// Restore the permission to prove the rejection was authorization, not a
	// damaged lease or stream fixture.
	require.NoError(t, server.auth.roleGrantPermission(plain, "allowed", &authpb.Permission{
		PermType: authpb.READWRITE, Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"),
	}))
}

func TestAuthLeaseKeyCheckRejectsConcurrentAuthRevisionChange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := setupAuthKVUser(t, server)

	caller, err := server.authCallerFromContext(ctx)
	require.NoError(t, err)
	require.NoError(t, caller.require([]byte("/allowed/leased"), nil, authpb.READ))
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(
		context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken),
	)
	rootCaller, err := server.authCallerFromContext(rootCtx)
	require.NoError(t, err)

	require.NoError(t, server.auth.roleRevokePermission(
		context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0"),
	))
	// The request-local snapshot still permits the key. The revision fence must
	// reject it rather than exposing attached keys or renewing a lease after the
	// concurrent RBAC mutation.
	require.NoError(t, caller.require([]byte("/allowed/leased"), nil, authpb.READ))
	require.ErrorIs(t,
		server.authorizeLeaseKeys(ctx, caller, []string{"/allowed/leased"}, authpb.READ),
		rpctypes.ErrAuthOldRevision,
	)
	require.ErrorIs(t,
		server.authorizeLeaseKeys(ctx, caller, []string{"/allowed/leased"}, authpb.WRITE),
		rpctypes.ErrAuthOldRevision,
	)
	require.NoError(t,
		server.authorizeLeaseKeys(rootCtx, rootCaller, []string{"/allowed/leased"}, authpb.WRITE),
		"etcd exempts admin callers from the lease auth revision fence",
	)
}

func TestAuthLeaseRevokeChecksKeysAfterAdmittedPut(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(
		context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken),
	)
	lease, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)

	shim := &blockLeasedPutShim{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	key := []byte("/denied/concurrent-leased")
	putDone := make(chan error, 1)
	go func() {
		_, err := server.Put(rootCtx, &etcdserverpb.PutRequest{Key: key, Value: []byte("secret"), Lease: lease.ID})
		putDone <- err
	}()
	<-shim.entered

	revokeDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseRevoke(aliceCtx, &etcdserverpb.LeaseRevokeRequest{ID: lease.ID})
		revokeDone <- err
	}()
	var (
		revokeErr      error
		completedEarly bool
	)
	select {
	case revokeErr = <-revokeDone:
		completedEarly = true
	case <-time.After(50 * time.Millisecond):
	}

	close(shim.release)
	require.NoError(t, <-putDone)
	if !completedEarly {
		revokeErr = <-revokeDone
	}
	require.False(t, completedEarly, "revoke bypassed admitted leased Put")
	require.ErrorIs(t, revokeErr, rpctypes.ErrPermissionDenied)
	stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1, "denied revoke must leave the newly protected key intact")
}
