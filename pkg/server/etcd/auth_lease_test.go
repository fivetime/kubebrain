package etcd

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

type blockingAuthConfigReadShim struct {
	BackendShim
	blockAt      int32
	reads        atomic.Int32
	entered      chan struct{}
	release      chan struct{}
	putOnce      sync.Once
	putCommitted chan struct{}
}

func (b *blockingAuthConfigReadShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, authConfigKey) && b.reads.Add(1) == b.blockAt {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.BackendShim.InternalGet(ctx, key)
}

func (b *blockingAuthConfigReadShim) TxnApply(
	ctx context.Context,
	ops []backend.TxnWriteOp,
	guards []backend.TxnGuard,
	prevKV []bool,
) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	responses, revision, results, err := b.BackendShim.TxnApply(ctx, ops, guards, prevKV)
	b.putOnce.Do(func() { close(b.putCommitted) })
	return responses, revision, results, err
}

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

func TestAuthLeaseReadsUseOneAuthorizedKeySnapshot(t *testing.T) {
	tests := []struct {
		name string
		read func(context.Context, *RPCServer, int64) ([]string, error)
	}{
		{
			name: "time-to-live",
			read: func(ctx context.Context, server *RPCServer, leaseID int64) ([]string, error) {
				resp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
					ID: leaseID, Keys: true,
				})
				if err != nil {
					return nil, err
				}
				keys := make([]string, len(resp.Keys))
				for i := range resp.Keys {
					keys[i] = string(resp.Keys[i])
				}
				return keys, nil
			},
		},
		{
			name: "list",
			read: func(ctx context.Context, server *RPCServer, _ int64) ([]string, error) {
				_, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
				return nil, err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			aliceCtx := setupAuthKVUser(t, server)
			lease, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
			require.NoError(t, err)
			_, err = server.Put(aliceCtx, &etcdserverpb.PutRequest{
				Key: []byte("/allowed/leased"), Value: []byte("allowed"), Lease: lease.ID,
			})
			require.NoError(t, err)

			shim := &blockingAuthConfigReadShim{
				BackendShim: server.backend,
				// authCallerFromContext reads the config three times: before
				// token verification, during verification, and after it. Pause
				// the subsequent authorization-revision fence.
				blockAt:      4,
				entered:      make(chan struct{}),
				release:      make(chan struct{}),
				putCommitted: make(chan struct{}),
			}
			server.backend = shim
			server.tokens.snapshots.repo.backend = shim

			type readResult struct {
				keys []string
				err  error
			}
			readDone := make(chan readResult, 1)
			go func() {
				keys, readErr := tt.read(aliceCtx, server, lease.ID)
				readDone <- readResult{keys: keys, err: readErr}
			}()
			select {
			case <-shim.entered:
			case <-time.After(time.Second):
				t.Fatalf("lease read did not reach auth revision fence; config reads=%d", shim.reads.Load())
			}
			releaseAuth := func() {
				select {
				case <-shim.release:
				default:
					close(shim.release)
				}
			}
			defer releaseAuth()

			putDone := make(chan error, 1)
			go func() {
				// Start at the already-authorized atomic write stage. It commits
				// the user value and durable attachment before publishing the
				// in-memory lease index under leaseMu.
				_, putErr := server.putLeasedAtomic(context.Background(), &etcdserverpb.PutRequest{
					Key: []byte("/denied/concurrent"), Value: []byte("secret"), Lease: lease.ID,
				}, 0)
				putDone <- putErr
			}()
			select {
			case <-shim.putCommitted:
			case <-time.After(time.Second):
				t.Fatal("concurrent protected Put did not commit")
			}
			select {
			case putErr := <-putDone:
				require.NoError(t, putErr)
				t.Fatal("protected attachment published before lease-read authorization completed")
			case <-time.After(50 * time.Millisecond):
			}

			releaseAuth()
			result := <-readDone
			require.NoError(t, result.err)
			require.NotContains(t, result.keys, "/denied/concurrent")
			require.NoError(t, <-putDone)
		})
	}
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

func TestAuthLeaseKeepAliveExcludesConcurrentProtectedAttachment(t *testing.T) {
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
	_, err = server.Put(aliceCtx, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/leased"), Value: []byte("allowed"), Lease: lease.ID,
	})
	require.NoError(t, err)

	shim := &blockingAuthConfigReadShim{
		BackendShim: server.backend,
		// Pause the authorization revision fence after KeepAlive has acquired
		// the exclusive lease write lock and checked its current key snapshot.
		blockAt:      4,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
		putCommitted: make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	releaseAuth := func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	}
	defer releaseAuth()

	stream := &fakeLeaseKeepAliveServer{
		ctx: aliceCtx, requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: lease.ID}},
	}
	keepAliveDone := make(chan error, 1)
	go func() { keepAliveDone <- server.LeaseKeepAlive(stream) }()
	select {
	case <-shim.entered:
	case <-time.After(time.Second):
		t.Fatalf("keepalive did not reach auth revision fence; config reads=%d", shim.reads.Load())
	}

	putDone := make(chan error, 1)
	go func() {
		_, putErr := server.Put(rootCtx, &etcdserverpb.PutRequest{
			Key: []byte("/denied/concurrent-renew"), Value: []byte("secret"), Lease: lease.ID,
		})
		putDone <- putErr
	}()
	select {
	case <-shim.putCommitted:
		t.Fatal("protected attachment committed before keepalive authorization completed")
	case <-time.After(50 * time.Millisecond):
	}

	releaseAuth()
	require.NoError(t, <-keepAliveDone)
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(60), stream.sent[0].TTL)
	require.NoError(t, <-putDone)
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
