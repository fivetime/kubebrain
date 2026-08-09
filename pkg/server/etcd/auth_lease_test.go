package etcd

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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

type authMutationCurrentRevisionShim struct {
	BackendShim
	fired atomic.Bool
	hook  func()
}

func (s *authMutationCurrentRevisionShim) GetCurrentRevision() uint64 {
	revision := s.BackendShim.GetCurrentRevision()
	if s.fired.CompareAndSwap(false, true) {
		s.hook()
	}
	return revision
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
	requireAuthLeaseError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID})
	require.NoError(t, err, "TTL without attached keys does not reveal protected key names")
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID, Keys: true})
	requireAuthLeaseError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	requireAuthLeaseError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	stored, err := server.backend.Get(plain, &etcdserverpb.RangeRequest{Key: []byte("/denied/leased")})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1, "denied lease revoke must not delete protected keys")

	_, err = server.LeaseGrant(plain, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	requireAuthLeaseError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	allowed, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: allowed.ID})
	require.NoError(t, err)
	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	requireAuthLeaseError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
}

func TestAuthLeaseRevokeMissingLeaseErrorPriorityMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	missingID := int64(10_730_001)

	response, err := server.LeaseRevoke(context.Background(), &etcdserverpb.LeaseRevokeRequest{ID: missingID})
	require.Nil(t, response)
	requireAuthLeaseError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")

	response, err = server.LeaseRevoke(aliceCtx, &etcdserverpb.LeaseRevokeRequest{ID: missingID})
	require.Nil(t, response)
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound,
		"etcdserver: requested lease not found")
}

func TestAuthLeaseTimeToLiveMissingLeaseErrorPriorityMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	missingID := int64(10_730_002)

	for _, keys := range []bool{false, true} {
		request := &etcdserverpb.LeaseTimeToLiveRequest{ID: missingID, Keys: keys}
		response, err := server.LeaseTimeToLive(context.Background(), request)
		require.Nil(t, response)
		requireAuthLeaseError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")

		response, err = server.LeaseTimeToLive(aliceCtx, request)
		require.NoError(t, err)
		require.Equal(t, missingID, response.ID)
		require.Equal(t, int64(-1), response.TTL)
		require.Zero(t, response.GrantedTTL)
		require.Empty(t, response.Keys)
		require.NotNil(t, response.Header)
		require.Positive(t, response.Header.Revision)
	}
}

func TestAuthLeaseFutureJWTRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	plain := context.Background()
	lease, err := server.LeaseGrant(plain, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.Put(plain, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/leased"), Value: []byte("value"), Lease: lease.ID,
	})
	require.NoError(t, err)
	setupAuthKVUser(t, server)
	secret := writeJWTKey(t, "secret", []byte("shared-secret"))
	require.NoError(t, server.tokens.configureProvider("jwt,sign-method=HS256,priv-key="+secret))
	now := time.Unix(2_000_000_000, 0)
	server.tokens.now = func() time.Time { return now }
	snapshot, err := server.tokens.snapshots.current(plain)
	require.NoError(t, err)
	token, err := server.tokens.jwt.issue("alice", snapshot.Config.Revision+10, now)
	require.NoError(t, err)
	ctx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))

	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("/allowed/leased")}, ttl.Keys)
	leases, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.Len(t, leases.Leases, 1)
	require.Equal(t, lease.ID, leases.Leases[0].ID)
}

func TestAuthLeaseReadsUseOneAuthorizedKeySnapshot(t *testing.T) {
	tests := []struct {
		name    string
		blockAt int32
		read    func(context.Context, *RPCServer, int64) ([]string, error)
	}{
		{
			name:    "time-to-live",
			blockAt: 5,
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
			name:    "list",
			blockAt: 4,
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
				// TTL additionally captures its start revision before the
				// authorization fence; List reaches that fence directly.
				blockAt:      tt.blockAt,
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

func TestAuthLeaseReadFenceSurvivesCanceledRequestLikeEtcd(t *testing.T) {
	tests := []struct {
		name    string
		blockAt int32
		read    func(context.Context, *RPCServer, int64) error
	}{
		{
			name:    "time-to-live",
			blockAt: 5,
			read: func(ctx context.Context, server *RPCServer, leaseID int64) error {
				_, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
					ID: leaseID, Keys: true,
				})
				return err
			},
		},
		{
			name:    "list",
			blockAt: 4,
			read: func(ctx context.Context, server *RPCServer, _ int64) error {
				_, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
				return err
			},
		},
	}
	for _, tt := range tests {
		for _, mutateAuth := range []bool{false, true} {
			name := "stable-auth"
			if mutateAuth {
				name = "changed-auth"
			}
			t.Run(tt.name+"/"+name, func(t *testing.T) {
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
					// authCallerFromContext consumes the first three config reads.
					// TTL then captures its start revision; block the operation's
					// authorization fence (read five), while List blocks read four.
					blockAt: tt.blockAt, entered: make(chan struct{}), release: make(chan struct{}),
					putCommitted: make(chan struct{}),
				}
				server.backend = shim
				server.tokens.snapshots.repo.backend = shim
				requestCtx, cancel := context.WithCancel(aliceCtx)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- tt.read(requestCtx, server, lease.ID) }()
				select {
				case <-shim.entered:
				case <-time.After(time.Second):
					t.Fatalf("lease read did not reach auth revision fence; config reads=%d", shim.reads.Load())
				}

				var mutationErr error
				if mutateAuth {
					mutationErr = server.auth.roleAdd(context.Background(), "lease-canceled-context-bump")
				}
				cancel()
				select {
				case err = <-done:
					// The old implementation reaches this arm because its TiKV
					// auth-config read still carries the canceled request context.
				case <-time.After(50 * time.Millisecond):
					close(shim.release)
					err = <-done
				}
				require.NoError(t, mutationErr)
				if mutateAuth {
					requireAuthLeaseError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown,
						"etcdserver: revision of auth store is old")
				} else {
					require.NoError(t, err, "etcd completes a local lease read after its in-memory auth fence")
				}
			})
		}
	}
}

func TestAuthFollowerLeaseTimeToLiveRechecksRevisionAfterProxyLikeEtcd(t *testing.T) {
	for _, keys := range []bool{false, true} {
		name := "without-keys"
		if keys {
			name = "with-keys"
		}
		t.Run(name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			aliceCtx := setupAuthKVUser(t, server)

			var mutationErr error
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				leaseTTLFn: func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					mutationErr = server.auth.roleAdd(context.Background(), "follower-ttl-proxy-revision-bump")
					return &etcdserverpb.LeaseTimeToLiveResponse{
						Header: txnHeader(1), ID: 123, TTL: 30, GrantedTTL: 30,
					}, nil
				},
			}

			response, err := server.LeaseTimeToLive(aliceCtx, &etcdserverpb.LeaseTimeToLiveRequest{
				ID: 123, Keys: keys,
			})
			require.NoError(t, mutationErr)
			if keys {
				require.Nil(t, response)
				requireAuthLeaseError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown,
					"etcdserver: revision of auth store is old")
				return
			}
			require.NoError(t, err, "etcd does not revision-fence TTL without attached keys")
			require.NotNil(t, response)
			require.Equal(t, int64(123), response.ID)
		})
	}

	t.Run("proxy-error-precedes-final-fence", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		aliceCtx := setupAuthKVUser(t, server)
		proxyErr := errors.New("injected lease TTL proxy failure")
		var mutationErr error
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			leaseTTLFn: func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
				mutationErr = server.auth.roleAdd(context.Background(), "follower-ttl-proxy-error-revision-bump")
				return nil, proxyErr
			},
		}

		response, err := server.LeaseTimeToLive(aliceCtx, &etcdserverpb.LeaseTimeToLiveRequest{
			ID: 123, Keys: true,
		})
		require.NoError(t, mutationErr)
		require.Nil(t, response)
		require.ErrorIs(t, err, proxyErr)
	})
}

func TestAuthFollowerLeaseTimeToLiveFinalFenceIncludesRootLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(
		context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken),
	)

	var mutationErr error
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		leaseTTLFn: func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
			mutationErr = server.auth.roleAdd(context.Background(), "follower-root-ttl-revision-bump")
			return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: 123, TTL: 30}, nil
		},
	}

	response, err := server.LeaseTimeToLive(rootCtx, &etcdserverpb.LeaseTimeToLiveRequest{ID: 123, Keys: true})
	require.NoError(t, mutationErr)
	require.Nil(t, response)
	requireAuthLeaseError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown,
		"etcdserver: revision of auth store is old")
}

func TestAuthFollowerLeaseTimeToLiveFinalFenceTracksAuthEnableDisableLikeEtcd(t *testing.T) {
	t.Run("enable-after-anonymous-admission", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			leaseTTLFn: func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
				setupAuthKVUser(t, server)
				return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: 123, TTL: 30}, nil
			},
		}

		response, err := server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{
			ID: 123, Keys: true,
		})
		require.Nil(t, response)
		requireAuthLeaseError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown,
			"etcdserver: revision of auth store is old")
	})

	t.Run("disable-after-authenticated-admission", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		aliceCtx := setupAuthKVUser(t, server)
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			leaseTTLFn: func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
				require.NoError(t, server.auth.disable(context.Background()))
				return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: 123, TTL: 30}, nil
			},
		}

		response, err := server.LeaseTimeToLive(aliceCtx, &etcdserverpb.LeaseTimeToLiveRequest{
			ID: 123, Keys: true,
		})
		require.NoError(t, err)
		require.NotNil(t, response)
		require.Equal(t, int64(123), response.ID)
	})
}

func TestAuthLeaderLeaseTimeToLiveRechecksRevisionAfterLookupLikeEtcd(t *testing.T) {
	for _, keys := range []bool{false, true} {
		name := "without-keys"
		if keys {
			name = "with-keys"
		}
		t.Run(name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			aliceCtx := setupAuthKVUser(t, server)
			lease, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
			require.NoError(t, err)
			_, err = server.Put(aliceCtx, &etcdserverpb.PutRequest{
				Key: []byte("/allowed/leader-ttl"), Value: []byte("value"), Lease: lease.ID,
			})
			require.NoError(t, err)

			var mutationErr error
			shim := &authMutationCurrentRevisionShim{BackendShim: server.backend}
			shim.hook = func() {
				mutationErr = server.auth.roleAdd(context.Background(), "leader-ttl-lookup-revision-bump")
			}
			server.backend = shim
			server.tokens.snapshots.repo.backend = shim

			response, err := server.LeaseTimeToLive(aliceCtx, &etcdserverpb.LeaseTimeToLiveRequest{
				ID: lease.ID, Keys: keys,
			})
			require.NoError(t, mutationErr)
			if keys {
				require.Nil(t, response)
				requireAuthLeaseError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown,
					"etcdserver: revision of auth store is old")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, lease.ID, response.ID)
		})
	}
}

func TestAuthLeaseTimeToLiveRejectsDemotionDuringAuthorizationLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	lease, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.Put(aliceCtx, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/demotion-during-ttl-auth"), Value: []byte("value"), Lease: lease.ID,
	})
	require.NoError(t, err)

	var leading atomic.Bool
	leading.Store(true)
	server.peers = testPeerService{isLeaderFn: leading.Load}
	shim := &blockingAuthConfigReadShim{
		BackendShim: server.backend,
		// authCallerFromContext uses three reads and TTL captures its start
		// revision in the fourth; pause the authorization fence at read five.
		blockAt: 5, entered: make(chan struct{}), release: make(chan struct{}),
		putCommitted: make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	done := make(chan error, 1)
	go func() {
		_, ttlErr := server.LeaseTimeToLive(aliceCtx, &etcdserverpb.LeaseTimeToLiveRequest{
			ID: lease.ID, Keys: true,
		})
		done <- ttlErr
	}()
	select {
	case <-shim.entered:
	case <-time.After(time.Second):
		t.Fatalf("TTL did not reach authorization fence; config reads=%d", shim.reads.Load())
	}
	leading.Store(false)
	close(shim.release)

	requireLeaseFollowerUnavailable(t, <-done, "lease time-to-live error addr is test-peer leader test-peer")
}

func TestAuthLeaseTimeToLiveProxiesDemotionDuringAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	lease, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)
	_, err = server.Put(aliceCtx, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/proxy-demotion-during-ttl-auth"), Value: []byte("value"), Lease: lease.ID,
	})
	require.NoError(t, err)

	var leading atomic.Bool
	leading.Store(true)
	var forwarded atomic.Bool
	server.peers = testPeerService{
		isLeaderFn: leading.Load, proxyEnabled: true,
		leaseTTLFn: func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
			forwarded.Store(true)
			return &etcdserverpb.LeaseTimeToLiveResponse{
				Header: txnHeader(123), ID: lease.ID, TTL: 59, GrantedTTL: 60,
			}, nil
		},
	}
	shim := &blockingAuthConfigReadShim{
		BackendShim: server.backend,
		blockAt:     5, entered: make(chan struct{}), release: make(chan struct{}),
		putCommitted: make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	type result struct {
		response *etcdserverpb.LeaseTimeToLiveResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, ttlErr := server.LeaseTimeToLive(aliceCtx, &etcdserverpb.LeaseTimeToLiveRequest{
			ID: lease.ID, Keys: true,
		})
		done <- result{response: response, err: ttlErr}
	}()
	select {
	case <-shim.entered:
	case <-time.After(time.Second):
		t.Fatalf("TTL did not reach authorization fence; config reads=%d", shim.reads.Load())
	}
	leading.Store(false)
	close(shim.release)

	got := <-done
	require.NoError(t, got.err)
	require.True(t, forwarded.Load())
	require.NotNil(t, got.response)
	require.Equal(t, lease.ID, got.response.ID)
	require.Equal(t, int64(59), got.response.TTL)
}

func TestAuthLeaseLeasesRejectsDemotionDuringAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	_, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)

	var leading atomic.Bool
	leading.Store(true)
	server.peers = testPeerService{isLeaderFn: leading.Load}
	shim := &blockingAuthConfigReadShim{
		BackendShim: server.backend,
		blockAt:     4, entered: make(chan struct{}), release: make(chan struct{}),
		putCommitted: make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	done := make(chan error, 1)
	go func() {
		_, listErr := server.LeaseLeases(aliceCtx, &etcdserverpb.LeaseLeasesRequest{})
		done <- listErr
	}()
	select {
	case <-shim.entered:
	case <-time.After(time.Second):
		t.Fatalf("LeaseLeases did not reach authorization fence; config reads=%d", shim.reads.Load())
	}
	leading.Store(false)
	close(shim.release)

	requireLeaseFollowerUnavailable(t, <-done, "lease leases error addr is test-peer leader test-peer")
}

func TestAuthLeaseLeasesProxiesDemotionDuringAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	_, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
	require.NoError(t, err)

	var leading atomic.Bool
	leading.Store(true)
	var forwarded atomic.Bool
	server.peers = testPeerService{
		isLeaderFn: leading.Load, proxyEnabled: true,
		leaseLeasesFn: func(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
			forwarded.Store(true)
			return &etcdserverpb.LeaseLeasesResponse{
				Header: txnHeader(123), Leases: []*etcdserverpb.LeaseStatus{{ID: 999}},
			}, nil
		},
	}
	shim := &blockingAuthConfigReadShim{
		BackendShim: server.backend,
		blockAt:     4, entered: make(chan struct{}), release: make(chan struct{}),
		putCommitted: make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	type result struct {
		response *etcdserverpb.LeaseLeasesResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, listErr := server.LeaseLeases(aliceCtx, &etcdserverpb.LeaseLeasesRequest{})
		done <- result{response: response, err: listErr}
	}()
	select {
	case <-shim.entered:
	case <-time.After(time.Second):
		t.Fatalf("LeaseLeases did not reach authorization fence; config reads=%d", shim.reads.Load())
	}
	leading.Store(false)
	close(shim.release)

	got := <-done
	require.NoError(t, got.err)
	require.True(t, forwarded.Load())
	require.NotNil(t, got.response)
	require.Len(t, got.response.Leases, 1)
	require.Equal(t, int64(999), got.response.Leases[0].ID)
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
	requireAuthLeaseError(t, server.LeaseKeepAlive(denied), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
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
	requireAuthLeaseError(t, server.LeaseKeepAlive(allowed), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
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

func TestAuthLeaseKeepAliveReauthorizesAfterCheckpointUnlock(t *testing.T) {
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
	require.NoError(t, server.persistLeaseCheckpoint(context.Background(), lease.ID, 60, 10))
	server.leaseMu.Lock()
	server.leases[lease.ID].remainingTTL = 10
	server.leaseMu.Unlock()

	shim := &blockingLeaseCheckpointBackend{
		BackendShim: server.backend,
		leaseID:     lease.ID,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	defer func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	}()

	stream := &fakeLeaseKeepAliveServer{
		ctx: aliceCtx, requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: lease.ID}},
	}
	keepAliveDone := make(chan error, 1)
	go func() { keepAliveDone <- server.LeaseKeepAlive(stream) }()
	<-shim.entered
	_, err = server.Put(rootCtx, &etcdserverpb.PutRequest{
		Key: []byte("/denied/checkpoint-unlock"), Value: []byte("secret"), Lease: lease.ID,
	})
	require.NoError(t, err, "checkpoint clear must release the lease write lock")

	close(shim.release)
	requireAuthLeaseError(t, <-keepAliveDone, rpctypes.ErrPermissionDenied, codes.Unknown,
		"etcdserver: permission denied")
	require.Empty(t, stream.sent, "a renewal must not publish after a newly attached key fails reauthorization")
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
	requireAuthLeaseError(t, revokeErr, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1, "denied revoke must leave the newly protected key intact")
}

func requireAuthLeaseError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
