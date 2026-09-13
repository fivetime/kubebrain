package main

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	etcdutlsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// Exercise every authentication-member -> request-member edge explicitly.
// Raw generated RPC clients have no etcd-client token refresh or retry wrapper.
// This local test does not claim to reproduce the intermittent deployed failure.
func TestRestoredAuthTokensAcrossAllMemberPairs(t *testing.T) {
	testRestoredAuthMemberClients(t, false)
}

// Complement raw member-edge coverage with the official client's shared token
// refresh path. Success here does not reproduce or resolve the deployed failure.
func TestRestoredAuthOfficialConcurrentStreamRefresh(t *testing.T) {
	testRestoredAuthMemberClients(t, true)
}

func testRestoredAuthMemberClients(t *testing.T, officialRefresh bool) {
	t.Helper()
	const prefix = "/probe/auth-cross-member/"
	fixture := newSnapshotAuthFixture(prefix)
	fixture.expected.revision = 31
	fixture.expected.enabled = true
	expected := newStreamProbeExpectations(prefix)
	state := snapshotAuthTestState(t, expected, &fixture.expected)
	state.Auth.Enabled = true
	state.Auth.Users = append(state.Auth.Users, &authpb.User{
		Name: []byte("root"), Roles: []string{"root"}, Options: &authpb.UserAddOptions{NoPassword: true},
	})
	state.Auth.Roles = append(state.Auth.Roles, &authpb.Role{Name: []byte("root")})
	for index, user := range fixture.expected.users {
		if user.noPassword {
			continue
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(user.password), bcrypt.DefaultCost)
		require.NoError(t, err)
		state.Auth.Users[index].Password = hash
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	artifact, err := os.CreateTemp(dir, "snapshot-*.db")
	require.NoError(t, err)
	defer artifact.Close()
	defer os.Remove(artifact.Name())
	_, version, err := consumeSnapshotTo(snapshotAuthReceiver(t, state), artifact)
	require.NoError(t, err)
	require.NoError(t, artifact.Sync())
	require.NoError(t, artifact.Close())
	verifier := func(ctx context.Context, cfg restoredSnapshotConfig, _ []streamProbeExpectation, _ int64) error {
		servers := make([]*embed.Etcd, len(cfg.members))
		defer func() {
			for _, server := range servers {
				if server != nil {
					server.Close()
				}
			}
		}()
		for index, member := range cfg.members {
			servers[index], err = embed.StartEtcd(newRestoredSnapshotEmbedConfig(cfg, member, cfg.initialCluster))
			require.NoError(t, err)
		}
		for index, server := range servers {
			require.NoError(t, waitForRestoredSnapshotMember(ctx, cfg.members[index], server))
		}
		tlsConfig, err := cfg.localTLS.passwordClient.ClientConfig()
		require.NoError(t, err)
		adminTLS, err := cfg.clientTLSConfig()
		require.NoError(t, err)
		adminConfig := clientv3.Config{
			Context: ctx, TLS: adminTLS, DialTimeout: 3 * time.Second, Logger: zap.NewNop(),
			DialOptions: []grpc.DialOption{grpc.WithAuthority(cfg.localTLS.server.ServerName)},
		}
		for _, member := range cfg.members {
			adminConfig.Endpoints = append(adminConfig.Endpoints, member.clientURL.String())
		}
		if officialRefresh {
			passwordConfig := adminConfig
			passwordConfig.TLS = tlsConfig
			verifyOfficialRestoredStreamRefresh(t, ctx, passwordConfig, fixture)
			return nil
		}
		connections := make([]*grpc.ClientConn, len(cfg.members))
		defer func() {
			for _, conn := range connections {
				if conn != nil {
					require.NoError(t, conn.Close())
				}
			}
		}()
		for index, member := range cfg.members {
			connections[index], err = grpc.NewClient(member.clientURL.Host,
				grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig.Clone())),
				grpc.WithAuthority(cfg.localTLS.server.ServerName))
			require.NoError(t, err)
		}
		watchEdges, leaseEdges := 0, 0
		for source, authConn := range connections {
			for _, userIndex := range []int{4, 1} { // exact-reader and write-only user from the deployed matrix
				user := fixture.expected.users[userIndex]
				response, err := etcdserverpb.NewAuthClient(authConn).Authenticate(ctx,
					&etcdserverpb.AuthenticateRequest{Name: user.name, Password: user.password})
				require.NoError(t, err, "authenticate source=%d user_index=%d", source, userIndex)
				require.NotNil(t, response)
				require.NotNil(t, response.Header)
				require.Equal(t, uint64(servers[source].Server.MemberID()), response.Header.MemberId,
					"authentication must reach the selected source member")
				require.NotEmpty(t, response.Token)
				tokenCtx := metadata.AppendToOutgoingContext(ctx, "token", response.Token)
				for target, conn := range connections {
					opCtx, stop := context.WithTimeout(tokenCtx, 5*time.Second)
					if userIndex == 4 {
						stream, err := etcdserverpb.NewWatchClient(conn).Watch(opCtx)
						require.NoError(t, err)
						require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
							CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte(fixture.expected.access[5].key)},
						}}))
						created, err := stream.Recv()
						require.NoError(t, err, "Watch source=%d target=%d", source, target)
						require.True(t, created.Created && !created.Canceled)
						require.NotNil(t, created.Header)
						require.Equal(t, uint64(servers[target].Server.MemberID()), created.Header.MemberId,
							"Watch must reach the selected target member")
						watchEdges++
					} else {
						leases := etcdserverpb.NewLeaseClient(conn)
						lease, err := leases.LeaseGrant(opCtx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
						require.NoError(t, err)
						put, err := etcdserverpb.NewKVClient(conn).Put(opCtx, &etcdserverpb.PutRequest{
							Key: []byte(fixture.expected.access[2].key), Value: []byte("leased"), Lease: lease.ID,
						})
						require.NoError(t, err)
						require.NotNil(t, put.Header)
						// Match the deployed probe's all-member attachment barrier and
						// denied TTL-with-keys request before testing the same token on
						// KeepAlive. The raw client still deliberately does not refresh it.
						// Do not forward the writer's outgoing token to the root
						// replication client: it takes precedence over certificate CN.
						barrierCtx, stopBarrier := context.WithTimeout(ctx, 5*time.Second)
						barrierErr := waitForRestoredAuthLeaseReplication(barrierCtx, adminConfig,
							fixture.expected.access[2].key, "leased", clientv3.LeaseID(lease.ID), put.Header.Revision)
						stopBarrier()
						require.NoError(t, barrierErr)
						_, err = leases.LeaseTimeToLive(opCtx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID, Keys: true})
						require.ErrorIs(t, rpctypes.Error(err), rpctypes.ErrPermissionDenied,
							"write-only lease keys source=%d target=%d", source, target)
						stream, err := leases.LeaseKeepAlive(opCtx)
						require.NoError(t, err)
						require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: lease.ID}))
						alive, err := stream.Recv()
						require.NoError(t, err, "LeaseKeepAlive source=%d target=%d", source, target)
						require.Equal(t, lease.ID, alive.ID)
						require.Positive(t, alive.TTL)
						_, err = leases.LeaseRevoke(opCtx, &etcdserverpb.LeaseRevokeRequest{ID: lease.ID})
						require.NoError(t, err)
						leaseEdges++
					}
					stop()
				}
			}
		}
		require.Equal(t, 9, watchEdges)
		require.Equal(t, 9, leaseEdges)
		t.Logf("verified all member pairs: Watch=%d LeaseKeepAlive=%d", watchEdges, leaseEdges)
		return nil
	}
	require.NoError(t, validateSnapshotArtifactWithClusterAuthVerifier(ctx, etcdutlsnapshot.NewV3(zap.NewNop()),
		artifact.Name(), version, dir, expected, newClientOnlySnapshotTLSFixture(t), &fixture.expected, 3, verifier))
}

func verifyOfficialRestoredStreamRefresh(t *testing.T, ctx context.Context, cfg clientv3.Config, fixture *snapshotAuthFixture) {
	t.Helper()
	user := fixture.expected.users[2] // readwriter: Watch and lease attachment permission
	cfg.Username, cfg.Password = user.name, user.password
	var authentications atomic.Uint64
	cfg.DialOptions = append(append([]grpc.DialOption(nil), cfg.DialOptions...), grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, request, response any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoke(ctx, method, request, response, cc, opts...)
			if method == "/etcdserverpb.Auth/Authenticate" && err == nil {
				authentications.Add(1)
			}
			return err
		}))
	trace := &restoredAuthRPCTrace{}
	client, err := clientv3.New(trace.config(cfg))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	lease, err := client.Grant(ctx, 30)
	require.NoError(t, err)
	defer func() { _, err := client.Revoke(ctx, lease.ID); require.NoError(t, err) }()
	key := fixture.expected.access[3].key
	_, err = client.Put(ctx, key, "refresh-fixture", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	for round := 0; round < 20; round++ {
		func() {
			before := authentications.Load()
			opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			// A fresh Watcher ensures a fresh stream, rather than multiplexing
			// another logical watch over a stream authenticated in an earlier round.
			watcher := clientv3.NewWatcher(client)
			defer func() { require.NoError(t, watcher.Close()) }()
			watch := watcher.Watch(opCtx, key, clientv3.WithCreatedNotify())
			// Watch establishment runs asynchronously; KeepAliveOnce exercises
			// the same client's token source while that stream is being opened.
			alive, err := client.KeepAliveOnce(opCtx, lease.ID)
			require.NoError(t, err, "round=%d trace=%s", round, trace.summary())
			require.Equal(t, lease.ID, alive.ID)
			require.Positive(t, alive.TTL)
			select {
			case response, ok := <-watch:
				require.True(t, ok, "round=%d trace=%s", round, trace.summary())
				require.NoError(t, response.Err(), "round=%d trace=%s", round, trace.summary())
				require.True(t, response.Created && !response.Canceled)
			case <-opCtx.Done():
				t.Fatalf("stream refresh timed out round=%d trace=%s", round, trace.summary())
			}
			require.GreaterOrEqual(t, authentications.Load()-before, uint64(2),
				"paired stream establishment must actually refresh credentials round=%d", round)
		}()
	}
	t.Log("official shared-client Watch/KeepAliveOnce refresh rounds=20; no injected faults or application retries")
}
