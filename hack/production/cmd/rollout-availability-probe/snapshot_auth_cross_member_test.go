package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
						_, err = etcdserverpb.NewKVClient(conn).Put(opCtx, &etcdserverpb.PutRequest{
							Key: []byte(fixture.expected.access[2].key), Value: []byte("leased"), Lease: lease.ID,
						})
						require.NoError(t, err)
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
