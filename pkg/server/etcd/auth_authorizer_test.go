package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
)

func verifiedTLSContext(ctx context.Context, commonName string) context.Context {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
	return peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}},
	}})
}

type authMutationReadShim struct {
	BackendShim
	onGet    sync.Once
	onStream sync.Once
	hook     func()
}

type authMutationWriteShim struct {
	BackendShim
	once sync.Once
	hook func()
}

func (b *authMutationWriteShim) Put(ctx context.Context, request *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	b.once.Do(b.hook)
	return b.BackendShim.Put(ctx, request)
}

func (b *authMutationReadShim) Get(ctx context.Context, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	response, err := b.BackendShim.Get(ctx, request)
	if err == nil {
		b.onGet.Do(b.hook)
	}
	return response, err
}

func (b *authMutationReadShim) RangeStreamChan(ctx context.Context, start, end []byte, revision uint64) (<-chan rangeStreamChunk, error) {
	input, err := b.BackendShim.RangeStreamChan(ctx, start, end, revision)
	if err != nil {
		return nil, err
	}
	output := make(chan rangeStreamChunk)
	go func() {
		defer close(output)
		for chunk := range input {
			select {
			case output <- chunk:
				b.onStream.Do(b.hook)
			case <-ctx.Done():
				return
			}
		}
	}()
	return output, nil
}

func TestAuthCallerAndPermissionRangeUnion(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	m := server.auth
	require.NoError(t, m.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "root-secret"}))
	require.NoError(t, m.roleAdd(ctx, "root"))
	require.NoError(t, m.userGrantRole(ctx, "root", "root"))
	require.NoError(t, m.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"}))
	for _, role := range []string{"left", "right"} {
		require.NoError(t, m.roleAdd(ctx, role))
		require.NoError(t, m.userGrantRole(ctx, "alice", role))
	}
	require.NoError(t, m.roleGrantPermission(ctx, "left", &authpb.Permission{PermType: authpb.READ, Key: []byte("a"), RangeEnd: []byte("m")}))
	require.NoError(t, m.roleGrantPermission(ctx, "right", &authpb.Permission{PermType: authpb.READWRITE, Key: []byte("m"), RangeEnd: []byte("z")}))
	require.NoError(t, m.enable(ctx))
	token, err := server.tokens.authenticate(ctx, "alice", "secret")
	require.NoError(t, err)
	caller, err := server.authCallerFromContext(metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token)))
	require.NoError(t, err)
	require.NoError(t, caller.require([]byte("b"), []byte("y"), authpb.READ), "adjacent permissions from separate roles must merge")
	requireAuthAuthorizerError(t, caller.require([]byte("b"), []byte("y"), authpb.WRITE), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	require.NoError(t, caller.require([]byte("m"), nil, authpb.WRITE))
	requireAuthAuthorizerError(t, caller.require([]byte("z"), nil, authpb.READ), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
}

func TestAuthCallerAcceptsBearerPrefixedToken(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	aliceCtx := setupAuthKVUser(t, server)
	tokens := metadata.ValueFromIncomingContext(aliceCtx, rpctypes.TokenFieldNameGRPC)
	require.Len(t, tokens, 1)
	token := tokens[0]

	for _, field := range []string{rpctypes.TokenFieldNameGRPC, rpctypes.TokenFieldNameSwagger} {
		for _, credential := range []string{token, "Bearer " + token} {
			incoming := metadata.NewIncomingContext(
				ctx, metadata.Pairs(field, credential),
			)
			caller, err := server.authCallerFromContext(incoming)
			require.NoError(t, err)
			require.Equal(t, "alice", caller.username)
			require.Equal(t, credential, caller.forwardToken)
			forwarded, err := server.forwardAuthToken(ctx, caller)
			require.NoError(t, err)
			md, ok := metadata.FromOutgoingContext(forwarded)
			require.True(t, ok)
			require.Equal(t, []string{credential}, md.Get(rpctypes.TokenFieldNameGRPC))
		}
	}

	incoming := metadata.NewIncomingContext(
		ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "bearer "+token),
	)
	_, err := server.authCallerFromContext(incoming)
	requireAuthAuthorizerError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
}

func TestAuthCallerUsesFirstRepeatedMetadataToken(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	aliceCtx := setupAuthKVUser(t, server)
	tokens := metadata.ValueFromIncomingContext(aliceCtx, rpctypes.TokenFieldNameGRPC)
	require.Len(t, tokens, 1)
	token := tokens[0]

	for _, field := range []string{rpctypes.TokenFieldNameGRPC, rpctypes.TokenFieldNameSwagger} {
		incoming := metadata.NewIncomingContext(
			ctx, metadata.Pairs(field, token, field, "invalid.second.token"),
		)
		caller, err := server.authCallerFromContext(incoming)
		require.NoError(t, err)
		require.Equal(t, "alice", caller.username)
		require.Equal(t, token, caller.forwardToken)
	}

	incoming := metadata.NewIncomingContext(
		ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "", rpctypes.TokenFieldNameGRPC, token),
	)
	_, err := server.authCallerFromContext(incoming)
	requireAuthAuthorizerError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
}

func TestAuthCallerFailsClosedAndDisabledBypasses(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	caller, err := server.authCallerFromContext(ctx)
	require.NoError(t, err)
	require.Nil(t, caller)

	_, _ = bootstrapAuthForToken(t, server)
	_, err = server.authCallerFromContext(ctx)
	requireAuthAuthorizerError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
}

func TestAuthorizedRangeRejectsAuthMutationDuringRead(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	_, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{Key: []byte("/allowed/a"), Value: []byte("value")})
	require.NoError(t, err)

	shim := &authMutationReadShim{BackendShim: server.backend}
	var mutationErr error
	shim.hook = func() {
		mutationErr = server.auth.roleRevokePermission(context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0"))
	}
	server.backend = shim

	_, err = server.Range(aliceCtx, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a")})
	requireAuthAuthorizerError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown, "etcdserver: revision of auth store is old")
	require.NoError(t, mutationErr)
	_, err = server.Range(aliceCtx, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a")})
	requireAuthAuthorizerError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
}

func TestAuthorizedRangeStreamRejectsAuthMutationDuringRead(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	_, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{Key: []byte("/allowed/a"), Value: []byte("value")})
	require.NoError(t, err)

	shim := &authMutationReadShim{BackendShim: server.backend}
	var mutationErr error
	shim.hook = func() {
		mutationErr = server.auth.roleRevokePermission(context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0"))
	}
	server.backend = shim
	stream := &fakeRangeStreamServer{ctx: aliceCtx}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0")}, stream)
	requireAuthAuthorizerError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown, "etcdserver: revision of auth store is old")
	require.NoError(t, mutationErr)
	require.NotEmpty(t, stream.sent, "the mutation must occur after streaming has begun")
}

func TestAuthorizedPutAtomicallyRejectsAuthMutationBeforeCommit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)

	shim := &authMutationWriteShim{BackendShim: server.backend}
	var mutationErr error
	shim.hook = func() {
		mutationErr = server.auth.roleRevokePermission(context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0"))
	}
	server.backend = shim
	_, err := server.Put(aliceCtx, &etcdserverpb.PutRequest{Key: []byte("/allowed/raced"), Value: []byte("must-not-commit")})
	requireAuthAuthorizerError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown, "etcdserver: revision of auth store is old")
	require.NoError(t, mutationErr)

	stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/allowed/raced")})
	require.NoError(t, err)
	require.Empty(t, stored.Kvs)
}

func TestAuthCallerUsesVerifiedClientCertificateCommonName(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	caller, err := server.authCallerFromContext(verifiedTLSContext(context.Background(), "alice"))
	require.NoError(t, err)
	require.Equal(t, "alice", caller.username)
	require.Equal(t, caller.snapshot.Config.Revision, caller.revision)
	require.Empty(t, caller.forwardToken, "leader-local certificate auth must not mint a proxy token")

	forwarded, err := server.forwardAuthToken(context.Background(), caller)
	require.NoError(t, err)
	md, ok := metadata.FromOutgoingContext(forwarded)
	require.True(t, ok)
	tokens := md.Get(rpctypes.TokenFieldNameGRPC)
	require.Len(t, tokens, 1)
	claims, err := server.tokens.verify(context.Background(), tokens[0])
	require.NoError(t, err)
	require.Equal(t, "alice", claims.Username)
}

func TestAuthCallerUsesOuterTLSListenerIdentity(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	cert := &x509.Certificate{Subject: pkix.Name{CommonName: "alice"}}
	ctx := transportidentity.WithTLSState(context.Background(), tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}},
	})
	caller, err := server.authCallerFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, "alice", caller.username)
}

func TestAuthCallerDoesNotFallbackFromInvalidTokenToClientCertificate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	ctx := verifiedTLSContext(context.Background(), "alice")
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "invalid"))
	_, err := server.authCallerFromContext(ctx)
	requireAuthAuthorizerError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
}

func TestAuthCallerPreservesUnknownCertificateIdentityForAuthorization(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	caller, err := server.authCallerFromContext(verifiedTLSContext(context.Background(), "external-cn"))
	require.NoError(t, err)
	require.Equal(t, "external-cn", caller.username)
	requireAuthAuthorizerError(t, caller.require([]byte("/allowed/key"), nil, authpb.READ), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	forwarded, err := server.forwardAuthToken(context.Background(), caller)
	require.NoError(t, err)
	md, ok := metadata.FromOutgoingContext(forwarded)
	require.True(t, ok)
	tokens := md.Get(rpctypes.TokenFieldNameGRPC)
	require.Len(t, tokens, 1)
	claims, err := server.tokens.verify(context.Background(), tokens[0])
	require.NoError(t, err)
	require.True(t, claims.Certificate)
	require.Equal(t, "external-cn", claims.Username)
}

func TestAuthCallerRejectsClientCertificateOnGRPCGatewayRequest(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	ctx := verifiedTLSContext(context.Background(), "alice")
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("grpcgateway-accept", "application/json"))
	_, err := server.authCallerFromContext(ctx)
	requireAuthAuthorizerError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
}

func TestClientCertificateIdentitySurvivesFollowerProxy(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	var forwardedUsername string
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		putFn: func(ctx context.Context, _ *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			tokens := md.Get(rpctypes.TokenFieldNameGRPC)
			require.Len(t, tokens, 1)
			claims, err := server.tokens.verify(context.Background(), tokens[0])
			require.NoError(t, err)
			forwardedUsername = claims.Username
			return &etcdserverpb.PutResponse{}, nil
		},
	}

	ctx := verifiedTLSContext(context.Background(), "alice")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/allowed/proxied"), Value: []byte("value")})
	require.NoError(t, err)
	require.Equal(t, "alice", forwardedUsername)
}

func TestFollowerWriteProxyDefersBearerAuthenticationToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)

	const credential = "not-a-valid-token"
	forwarded := false
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		putFn: func(ctx context.Context, _ *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
			forwarded = true
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			require.Equal(t, []string{credential}, md.Get(rpctypes.TokenFieldNameGRPC))
			return nil, rpctypes.ErrInvalidAuthToken
		},
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, credential,
	))
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/proxied"), Value: []byte("value"),
	})
	require.True(t, forwarded)
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
}

func TestForwardedCertificateIdentityUsesCurrentPermissions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)

	caller, err := server.authCallerFromContext(verifiedTLSContext(context.Background(), "alice"))
	require.NoError(t, err)
	require.NoError(t, caller.require([]byte("/allowed/key"), nil, authpb.WRITE))
	forwarded, err := server.forwardAuthToken(context.Background(), caller)
	require.NoError(t, err)
	md, ok := metadata.FromOutgoingContext(forwarded)
	require.True(t, ok)
	tokens := md.Get(rpctypes.TokenFieldNameGRPC)
	require.Len(t, tokens, 1)
	require.NoError(t, server.auth.roleRevokePermission(
		context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0"),
	))

	forwardedCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, tokens[0],
	))
	leaderCaller, err := server.authCallerFromContext(forwardedCtx)
	require.NoError(t, err, "certificate identity must survive unrelated auth revision changes")
	require.Equal(t, "alice", leaderCaller.username)
	requireAuthAuthorizerError(t, leaderCaller.require([]byte("/allowed/key"), nil, authpb.WRITE), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
}

func TestAuthPermissionOpenEndedAndGap(t *testing.T) {
	caller := &authCaller{username: "alice", snapshot: &authSnapshot{
		Users: map[string]*authpb.User{"alice": {Name: []byte("alice"), Roles: []string{"reader"}}},
		Roles: map[string]*authpb.Role{"reader": {Name: []byte("reader"), KeyPermission: []*authpb.Permission{
			{PermType: authpb.READ, Key: []byte("a"), RangeEnd: []byte("m")},
			{PermType: authpb.READ, Key: []byte("n"), RangeEnd: []byte{0}},
		}}},
	}}
	require.True(t, caller.permits([]byte("n"), []byte{0}, authpb.READ))
	require.True(t, caller.permits([]byte("zz"), nil, authpb.READ))
	require.False(t, caller.permits([]byte("b"), []byte("z"), authpb.READ), "a gap between m and n must deny the whole range")
	require.False(t, caller.permits([]byte("n"), []byte{0}, authpb.WRITE))
}

func TestAuthUnknownPermissionTypeGrantsNoAccess(t *testing.T) {
	caller := &authCaller{username: "alice", revision: 1, snapshot: &authSnapshot{
		Config: authConfig{Enabled: true, Revision: 1},
		Users:  map[string]*authpb.User{"alice": {Name: []byte("alice"), Roles: []string{"unknown"}}},
		Roles: map[string]*authpb.Role{"unknown": {Name: []byte("unknown"), KeyPermission: []*authpb.Permission{
			{PermType: authpb.Permission_Type(99), Key: []byte("/unknown/"), RangeEnd: []byte("/unknown0")},
		}}},
	}}

	require.False(t, caller.permits([]byte("/unknown/key"), nil, authpb.READ))
	require.False(t, caller.permits([]byte("/unknown/key"), nil, authpb.WRITE))
	requireAuthAuthorizerError(t, caller.require([]byte("/unknown/key"), nil, authpb.READ), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	requireAuthAuthorizerError(t, caller.require([]byte("/unknown/key"), nil, authpb.WRITE), rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
}

func requireAuthAuthorizerError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
