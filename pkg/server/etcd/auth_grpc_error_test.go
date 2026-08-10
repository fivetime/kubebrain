package etcd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

type transportErrorKVServer struct {
	etcdserverpb.UnimplementedKVServer
	err error
}

func (s *transportErrorKVServer) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return nil, s.err
}

func TestAuthGRPCErrorMapsPublicStatusCodes(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
	}{
		{rpctypes.ErrUserEmpty, codes.InvalidArgument},
		{rpctypes.ErrPermissionDenied, codes.PermissionDenied},
		{rpctypes.ErrInvalidAuthToken, codes.Unauthenticated},
		{rpctypes.ErrAuthNotEnabled, codes.FailedPrecondition},
		{rpctypes.ErrRootUserNotExist, codes.FailedPrecondition},
		{fmt.Errorf("tso: %w", storage.ErrUnavailable), codes.Unavailable},
		{storage.NewErrUncertainResult(context.DeadlineExceeded), codes.Unavailable},
		{fmt.Errorf("failed to get key: %w", fmt.Errorf("epoch_not_match:<>")), codes.Unavailable},
		{fmt.Errorf("failed to get key: %w", fmt.Errorf("no available connections")), codes.Unavailable},
		{markInvalidAuthMetadata(errors.New("decode auth config")), codes.DataLoss},
		{markInvalidLeaseMetadata(errors.New("decode lease record")), codes.DataLoss},
		{fmt.Errorf("%w: decode alarm set", backend.ErrInvalidAlarmMetadata), codes.DataLoss},
		{fmt.Errorf("%w: decode quota usage", backend.ErrInvalidQuotaMetadata), codes.DataLoss},
		{fmt.Errorf("%w: decode legacy value metadata", backend.ErrInvalidMVCCMetadata), codes.DataLoss},
		{errAuthRevisionExhausted, codes.ResourceExhausted},
		{backend.ErrQuotaUninitialized, codes.Unavailable},
	}
	for _, test := range tests {
		require.Equal(t, test.code, status.Code(authGRPCError(test.err)))
	}
}

func TestRetryableBackendTransportErrorRejectsEmbeddedMarker(t *testing.T) {
	require.False(t, isRetryableBackendTransportError(
		fmt.Errorf("failed to get key /registry/epoch_not_match:<>: permanent corruption")))
	require.False(t, isRetryableBackendTransportError(status.Error(codes.InvalidArgument, "epoch_not_match:<>")))
	require.False(t, isRetryableBackendTransportError(fmt.Errorf("no available connections remain permanently")))
}

func TestRetryableBackendTransportErrorUsesInnermostCause(t *testing.T) {
	for _, cause := range []error{
		fmt.Errorf("epoch_not_match:<>"),
		fmt.Errorf("no available connections"),
	} {
		wrapped := fmt.Errorf("snapshot scan: %w", fmt.Errorf("region request: %w", cause))
		require.True(t, isRetryableBackendTransportError(wrapped), cause.Error())
	}

	// A non-Unknown gRPC status remains authoritative even when a wrapper hides
	// GRPCStatus from status.Code on the outer error. Its message is not a TiKV
	// transport cause and must not be upgraded to retryable Unavailable.
	wrappedStatus := fmt.Errorf("backend rejected request: %w",
		status.Error(codes.InvalidArgument, "epoch_not_match:<>"))
	require.False(t, isRetryableBackendTransportError(wrappedStatus))

	// Text that merely looks like a wrapped cause is not an error chain. This
	// keeps user-controlled diagnostics from opting themselves into retry.
	require.False(t, isRetryableBackendTransportError(
		fmt.Errorf("failed to get user key: no available connections")))
}

func TestClientInterceptorClassifiesOnlyLeafBackendTransportCause(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "wrapped TiKV cause",
			err:  fmt.Errorf("range failed: %w", fmt.Errorf("region send failed: %w", fmt.Errorf("epoch_not_match:<>"))),
			code: codes.Unavailable,
		},
		{
			name: "wrapped gRPC status",
			err:  fmt.Errorf("range failed: %w", status.Error(codes.InvalidArgument, "epoch_not_match:<>")),
			code: codes.InvalidArgument,
		},
		{
			name: "plain suffix text",
			err:  fmt.Errorf("failed to get user key: no available connections"),
			code: codes.Unknown,
		},
		{
			name: "invalid auth metadata",
			err:  markInvalidAuthMetadata(errors.New("decode auth config")),
			code: codes.DataLoss,
		},
		{
			name: "invalid lease metadata",
			err:  markInvalidLeaseMetadata(errors.New("decode lease record")),
			code: codes.DataLoss,
		},
		{
			name: "invalid alarm metadata",
			err:  fmt.Errorf("%w: decode alarm set", backend.ErrInvalidAlarmMetadata),
			code: codes.DataLoss,
		},
		{
			name: "invalid MVCC metadata",
			err:  fmt.Errorf("%w: invalid etcd metadata length 1", backend.ErrInvalidMVCCMetadata),
			code: codes.DataLoss,
		},
		{
			name: "revision exhausted",
			err:  backend.ErrRevisionExhausted,
			code: codes.ResourceExhausted,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc, closeFn := newTestRPCServer(t)
			defer closeFn()
			grpcServer := grpc.NewServer(rpc.ClientServerOptions()...)
			etcdserverpb.RegisterKVServer(grpcServer, &transportErrorKVServer{err: test.err})
			listener := bufconn.Listen(1 << 20)
			go func() { _ = grpcServer.Serve(listener) }()
			t.Cleanup(grpcServer.Stop)

			connection, err := grpc.NewClient("passthrough:///transport-error",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return listener.Dial()
				}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			_, err = etcdserverpb.NewKVClient(connection).Range(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("key")})
			require.Equal(t, test.code, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), test.err.Error())
		})
	}
}

func TestClientRPCsClassifyPersistedMetadataCorruption(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(context.Context, *RPCServer) error
		call    func(context.Context, *grpc.ClientConn) error
		want    string
		code    codes.Code
	}{
		{
			name: "auth config",
			corrupt: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, authConfigKey, []byte{2})
			},
			call: func(ctx context.Context, connection *grpc.ClientConn) error {
				_, err := etcdserverpb.NewKVClient(connection).Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
				return err
			},
			want: "invalid auth config encoding",
		},
		{
			name: "zero auth revision",
			corrupt: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, authConfigKey, encodeAuthConfig(authConfig{}))
			},
			call: func(ctx context.Context, connection *grpc.ClientConn) error {
				_, err := etcdserverpb.NewKVClient(connection).Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
				return err
			},
			want: "auth config revision is zero",
		},
		{
			name: "invalid token generation length",
			corrupt: func(ctx context.Context, server *RPCServer) error {
				value, err := proto.Marshal(&authpb.User{Name: []byte("alice"), Password: make([]byte, authUserTokenGenerationBytes-1)})
				if err != nil {
					return err
				}
				return server.backend.InternalPut(ctx, authRecordKey(authTokenGenerationsKey, "alice"), value)
			},
			call: func(ctx context.Context, connection *grpc.ClientConn) error {
				_, err := etcdserverpb.NewKVClient(connection).Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
				return err
			},
			want: `auth token generation "alice" has length 15, want 16`,
		},
		{
			name: "invalid role permission range",
			corrupt: func(ctx context.Context, server *RPCServer) error {
				value, err := proto.Marshal(&authpb.Role{
					Name: []byte("reader"), KeyPermission: []*authpb.Permission{{
						PermType: authpb.READ, RangeEnd: []byte{0},
					}},
				})
				if err != nil {
					return err
				}
				return server.backend.InternalPut(ctx, authRecordKey(authRolesKey, "reader"), value)
			},
			call: func(ctx context.Context, connection *grpc.ClientConn) error {
				_, err := etcdserverpb.NewKVClient(connection).Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("key")})
				return err
			},
			want: `auth role "reader" permission 0 has invalid range`,
		},
		{
			name: "generic alarm",
			corrupt: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, genericAlarmKey, []byte("null"))
			},
			call: func(ctx context.Context, connection *grpc.ClientConn) error {
				_, err := etcdserverpb.NewMaintenanceClient(connection).Alarm(ctx, &etcdserverpb.AlarmRequest{
					Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType(127),
				})
				return err
			},
			want: "generic alarm metadata must be a JSON array",
		},
		{
			name: "auth revision exhausted",
			corrupt: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, authConfigKey, encodeAuthConfig(authConfig{Revision: math.MaxUint64}))
			},
			call: func(ctx context.Context, connection *grpc.ClientConn) error {
				_, err := etcdserverpb.NewAuthClient(connection).RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "must-not-exist"})
				return err
			},
			want: "etcd auth revision space exhausted",
			code: codes.ResourceExhausted,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, test.corrupt(ctx, server))

			grpcServer := grpc.NewServer(server.ClientServerOptions()...)
			etcdserverpb.RegisterKVServer(grpcServer, server)
			etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
			etcdserverpb.RegisterAuthServer(grpcServer, server)
			listener := bufconn.Listen(1 << 20)
			go func() { _ = grpcServer.Serve(listener) }()
			t.Cleanup(grpcServer.Stop)
			connection, err := grpc.NewClient("passthrough:///metadata-corruption",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })

			err = test.call(ctx, connection)
			wantCode := test.code
			if wantCode == codes.OK {
				wantCode = codes.DataLoss
			}
			require.Equal(t, wantCode, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), test.want)
		})
	}
}

func TestAuthGRPCErrorPreservesEtcdNoPasswordBehavior(t *testing.T) {
	err := authGRPCError(errNoPasswordUser)
	requireAuthGRPCStatusError(t, err, codes.Unknown, errNoPasswordUser.Error())
}

func TestAuthTokenSigningKeyCorruptionIsDataLossOverGRPC(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.enable(ctx))
	token, err := server.tokens.authenticate(ctx, "root", "secret")
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, authTokenSigningKey, []byte("truncated")))

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	connection, err := grpc.NewClient("passthrough:///auth-signing-key-corruption",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })

	_, err = etcdserverpb.NewAuthClient(connection).Authenticate(ctx,
		&etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "invalid auth token signing key")

	authenticated := metadata.NewOutgoingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))
	_, err = etcdserverpb.NewKVClient(connection).Range(authenticated, &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "invalid auth token signing key")
}

func requireAuthGRPCStatusError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, message)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func TestDedicatedConcurrencyMethodClassification(t *testing.T) {
	actual := make([]string, 0, len(dedicatedConcurrencyMethods))
	for method := range dedicatedConcurrencyMethods {
		actual = append(actual, method)
	}
	sort.Strings(actual)
	expected := []string{
		v3electionpb.Election_Campaign_FullMethodName,
		v3electionpb.Election_Leader_FullMethodName,
		v3electionpb.Election_Observe_FullMethodName,
		v3electionpb.Election_Proclaim_FullMethodName,
		v3electionpb.Election_Resign_FullMethodName,
		v3lockpb.Lock_Lock_FullMethodName,
		v3lockpb.Lock_Unlock_FullMethodName,
	}
	sort.Strings(expected)
	require.Equal(t, expected, actual)
	for _, method := range expected {
		require.True(t, isDedicatedConcurrencyMethod(method), method)
	}
	for _, method := range []string{
		"/etcdserverpb.KV/Range",
		"/etcdserverpb.Lease/LeaseKeepAlive",
		"/v3lockpb.Other/Lock",
		"/v3lockpb.Lock/NotARealRPC",
		"/v3electionpb.Election/NotARealRPC",
		"",
	} {
		require.False(t, isDedicatedConcurrencyMethod(method), method)
	}
}
