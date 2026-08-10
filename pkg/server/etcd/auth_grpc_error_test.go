package etcd

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

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

func TestAuthGRPCErrorPreservesEtcdNoPasswordBehavior(t *testing.T) {
	err := authGRPCError(errNoPasswordUser)
	requireAuthGRPCStatusError(t, err, codes.Unknown, errNoPasswordUser.Error())
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
