package etcd

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

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
	}
	for _, test := range tests {
		require.Equal(t, test.code, status.Code(authGRPCError(test.err)))
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
