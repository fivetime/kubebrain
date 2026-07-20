package etcd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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
	}
	for _, test := range tests {
		require.Equal(t, test.code, status.Code(authGRPCError(test.err)))
	}
}

func TestAuthGRPCErrorPreservesEtcdNoPasswordBehavior(t *testing.T) {
	err := authGRPCError(errNoPasswordUser)
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Equal(t, errNoPasswordUser.Error(), status.Convert(err).Message())
}

func TestDedicatedConcurrencyMethodClassification(t *testing.T) {
	for _, method := range []string{
		"/v3lockpb.Lock/Lock",
		"/v3lockpb.Lock/Unlock",
		"/v3electionpb.Election/Campaign",
		"/v3electionpb.Election/Observe",
	} {
		require.True(t, isDedicatedConcurrencyMethod(method), method)
	}
	for _, method := range []string{
		"/etcdserverpb.KV/Range",
		"/etcdserverpb.Lease/LeaseKeepAlive",
		"/v3lockpb.Other/Lock",
		"",
	} {
		require.False(t, isDedicatedConcurrencyMethod(method), method)
	}
}
