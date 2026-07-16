package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
