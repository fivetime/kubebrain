package etcd

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

// Change durable auth state after capturing a read, without invalidating this
// member's cache, as an auth mutation on another member would do.
type authConfigAfterReadShim struct {
	BackendShim
	fired       atomic.Bool
	reads       atomic.Int32
	afterReadAt int32
	afterRead   func()
}

func (s *authConfigAfterReadShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	value, err := s.BackendShim.InternalGet(ctx, key)
	if err == nil && bytes.Equal(key, authConfigKey) {
		at := s.afterReadAt
		if at == 0 {
			at = 1
		}
		if s.reads.Add(1) == at && s.fired.CompareAndSwap(false, true) {
			s.afterRead()
		}
	}
	return value, err
}

// Upstream stamps the first AuthInfo.Revision into the raft request header.
// Apply checks that version against the current auth store, rather than
// authenticating again and silently adopting a newer version.
func TestKVWritePreservesInitialAuthRevisionAtApply(t *testing.T) {
	for _, op := range []string{"put", "delete", "txn"} {
		t.Run(op, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := setupAuthKVUser(t, server)
			key := []byte("/allowed/auth-revision-at-apply")
			_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("original")})
			require.NoError(t, err)
			original := server.backend
			before, err := original.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			snapshot, err := server.tokens.snapshots.current(ctx)
			require.NoError(t, err)
			// A warm simple-token authentication reads config before verify,
			// during verify, and when assigning AuthInfo.Revision.
			shim := &authConfigAfterReadShim{BackendShim: original, afterReadAt: 3}
			shim.afterRead = func() {
				config := snapshot.Config
				config.Revision++
				require.NoError(t, original.InternalPut(context.Background(), authConfigKey, encodeAuthConfig(config)))
			}
			server.tokens.snapshots.repo.backend = shim
			switch op {
			case "put":
				_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")})
			case "delete":
				_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
			case "txn":
				_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")}}}}})
			}
			require.True(t, shim.fired.Load())
			require.ErrorIs(t, err, rpctypes.ErrAuthOldRevision)
			after, err := original.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Equal(t, before, after, "stale auth must not change the value or user revision")
		})
	}
}

func TestPutRechecksAuthEnabledAfterInitialAuthInfoRead(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"missing credentials", context.Background(), rpctypes.ErrUserEmpty},
		{"empty token", metadata.NewIncomingContext(context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, "")), rpctypes.ErrInvalidAuthToken},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
			require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
			snapshot, err := server.tokens.snapshots.current(ctx)
			require.NoError(t, err)
			require.False(t, snapshot.Config.Enabled)
			original := server.backend
			shim := &authConfigAfterReadShim{BackendShim: original}
			shim.afterRead = func() {
				config := snapshot.Config
				config.Enabled = true
				config.Revision++
				require.NoError(t, original.InternalPut(ctx, authConfigKey, encodeAuthConfig(config)))
			}
			server.tokens.snapshots.repo.backend = shim
			key := []byte("/auth-enable-admission")
			response, err := server.Put(test.ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("must-not-commit")})
			require.True(t, shim.fired.Load(), "must exercise the auth state transition")
			require.ErrorIs(t, err, test.want)
			require.Nil(t, response)
			stored, err := original.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Empty(t, stored.Kvs, "a cached disabled-auth result must not admit this write")
		})
	}
}
