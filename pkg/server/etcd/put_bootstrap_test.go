package etcd

import (
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// A native Put shortcut must not silently depend on an earlier Range or leased
// write to bootstrap the empty-store revision and ordered collector cursor.
func TestPlainPutBootstrapsColdBackendWithoutPriorRead(t *testing.T) {
	shim := newBackendShimIgnoreTest(t)
	t.Cleanup(func() {
		shim.Close()
		require.NoError(t, shim.backend.(interface{ Close() error }).Close())
	})
	require.Zero(t, shim.backend.GetCurrentRevision())
	key := []byte("/put/cold/plain")
	ctx := backend.WithPreviousLease(t.Context(), 0)
	first, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("first")})
	require.NoError(t, err)
	require.EqualValues(t, 2, first.Header.Revision, "revision 1 belongs to initialized empty etcd")
	second, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("second")})
	require.NoError(t, err)
	require.EqualValues(t, 3, second.Header.Revision)
	require.Nil(t, second.PrevKv)
	current, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("second"), current.Kvs[0].Value)
	require.EqualValues(t, 2, current.Kvs[0].Version)
	require.Equal(t, first.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, second.Header.Revision, current.Kvs[0].ModRevision)
}
