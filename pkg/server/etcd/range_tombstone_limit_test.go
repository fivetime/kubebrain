package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestHistoricalRangeLimitCountsOnlyLiveKeysAcrossTombstones(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/range-tombstone-limit/"
	end := prefixEnd([]byte(prefix))
	put := func(suffix string) int64 {
		response, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
		})
		require.NoError(t, err)
		return response.Header.Revision
	}
	del := func(suffix string) int64 {
		response, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + suffix),
		})
		require.NoError(t, err)
		return response.Header.Revision
	}

	var beforeDeletes int64
	for _, suffix := range []string{"a", "b", "c", "d"} {
		beforeDeletes = put(suffix)
	}
	del("b")
	afterDeletes := del("d")
	put("c")
	put("b")
	put("e")

	assertRange := func(revision, limit int64, wantKeys []string, wantCount int64, wantMore bool) {
		response, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: end, Revision: revision, Limit: limit, KeysOnly: true,
		})
		require.NoError(t, err)
		require.Equal(t, wantCount, response.Count)
		require.Equal(t, wantMore, response.More)
		require.Len(t, response.Kvs, len(wantKeys))
		for i, suffix := range wantKeys {
			require.Equal(t, []byte(prefix+suffix), response.Kvs[i].Key)
			require.Empty(t, response.Kvs[i].Value)
		}
	}

	assertRange(beforeDeletes, 2, []string{"a", "b"}, 4, true)
	assertRange(afterDeletes, 1, []string{"a"}, 2, true)
	assertRange(0, 2, []string{"a", "b"}, 4, true)
}

func TestHistoricalRangeCountOnlyIgnoresLimitAcrossTombstones(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/range-tombstone-count-limit/"
	end := prefixEnd([]byte(prefix))
	put := func(suffix string) int64 {
		response, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
		})
		require.NoError(t, err)
		return response.Header.Revision
	}
	del := func(suffix string) int64 {
		response, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + suffix),
		})
		require.NoError(t, err)
		return response.Header.Revision
	}

	var beforeDeletes int64
	for _, suffix := range []string{"a", "b", "c", "d"} {
		beforeDeletes = put(suffix)
	}
	del("b")
	afterDeletes := del("d")
	put("c")
	put("b")
	put("e")

	assertCountOnly := func(revision int64, wantCount int64) {
		response, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: end, Revision: revision,
			Limit: 1, CountOnly: true,
		})
		require.NoError(t, err)
		require.Equal(t, wantCount, response.Count)
		require.Empty(t, response.Kvs)
		require.False(t, response.More)
	}

	assertCountOnly(beforeDeletes, 4)
	assertCountOnly(afterDeletes, 2)
	assertCountOnly(0, 4)
}
