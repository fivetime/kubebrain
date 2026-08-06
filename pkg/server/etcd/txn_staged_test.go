package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestStagedTxnHistoricalEmptyRangesReturnEmptyResponse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/txn-empty/b"), Value: []byte("value")})
	require.NoError(t, err)
	require.NotNil(t, put.Header)
	executor := &stagedTxnExecutor{
		srv: server, ctx: ctx, baseRev: put.Header.Revision, pendingRev: put.Header.Revision + 1,
		mutations: make(map[string]*stagedMutation),
	}

	for _, request := range []*etcdserverpb.RangeRequest{
		{Key: []byte("/txn-empty/a"), RangeEnd: []byte("/txn-empty/a"), Revision: put.Header.Revision},
		{Key: []byte("/txn-empty/z"), RangeEnd: []byte("/txn-empty/a"), Revision: put.Header.Revision},
	} {
		response, err := executor.rangeResponse(request)
		require.NoError(t, err)
		require.NotNil(t, response.Header)
		require.Equal(t, put.Header.Revision, response.Header.Revision)
		require.Zero(t, response.Count)
		require.Empty(t, response.Kvs)
		require.False(t, response.More)
	}
}
