package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

type readonlyTxnCountBackendShim struct {
	BackendShim
	requests   []*etcdserverpb.RangeRequest
	getCalled  bool
	listCalled bool
}

func (b *readonlyTxnCountBackendShim) Count(_ context.Context, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	b.requests = append(b.requests, proto.Clone(request).(*etcdserverpb.RangeRequest))
	count := int64(5)
	if len(request.RangeEnd) == 0 {
		count = 1
	}
	return &etcdserverpb.RangeResponse{Header: txnHeader(int64(b.GetCurrentRevision())), Count: count}, nil
}

func (b *readonlyTxnCountBackendShim) Get(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	b.getCalled = true
	return nil, errors.New("read-only Txn CountOnly unexpectedly materialized Get")
}

func (b *readonlyTxnCountBackendShim) List(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	b.listCalled = true
	return nil, errors.New("read-only Txn CountOnly unexpectedly materialized List")
}

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

func TestReadonlyTxnCountOnlyUsesCountPath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	shim := &readonlyTxnCountBackendShim{BackendShim: server.backend}
	server.backend = shim
	requests := []*etcdserverpb.RangeRequest{
		{
			Key: []byte("/txn-count/point"), CountOnly: true, Limit: 1,
			SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
			MinModRevision: 11, MaxModRevision: 10,
		},
		{
			Key: []byte("/txn-count/"), RangeEnd: []byte("/txn-count0"), CountOnly: true, Limit: 1,
			SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
			MinCreateRevision: 9, MaxCreateRevision: 8,
		},
	}
	operations := make([]*etcdserverpb.RequestOp, 0, len(requests))
	for _, request := range requests {
		operations = append(operations, &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: request}})
	}

	response, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: operations})
	require.NoError(t, err)
	require.False(t, shim.getCalled)
	require.False(t, shim.listCalled)
	require.Len(t, shim.requests, 2)
	for index, request := range shim.requests {
		require.Positive(t, request.Revision, "latest requests must be pinned to the Txn snapshot")
		require.Equal(t, requests[index].Key, request.Key)
		require.Equal(t, requests[index].RangeEnd, request.RangeEnd, "point auth bounds must remain unchanged")
		require.Equal(t, requests[index].CountOnly, request.CountOnly)
		require.Equal(t, requests[index].Limit, request.Limit)
		require.Equal(t, requests[index].SortOrder, request.SortOrder)
		require.Equal(t, requests[index].SortTarget, request.SortTarget)
		require.Equal(t, requests[index].MinModRevision, request.MinModRevision)
		require.Equal(t, requests[index].MaxModRevision, request.MaxModRevision)
		require.Equal(t, requests[index].MinCreateRevision, request.MinCreateRevision)
		require.Equal(t, requests[index].MaxCreateRevision, request.MaxCreateRevision)
	}
	require.Equal(t, int64(1), response.Responses[0].GetResponseRange().Count)
	require.Equal(t, int64(5), response.Responses[1].GetResponseRange().Count)
	for _, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		require.Equal(t, response.Header.Revision, rangeResponse.Header.Revision)
		require.Empty(t, rangeResponse.Kvs)
		require.False(t, rangeResponse.More)
	}
}
