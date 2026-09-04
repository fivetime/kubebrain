package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

type readonlyTxnCountBackendShim struct {
	BackendShim
	requests   []*etcdserverpb.RangeRequest
	getCalled  bool
	listCalled bool
}

type projectedTxnPointBatchCall struct {
	keys         []string
	metadataOnly []bool
}

type projectedTxnPointBatchBackendShim struct {
	BackendShim
	calls          []projectedTxnPointBatchCall
	ordinaryCalled bool
	omitKey        string
}

type legacyTxnPointBatchBackendShim struct {
	BackendShim
	calls int
}

func (b *legacyTxnPointBatchBackendShim) BatchGetAtRevision(
	_ context.Context, keys [][]byte, _ int64,
) (map[string]*mvccpb.KeyValue, error) {
	b.calls++
	result := make(map[string]*mvccpb.KeyValue, len(keys))
	for _, key := range keys {
		result[string(key)] = &mvccpb.KeyValue{
			Key: append([]byte(nil), key...), Value: []byte("legacy-full-value"),
			CreateRevision: 3, ModRevision: 7, Version: 2, Lease: 19,
		}
	}
	return result, nil
}

func (b *projectedTxnPointBatchBackendShim) BatchGetAtRevision(
	context.Context, [][]byte, int64,
) (map[string]*mvccpb.KeyValue, error) {
	b.ordinaryCalled = true
	return nil, errors.New("KeysOnly Txn point prefetch unexpectedly retained full values")
}

func (b *projectedTxnPointBatchBackendShim) BatchGetAtRevisionProjected(
	_ context.Context, keys [][]byte, metadataOnly []bool, revision int64,
) (map[string]*mvccpb.KeyValue, error) {
	call := projectedTxnPointBatchCall{
		keys:         make([]string, len(keys)),
		metadataOnly: append([]bool(nil), metadataOnly...),
	}
	result := make(map[string]*mvccpb.KeyValue, len(keys))
	for index, key := range keys {
		call.keys[index] = string(key)
		if string(key) == b.omitKey {
			continue
		}
		kv := &mvccpb.KeyValue{
			Key: append([]byte(nil), key...), CreateRevision: 3, ModRevision: 7,
			Version: 2, Value: []byte("full-" + string(key)), Lease: 19,
		}
		if metadataOnly[index] {
			kv.Value = nil
			kv.Lease = 0
		}
		result[string(key)] = kv
	}
	b.calls = append(b.calls, call)
	return result, nil
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

func TestReadonlyTxnPointPrefetchProjectsPerKeyAndUpgradesNestedCache(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	shim := &projectedTxnPointBatchBackendShim{BackendShim: server.backend}
	server.backend = shim
	fast := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte(key), KeysOnly: true, SortTarget: etcdserverpb.RangeRequest_VERSION,
			},
		}}
	}
	full := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)},
		}}
	}

	t.Run("pure fast keys-only", func(t *testing.T) {
		shim.calls = nil
		shim.ordinaryCalled = false
		executor := &stagedTxnExecutor{
			srv: server, ctx: context.Background(), baseRev: 11, pendingRev: 12,
			paths: &txnPathCursor{paths: []bool{true}}, mutations: make(map[string]*stagedMutation), readonly: true,
		}
		response, err := executor.execute(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			fast("/txn-keys/a"), fast("/txn-keys/b"),
		}})
		require.NoError(t, err)
		require.False(t, shim.ordinaryCalled)
		require.Equal(t, []projectedTxnPointBatchCall{{
			keys: []string{"/txn-keys/a", "/txn-keys/b"}, metadataOnly: []bool{true, true},
		}}, shim.calls)
		require.Empty(t, response.Responses[0].GetResponseRange().Kvs[0].Value)
		require.Empty(t, response.Responses[1].GetResponseRange().Kvs[0].Value)
	})

	t.Run("duplicate mixed requirement", func(t *testing.T) {
		shim.calls = nil
		shim.ordinaryCalled = false
		executor := &stagedTxnExecutor{
			srv: server, ctx: context.Background(), baseRev: 11, pendingRev: 12,
			paths: &txnPathCursor{paths: []bool{true}}, mutations: make(map[string]*stagedMutation), readonly: true,
		}
		response, err := executor.execute(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			fast("/txn-keys/a"), full("/txn-keys/a"), fast("/txn-keys/b"),
		}})
		require.NoError(t, err)
		require.False(t, shim.ordinaryCalled)
		require.Equal(t, []projectedTxnPointBatchCall{{
			keys: []string{"/txn-keys/a", "/txn-keys/b"}, metadataOnly: []bool{false, true},
		}}, shim.calls)
		require.Empty(t, response.Responses[0].GetResponseRange().Kvs[0].Value)
		require.Equal(t, []byte("full-/txn-keys/a"), response.Responses[1].GetResponseRange().Kvs[0].Value)
		require.Empty(t, response.Responses[2].GetResponseRange().Kvs[0].Value)
	})

	t.Run("nested full read upgrades projected cache", func(t *testing.T) {
		shim.calls = nil
		shim.ordinaryCalled = false
		executor := &stagedTxnExecutor{
			srv: server, ctx: context.Background(), baseRev: 11, pendingRev: 12,
			paths: &txnPathCursor{paths: []bool{true, true}}, mutations: make(map[string]*stagedMutation), readonly: true,
		}
		response, err := executor.execute(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			fast("/txn-keys/a"), {
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{full("/txn-keys/a")},
				}},
			},
		}})
		require.NoError(t, err)
		require.False(t, shim.ordinaryCalled)
		require.Equal(t, []projectedTxnPointBatchCall{
			{keys: []string{"/txn-keys/a"}, metadataOnly: []bool{true}},
			{keys: []string{"/txn-keys/a"}, metadataOnly: []bool{false}},
		}, shim.calls)
		require.Empty(t, response.Responses[0].GetResponseRange().Kvs[0].Value)
		nested := response.Responses[1].GetResponseTxn()
		require.Equal(t, []byte("full-/txn-keys/a"), nested.Responses[0].GetResponseRange().Kvs[0].Value)
	})

	t.Run("missing adapter result fails closed", func(t *testing.T) {
		shim.calls = nil
		shim.ordinaryCalled = false
		shim.omitKey = "/txn-keys/b"
		defer func() { shim.omitKey = "" }()
		executor := &stagedTxnExecutor{
			srv: server, ctx: context.Background(), baseRev: 11, pendingRev: 12,
			paths: &txnPathCursor{paths: []bool{true}}, mutations: make(map[string]*stagedMutation), readonly: true,
		}
		_, err := executor.execute(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
			fast("/txn-keys/a"), fast("/txn-keys/b"),
		}})
		require.EqualError(t, err, `point batch response omitted key "/txn-keys/b"`)
	})
}

func TestReadonlyTxnPointPrefetchFallsBackToLegacyFullBatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	shim := &legacyTxnPointBatchBackendShim{BackendShim: server.backend}
	server.backend = shim
	executor := &stagedTxnExecutor{
		srv: server, ctx: context.Background(), baseRev: 11, pendingRev: 12,
		paths: &txnPathCursor{paths: []bool{true}}, mutations: make(map[string]*stagedMutation), readonly: true,
	}
	point := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte(key), KeysOnly: true, SortTarget: etcdserverpb.RangeRequest_VERSION,
			},
		}}
	}
	response, err := executor.execute(&etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		point("/txn-keys/legacy-a"), point("/txn-keys/legacy-b"),
	}})
	require.NoError(t, err)
	require.Equal(t, 1, shim.calls)
	require.Empty(t, response.Responses[0].GetResponseRange().Kvs[0].Value)
	require.Empty(t, response.Responses[1].GetResponseRange().Kvs[0].Value)
}
