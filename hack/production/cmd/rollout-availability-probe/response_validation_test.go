package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func rolloutHeader(revision int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9, Revision: revision}
}

func TestValidateTxnOperationHeaderAcceptsEtcdRevisionOnlyShape(t *testing.T) {
	outer := rolloutHeader(11)
	outer.RaftTerm = 3
	require.NoError(t, validateTxnOperationHeader(&etcdserverpb.ResponseHeader{Revision: 11}, outer))
	require.NoError(t, validateTxnOperationHeader(&etcdserverpb.ResponseHeader{
		ClusterId: 7, MemberId: 9, Revision: 11, RaftTerm: 3,
	}, outer))
}

func TestValidateTxnOperationHeaderRejectsMalformedShape(t *testing.T) {
	outer := rolloutHeader(11)
	outer.RaftTerm = 3
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"nil":              nil,
		"zero revision":    {},
		"wrong revision":   {Revision: 10},
		"partial identity": {ClusterId: 7, Revision: 11},
		"wrong cluster":    {ClusterId: 8, MemberId: 9, Revision: 11},
		"wrong member":     {ClusterId: 7, MemberId: 8, Revision: 11},
		"wrong Raft term":  {Revision: 11, RaftTerm: 4},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateTxnOperationHeader(header, outer))
		})
	}
}

func nestedFailureTxnFixture() (*clientv3.TxnResponse, streamProbeNestedSeeds) {
	initial := func(key string, revision int64) *streamProbeExpectation {
		value := key + "-old"
		return &streamProbeExpectation{key: key, value: value, revision: revision, events: []streamProbeEventExpectation{{
			eventType: mvccpb.PUT, value: value, revision: revision, createRevision: revision, version: 1,
		}}}
	}
	seeds := streamProbeNestedSeeds{
		outerPut: initial("probe/z", 4),
		deleted:  initial("probe/a", 5),
		innerPut: initial("probe/m", 6),
	}
	outerValue := "probe/z-new"
	header := rolloutHeader(7)
	response := &clientv3.TxnResponse{
		Header: header, Succeeded: true,
		Responses: []*etcdserverpb.ResponseOp{
			{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 7},
			}}},
			{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: &etcdserverpb.TxnResponse{
				Header: &etcdserverpb.ResponseHeader{}, Succeeded: false,
				Responses: []*etcdserverpb.ResponseOp{
					{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
						Header: &etcdserverpb.ResponseHeader{Revision: 7}, Count: 1,
						Kvs: []*mvccpb.KeyValue{{Key: []byte("probe/z"), Value: []byte(outerValue), CreateRevision: 4, ModRevision: 7, Version: 2}},
					}}},
					{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
						Header: &etcdserverpb.ResponseHeader{Revision: 7}, Deleted: 1,
					}}},
					{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
						Header: &etcdserverpb.ResponseHeader{Revision: 7},
					}}},
				},
			}}},
		},
	}
	return response, seeds
}

func TestValidateNestedFailureTxnResponse(t *testing.T) {
	response, seeds := nestedFailureTxnFixture()
	revision, err := validateNestedFailureTxnResponse(response, 7, 6, seeds, "probe/z-new")
	require.NoError(t, err)
	require.Equal(t, int64(7), revision)
}

func TestValidateNestedFailureTxnResponseRejectsMalformedSuccess(t *testing.T) {
	for name, mutate := range map[string]func(*clientv3.TxnResponse){
		"outer did not advance": func(response *clientv3.TxnResponse) { response.Header.Revision = 6 },
		"missing nested":        func(response *clientv3.TxnResponse) { response.Responses = response.Responses[:1] },
		"wrong nested union": func(response *clientv3.TxnResponse) {
			response.Responses[1] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{
				ResponsePut: &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 7}},
			}}
		},
		"nested header stamped": func(response *clientv3.TxnResponse) {
			response.Responses[1].GetResponseTxn().Header.Revision = 7
		},
		"nested success branch": func(response *clientv3.TxnResponse) {
			response.Responses[1].GetResponseTxn().Succeeded = true
		},
		"wrong nested operation count": func(response *clientv3.TxnResponse) {
			nested := response.Responses[1].GetResponseTxn()
			nested.Responses = nested.Responses[:2]
		},
		"staged old value": func(response *clientv3.TxnResponse) {
			response.Responses[1].GetResponseTxn().Responses[0].GetResponseRange().Kvs[0].Value = []byte("probe/z-old")
		},
		"delete did not apply": func(response *clientv3.TxnResponse) {
			response.Responses[1].GetResponseTxn().Responses[1].GetResponseDeleteRange().Deleted = 0
		},
		"inner put previous value": func(response *clientv3.TxnResponse) {
			response.Responses[1].GetResponseTxn().Responses[2].GetResponsePut().PrevKv = &mvccpb.KeyValue{}
		},
		"wrong inner revision": func(response *clientv3.TxnResponse) {
			response.Responses[1].GetResponseTxn().Responses[2].GetResponsePut().Header.Revision = 8
		},
	} {
		t.Run(name, func(t *testing.T) {
			response, seeds := nestedFailureTxnFixture()
			mutate(response)
			_, err := validateNestedFailureTxnResponse(response, 7, 6, seeds, "probe/z-new")
			require.Error(t, err)
		})
	}
}

func TestValidateRolloutMutationResponses(t *testing.T) {
	deleted := &clientv3.DeleteResponse{Header: rolloutHeader(10), Deleted: 2}
	clusterID, revision, err := validateDeleteResponse(deleted, 0, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(7), clusterID)
	require.Equal(t, int64(10), revision)

	grant := &clientv3.LeaseGrantResponse{ResponseHeader: rolloutHeader(10), ID: 42, TTL: 20}
	leaseID, revision, err := validateGrantResponse(grant, clusterID, revision, 15)
	require.NoError(t, err)
	require.Equal(t, clientv3.LeaseID(42), leaseID)

	revision, err = validateKeepAliveResponse(&clientv3.LeaseKeepAliveResponse{ResponseHeader: rolloutHeader(10), ID: 42, TTL: 15}, clusterID, revision, leaseID, grant.TTL)
	require.NoError(t, err)

	revision, err = validatePutResponse(&clientv3.PutResponse{Header: rolloutHeader(11)}, clusterID, revision)
	require.NoError(t, err)
	require.Equal(t, int64(11), revision)

	revision, err = validateTimeToLiveResponse(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: rolloutHeader(11), ID: 42, TTL: 14, GrantedTTL: 20, Keys: [][]byte{[]byte("/probe/lease")}}, clusterID, revision, leaseID, grant.TTL, "/probe/lease")
	require.NoError(t, err)
	require.Equal(t, int64(11), revision)
}

func TestValidateRolloutMutationResponsesRejectMalformedSuccess(t *testing.T) {
	for name, test := range map[string]func() error{
		"nil delete": func() error { _, _, err := validateDeleteResponse(nil, 0, 1); return err },
		"delete negative count": func() error {
			_, _, err := validateDeleteResponse(&clientv3.DeleteResponse{Header: rolloutHeader(1), Deleted: -1}, 0, 1)
			return err
		},
		"delete prev kv": func() error {
			_, _, err := validateDeleteResponse(&clientv3.DeleteResponse{Header: rolloutHeader(1), PrevKvs: []*mvccpb.KeyValue{{Key: []byte("hidden")}}}, 0, 1)
			return err
		},
		"grant nil header": func() error {
			_, _, err := validateGrantResponse(&clientv3.LeaseGrantResponse{ID: 1, TTL: 15}, 7, 1, 15)
			return err
		},
		"grant wrong cluster": func() error {
			_, _, err := validateGrantResponse(&clientv3.LeaseGrantResponse{ResponseHeader: &etcdserverpb.ResponseHeader{ClusterId: 8, MemberId: 9, Revision: 1}, ID: 1, TTL: 15}, 7, 1, 15)
			return err
		},
		"grant legacy error": func() error {
			_, _, err := validateGrantResponse(&clientv3.LeaseGrantResponse{ResponseHeader: rolloutHeader(1), ID: 1, TTL: 15, Error: "denied"}, 7, 1, 15)
			return err
		},
		"grant short ttl": func() error {
			_, _, err := validateGrantResponse(&clientv3.LeaseGrantResponse{ResponseHeader: rolloutHeader(1), ID: 1, TTL: 14}, 7, 1, 15)
			return err
		},
		"keepalive wrong lease": func() error {
			_, err := validateKeepAliveResponse(&clientv3.LeaseKeepAliveResponse{ResponseHeader: rolloutHeader(1), ID: 2, TTL: 10}, 7, 1, 1, 15)
			return err
		},
		"put prev kv": func() error {
			_, err := validatePutResponse(&clientv3.PutResponse{Header: rolloutHeader(2), PrevKv: &mvccpb.KeyValue{}}, 7, 1)
			return err
		},
		"put stale revision": func() error {
			_, err := validatePutResponse(&clientv3.PutResponse{Header: rolloutHeader(1)}, 7, 1)
			return err
		},
		"ttl wrong keys": func() error {
			_, err := validateTimeToLiveResponse(&clientv3.LeaseTimeToLiveResponse{ResponseHeader: rolloutHeader(2), ID: 1, TTL: 10, GrantedTTL: 15, Keys: [][]byte{[]byte("wrong")}}, 7, 1, 1, 15, "key")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) { require.Error(t, test()) })
	}
}

func TestValidateObservedRolloutPutAndCleanupRange(t *testing.T) {
	observed := &clientv3.GetResponse{Header: rolloutHeader(12), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("key"), Value: []byte("value"), CreateRevision: 10, ModRevision: 12, Version: 2}}}
	revision, err := validateObservedPut(observed, 7, 10, "key", "value")
	require.NoError(t, err)
	require.Equal(t, int64(12), revision)
	require.NoError(t, validateAbsentRange(&clientv3.GetResponse{Header: rolloutHeader(13)}, 7, 12))
}

func TestValidateObservedRolloutPutAndCleanupRangeRejectMalformedSuccess(t *testing.T) {
	validKV := &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), CreateRevision: 10, ModRevision: 12, Version: 2}
	for name, response := range map[string]*clientv3.GetResponse{
		"nil":             nil,
		"hidden kv":       {Header: rolloutHeader(12), Count: 0, Kvs: []*mvccpb.KeyValue{validKV}},
		"more":            {Header: rolloutHeader(12), Count: 1, More: true, Kvs: []*mvccpb.KeyValue{validKV}},
		"future revision": {Header: rolloutHeader(11), Count: 1, Kvs: []*mvccpb.KeyValue{validKV}},
		"lease attached":  {Header: rolloutHeader(12), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("key"), Value: []byte("value"), CreateRevision: 10, ModRevision: 12, Version: 2, Lease: 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateObservedPut(response, 7, 10, "key", "value")
			require.Error(t, err)
		})
	}
	require.Error(t, validateAbsentRange(&clientv3.GetResponse{Header: rolloutHeader(13), Count: 0, Kvs: []*mvccpb.KeyValue{{Key: []byte("hidden")}}}, 7, 12))
}

func TestValidateRolloutWatchResponses(t *testing.T) {
	created := clientv3.WatchResponse{Header: rolloutHeader(11), Created: true}
	revision, err := validateCreatedWatch(created, 7, 10)
	require.NoError(t, err)
	require.Equal(t, int64(11), revision)

	put := clientv3.WatchResponse{Header: rolloutHeader(12), Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), CreateRevision: 12, ModRevision: 12, Version: 1}}}}
	revision, err = validatePutWatch(put, 7, 12, "key", "value")
	require.NoError(t, err)
	require.Equal(t, int64(12), revision)
}

func TestValidateRolloutWatchResponsesRejectMalformedSuccess(t *testing.T) {
	validEvent := &clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), CreateRevision: 12, ModRevision: 12, Version: 1}}
	for name, response := range map[string]clientv3.WatchResponse{
		"created with event":    {Header: rolloutHeader(11), Created: true, Events: []*clientv3.Event{validEvent}},
		"created wrong cluster": {Header: &etcdserverpb.ResponseHeader{ClusterId: 8, MemberId: 9, Revision: 11}, Created: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateCreatedWatch(response, 7, 10)
			require.Error(t, err)
		})
	}
	for name, response := range map[string]clientv3.WatchResponse{
		"created flag":   {Header: rolloutHeader(12), Created: true, Events: []*clientv3.Event{validEvent}},
		"wrong revision": {Header: rolloutHeader(12), Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key"), Value: []byte("value"), CreateRevision: 12, ModRevision: 11, Version: 1}}}},
		"delete event":   {Header: rolloutHeader(12), Events: []*clientv3.Event{{Type: mvccpb.DELETE, Kv: validEvent.Kv}}},
		"previous value": {Header: rolloutHeader(12), Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: validEvent.Kv, PrevKv: &mvccpb.KeyValue{}}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validatePutWatch(response, 7, 12, "key", "value")
			require.Error(t, err)
		})
	}
}
