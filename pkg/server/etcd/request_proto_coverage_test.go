package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestCoreRequestProtoFieldCoverage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		msg    proto.Message
		fields map[string]protoreflect.FieldNumber
	}{
		{
			name: "RangeRequest", msg: &etcdserverpb.RangeRequest{},
			fields: map[string]protoreflect.FieldNumber{
				"key": 1, "range_end": 2, "limit": 3, "revision": 4, "sort_order": 5,
				"sort_target": 6, "serializable": 7, "keys_only": 8, "count_only": 9,
				"min_mod_revision": 10, "max_mod_revision": 11,
				"min_create_revision": 12, "max_create_revision": 13,
			},
		},
		{
			name: "PutRequest", msg: &etcdserverpb.PutRequest{},
			fields: map[string]protoreflect.FieldNumber{
				"key": 1, "value": 2, "lease": 3, "prev_kv": 4, "ignore_value": 5, "ignore_lease": 6,
			},
		},
		{
			name: "DeleteRangeRequest", msg: &etcdserverpb.DeleteRangeRequest{},
			fields: map[string]protoreflect.FieldNumber{"key": 1, "range_end": 2, "prev_kv": 3},
		},
		{
			name: "Compare", msg: &etcdserverpb.Compare{},
			fields: map[string]protoreflect.FieldNumber{
				"result": 1, "target": 2, "key": 3, "version": 4, "create_revision": 5,
				"mod_revision": 6, "value": 7, "lease": 8, "range_end": 64,
			},
		},
		{
			name: "RequestOp", msg: &etcdserverpb.RequestOp{},
			fields: map[string]protoreflect.FieldNumber{
				"request_range": 1, "request_put": 2, "request_delete_range": 3, "request_txn": 4,
			},
		},
		{
			name: "TxnRequest", msg: &etcdserverpb.TxnRequest{},
			fields: map[string]protoreflect.FieldNumber{"compare": 1, "success": 2, "failure": 3},
		},
		{
			name: "CompactionRequest", msg: &etcdserverpb.CompactionRequest{},
			fields: map[string]protoreflect.FieldNumber{"revision": 1, "physical": 2},
		},
		{
			name: "WatchCreateRequest", msg: &etcdserverpb.WatchCreateRequest{},
			fields: map[string]protoreflect.FieldNumber{
				"key": 1, "range_end": 2, "start_revision": 3, "progress_notify": 4,
				"filters": 5, "prev_kv": 6, "watch_id": 7, "fragment": 8,
			},
		},
		{
			name: "WatchRequest", msg: &etcdserverpb.WatchRequest{},
			fields: map[string]protoreflect.FieldNumber{
				"create_request": 1, "cancel_request": 2, "progress_request": 3,
			},
		},
		{
			name: "LeaseGrantRequest", msg: &etcdserverpb.LeaseGrantRequest{},
			fields: map[string]protoreflect.FieldNumber{"TTL": 1, "ID": 2},
		},
		{
			name: "LeaseRevokeRequest", msg: &etcdserverpb.LeaseRevokeRequest{},
			fields: map[string]protoreflect.FieldNumber{"ID": 1},
		},
		{
			name: "LeaseKeepAliveRequest", msg: &etcdserverpb.LeaseKeepAliveRequest{},
			fields: map[string]protoreflect.FieldNumber{"ID": 1},
		},
		{
			name: "LeaseTimeToLiveRequest", msg: &etcdserverpb.LeaseTimeToLiveRequest{},
			fields: map[string]protoreflect.FieldNumber{"ID": 1, "keys": 2},
		},
		{
			name: "LeaseLeasesRequest", msg: &etcdserverpb.LeaseLeasesRequest{},
			fields: map[string]protoreflect.FieldNumber{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := tc.msg.ProtoReflect().Descriptor()
			actual := make(map[string]protoreflect.FieldNumber, descriptor.Fields().Len())
			for i := 0; i < descriptor.Fields().Len(); i++ {
				field := descriptor.Fields().Get(i)
				actual[string(field.Name())] = field.Number()
			}
			require.Equal(t, tc.fields, actual,
				"etcd API request fields changed; audit validation, authorization, execution, Txn nesting, and forwarding before updating this guard")
		})
	}
}

func TestConcurrencyProtoFieldCoverage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		msg    proto.Message
		fields map[string]protoreflect.FieldNumber
	}{
		{
			name: "LockRequest", msg: &v3lockpb.LockRequest{},
			fields: map[string]protoreflect.FieldNumber{"name": 1, "lease": 2},
		},
		{
			name: "LockResponse", msg: &v3lockpb.LockResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "key": 2},
		},
		{
			name: "UnlockRequest", msg: &v3lockpb.UnlockRequest{},
			fields: map[string]protoreflect.FieldNumber{"key": 1},
		},
		{
			name: "UnlockResponse", msg: &v3lockpb.UnlockResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1},
		},
		{
			name: "CampaignRequest", msg: &v3electionpb.CampaignRequest{},
			fields: map[string]protoreflect.FieldNumber{"name": 1, "lease": 2, "value": 3},
		},
		{
			name: "CampaignResponse", msg: &v3electionpb.CampaignResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "leader": 2},
		},
		{
			name: "LeaderKey", msg: &v3electionpb.LeaderKey{},
			fields: map[string]protoreflect.FieldNumber{"name": 1, "key": 2, "rev": 3, "lease": 4},
		},
		{
			name: "LeaderRequest", msg: &v3electionpb.LeaderRequest{},
			fields: map[string]protoreflect.FieldNumber{"name": 1},
		},
		{
			name: "LeaderResponse", msg: &v3electionpb.LeaderResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "kv": 2},
		},
		{
			name: "ResignRequest", msg: &v3electionpb.ResignRequest{},
			fields: map[string]protoreflect.FieldNumber{"leader": 1},
		},
		{
			name: "ResignResponse", msg: &v3electionpb.ResignResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1},
		},
		{
			name: "ProclaimRequest", msg: &v3electionpb.ProclaimRequest{},
			fields: map[string]protoreflect.FieldNumber{"leader": 1, "value": 2},
		},
		{
			name: "ProclaimResponse", msg: &v3electionpb.ProclaimResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := tc.msg.ProtoReflect().Descriptor()
			actual := make(map[string]protoreflect.FieldNumber, descriptor.Fields().Len())
			for i := 0; i < descriptor.Fields().Len(); i++ {
				field := descriptor.Fields().Get(i)
				actual[string(field.Name())] = field.Number()
			}
			require.Equal(t, tc.fields, actual,
				"etcd concurrency API fields changed; audit Lock/Election wrappers, gateway JSON behavior, authorization, and error normalization before updating this guard")
		})
	}
}

func TestCoreResponseProtoFieldCoverage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		msg    proto.Message
		fields map[string]protoreflect.FieldNumber
	}{
		{
			name: "ResponseHeader", msg: &etcdserverpb.ResponseHeader{},
			fields: map[string]protoreflect.FieldNumber{
				"cluster_id": 1, "member_id": 2, "revision": 3, "raft_term": 4,
			},
		},
		{
			name: "RangeResponse", msg: &etcdserverpb.RangeResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "kvs": 2, "more": 3, "count": 4},
		},
		{
			name: "PutResponse", msg: &etcdserverpb.PutResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "prev_kv": 2},
		},
		{
			name: "DeleteRangeResponse", msg: &etcdserverpb.DeleteRangeResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "deleted": 2, "prev_kvs": 3},
		},
		{
			name: "ResponseOp", msg: &etcdserverpb.ResponseOp{},
			fields: map[string]protoreflect.FieldNumber{
				"response_range": 1, "response_put": 2, "response_delete_range": 3, "response_txn": 4,
			},
		},
		{
			name: "TxnResponse", msg: &etcdserverpb.TxnResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "succeeded": 2, "responses": 3},
		},
		{
			name: "CompactionResponse", msg: &etcdserverpb.CompactionResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1},
		},
		{
			name: "WatchResponse", msg: &etcdserverpb.WatchResponse{},
			fields: map[string]protoreflect.FieldNumber{
				"header": 1, "watch_id": 2, "created": 3, "canceled": 4,
				"compact_revision": 5, "cancel_reason": 6, "fragment": 7, "events": 11,
			},
		},
		{
			name: "LeaseGrantResponse", msg: &etcdserverpb.LeaseGrantResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "ID": 2, "TTL": 3, "error": 4},
		},
		{
			name: "LeaseRevokeResponse", msg: &etcdserverpb.LeaseRevokeResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1},
		},
		{
			name: "LeaseKeepAliveResponse", msg: &etcdserverpb.LeaseKeepAliveResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "ID": 2, "TTL": 3},
		},
		{
			name: "LeaseTimeToLiveResponse", msg: &etcdserverpb.LeaseTimeToLiveResponse{},
			fields: map[string]protoreflect.FieldNumber{
				"header": 1, "ID": 2, "TTL": 3, "grantedTTL": 4, "keys": 5,
			},
		},
		{
			name: "LeaseStatus", msg: &etcdserverpb.LeaseStatus{},
			fields: map[string]protoreflect.FieldNumber{"ID": 1},
		},
		{
			name: "LeaseLeasesResponse", msg: &etcdserverpb.LeaseLeasesResponse{},
			fields: map[string]protoreflect.FieldNumber{"header": 1, "leases": 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := tc.msg.ProtoReflect().Descriptor()
			actual := make(map[string]protoreflect.FieldNumber, descriptor.Fields().Len())
			for i := 0; i < descriptor.Fields().Len(); i++ {
				field := descriptor.Fields().Get(i)
				actual[string(field.Name())] = field.Number()
			}
			require.Equal(t, tc.fields, actual,
				"etcd API response fields changed; audit every constructor, merge, fragmentation, Txn nesting, and forwarding path before updating this guard")
		})
	}
}
