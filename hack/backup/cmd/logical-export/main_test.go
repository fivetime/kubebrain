package main

import (
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNextKeyDoesNotMutateInput(t *testing.T) {
	in := []byte("abc")
	out := nextKey(in)

	require.Equal(t, []byte("abc"), in)
	require.Equal(t, []byte{'a', 'b', 'c', 0}, out)
}

func TestValidateExportPage(t *testing.T) {
	validKV := func(key string) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), CreateRevision: 1, ModRevision: 2, Version: 1}
	}
	tests := map[string]struct {
		response      *clientv3.GetResponse
		fixed         int64
		start         string
		end           string
		limit         int64
		expectedCount int64
		wantRemaining int64
		wantErr       string
	}{
		"initial empty snapshot": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}},
			start:         "/registry/",
			end:           "/registry0",
			limit:         2,
			expectedCount: -1,
		},
		"continued snapshot": {
			response: &clientv3.GetResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 43},
				Kvs:    []*mvccpb.KeyValue{validKV("/registry/a")},
				Count:  2,
				More:   true,
			},
			fixed:         42,
			start:         "/registry/",
			end:           "/registry0",
			limit:         1,
			expectedCount: 2,
			wantRemaining: 1,
		},
		"unbounded from-key range": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1, Kvs: []*mvccpb.KeyValue{validKV("b")}},
			start:         "a",
			end:           "\x00",
			limit:         1,
			expectedCount: -1,
		},
		"empty response": {
			limit:         2,
			expectedCount: -1,
			wantErr:       "empty response",
		},
		"missing header": {
			response:      &clientv3.GetResponse{},
			limit:         2,
			expectedCount: -1,
			wantErr:       "omitted its header",
		},
		"zero initial revision": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}},
			limit:         2,
			expectedCount: -1,
			wantErr:       "invalid revision 0",
		},
		"stale continued revision": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 41}},
			fixed:         42,
			limit:         2,
			expectedCount: -1,
			wantErr:       "revision 41 is behind snapshot revision 42",
		},
		"empty continuation page": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1, More: true},
			fixed:         42,
			limit:         2,
			expectedCount: -1,
			wantErr:       "non-full page",
		},
		"count below records": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Kvs: []*mvccpb.KeyValue{validKV("a")}},
			start:         "a",
			end:           "z",
			limit:         2,
			expectedCount: -1,
			wantErr:       "count 0 for 1 records",
		},
		"broken count continuity": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 2, More: true, Kvs: []*mvccpb.KeyValue{validKV("a")}},
			start:         "a",
			end:           "z",
			limit:         1,
			expectedCount: 3,
			wantErr:       "does not continue previous remaining count 3",
		},
		"above page limit": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a"), validKV("b")}},
			start:         "a",
			end:           "z",
			limit:         1,
			expectedCount: -1,
			wantErr:       "above page limit 1",
		},
		"inconsistent more": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a")}},
			start:         "a",
			end:           "z",
			limit:         1,
			expectedCount: -1,
			wantErr:       "inconsistent count/more metadata",
		},
		"nil record": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1, Kvs: []*mvccpb.KeyValue{nil}},
			start:         "a",
			end:           "z",
			limit:         1,
			expectedCount: -1,
			wantErr:       "nil record at index 0",
		},
		"key below start": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1, Kvs: []*mvccpb.KeyValue{validKV("a")}},
			start:         "b",
			end:           "z",
			limit:         1,
			expectedCount: -1,
			wantErr:       "outside requested range",
		},
		"key at end": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1, Kvs: []*mvccpb.KeyValue{validKV("z")}},
			start:         "a",
			end:           "z",
			limit:         1,
			expectedCount: -1,
			wantErr:       "outside requested range",
		},
		"descending records": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 2, Kvs: []*mvccpb.KeyValue{validKV("b"), validKV("a")}},
			start:         "a",
			end:           "z",
			limit:         2,
			expectedCount: -1,
			wantErr:       "not in strict ascending",
		},
		"duplicate records": {
			response:      &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 2, Kvs: []*mvccpb.KeyValue{validKV("a"), validKV("a")}},
			start:         "a",
			end:           "z",
			limit:         2,
			expectedCount: -1,
			wantErr:       "not in strict ascending",
		},
		"invalid MVCC metadata": {
			response: &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1, Kvs: []*mvccpb.KeyValue{{
				Key: []byte("a"), CreateRevision: 1, ModRevision: 43, Version: 1,
			}}},
			start:         "a",
			end:           "z",
			limit:         1,
			expectedCount: -1,
			wantErr:       "invalid MVCC metadata",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			remaining, err := validateExportPage(tc.response, tc.fixed, []byte(tc.start), []byte(tc.end), tc.limit, tc.expectedCount)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRemaining, remaining)
		})
	}
}

func TestExportLeaseRecord(t *testing.T) {
	header := &etcdserverpb.ResponseHeader{Revision: 42}
	tests := map[string]struct {
		response *clientv3.LeaseTimeToLiveResponse
		want     record.Lease
		wantErr  string
	}{
		"current lease": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 123, TTL: 30, GrantedTTL: 60},
			want:     record.Lease{ID: 123, TTL: 30, GrantedTTL: 60},
		},
		"promotion extension": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 123, TTL: 63, GrantedTTL: 60},
			want:     record.Lease{ID: 123, TTL: 63, GrantedTTL: 60},
		},
		"exact maximum": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 123, TTL: clientv3.MaxLeaseTTL, GrantedTTL: clientv3.MaxLeaseTTL},
			want:     record.Lease{ID: 123, TTL: clientv3.MaxLeaseTTL, GrantedTTL: clientv3.MaxLeaseTTL},
		},
		"empty response": {
			wantErr: "empty TTL response",
		},
		"mismatched ID": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 124, TTL: 30, GrantedTTL: 60},
			wantErr:  "mismatched ID 124",
		},
		"missing header": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 123, TTL: 30, GrantedTTL: 60},
			wantErr:  "omitted its header",
		},
		"stale header": {
			response: &clientv3.LeaseTimeToLiveResponse{
				ResponseHeader: &etcdserverpb.ResponseHeader{Revision: 41}, ID: 123, TTL: 30, GrantedTTL: 60,
			},
			wantErr: "revision 41 is behind snapshot revision 42",
		},
		"expired": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 123, TTL: 0, GrantedTTL: 60},
			wantErr:  "expired while exporting snapshot revision 42",
		},
		"missing grant": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 123, TTL: 30},
			wantErr:  "invalid granted TTL 0",
		},
		"oversized grant": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header, ID: 123, TTL: 30, GrantedTTL: clientv3.MaxLeaseTTL + 1},
			wantErr:  "invalid granted TTL 9000000001",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lease, err := exportLeaseRecord(123, tc.response, 42)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, lease)
		})
	}
}

func TestRunReturnsEnvironmentValidationError(t *testing.T) {
	t.Setenv("BATCH_SIZE", "zero")

	require.ErrorContains(t, run(), `invalid BATCH_SIZE: "zero"`)
}
