package main

import (
	"errors"
	"os"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/keyrewrite"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRewriteKey(t *testing.T) {
	require.Equal(t, []byte("/target/a"), keyrewrite.Rewrite([]byte("/source/a"), "/source", "/target"))
	require.Equal(t, []byte("/other/a"), keyrewrite.Rewrite([]byte("/other/a"), "/source", "/target"))
	require.Equal(t, []byte("/source/a"), keyrewrite.Rewrite([]byte("/source/a"), "", "/target"))
}

func TestValidateLeaseReference(t *testing.T) {
	require.NoError(t, validateLeaseReference(record.Record{}, nil))
	require.NoError(t, validateLeaseReference(record.Record{Lease: 123}, map[int64]int64{123: 30}))
	err := validateLeaseReference(record.Record{Lease: 123}, nil)
	require.ErrorContains(t, err, "unrestorable lease 123")
}

func TestRestorableLeaseTTL(t *testing.T) {
	tests := map[string]struct {
		lease record.Lease
		want  int64
	}{
		"remaining checkpoint": {
			lease: record.Lease{TTL: 30, GrantedTTL: 60},
			want:  30,
		},
		"promotion extension": {
			lease: record.Lease{TTL: 63, GrantedTTL: 60},
			want:  60,
		},
		"legacy remaining": {
			lease: record.Lease{TTL: 30},
			want:  30,
		},
		"legacy above etcd maximum": {
			lease: record.Lease{TTL: clientv3.MaxLeaseTTL + 1},
			want:  clientv3.MaxLeaseTTL,
		},
		"exact etcd maximum": {
			lease: record.Lease{TTL: clientv3.MaxLeaseTTL, GrantedTTL: clientv3.MaxLeaseTTL},
			want:  clientv3.MaxLeaseTTL,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, restorableLeaseTTL(tc.lease))
		})
	}
}

func TestValidateRestoredLeaseGrant(t *testing.T) {
	header := &etcdserverpb.ResponseHeader{Revision: 1}
	tests := map[string]struct {
		response *clientv3.LeaseGrantResponse
		wantErr  string
	}{
		"exact TTL": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: header, ID: 10, TTL: 30},
		},
		"server-chosen longer TTL": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: header, ID: -10, TTL: 31},
		},
		"empty response": {
			wantErr: "empty lease grant response",
		},
		"zero ID": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: header, TTL: 30},
			wantErr:  "zero lease ID",
		},
		"missing header": {
			response: &clientv3.LeaseGrantResponse{ID: 10, TTL: 30},
			wantErr:  "omitted a valid header",
		},
		"zero header revision": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: &etcdserverpb.ResponseHeader{}, ID: 10, TTL: 30},
			wantErr:  "omitted a valid header",
		},
		"legacy error": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: header, ID: 10, TTL: 30, Error: "failed"},
			wantErr:  "legacy error",
		},
		"TTL below request": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: header, ID: 10, TTL: 29},
			wantErr:  "TTL 29 below requested TTL 30",
		},
		"TTL above maximum": {
			response: &clientv3.LeaseGrantResponse{ResponseHeader: header, ID: 10, TTL: clientv3.MaxLeaseTTL + 1},
			wantErr:  "above maximum 9000000000",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateRestoredLeaseGrant(1, 30, tc.response, make(map[clientv3.LeaseID]int64))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}

	seen := map[clientv3.LeaseID]int64{10: 1}
	err := validateRestoredLeaseGrant(2, 30, &clientv3.LeaseGrantResponse{
		ResponseHeader: header, ID: 10, TTL: 30,
	}, seen)
	require.ErrorContains(t, err, "reused for source leases 1 and 2")
}

func TestValidateRestorePutTxnResponse(t *testing.T) {
	header := func(revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{Revision: revision}
	}
	put := func(revision int64) *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{
			ResponsePut: &etcdserverpb.PutResponse{Header: header(revision)},
		}}
	}
	tests := map[string]struct {
		response *clientv3.TxnResponse
		expected int
		wantRev  int64
		wantErr  string
	}{
		"valid": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{put(5), put(5)}},
			expected: 2,
			wantRev:  5,
		},
		"empty response": {
			expected: 1,
			wantErr:  "empty transaction response",
		},
		"compare failed": {
			response: &clientv3.TxnResponse{Header: header(5)},
			expected: 1,
			wantErr:  "refusing to overwrite",
		},
		"missing header": {
			response: &clientv3.TxnResponse{Succeeded: true, Responses: []*etcdserverpb.ResponseOp{put(5)}},
			expected: 1,
			wantErr:  "without a valid response revision",
		},
		"wrong response count": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{put(5)}},
			expected: 2,
			wantErr:  "1 responses for 2 puts",
		},
		"nil operation": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil}},
			expected: 1,
			wantErr:  "response 0 is not a put response",
		},
		"wrong operation type": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{Header: header(5)}},
			}}},
			expected: 1,
			wantErr:  "response 0 is not a put response",
		},
		"missing put header": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{put(0)}},
			expected: 1,
			wantErr:  "put response 0 has revision 0",
		},
		"mismatched put revision": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{put(4)}},
			expected: 1,
			wantErr:  "put response 0 has revision 4, transaction revision is 5",
		},
		"unrequested previous key": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{
					Header: header(5), PrevKv: &mvccpb.KeyValue{Key: []byte("a")},
				}},
			}}},
			expected: 1,
			wantErr:  "unrequested previous key",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			revision, err := validateRestorePutTxnResponse(tc.response, tc.expected)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRev, revision)
		})
	}
}

func TestValidateRestorePreflightTxnResponse(t *testing.T) {
	header := func(revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{Revision: revision}
	}
	ranged := func(revision, count int64, more bool, kvs ...*mvccpb.KeyValue) *etcdserverpb.ResponseOp {
		return &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{
			ResponseRange: &etcdserverpb.RangeResponse{Header: header(revision), Count: count, More: more, Kvs: kvs},
		}}
	}
	tests := map[string]struct {
		response *clientv3.TxnResponse
		keys     []string
		wantErr  string
	}{
		"empty target": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(5, 0, false)}},
			keys:     []string{"a"},
		},
		"previous revision window": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(4, 0, false)}},
			keys:     []string{"a"},
		},
		"empty transaction response": {keys: []string{"a"}, wantErr: "empty transaction response"},
		"failure branch": {
			response: &clientv3.TxnResponse{Header: header(5), Responses: []*etcdserverpb.ResponseOp{ranged(5, 0, false)}},
			keys:     []string{"a"},
			wantErr:  "failure branch",
		},
		"missing outer header": {
			response: &clientv3.TxnResponse{Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(5, 0, false)}},
			keys:     []string{"a"},
			wantErr:  "no valid response revision",
		},
		"wrong response count": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true},
			keys:     []string{"a"},
			wantErr:  "0 responses for 1 keys",
		},
		"nil operation": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{nil}},
			keys:     []string{"a"},
			wantErr:  "not a range response",
		},
		"wrong operation type": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: header(5)}},
			}}},
			keys:    []string{"a"},
			wantErr: "not a range response",
		},
		"missing range header": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{}},
			}}},
			keys:    []string{"a"},
			wantErr: "revision 0 outside",
		},
		"stale range revision": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(3, 0, false)}},
			keys:     []string{"a"},
			wantErr:  "outside transaction revision window [4,5]",
		},
		"future range revision": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(6, 0, false)}},
			keys:     []string{"a"},
			wantErr:  "outside transaction revision window [4,5]",
		},
		"hidden existing key": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(5, 1, false)}},
			keys:     []string{"a"},
			wantErr:  "inconsistent count/more metadata",
		},
		"unexpected continuation": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{ranged(5, 0, true)}},
			keys:     []string{"a"},
			wantErr:  "inconsistent count/more metadata",
		},
		"existing key": {
			response: &clientv3.TxnResponse{Header: header(5), Succeeded: true, Responses: []*etcdserverpb.ResponseOp{
				ranged(5, 1, false, &mvccpb.KeyValue{Key: []byte("a")}),
			}},
			keys:    []string{"a"},
			wantErr: "refusing to overwrite existing key \"a\"",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateRestorePreflightTxnResponse(tc.response, tc.keys)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateBatchSize(t *testing.T) {
	require.NoError(t, validateBatchSize(128, 128))
	require.ErrorContains(t, validateBatchSize(129, 128), "exceeds")
	require.ErrorContains(t, validateBatchSize(1, 0), "must be positive")
}

func TestRollbackCommittedBatchesRunsInReverseOrder(t *testing.T) {
	batches := []committedBatch{
		{keys: []string{"a"}, revision: 10},
		{keys: []string{"b"}, revision: 11},
		{keys: []string{"c"}, revision: 12},
	}
	var revisions []int64
	require.NoError(t, rollbackCommittedBatches(batches, func(batch committedBatch) error {
		revisions = append(revisions, batch.revision)
		return nil
	}))
	require.Equal(t, []int64{12, 11, 10}, revisions)
}

func TestRollbackCommittedBatchesStopsOnConflict(t *testing.T) {
	batches := []committedBatch{
		{keys: []string{"a"}, revision: 10},
		{keys: []string{"b"}, revision: 11},
	}
	var revisions []int64
	err := rollbackCommittedBatches(batches, func(batch committedBatch) error {
		revisions = append(revisions, batch.revision)
		return errors.New("changed")
	})
	require.ErrorContains(t, err, "rollback batch 1 at revision 11")
	require.Equal(t, []int64{11}, revisions)
}

func TestEnvBool(t *testing.T) {
	old := os.Getenv("ALLOW_OVERWRITE")
	defer os.Setenv("ALLOW_OVERWRITE", old)

	for _, value := range []string{"true", "TRUE", "1", "yes", " yes "} {
		require.NoError(t, os.Setenv("ALLOW_OVERWRITE", value))
		parsed, err := envBool("ALLOW_OVERWRITE")
		require.NoError(t, err)
		require.True(t, parsed)
	}

	for _, value := range []string{"", "false", "FALSE", "0", "no", " no "} {
		require.NoError(t, os.Setenv("ALLOW_OVERWRITE", value))
		parsed, err := envBool("ALLOW_OVERWRITE")
		require.NoError(t, err)
		require.False(t, parsed)
	}

	require.NoError(t, os.Setenv("ALLOW_OVERWRITE", "maybe"))
	parsed, err := envBool("ALLOW_OVERWRITE")
	require.ErrorContains(t, err, "ALLOW_OVERWRITE must be a boolean")
	require.False(t, parsed)
}

func TestRunReturnsEnvironmentValidationError(t *testing.T) {
	t.Setenv("BATCH_SIZE", "16")
	t.Setenv("MAX_TXN_OPS", "16")
	t.Setenv("REWRITE_FROM", "")
	t.Setenv("REWRITE_TO", "/target")

	require.ErrorContains(t, run(), "REWRITE_TO requires REWRITE_FROM")
}
