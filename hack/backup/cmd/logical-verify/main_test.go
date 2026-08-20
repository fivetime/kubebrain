package main

import (
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/keyrewrite"
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

func TestReceiptTargetPrefix(t *testing.T) {
	target, err := receiptTargetPrefix("/source", "", "")
	require.NoError(t, err)
	require.Equal(t, "/source", target)

	target, err = receiptTargetPrefix("/source", "/source", "/target")
	require.NoError(t, err)
	require.Equal(t, "/target", target)

	_, err = receiptTargetPrefix("/source", "/source/subtree", "/target")
	require.ErrorContains(t, err, "to equal artifact prefix")
	_, err = receiptTargetPrefix("/source", "/source", "")
	require.ErrorContains(t, err, "non-empty REWRITE_TO")
}

func TestValidateTargetGetResponse(t *testing.T) {
	header := func(revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{Revision: revision}
	}
	kv := func(key string, modRevision int64) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), CreateRevision: 1, ModRevision: modRevision, Version: 1}
	}
	tests := map[string]struct {
		response *clientv3.GetResponse
		wantRev  int64
		wantErr  string
	}{
		"valid": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 4)}},
			wantRev:  5,
		},
		"empty response": {wantErr: "empty range response"},
		"missing header": {
			response: &clientv3.GetResponse{Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 4)}},
			wantErr:  "no valid response revision",
		},
		"hidden key count": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1},
			wantErr:  "inconsistent count/more metadata",
		},
		"unexpected continuation": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, More: true, Kvs: []*mvccpb.KeyValue{kv("a", 4)}},
			wantErr:  "inconsistent count/more metadata",
		},
		"missing key": {
			response: &clientv3.GetResponse{Header: header(5)},
			wantErr:  "exactly once, got 0",
		},
		"nil key-value": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{nil}},
			wantErr:  "nil key-value",
		},
		"mismatched key": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("b", 4)}},
			wantErr:  `returned mismatched key "b"`,
		},
		"future MVCC revision": {
			response: &clientv3.GetResponse{Header: header(5), Count: 1, Kvs: []*mvccpb.KeyValue{kv("a", 6)}},
			wantErr:  "invalid MVCC metadata",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, revision, err := validateTargetGetResponse(tc.response, []byte("a"), 0)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []byte("a"), got.Key)
			require.Equal(t, tc.wantRev, revision)
		})
	}
}

func TestPinnedTargetReadContract(t *testing.T) {
	require.Empty(t, targetReadOptions(0))
	op := clientv3.OpGet("a", targetReadOptions(7)...)
	require.Equal(t, int64(7), op.Rev())

	valid := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 9}, Count: 1, Kvs: []*mvccpb.KeyValue{{
		Key: []byte("a"), CreateRevision: 1, ModRevision: 7, Version: 1,
	}}}
	_, _, err := validateTargetGetResponse(valid, []byte("a"), 7)
	require.NoError(t, err)

	valid.Header.Revision = 6
	_, _, err = validateTargetGetResponse(valid, []byte("a"), 7)
	require.ErrorContains(t, err, "behind pinned verification revision")

	valid.Header.Revision = 9
	valid.Kvs[0].ModRevision = 8
	_, _, err = validateTargetGetResponse(valid, []byte("a"), 7)
	require.ErrorContains(t, err, "invalid MVCC metadata")
}

func TestValidateTargetLeaseTTLResponse(t *testing.T) {
	header := func(revision int64) *etcdserverpb.ResponseHeader {
		return &etcdserverpb.ResponseHeader{Revision: revision}
	}
	tests := map[string]struct {
		response *clientv3.LeaseTimeToLiveResponse
		wantErr  string
	}{
		"valid": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 20, TTL: 30, GrantedTTL: 60},
		},
		"promotion extension": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 20, TTL: 63, GrantedTTL: 60},
		},
		"empty response": {wantErr: "empty TTL response"},
		"mismatched ID": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 21, TTL: 30, GrantedTTL: 60},
			wantErr:  "expected target ID 20, got 21",
		},
		"missing header": {
			response: &clientv3.LeaseTimeToLiveResponse{ID: 20, TTL: 30, GrantedTTL: 60},
			wantErr:  "no valid response revision",
		},
		"stale revision": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(4), ID: 20, TTL: 30, GrantedTTL: 60},
			wantErr:  "revision 4 behind key observation revision 5",
		},
		"expired": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 20, GrantedTTL: 60},
			wantErr:  "is expired",
		},
		"missing granted TTL": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 20, TTL: 30},
			wantErr:  "invalid granted TTL 0",
		},
		"oversized granted TTL": {
			response: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: header(6), ID: 20, TTL: 30, GrantedTTL: clientv3.MaxLeaseTTL + 1},
			wantErr:  "invalid granted TTL 9000000001",
		},
		"unrequested keys": {
			response: &clientv3.LeaseTimeToLiveResponse{
				ResponseHeader: header(6), ID: 20, TTL: 30, GrantedTTL: 60, Keys: [][]byte{[]byte("a")},
			},
			wantErr: "1 attached keys when none were requested",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateTargetLeaseTTLResponse(10, 20, tc.response, 5)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRunReturnsEnvironmentValidationError(t *testing.T) {
	t.Setenv("REWRITE_FROM", "")
	t.Setenv("REWRITE_TO", "/target")

	require.ErrorContains(t, run(), "REWRITE_TO requires REWRITE_FROM")
}
