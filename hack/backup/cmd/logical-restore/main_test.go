package main

import (
	"errors"
	"os"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/keyrewrite"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
