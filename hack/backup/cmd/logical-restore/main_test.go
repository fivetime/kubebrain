package main

import (
	"errors"
	"os"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRewriteKey(t *testing.T) {
	require.Equal(t, []byte("/target/a"), rewriteKey([]byte("/source/a"), "/source", "/target"))
	require.Equal(t, []byte("/other/a"), rewriteKey([]byte("/other/a"), "/source", "/target"))
	require.Equal(t, []byte("/source/a"), rewriteKey([]byte("/source/a"), "", "/target"))
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
