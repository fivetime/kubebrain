package main

import (
	"errors"
	"os"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
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
		require.True(t, envBool("ALLOW_OVERWRITE"))
	}

	for _, value := range []string{"", "false", "0", "no", "maybe"} {
		require.NoError(t, os.Setenv("ALLOW_OVERWRITE", value))
		require.False(t, envBool("ALLOW_OVERWRITE"))
	}
}
