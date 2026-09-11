package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
)

type atomicPrefetchRecorder struct {
	values map[string][]byte
	reads  [][]byte
	writes []string
}

func (r *atomicPrefetchRecorder) Get(_ context.Context, key []byte) ([]byte, error) {
	r.reads = append(r.reads, key)
	if value, ok := r.values[string(key)]; ok {
		return value, nil
	}
	return nil, storage.ErrKeyNotFound
}
func (r *atomicPrefetchRecorder) Put(key, value []byte, _ int64) error {
	r.writes = append(r.writes, "put:"+string(key)+":"+string(value))
	r.values[string(key)] = value
	return nil
}
func (r *atomicPrefetchRecorder) Del(key []byte) error {
	r.writes = append(r.writes, "del:"+string(key))
	delete(r.values, string(key))
	return nil
}

type prefetchingAtomicRecorder struct {
	*atomicPrefetchRecorder
	keys [][]byte
	err  error
}

func (r *prefetchingAtomicRecorder) Prefetch(_ context.Context, keys [][]byte) error {
	r.keys = keys
	return r.err
}

func TestTxnAtomicPrefetchPreservesComparisonsAndMutations(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	b.config.QuotaBackendBytes = 100
	guard := corruptAlarmCommitGuard{key: []byte("guard"), exists: true, expected: []byte("clean")}
	guardKey := b.ks.EncodeInternalKey(guard.key)
	quotaKey := b.ks.EncodeInternalKey(quotaUsageKey)
	preps := []txnPrep{
		{op: TxnWriteOp{Key: []byte("new"), Value: []byte("value")}, effective: true, create: true},
		{op: TxnWriteOp{Key: []byte("internal"), Value: []byte("metadata"), Internal: true}, effective: true},
		{op: TxnWriteOp{Key: []byte("absent-delete"), Delete: true}},
	}
	for _, conflict := range []string{"", "corrupt", "quota", "created"} {
		t.Run("conflict="+conflict, func(t *testing.T) {
			newRecorder := func() *atomicPrefetchRecorder {
				r := &atomicPrefetchRecorder{values: map[string][]byte{
					string(guardKey): guard.expected, string(quotaKey): encodeQuotaUsage(0),
				}}
				switch conflict {
				case "corrupt":
					r.values[string(guardKey)] = []byte("changed")
				case "quota":
					r.values[string(quotaKey)] = encodeQuotaUsage(1)
				case "created":
					r.values[string(b.coder.EncodeRevisionKey([]byte("new")))] = []byte("changed")
				}
				return r
			}
			fallback := newRecorder()
			batched := &prefetchingAtomicRecorder{atomicPrefetchRecorder: newRecorder()}
			firstErr := b.stageTxnAtomic(ctx, fallback, append([]txnPrep(nil), preps...), nil, 10, encodeQuotaUsage(0), 8, guard)
			secondErr := b.stageTxnAtomic(ctx, batched, append([]txnPrep(nil), preps...), nil, 10, encodeQuotaUsage(0), 8, guard)
			if conflict == "" {
				require.NoError(t, firstErr)
				require.NoError(t, secondErr)
			} else {
				require.ErrorIs(t, firstErr, storage.ErrCASFailed)
				require.ErrorIs(t, secondErr, storage.ErrCASFailed)
			}
			require.Equal(t, fallback.reads, batched.reads, "prefetch must not remove comparisons")
			require.Equal(t, fallback.writes, batched.writes, "conflict-protecting writes must remain")
			for _, key := range batched.reads {
				require.Contains(t, batched.keys, key)
			}
			if conflict == "" {
				require.ElementsMatch(t, batched.reads, batched.keys)
			}
		})
	}
}

func TestTxnAtomicPrefetchErrorStopsBeforeReadsAndWrites(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	want := errors.New("prefetch failed")
	txn := &prefetchingAtomicRecorder{atomicPrefetchRecorder: &atomicPrefetchRecorder{}, err: want}
	err := b.stageTxnAtomic(ctx, txn, nil, nil, 10, nil, 0, corruptAlarmCommitGuard{key: []byte("guard")})
	require.ErrorIs(t, err, want)
	require.Empty(t, txn.reads)
	require.Empty(t, txn.writes)
}

func TestTxnAtomicReadKeysIncludesMigrationAndDeduplicates(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	key := []byte("migrating")
	revisionKey := b.coder.EncodeRevisionKey(key)
	guard := corruptAlarmCommitGuard{key: []byte("guard")}
	keys := b.txnAtomicReadKeys([]txnPrep{{op: TxnWriteOp{Key: key}, effective: true, migratePrev: true, curRev: 4}},
		[]txnGuardPrep{{key: revisionKey}, {key: revisionKey}}, guard)
	require.ElementsMatch(t, [][]byte{b.ks.EncodeInternalKey(guard.key), revisionKey, b.coder.EncodeObjectKey(key, 4)}, keys)
}
