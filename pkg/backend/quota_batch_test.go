package backend

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type quotaStatusReadStore struct {
	storage.KvStorage
	values        map[string][]byte
	err           error
	gets, batches int
	keys          [][]byte
	ctx           context.Context
	timestamp     uint64
}

type txnQuotaChangedContextKey struct{}

// Change usage after the index preparation snapshot. Quota admission must use
// the later commit snapshot, not the index snapshot or a cached usage total.
type txnQuotaChangedStore struct {
	storage.KvStorage
	usageKey []byte
	target   []byte
	armed    atomic.Bool
	reads    atomic.Int32
}

func (s *txnQuotaChangedStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	values, err := s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
	if err != nil || ctx.Value(txnQuotaChangedContextKey{}) != s {
		return values, err
	}
	for _, key := range keys {
		if !bytes.Equal(key, s.target) {
			continue
		}
		s.reads.Add(1)
		if s.armed.CompareAndSwap(true, false) {
			batch := s.KvStorage.BeginBatchWrite()
			batch.Put(s.usageKey, encodeQuotaUsage(40), 0)
			if err := batch.Commit(ctx); err != nil {
				return nil, err
			}
		}
		break
	}
	return values, nil
}

func TestTxnQuotaCommitSnapshotIncludesConcurrentUsage(t *testing.T) {
	store := &txnQuotaChangedStore{KvStorage: memkv.NewKvStorage()}
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
		QuotaBackendBytes: 100,
	}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
	b.SetCurrentRevision(100)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	store.usageKey = b.ks.EncodeInternalKey(quotaUsageKey)
	store.target = b.coder.EncodeRevisionKey([]byte("k"))
	store.armed.Store(true)
	ctx := context.WithValue(context.Background(), txnQuotaChangedContextKey{}, store)
	results, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("k"), Value: []byte("v")}}, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.EqualValues(t, 101, revision, "usage change must not consume another public revision")
	usage, quota, alarm, err := b.QuotaStatus(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 42, usage, "must retain concurrent usage and add this key/value, not overwrite with stale total 2")
	require.EqualValues(t, 100, quota)
	require.False(t, alarm)
	require.EqualValues(t, 1, store.reads.Load(), "commit snapshot must include concurrent usage without re-preparing")
	require.False(t, store.armed.Load(), "the concurrent usage write must actually execute")
}

func (s *quotaStatusReadStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.gets++
	s.ctx = ctx
	if s.err != nil {
		return nil, s.err
	}
	value, ok := s.values[string(key)]
	if !ok {
		return nil, storage.ErrKeyNotFound
	}
	return value, nil
}

func (s *quotaStatusReadStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	s.batches++
	s.keys, s.ctx = keys, ctx
	return s.values, s.err
}

func (s *quotaStatusReadStore) GetAt(ctx context.Context, key []byte, timestamp uint64) ([]byte, error) {
	s.timestamp = timestamp
	return s.Get(ctx, key)
}

func (s *quotaStatusReadStore) BatchGetAt(context.Context, [][]byte, uint64) (map[string][]byte, error) {
	panic("quota status must retain pinned point-read path")
}

func TestQuotaStatusBatchMatchesFallback(t *testing.T) {
	for _, tc := range []struct {
		name            string
		tracking, usage []byte
		alarm           bool
		readErr         error
	}{
		{name: "clean", tracking: quotaTrackingClean, usage: encodeQuotaUsage(7)},
		{name: "sticky alarm", tracking: quotaTrackingClean, usage: encodeQuotaUsage(7), alarm: true},
		{name: "missing tracking", usage: encodeQuotaUsage(7)},
		{name: "dirty tracking", tracking: quotaTrackingDirty, usage: encodeQuotaUsage(7)},
		{name: "missing usage", tracking: quotaTrackingClean},
		{name: "malformed usage", tracking: quotaTrackingClean, usage: []byte{9}},
		{name: "tracking validation first", tracking: quotaTrackingDirty, usage: []byte{9}},
		{name: "cancellation", readErr: context.Canceled},
		{name: "backend unavailable", readErr: storage.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, ctx := newQuotaBackend(t, 100)
			// QuotaStatus needs no workers. Keep the initialized backend untouched;
			// replacing its store would race its checkpoint worker.
			b := &backend{config: base.config, ks: base.ks, kv: base.kv, metricCli: base.metricCli}
			keys := [][]byte{b.ks.EncodeInternalKey(quotaTrackingKey), b.ks.EncodeInternalKey(quotaUsageKey), b.ks.EncodeInternalKey(quotaAlarmKey)}
			values := map[string][]byte{}
			if tc.tracking != nil {
				values[string(keys[0])] = tc.tracking
			}
			if tc.usage != nil {
				values[string(keys[1])] = tc.usage
			}
			if tc.alarm {
				values[string(keys[2])] = []byte{}
			}
			s := &quotaStatusReadStore{KvStorage: b.kv, values: values, err: tc.readErr}
			// Hide only optional capabilities to exercise the legacy path.
			b.kv = &struct{ storage.KvStorage }{s}
			wantUsage, wantQuota, wantAlarm, wantErr := b.QuotaStatus(ctx)
			s.gets = 0
			b.kv = s
			usage, quota, alarm, err := b.QuotaStatus(ctx)
			require.Equal(t, wantUsage, usage)
			require.Equal(t, wantQuota, quota)
			require.Equal(t, wantAlarm, alarm)
			if wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, wantErr.Error(), err.Error())
				if tc.readErr != nil {
					require.True(t, errors.Is(err, tc.readErr))
				}
			}
			require.Zero(t, s.gets)
			require.Equal(t, 1, s.batches)
			require.Equal(t, keys, s.keys)
			require.Equal(t, ctx, s.ctx)
		})
	}
}

func TestQuotaStatusBatchPreservesPinnedReads(t *testing.T) {
	base, ctx := newQuotaBackend(t, 100)
	b := &backend{config: base.config, ks: base.ks, kv: base.kv, metricCli: base.metricCli}
	s := &quotaStatusReadStore{KvStorage: b.kv, values: map[string][]byte{
		string(b.ks.EncodeInternalKey(quotaTrackingKey)): quotaTrackingClean,
		string(b.ks.EncodeInternalKey(quotaUsageKey)):    encodeQuotaUsage(7),
	}}
	b.kv = s
	ctx = storage.WithSnapshotTimestamp(ctx, 123)
	usage, quota, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(7), usage)
	require.Equal(t, int64(100), quota)
	require.False(t, alarm)
	require.Equal(t, 3, s.gets)
	require.Zero(t, s.batches)
	require.Equal(t, uint64(123), s.timestamp)
}

func TestTxnQuotaStateBatchMatchesFallback(t *testing.T) {
	for _, tc := range []struct {
		name            string
		tracking, usage []byte
		alarm           bool
		readErr         error
	}{
		{name: "clean", tracking: quotaTrackingClean, usage: encodeQuotaUsage(7)},
		{name: "sticky alarm", tracking: quotaTrackingClean, usage: encodeQuotaUsage(7), alarm: true},
		{name: "alarm before invalid metadata", tracking: quotaTrackingDirty, usage: []byte{9}, alarm: true},
		{name: "missing tracking", usage: encodeQuotaUsage(7)},
		{name: "dirty tracking", tracking: quotaTrackingDirty, usage: encodeQuotaUsage(7)},
		{name: "missing usage", tracking: quotaTrackingClean},
		{name: "malformed usage", tracking: quotaTrackingClean, usage: []byte{9}},
		{name: "cancellation", readErr: context.Canceled},
		{name: "unavailable", readErr: storage.ErrUnavailable},
	} {
		for _, hasPut := range []bool{false, true} {
			name := tc.name + "/delete"
			if hasPut {
				name = tc.name + "/put"
			}
			t.Run(name, func(t *testing.T) {
				base, ctx := newQuotaBackend(t, 100)
				b := &backend{config: base.config, ks: base.ks}
				keys := [][]byte{b.ks.EncodeInternalKey(quotaTrackingKey), b.ks.EncodeInternalKey(quotaUsageKey)}
				if hasPut {
					keys = append(keys, b.ks.EncodeInternalKey(quotaAlarmKey))
				}
				values := map[string][]byte{}
				if tc.tracking != nil {
					values[string(keys[0])] = tc.tracking
				}
				if tc.usage != nil {
					values[string(keys[1])] = tc.usage
				}
				if tc.alarm {
					values[string(b.ks.EncodeInternalKey(quotaAlarmKey))] = []byte{}
				}
				s := &quotaStatusReadStore{KvStorage: base.kv, values: values, err: tc.readErr}
				b.kv = &struct{ storage.KvStorage }{s}
				wantRaw, wantUsage, wantErr := b.readTxnQuotaState(ctx, hasPut)
				s.gets = 0
				b.kv = s
				raw, usage, err := b.readTxnQuotaState(ctx, hasPut)
				require.Equal(t, wantRaw, raw)
				require.Equal(t, wantUsage, usage)
				if wantErr == nil {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Equal(t, wantErr.Error(), err.Error())
				}
				if tc.readErr != nil {
					require.ErrorIs(t, err, tc.readErr)
				}
				if tc.readErr == nil && !(tc.alarm && hasPut) {
					switch tc.name {
					case "clean", "sticky alarm":
						require.NoError(t, err)
						require.EqualValues(t, 7, usage)
					case "missing tracking", "dirty tracking", "missing usage", "alarm before invalid metadata":
						require.ErrorIs(t, err, ErrQuotaUninitialized)
					case "malformed usage":
						require.ErrorIs(t, err, ErrInvalidQuotaMetadata)
					}
				}
				if tc.readErr == nil && tc.alarm && hasPut {
					require.ErrorIs(t, err, ErrNoSpace)
				}
				if tc.name == "sticky alarm" && !hasPut {
					require.NoError(t, err)
					require.EqualValues(t, 7, usage)
				}
				require.Zero(t, s.gets, "batch failure must not fall back to point reads")
				require.Equal(t, 1, s.batches)
				require.Equal(t, keys, s.keys)
				require.Equal(t, ctx, s.ctx)
			})
		}
	}
}

func TestTxnQuotaStatePreservesDisabledAndPinnedReads(t *testing.T) {
	base, ctx := newQuotaBackend(t, 100)
	for _, enabled := range []bool{false, true} {
		for _, hasPut := range []bool{false, true} {
			b := &backend{config: base.config, ks: base.ks}
			if !enabled {
				b.config.QuotaBackendBytes = 0
			}
			s := &quotaStatusReadStore{KvStorage: base.kv, values: map[string][]byte{
				string(b.ks.EncodeInternalKey(quotaTrackingKey)): quotaTrackingClean,
				string(b.ks.EncodeInternalKey(quotaUsageKey)):    encodeQuotaUsage(7),
			}}
			b.kv = s
			readCtx := ctx
			if enabled {
				readCtx = storage.WithSnapshotTimestamp(ctx, 123)
			}
			raw, usage, err := b.readTxnQuotaState(readCtx, hasPut)
			require.NoError(t, err)
			wantGets := 0
			if hasPut {
				wantGets++
			}
			if enabled {
				wantGets += 2
				require.EqualValues(t, 7, usage)
				require.Equal(t, encodeQuotaUsage(7), raw)
			} else {
				require.Nil(t, raw)
				require.Zero(t, usage)
			}
			require.Equal(t, wantGets, s.gets)
			require.Zero(t, s.batches)
		}
	}
}
