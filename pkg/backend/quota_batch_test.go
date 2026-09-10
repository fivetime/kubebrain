package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
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
