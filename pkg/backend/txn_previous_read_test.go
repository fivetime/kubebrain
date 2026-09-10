package backend

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type previousReadMarker struct{}

var _ storage.SnapshotGetter = (*previousReadSpy)(nil)

type previousReadSpy struct {
	storage.KvStorage
	mu      sync.Mutex
	calls   []string
	failure error
}

func (s *previousReadSpy) record(ctx context.Context, call string) bool {
	if ctx.Value(previousReadMarker{}) != s {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
	return true
}
func (s *previousReadSpy) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.record(ctx, "Get:"+string(key)) && s.failure != nil {
		return nil, s.failure
	}
	return s.KvStorage.Get(ctx, key)
}

// This fake asserts snapshot dispatch, NOT historical MVCC fidelity.
func (s *previousReadSpy) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.record(ctx, "GetAt:"+string(key))
	return s.KvStorage.Get(ctx, key)
}

func (s *previousReadSpy) BatchGetAt(ctx context.Context, keys [][]byte, ts uint64) (map[string][]byte, error) {
	s.record(ctx, "BatchGetAt")
	return s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
}

func TestTxnPreviousObjectReadBoundary(t *testing.T) {
	for _, mode := range []string{"current", "pinned-dispatch", "legacy-fallback", "unavailable", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			spy := &previousReadSpy{KvStorage: memkv.NewKvStorage()}
			t.Cleanup(func() { require.NoError(t, spy.Close()) })
			b := NewBackend(spy, Config{Prefix: "/prev-read", Identity: "prev-read", EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
			b.SetCurrentRevision(100)
			key := []byte("/prev-read/key")
			created, err := b.Create(context.Background(), &proto.CreateRequest{Key: key, Value: []byte("value")})
			require.NoError(t, err)
			require.True(t, created.Succeeded)
			revision := created.Header.Revision
			objectKey := b.coder.EncodeObjectKey(key, revision)
			expected, err := spy.KvStorage.Get(context.Background(), objectKey)
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), previousReadMarker{}, spy)
			switch mode {
			case "pinned-dispatch":
				ctx = storage.WithSnapshotTimestamp(ctx, 123)
			case "legacy-fallback":
				batch := spy.BeginBatchWrite()
				batch.Del(b.coder.EncodeRevisionKey(key))
				require.NoError(t, batch.Commit(context.Background()))
				revision++
			case "unavailable":
				spy.failure = storage.ErrUnavailable
			case "cancelled":
				spy.failure = context.Canceled
			}
			value, err := b.readTxnPreviousObject(ctx, key, revision)
			if spy.failure != nil {
				require.ErrorIs(t, err, spy.failure)
				require.Nil(t, value)
			} else {
				require.NoError(t, err)
				require.Equal(t, expected, value)
			}
			spy.mu.Lock()
			calls := append([]string(nil), spy.calls...)
			spy.mu.Unlock()
			switch mode {
			case "pinned-dispatch":
				require.Equal(t, []string{"GetAt:" + string(b.coder.EncodeRevisionKey(key)), "GetAt:" + string(objectKey)}, calls)
			case "legacy-fallback":
				require.Equal(t, []string{"Get:" + string(b.coder.EncodeObjectKey(key, revision)), "Get:" + string(b.coder.EncodeRevisionKey(key))}, calls)
			default:
				require.Equal(t, []string{"Get:" + string(objectKey)}, calls)
			}
		})
	}
}
