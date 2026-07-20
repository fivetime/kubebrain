package etcd

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	clientproto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type fetchedRowCountingStorage struct {
	storage.KvStorage
	fetched atomic.Int64
}

func (s *fetchedRowCountingStorage) Iter(
	ctx context.Context, start, end []byte, timestamp uint64, limit uint64,
) (storage.Iter, error) {
	iter, err := s.KvStorage.Iter(ctx, start, end, timestamp, limit)
	if err != nil {
		return nil, err
	}
	return &fetchedRowCountingIter{Iter: iter, fetched: &s.fetched}, nil
}

type fetchedRowCountingIter struct {
	storage.Iter
	fetched *atomic.Int64
}

func (i *fetchedRowCountingIter) Next(ctx context.Context) error {
	err := i.Iter.Next(ctx)
	if err == nil {
		i.fetched.Add(1)
	}
	return err
}

func TestKeysOnlyLimitedRangeBoundsStorageReadsAndReturnsTotalCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	metricsClient := mock.NewMinimalMetrics(ctrl)
	rawStorage := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, rawStorage.Close()) })
	countingStorage := &fetchedRowCountingStorage{KvStorage: rawStorage}
	rawBackend := backend.NewBackend(countingStorage, backend.Config{
		Prefix:                  "/range-keys-limit-system",
		Identity:                "range-keys-limit",
		EnableEtcdCompatibility: true,
		EnableCountIndex:        true,
	}, metricsClient)
	rawBackend.SetCurrentRevision(uint64(time.Now().UnixNano()))

	ctx := context.Background()
	const keyCount = 100
	prefix := "/range-keys-limit/items/"
	for i := 0; i < keyCount; i++ {
		response, err := rawBackend.Update(ctx, &clientproto.UpdateRequest{
			Kv: &clientproto.KeyValue{
				Key:   []byte(fmt.Sprintf("%s%03d", prefix, i)),
				Value: []byte("large-value-that-must-not-be-materialized-beyond-the-page"),
			},
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return rawBackend.GetCurrentRevision() >= response.Header.Revision
		}, 5*time.Second, time.Millisecond)
	}
	require.NoError(t, rawBackend.RebuildCountIndex(ctx))
	countingStorage.fetched.Store(0)

	shim := NewBackendShim(rawBackend, metricsClient)
	response, err := shim.List(ctx, &etcdserverpb.RangeRequest{
		Key:        []byte(prefix),
		RangeEnd:   prefixEnd([]byte(prefix)),
		Limit:      3,
		KeysOnly:   true,
		SortOrder:  etcdserverpb.RangeRequest_ASCEND,
		SortTarget: etcdserverpb.RangeRequest_KEY,
	})
	require.NoError(t, err)
	require.Len(t, response.Kvs, 3)
	require.Equal(t, int64(keyCount), response.Count)
	require.True(t, response.More)
	for _, kv := range response.Kvs {
		require.Empty(t, kv.Value)
	}
	require.LessOrEqual(t, countingStorage.fetched.Load(), int64(10),
		"a three-key page may read only Limit+1 rows plus constant boundary probes; total count must come from the index")
}
