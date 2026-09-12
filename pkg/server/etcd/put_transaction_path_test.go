package etcd

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type putPathReadCounter struct {
	backend.Backend
	gets atomic.Int64
}

func (b *putPathReadCounter) Get(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error) {
	b.gets.Add(1)
	return b.Backend.Get(ctx, r)
}

// This compares two existing entrypoints, not a new public Put implementation.
// Counts are adapter-to-backend calls, NOT TiKV RPCs or latency measurements.
// Ignore options, legacy repair, auth changes and contention need separate
// coverage before replacing the public path with an unconditional transaction.
func TestPlainPutTransactionPathDifferential(t *testing.T) {
	newShim := func() (*backendShim, *putPathReadCounter) {
		m := mock.NewMinimalMetrics(gomock.NewController(t))
		raw := backend.NewBackend(memkv.NewKvStorage(), backend.Config{
			Identity: "put-path", EnableEtcdCompatibility: true,
		}, m)
		t.Cleanup(func() { require.NoError(t, raw.(interface{ Close() error }).Close()) })
		raw.SetCurrentRevision(100)
		counter := &putPathReadCounter{Backend: raw}
		return NewBackendShim(counter, m).(*backendShim), counter
	}
	plain, plainReads := newShim()
	transaction, transactionReads := newShim()
	ctx := context.Background()
	key := []byte("/put-path/key")
	for i, value := range []string{"created", "updated", "", "recreated"} {
		if i == 3 {
			left, err := plain.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
			require.NoError(t, err)
			right, err := transaction.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
			require.NoError(t, err)
			require.Equal(t, left, right)
		}
		plainReads.gets.Store(0)
		transactionReads.gets.Store(0)
		left, err := plain.Put(backend.WithPreviousLease(ctx, 0), &etcdserverpb.PutRequest{
			Key: key, Value: []byte(value), PrevKv: true,
		})
		require.NoError(t, err)
		right, rev, results, err := transaction.TxnApply(ctx, []backend.TxnWriteOp{{
			Key: key, Value: []byte(value), PrevLeaseKnown: true, PrevLease: 0,
		}}, nil, []bool{true})
		require.NoError(t, err)
		require.Len(t, right, 1)
		require.Len(t, results, 1)
		require.Equal(t, left, right[0].GetResponsePut())
		require.Equal(t, left.Header.Revision, int64(rev))
		require.Zero(t, transactionReads.gets.Load(), "transaction entrypoint needs no shim pre-read")
		t.Logf("operation=%d plain_adapter_gets=%d transaction_adapter_gets=%d", i,
			plainReads.gets.Load(), transactionReads.gets.Load())
		// Compare both latest state and every retained historical version,
		// including the deletion revision and the recreated lifecycle.
		for revision := int64(101); revision <= int64(rev); revision++ {
			request := &etcdserverpb.RangeRequest{Key: key, Revision: revision}
			leftRange, err := plain.Get(ctx, request)
			require.NoError(t, err)
			rightRange, err := transaction.Get(ctx, request)
			require.NoError(t, err)
			require.Equal(t, leftRange, rightRange)
		}
	}
}
