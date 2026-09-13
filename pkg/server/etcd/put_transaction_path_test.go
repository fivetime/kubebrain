package etcd

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type putPathReadCounter struct {
	backend.Backend
	gets         atomic.Int64
	metadataGets atomic.Int64
	metadataErr  error
}

// A plain Put must not turn a read-visible legacy orphan into a new lifecycle.
// This pins a responsibility of the pre-read/Create/Update path before it can
// be replaced by an unconditional transaction. Count-index corruption policy
// is separate; this fixture deliberately exercises the legacy recovery mode.
func TestPlainPutPreservesOrphanLifecycle(t *testing.T) {
	t.Run("return_previous", func(t *testing.T) { checkPlainPutOrphanLifecycle(t, true, false) })
	t.Run("metadata_only", func(t *testing.T) { checkPlainPutOrphanLifecycle(t, false, false) })
}

func TestTransactionPutPreservesOrphanLifecycle(t *testing.T) {
	t.Run("return_previous", func(t *testing.T) { checkPlainPutOrphanLifecycle(t, true, true) })
	t.Run("without_previous", func(t *testing.T) { checkPlainPutOrphanLifecycle(t, false, true) })
}

func checkPlainPutOrphanLifecycle(t *testing.T, previous, transaction bool) {
	t.Helper()
	m := mock.NewMinimalMetrics(gomock.NewController(t))
	kv := memkv.NewKvStorage()
	raw := backend.NewBackend(kv, backend.Config{Identity: "put-orphan", EnableEtcdCompatibility: true}, m)
	t.Cleanup(func() { require.NoError(t, raw.(interface{ Close() error }).Close()) })
	raw.SetCurrentRevision(100)
	shim := NewBackendShim(raw, m).(*backendShim)
	ctx := backend.WithPreviousLease(context.Background(), 0)
	key := []byte("/put-path/orphan")
	created, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)
	ks, err := coder.NewKeyspace("")
	require.NoError(t, err)
	require.NoError(t, kv.Del(ctx, ks.NewCoder().EncodeRevisionKey(key)))
	var updated *etcdserverpb.PutResponse
	if transaction {
		responses, _, _, txnErr := shim.TxnApply(ctx, []backend.TxnWriteOp{{
			Key: key, Value: []byte("new"), PrevLeaseKnown: true,
		}}, nil, []bool{previous})
		require.NoError(t, txnErr)
		require.Len(t, responses, 1)
		updated = responses[0].GetResponsePut()
		require.NotNil(t, updated)
	} else {
		updated, err = shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("new"), PrevKv: previous})
	}
	require.NoError(t, err)
	if previous {
		require.NotNil(t, updated.PrevKv)
		require.Equal(t, []byte("old"), updated.PrevKv.Value)
		require.Equal(t, created.Header.Revision, updated.PrevKv.CreateRevision)
	} else {
		require.Nil(t, updated.PrevKv)
	}
	require.Equal(t, created.Header.Revision+1, updated.Header.Revision)
	current, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("new"), current.Kvs[0].Value)
	require.EqualValues(t, 2, current.Kvs[0].Version)
	require.Equal(t, created.Header.Revision, current.Kvs[0].CreateRevision)
}

func (b *putPathReadCounter) Get(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error) {
	b.gets.Add(1)
	return b.Backend.Get(ctx, r)
}

func (b *putPathReadCounter) GetKeysOnly(ctx context.Context, r *proto.GetRequest) (*proto.GetResponse, error) {
	b.metadataGets.Add(1)
	if b.metadataErr != nil {
		return nil, b.metadataErr
	}
	return b.Backend.(interface {
		GetKeysOnly(context.Context, *proto.GetRequest) (*proto.GetResponse, error)
	}).GetKeysOnly(ctx, r)
}

func TestPlainPutMetadataReadFailureDoesNotFallback(t *testing.T) {
	shim := newBackendShimIgnoreTest(t)
	want := errors.New("metadata snapshot unavailable")
	reads := &putPathReadCounter{Backend: shim.backend, metadataErr: want}
	shim.backend = reads
	revision := reads.GetCurrentRevision()
	_, err := shim.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("/put-path/failed"), Value: []byte("no")})
	require.ErrorIs(t, err, want)
	require.EqualValues(t, 1, reads.metadataGets.Load())
	require.Zero(t, reads.gets.Load(), "a failed metadata snapshot must not silently switch reads")
	require.Equal(t, revision, reads.GetCurrentRevision())
}

// This compares two existing entrypoints, not a new public Put implementation.
// Counts are adapter-to-backend calls, NOT TiKV RPCs or latency measurements.
// Ignore options, legacy repair, auth changes and contention need separate
// coverage before replacing the public path with an unconditional transaction.
func TestPlainPutTransactionPathDifferential(t *testing.T) {
	for _, projection := range []struct {
		name string
		prev bool
	}{{"return_previous", true}, {"discard_previous", false}} {
		for _, lease := range []struct {
			name string
			ids  []int64
		}{{"unleased", []int64{0, 0, 0, 0}}, {"lease_changes", []int64{11, 22, 0, 33}}} {
			t.Run(projection.name+"/"+lease.name, func(t *testing.T) {
				comparePlainPutTransactionPath(t, projection.prev, lease.ids)
			})
		}
	}
}

func comparePlainPutTransactionPath(t *testing.T, prev bool, leases []int64) {
	t.Helper()
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
	var previousLease int64
	for i, value := range []string{"created", "updated", "", "recreated"} {
		if i == 3 {
			left, err := plain.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
			require.NoError(t, err)
			right, err := transaction.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true})
			require.NoError(t, err)
			require.Equal(t, left, right)
			previousLease = 0
		}
		plainReads.gets.Store(0)
		plainReads.metadataGets.Store(0)
		transactionReads.gets.Store(0)
		left, err := plain.Put(backend.WithPreviousLease(ctx, previousLease), &etcdserverpb.PutRequest{
			Key: key, Value: []byte(value), PrevKv: prev, Lease: leases[i],
		})
		require.NoError(t, err)
		right, rev, results, err := transaction.TxnApply(ctx, []backend.TxnWriteOp{{
			Key: key, Value: []byte(value), Lease: leases[i], PrevLeaseKnown: true, PrevLease: previousLease,
		}}, nil, []bool{prev})
		require.NoError(t, err)
		require.Len(t, right, 1)
		require.Len(t, results, 1)
		require.Equal(t, left, right[0].GetResponsePut())
		require.Equal(t, left.Header.Revision, int64(rev))
		if !prev || i == 0 || i == 3 {
			require.Nil(t, left.PrevKv)
		} else {
			require.NotNil(t, left.PrevKv)
			require.Equal(t, previousLease, left.PrevKv.Lease)
		}
		require.Zero(t, transactionReads.gets.Load(), "transaction entrypoint needs no shim pre-read")
		if prev {
			require.EqualValues(t, 1, plainReads.gets.Load())
			require.Zero(t, plainReads.metadataGets.Load())
		} else {
			require.Zero(t, plainReads.gets.Load())
			require.EqualValues(t, 1, plainReads.metadataGets.Load())
		}
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
			if revision == int64(rev) {
				require.Len(t, leftRange.Kvs, 1)
				require.Equal(t, []byte(value), leftRange.Kvs[0].Value)
				require.Equal(t, leases[i], leftRange.Kvs[0].Lease)
				version, created := int64(i+1), int64(101)
				if i == 3 {
					version, created = 1, int64(rev)
				}
				require.Equal(t, version, leftRange.Kvs[0].Version)
				require.Equal(t, created, leftRange.Kvs[0].CreateRevision)
			}
		}
		previousLease = leases[i]
	}
}
