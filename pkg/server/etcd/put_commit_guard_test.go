package etcd

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type putCommitGuardMarker struct{}

type putCommitGuardStorage struct {
	storage.KvStorage
	armed  atomic.Bool
	change func() error
}

type putCommitGuardBatch struct {
	owner *putCommitGuardStorage
	ops   []func(storage.BatchWrite)
}

func (s *putCommitGuardStorage) BeginBatchWrite() storage.BatchWrite {
	// memkv holds its write lock from BeginBatchWrite through Commit. Defer
	// opening that batch so the competing metadata write is not lock-reentrant.
	return &putCommitGuardBatch{owner: s}
}

func (b *putCommitGuardBatch) PutIfNotExist(k, v []byte, ttl int64) {
	k, v = bytes.Clone(k), bytes.Clone(v)
	b.ops = append(b.ops, func(w storage.BatchWrite) { w.PutIfNotExist(k, v, ttl) })
}
func (b *putCommitGuardBatch) CAS(k, v, old []byte, ttl int64) {
	k, v, old = bytes.Clone(k), bytes.Clone(v), bytes.Clone(old)
	b.ops = append(b.ops, func(w storage.BatchWrite) { w.CAS(k, v, old, ttl) })
}
func (b *putCommitGuardBatch) Put(k, v []byte, ttl int64) {
	k, v = bytes.Clone(k), bytes.Clone(v)
	b.ops = append(b.ops, func(w storage.BatchWrite) { w.Put(k, v, ttl) })
}
func (b *putCommitGuardBatch) Del(k []byte) {
	k = bytes.Clone(k)
	b.ops = append(b.ops, func(w storage.BatchWrite) { w.Del(k) })
}
func (b *putCommitGuardBatch) DelCurrent(it storage.Iter) {
	b.Del(it.Key())
}
func (b *putCommitGuardBatch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.ops = append(b.ops, func(w storage.BatchWrite) { w.Atomic(fn) })
}

func (b *putCommitGuardBatch) Commit(ctx context.Context) error {
	if ctx.Value(putCommitGuardMarker{}) == true && b.owner.armed.CompareAndSwap(true, false) {
		if err := b.owner.change(); err != nil {
			return err
		}
	}
	batch := b.owner.KvStorage.BeginBatchWrite()
	for _, op := range b.ops {
		op(batch)
	}
	return batch.Commit(ctx)
}

// Change the authorization guard after the adapter and backend have prepared
// the write, immediately before storage commit. This tests guard plumbing, not
// token authentication, RBAC policy evaluation, or real TiKV conflict handling.
func TestPutPathsRejectGuardChangedAtCommit(t *testing.T) {
	for _, path := range []string{"plain", "transaction"} {
		for _, state := range []string{"create", "update"} {
			t.Run(path+"/"+state, func(t *testing.T) {
				ctx := context.Background()
				inner := memkv.NewKvStorage()
				kv := &putCommitGuardStorage{KvStorage: inner}
				m := mock.NewMinimalMetrics(gomock.NewController(t))
				raw := backend.NewBackend(kv, backend.Config{Identity: "put-guard", EnableEtcdCompatibility: true}, m)
				t.Cleanup(func() { require.NoError(t, raw.(interface{ Close() error }).Close()) })
				raw.SetCurrentRevision(100)
				shim := NewBackendShim(raw, m).(*backendShim)
				key, guard := []byte("/put-guard/key"), []byte("auth/config")
				old, changed := []byte("authorized-version"), []byte("changed-version")
				require.NoError(t, raw.InternalPut(ctx, guard, old))
				if state == "update" {
					_, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("original")})
					require.NoError(t, err)
				}
				before, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
				require.NoError(t, err)
				baseline := raw.GetCurrentRevision()
				ks, err := coder.NewKeyspace("")
				require.NoError(t, err)
				kv.change = func() error {
					batch := inner.BeginBatchWrite()
					batch.CAS(ks.EncodeInternalKey(guard), changed, old, 0)
					return batch.Commit(ctx)
				}
				write := func(ctx context.Context) (uint64, error) {
					if path == "plain" {
						r, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")})
						if err != nil {
							return 0, err
						}
						return uint64(r.Header.Revision), nil
					}
					_, rev, _, err := shim.TxnApply(ctx, []backend.TxnWriteOp{{Key: key, Value: []byte("replacement")}}, nil, []bool{true})
					return rev, err
				}
				kv.armed.Store(true)
				marked := context.WithValue(ctx, putCommitGuardMarker{}, true)
				_, err = write(backend.WithInternalWriteGuard(marked, guard, old))
				require.ErrorIs(t, err, backend.ErrInternalWriteGuardConflict)
				require.False(t, kv.armed.Load(), "the selected commit hook must execute")
				require.Equal(t, baseline, raw.GetCurrentRevision())
				after, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
				actualGuard, err := raw.InternalGet(ctx, guard)
				require.NoError(t, err)
				require.Equal(t, changed, actualGuard)
				revision, err := write(backend.WithInternalWriteGuard(ctx, guard, changed))
				require.NoError(t, err)
				require.Equal(t, baseline+1, revision, "failed guard cannot consume a durable user revision")
			})
		}
	}
}
