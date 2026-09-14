package backend

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type previousConflictContext struct{}
type previousConflictStore struct {
	storage.KvStorage
	target, revisionKey []byte
	armed               atomic.Bool
	indexReads          atomic.Int64
	concurrentWrite     func() error
}

func (s *previousConflictStore) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }
func (s *previousConflictStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if ctx.Value(previousConflictContext{}) == s {
		for _, key := range keys {
			if bytes.Equal(key, s.revisionKey) {
				s.indexReads.Add(1)
			}
		}
	}
	values, err := s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
	if err == nil && ctx.Value(previousConflictContext{}) == s {
		if _, found := values[string(s.target)]; found && s.armed.CompareAndSwap(true, false) {
			if err := s.concurrentWrite(); err != nil {
				return nil, err
			}
		}
	}
	return values, err
}

func (s *previousConflictStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := s.KvStorage.Get(ctx, key)
	if ctx.Value(previousConflictContext{}) == s {
		if bytes.Equal(key, s.revisionKey) {
			s.indexReads.Add(1)
		}
		if err == nil && bytes.Equal(key, s.target) && s.armed.CompareAndSwap(true, false) {
			if err := s.concurrentWrite(); err != nil {
				return nil, err
			}
		}
	}
	return value, err
}

func TestTxnPreviousObjectReadRevisionConflict(t *testing.T) {
	for _, scenario := range []struct{ deleting, multi bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		deleting := scenario.deleting
		name := "update"
		if deleting {
			name = "delete"
		}
		if scenario.multi {
			name += "-batch-previous"
		}
		t.Run(name, func(t *testing.T) {
			raw := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, raw.Close()) })
			store := &previousConflictStore{KvStorage: raw}
			cfg := Config{Prefix: "/previous-conflict", Identity: "previous-conflict", EnableEtcdCompatibility: true}
			metric := mock.NewMinimalMetrics(gomock.NewController(t))
			first := NewBackend(store, cfg, metric).(*backend)
			other := NewBackend(raw, cfg, metric).(*backend)
			first.SetCurrentRevision(100)
			key := []byte("/previous-conflict/key")
			seedOps := []TxnWriteOp{{Key: key, Value: []byte("seed")}}
			if scenario.multi {
				seedOps = append(seedOps, TxnWriteOp{Key: []byte("/previous-conflict/other"), Value: []byte("other")})
			}
			_, seedRev, err := first.TxnApply(context.Background(), seedOps, nil)
			require.NoError(t, err)
			other.SetCurrentRevision(seedRev)
			store.target = first.coder.EncodeObjectKey(key, seedRev)
			store.revisionKey = first.coder.EncodeRevisionKey(key)
			var competingRevision uint64
			store.concurrentWrite = func() error {
				_, competingRevision, err = other.TxnApply(context.Background(), []TxnWriteOp{{Key: key, Value: []byte("concurrent")}}, nil)
				if err != nil {
					return err
				}
				// Controlled test-only delivery of the competing writer's actual
				// committed value; this fixture does not implement peer replication.
				value, readErr := raw.Get(context.Background(), first.coder.EncodeObjectKey(key, competingRevision))
				if readErr != nil {
					return readErr
				}
				notifyTestEvent(first, key, value, competingRevision, seedRev, true, proto.Event_PUT, nil)
				return nil
			}
			store.armed.Store(true)
			ctx := context.WithValue(context.Background(), previousConflictContext{}, store)
			ops := []TxnWriteOp{{Key: key, Value: []byte("final"), Delete: deleting}}
			if scenario.multi {
				ops = append(ops, TxnWriteOp{Key: []byte("/previous-conflict/other"), Value: []byte("other-final")})
			}
			result, revision, err := first.TxnApply(ctx, ops, nil)
			require.NoError(t, err)
			require.Len(t, result, len(ops))
			require.False(t, store.armed.Load())
			require.Equal(t, seedRev+1, competingRevision)
			require.Equal(t, competingRevision+1, revision)
			require.Equal(t, competingRevision, result[0].PrevRevision)
			require.Equal(t, []byte("concurrent"), StripInlineValue(result[0].PrevValue))
			require.EqualValues(t, 2, store.indexReads.Load(), "stale revision must force exactly one fresh preparation")
			value, modRev := liveValue(t, first, context.Background(), key)
			if deleting {
				require.True(t, result[0].Deleted)
				require.Empty(t, value)
			} else {
				require.False(t, result[0].Created)
				require.Equal(t, "final", value)
				require.Equal(t, revision, modRev)
				require.Equal(t, seedRev, result[0].Meta.CreateRevision)
				require.EqualValues(t, 3, result[0].Meta.Version)
			}
		})
	}
}
