package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type nativePutRouteRecorder struct {
	BackendShim
	txns, puts int
	ops        []backend.TxnWriteOp
	txnErr     error
}

func (s *nativePutRouteRecorder) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.puts++
	return s.BackendShim.Put(ctx, r)
}

func (s *nativePutRouteRecorder) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prev []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	s.txns++
	s.ops = append([]backend.TxnWriteOp(nil), ops...)
	if s.txnErr != nil {
		return nil, 0, nil, s.txnErr
	}
	return s.BackendShim.TxnApply(ctx, ops, guards, prev)
}

func TestPlainPutNativeTransactionErrorDoesNotRetryOldPath(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	key := []byte("/native-put/no-fallback")
	created, err := s.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)
	want := errors.New("transaction outcome unavailable")
	recorder := &nativePutRouteRecorder{BackendShim: s.backend, txnErr: want}
	s.backend = recorder
	response, err := s.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("new")})
	require.ErrorIs(t, err, want)
	require.Nil(t, response)
	require.Equal(t, 1, recorder.txns)
	require.Zero(t, recorder.puts, "uncertain/error outcomes must not invoke another write path")
	current, err := s.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("old"), current.Kvs[0].Value)
	require.Equal(t, created.Header.Revision, current.Kvs[0].ModRevision)
}

func TestPlainPutUsesNativeTransactionOnlyWithoutPreviousStateOptions(t *testing.T) {
	for _, name := range []string{"plain", "previous", "ignore_value", "ignore_lease"} {
		t.Run(name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			key := []byte("/native-put/options")
			_, err := s.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
			require.NoError(t, err)
			recorder := &nativePutRouteRecorder{BackendShim: s.backend}
			s.backend = recorder
			r := &etcdserverpb.PutRequest{Key: key, Value: []byte("new")}
			switch name {
			case "previous":
				r.PrevKv = true
			case "ignore_value":
				r.IgnoreValue = true
				r.Value = nil
			case "ignore_lease":
				r.IgnoreLease = true
			}
			response, err := s.Put(ctx, r)
			require.NoError(t, err)
			require.NotNil(t, response)
			if name == "plain" {
				require.Equal(t, 1, recorder.txns)
				require.Zero(t, recorder.puts)
				require.Len(t, recorder.ops, 1)
				require.True(t, recorder.ops[0].PrevLeaseKnown)
				require.Zero(t, recorder.ops[0].PrevLease)
				require.True(t, recorder.ops[0].DiscardPrevValue)
			} else {
				require.Zero(t, recorder.txns)
				require.Equal(t, 1, recorder.puts)
			}
		})
	}
}
