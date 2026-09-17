package etcdproxy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ambiguousTxnServer struct {
	etcdserverpb.UnimplementedKVServer
	effects atomic.Int32
	err     error
}

func (s *ambiguousTxnServer) Txn(context.Context, *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// Model an admitted transaction whose side effect precedes a lost result.
	// The forwarding layer cannot infer non-commit from the returned status.
	s.effects.Add(1)
	return nil, s.err
}

// A Kubernetes update can surface LeaderChanged during failover. Replaying an
// ambiguous Txn to hide that error is unsafe, including when a retiring peer
// connection maps an internal Canceled status to LeaderChanged.
func TestTxnDoesNotReplayAmbiguousForwardResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"leader changed", rpctypes.ErrGRPCLeaderChanged, rpctypes.ErrGRPCLeaderChanged},
		{"internal cancellation", status.Error(codes.Canceled, "peer connection retired"), rpctypes.ErrGRPCLeaderChanged},
		{"unavailable", status.Error(codes.Unavailable, "result lost"), status.Error(codes.Unavailable, "result lost")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &ambiguousTxnServer{err: tc.err}
			address := startPutResultServer(t, upstream)
			proxy := NewEtcdProxy(t.Context(), newSwitchingLeaderElection(address), nil, false, 0).(*etcdProxy)
			t.Cleanup(func() { require.NoError(t, proxy.Close()) })
			require.Eventually(t, func() bool { return proxy.Ready() == nil }, 5*time.Second, 10*time.Millisecond)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			response, err := proxy.Txn(ctx, &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: []byte("ambiguous-txn"), Value: []byte("value")},
				}}},
			})
			require.Nil(t, response)
			require.Equal(t, status.Code(tc.want), status.Code(err))
			require.Equal(t, status.Convert(tc.want).Message(), status.Convert(err).Message())
			require.NoError(t, ctx.Err(), "caller must remain live; this is not a deadline test")
			require.Equal(t, int32(1), upstream.effects.Load(), "ambiguous admitted transaction must not be replayed")
		})
	}
}
