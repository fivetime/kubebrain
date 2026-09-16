package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestKVLeaseWriteAdmissionDeadline(t *testing.T) {
	for _, op := range []string{"put", "txn", "delete"} {
		t.Run(op, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			key := []byte("/lease-admission-deadline")
			_, err := s.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("original")})
			require.NoError(t, err)
			before, err := s.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			s.leaseWriteMu.Lock()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var callErr error
				switch op {
				case "put":
					_, callErr = s.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")})
				case "txn":
					_, callErr = s.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("replacement")}}}}})
				case "delete":
					_, callErr = s.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
				}
				done <- callErr
			}()
			// On regression, release the owner and reap the request before failing;
			// never leak a blocked handler into server cleanup.
			select {
			case err = <-done:
				s.leaseWriteMu.Unlock()
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				s.leaseWriteMu.Unlock()
				<-done
				t.Fatal("expired request remained queued behind lease binding owner")
			}
			after, err := s.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Equal(t, before, after, "canceled admission must not mutate data or revision")
			_, err = s.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("live")})
			require.NoError(t, err, "canceled waiter must not retain the lease lock")
		})
	}
}
