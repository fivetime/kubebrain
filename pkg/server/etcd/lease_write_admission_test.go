package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestLeaseRPCWriteAdmissionDeadline(t *testing.T) {
	for _, tc := range []struct {
		name   string
		revoke bool
		reader bool
	}{
		{name: "grant behind exclusive owner"},
		{name: "revoke behind exclusive owner", revoke: true},
		{name: "revoke behind shared owner", revoke: true, reader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			const id = int64(78321)
			key := []byte("/lease-rpc-admission-deadline")
			if tc.revoke {
				_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
				require.NoError(t, err)
				_, err = s.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("original"), Lease: id})
				require.NoError(t, err)
			}
			before, err := s.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			metadataBefore, metadataErr := s.backend.InternalGet(context.Background(), leaseStorageKey(id))
			if tc.revoke {
				require.NoError(t, metadataErr)
			} else {
				require.ErrorIs(t, metadataErr, storage.ErrKeyNotFound)
			}
			unlock := s.leaseWriteMu.Unlock
			if tc.reader {
				s.leaseWriteMu.RLock()
				unlock = s.leaseWriteMu.RUnlock
			} else {
				s.leaseWriteMu.Lock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var callErr error
				if tc.revoke {
					_, callErr = s.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
				} else {
					_, callErr = s.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
				}
				done <- callErr
			}()
			// Reap even the regressed handler before shutting down the server.
			select {
			case err = <-done:
				unlock()
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				unlock()
				<-done
				t.Fatal("expired lease RPC remained queued behind lease binding owner")
			}
			after, err := s.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Equal(t, before, after, "canceled admission must preserve data, binding and revision")
			metadataAfter, err := s.backend.InternalGet(context.Background(), leaseStorageKey(id))
			if tc.revoke {
				require.NoError(t, err)
				require.Equal(t, metadataBefore, metadataAfter, "canceled revoke must preserve durable lease")
			} else {
				require.ErrorIs(t, err, storage.ErrKeyNotFound)
			}
			s.leaseMu.Lock()
			_, active := s.leases[id]
			_, pending := s.pendingLeases[id]
			s.leaseMu.Unlock()
			require.Equal(t, tc.revoke, active)
			require.False(t, pending)
			require.Zero(t, s.leaseTeardowns.Load())
			// Same-ID live operations prove no hidden waiter/pending reservation
			// survives cancellation, and a canceled revoke kept the lease usable.
			if !tc.revoke {
				_, err = s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
				require.NoError(t, err)
			}
			_, err = s.LeaseRevoke(context.Background(), &etcdserverpb.LeaseRevokeRequest{ID: id})
			require.NoError(t, err)
		})
	}
}

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
