package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestLeaseReadStateAdmissionDeadline(t *testing.T) {
	for _, op := range []string{"ttl", "ttl-keys", "list"} {
		t.Run(op, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			const id = int64(78324)
			_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			require.NoError(t, err)
			read := func(ctx context.Context) error {
				if op == "list" {
					_, err := s.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
					return err
				}
				_, err := s.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: op == "ttl-keys"})
				return err
			}
			s.leaseMu.Lock()
			before := s.leases[id].deadline
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- read(ctx) }()
			select {
			case err = <-done:
				s.leaseMu.Unlock()
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				s.leaseMu.Unlock()
				<-done // Reap the regressed handler before server shutdown.
				t.Fatal("expired lease read stayed queued behind state lock")
			}
			s.leaseMu.Lock()
			after := s.leases[id].deadline
			s.leaseMu.Unlock()
			require.Equal(t, before, after, "read admission must not renew the lease")
			follow, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			require.NoError(t, read(follow), "canceled waiter must not retain ownership")
		})
	}
}
