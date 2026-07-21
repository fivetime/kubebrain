package compat

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseRenewalSoakAcrossRepeatedLeaderFailover keeps multiple clientv3
// keepalive loops live across repeated leader replacement. The command must
// discover and delete the current leader on every invocation.
func TestLeaseRenewalSoakAcrossRepeatedLeaderFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	const (
		clientCount     = 8
		leasesPerClient = 8
		failoverCycles  = 3
		leaseTTL        = 30
	)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	keepAliveCtx, stopKeepAlives := context.WithCancel(ctx)
	defer stopKeepAlives()

	clients := make([]*clientv3.Client, clientCount)
	for i := range clients {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{compatEndpoint()},
			DialTimeout: 3 * time.Second,
		})
		require.NoError(t, err)
		clients[i] = cli
	}
	defer func() {
		stopKeepAlives()
		for _, cli := range clients {
			require.NoError(t, cli.Close())
		}
	}()

	prefix := fmt.Sprintf("/dbaas-lease-renewal-soak/%d/", time.Now().UnixNano())
	type liveLease struct {
		id        clientv3.LeaseID
		key       string
		responses atomic.Int64
	}
	leases := make([]*liveLease, 0, clientCount*leasesPerClient)
	errs := make(chan error, clientCount*leasesPerClient)
	var readers sync.WaitGroup

	for clientIndex, cli := range clients {
		for leaseIndex := range leasesPerClient {
			grant, err := cli.Grant(ctx, leaseTTL)
			require.NoError(t, err)
			key := fmt.Sprintf("%s%02d-%02d", prefix, clientIndex, leaseIndex)
			_, err = cli.Put(ctx, key, "live", clientv3.WithLease(grant.ID))
			require.NoError(t, err)
			live := &liveLease{id: grant.ID, key: key}
			leases = append(leases, live)

			responses, err := cli.KeepAlive(keepAliveCtx, grant.ID)
			require.NoError(t, err)
			readers.Add(1)
			go func() {
				defer readers.Done()
				for response := range responses {
					if response == nil || response.ID != live.id || response.TTL <= 0 {
						select {
						case errs <- fmt.Errorf("lease %d returned invalid keepalive response: %#v", live.id, response):
						default:
						}
						return
					}
					live.responses.Add(1)
				}
				if keepAliveCtx.Err() == nil {
					select {
					case errs <- fmt.Errorf("lease %d keepalive channel closed while soak was active", live.id):
					default:
					}
				}
			}()
		}
	}

	counts := make([]*atomic.Int64, len(leases))
	for i := range leases {
		counts[i] = &leases[i].responses
	}
	require.NoError(t, waitForLeaseResponses(ctx, errs, counts, nil, 20*time.Second),
		"every lease must receive an initial keepalive response")

	for cycle := 1; cycle <= failoverCycles; cycle++ {
		output, err := exec.CommandContext(ctx, "bash", "-c", failoverCommand).CombinedOutput()
		require.NoErrorf(t, err, "failover cycle %d command: %s", cycle, strings.TrimSpace(string(output)))
		output, err = waitForKubeBrainRollout(ctx, namespace)
		require.NoErrorf(t, err, "failover cycle %d recovery: %s", cycle, strings.TrimSpace(string(output)))

		baseline := make([]int64, len(leases))
		for i := range leases {
			baseline[i] = leases[i].responses.Load()
		}
		require.NoErrorf(t, waitForLeaseResponses(ctx, errs, counts, baseline, 30*time.Second),
			"all leases must receive a fresh keepalive response after failover cycle %d", cycle)

		got, err := clients[0].Get(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, got.Kvs, len(leases))
		expectedLeases := make(map[string]clientv3.LeaseID, len(leases))
		for _, live := range leases {
			expectedLeases[live.key] = live.id
		}
		for _, kv := range got.Kvs {
			require.Equal(t, expectedLeases[string(kv.Key)], clientv3.LeaseID(kv.Lease))
		}
	}

	stopKeepAlives()
	readers.Wait()
	select {
	case keepAliveErr := <-errs:
		require.NoError(t, keepAliveErr)
	default:
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	for i := range leases {
		_, err := clients[i%len(clients)].Revoke(cleanupCtx, leases[i].id)
		require.NoError(t, err)
	}
	for i := range leases {
		response, err := clients[i%len(clients)].TimeToLive(cleanupCtx, leases[i].id)
		require.NoError(t, err)
		require.Equal(t, int64(-1), response.TTL, "lease %d remained live after soak cleanup", leases[i].id)
	}
	listed, err := clients[0].Leases(cleanupCtx)
	require.NoError(t, err)
	owned := make(map[clientv3.LeaseID]struct{}, len(leases))
	for _, lease := range leases {
		owned[lease.id] = struct{}{}
	}
	for _, lease := range listed.Leases {
		_, belongsToSoak := owned[lease.ID]
		require.False(t, belongsToSoak, "lease %d remained listed after soak cleanup", lease.ID)
	}
	got, err := clients[0].Get(cleanupCtx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, got.Kvs)
}

func waitForLeaseResponses(
	ctx context.Context,
	errs <-chan error,
	counts []*atomic.Int64,
	baseline []int64,
	timeout time.Duration,
) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-errs:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for fresh responses from all %d leases", len(counts))
		case <-ticker.C:
			allFresh := true
			for i, count := range counts {
				want := int64(0)
				if baseline != nil {
					want = baseline[i]
				}
				if count.Load() <= want {
					allFresh = false
					break
				}
			}
			if allFresh {
				return nil
			}
		}
	}
}
