package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLeaseFailoverSpreadsRecoveredExpiryPileup(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const leaseCount = 1200
	baseID := time.Now().UnixNano() & 0x3fffffffffffffff
	ids := make([]int64, leaseCount)
	for i := range ids {
		ids[i] = baseID + int64(i)
		_, err = rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: ids[i], TTL: 600})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupCli, cleanupErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{compatEndpoint(t)},
			DialTimeout: 3 * time.Second,
		})
		require.NoError(t, cleanupErr)
		defer func() { require.NoError(t, cleanupCli.Close()) }()
		cleanupLease := etcdserverpb.NewLeaseClient(cleanupCli.ActiveConnection())
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		var wg sync.WaitGroup
		work := make(chan int64)
		errs := make(chan error, len(ids))
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range work {
					for cleanupCtx.Err() == nil {
						_, revokeErr := cleanupLease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
						if revokeErr == nil || status.Code(revokeErr) == codes.NotFound {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					if cleanupCtx.Err() != nil {
						errs <- fmt.Errorf("revoke lease %d: %w", id, cleanupCtx.Err())
					}
				}
			}()
		}
		for _, id := range ids {
			work <- id
		}
		close(work)
		wg.Wait()
		close(errs)
		for cleanupErr := range errs {
			require.NoError(t, cleanupErr)
		}
		listed, listErr := cleanupLease.LeaseLeases(cleanupCtx, &etcdserverpb.LeaseLeasesRequest{})
		require.NoError(t, listErr)
		for _, lease := range listed.Leases {
			require.Falsef(t, lease.ID >= ids[0] && lease.ID <= ids[len(ids)-1],
				"test lease %d remained after cleanup", lease.ID)
		}
	})

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	require.Eventually(t, func() bool {
		type result struct {
			ttl int64
			err error
		}
		firstC := make(chan result, 1)
		lastC := make(chan result, 1)
		go func() {
			resp, ttlErr := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: ids[0]})
			if ttlErr != nil {
				firstC <- result{err: ttlErr}
				return
			}
			firstC <- result{ttl: resp.TTL}
		}()
		go func() {
			resp, ttlErr := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: ids[len(ids)-1]})
			if ttlErr != nil {
				lastC <- result{err: ttlErr}
				return
			}
			lastC <- result{ttl: resp.TTL}
		}()
		first, last := <-firstC, <-lastC
		return first.err == nil && last.err == nil && last.ttl >= first.ttl+1
	}, 30*time.Second, 250*time.Millisecond,
		fmt.Sprintf("lease %d should expire later than lease %d after promotion spreading", ids[len(ids)-1], ids[0]))
}

func TestEmptyLeaseRevokeRemainsDeletedAfterFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	ttl, err := cli.TimeToLive(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, lease.ID)
		if ttlErr != nil || ttl.TTL != -1 {
			return false
		}
		leases, listErr := cli.Leases(ctx)
		if listErr != nil {
			return false
		}
		for _, listed := range leases.Leases {
			if listed.ID == lease.ID {
				return false
			}
		}
		return true
	}, 30*time.Second, 250*time.Millisecond)
}

func TestEmptyLeaseNaturalExpiryRemainsDeletedAfterFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-empty-lease-expiry-failover/%d", time.Now().UnixNano())
	base, err := cli.Get(ctx, key)
	require.NoError(t, err)
	id := time.Now().UnixNano() & ((1 << 62) - 1)
	grant, err := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 1})
	require.NoError(t, err)
	require.NotNil(t, base.Header)
	require.NotNil(t, grant.Header)
	reused := false
	t.Cleanup(func() {
		if !reused {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = rawLease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
	})
	require.Equal(t, base.Header.Revision, grant.Header.Revision)

	require.Eventually(t, func() bool {
		ttl, ttlErr := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
		return ttlErr == nil && ttl.Header != nil && ttl.TTL == -1 && ttl.Header.Revision == base.Header.Revision
	}, 10*time.Second, 100*time.Millisecond)

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	require.Eventually(t, func() bool {
		ttl, ttlErr := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
		if ttlErr != nil || ttl.Header == nil || ttl.TTL != -1 || ttl.Header.Revision != base.Header.Revision {
			return false
		}
		leases, listErr := rawLease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
		if listErr != nil || leases.Header == nil || leases.Header.Revision != base.Header.Revision {
			return false
		}
		for _, listed := range leases.Leases {
			if listed.ID == id {
				return false
			}
		}
		current, rangeErr := cli.Get(ctx, key)
		return rangeErr == nil && current.Header != nil && current.Header.Revision == base.Header.Revision && len(current.Kvs) == 0
	}, 30*time.Second, 250*time.Millisecond)

	regrant, err := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
	require.NoError(t, err)
	require.NotNil(t, regrant.Header)
	reused = true
	require.Equal(t, base.Header.Revision, regrant.Header.Revision)
	ttl, err := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	revoke, err := rawLease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
	require.NoError(t, err)
	require.NotNil(t, revoke.Header)
	reused = false
	require.Equal(t, base.Header.Revision, revoke.Header.Revision)
}

func TestFutureWatchSurvivesEmptyLeaseExpiryAndLeaderFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-empty-expiry-watch-failover/%d", time.Now().UnixNano())
	base, err := cli.Get(ctx, key)
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})

	watchCtx, stopWatch := context.WithCancel(ctx)
	t.Cleanup(stopWatch)
	watch := cli.Watch(watchCtx, key, clientv3.WithRev(base.Header.Revision+1), clientv3.WithCreatedNotify())
	created := receiveTxnLeaseLiveWatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	require.Empty(t, created.Events)
	require.Equal(t, base.Header.Revision, created.Header.Revision)
	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, lease.ID)
		return ttlErr == nil && ttl.TTL == -1 && ttl.ResponseHeader.Revision == base.Header.Revision
	}, 10*time.Second, 100*time.Millisecond)

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))
	require.NoError(t, cli.RequestProgress(ctx))

	quiet := time.NewTimer(300 * time.Millisecond)
	defer quiet.Stop()
	quietDone := false
	for !quietDone {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "future watch closed after leader replacement")
			require.NoError(t, response.Err())
			require.False(t, response.Canceled)
			require.Empty(t, response.Events, "empty lease expiry/failover must not emit a user event")
			require.Equal(t, base.Header.Revision, response.Header.Revision)
		case <-quiet.C:
			quietDone = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	put, err := cli.Put(ctx, key, "after-failover")
	require.NoError(t, err)
	require.Equal(t, base.Header.Revision+1, put.Header.Revision)
	eventSeen := false
	for !eventSeen {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "future watch closed before the post-failover event")
			require.NoError(t, response.Err())
			require.False(t, response.Canceled)
			for _, event := range response.Events {
				require.False(t, eventSeen, "post-failover event must not be replayed")
				require.Equal(t, mvccpb.PUT, event.Type)
				require.NotNil(t, event.Kv)
				require.Equal(t, key, string(event.Kv.Key))
				require.Equal(t, "after-failover", string(event.Kv.Value))
				require.Equal(t, put.Header.Revision, event.Kv.CreateRevision)
				require.Equal(t, put.Header.Revision, event.Kv.ModRevision)
				require.Equal(t, int64(1), event.Kv.Version)
				require.Zero(t, event.Kv.Lease)
				eventSeen = true
			}
		case <-time.After(30 * time.Second):
			t.Fatal("future watch did not deliver the post-failover user write")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	ttl, err := cli.TimeToLive(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)
	require.Equal(t, put.Header.Revision, ttl.ResponseHeader.Revision)
}
