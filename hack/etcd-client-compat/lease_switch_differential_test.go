package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseSwitchOutcome struct {
	Rounds                   int
	NewValuePreserved        bool
	NewLeasePreserved        bool
	OldLeaseGone             bool
	NewLeaseRevokeDeletes    bool
	UnexpectedKeyCardinality bool
}

func TestLeaseSwitchConcurrentRevokeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	const rounds = 24
	require.Equal(t,
		runLeaseSwitchScenario(t, reference, "etcd", rounds, nil),
		runLeaseSwitchScenario(t, compatEndpoint(), "kubebrain", rounds, nil),
	)
}

func TestLeaseSwitchSurvivesLeaderFailover(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	pod := linearizabilityDeletePod()
	if endpoint == "" || pod == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT and LINEARIZABILITY_DELETE_POD to run lease-switch failover")
	}

	var operationClock atomic.Int64
	errCh := make(chan error, 1)
	var fault sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	startLinearizabilityPodDeletion(ctx, &operationClock, pod, errCh, &fault)

	outcome := runLeaseSwitchScenario(t, endpoint, "kubebrain-failover", 32, &operationClock)
	fault.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.Equal(t, leaseSwitchOutcome{
		Rounds:                32,
		NewValuePreserved:     true,
		NewLeasePreserved:     true,
		OldLeaseGone:          true,
		NewLeaseRevokeDeletes: true,
	}, outcome)
}

func runLeaseSwitchScenario(
	t *testing.T,
	endpoint string,
	instance string,
	rounds int,
	operationClock *atomic.Int64,
) leaseSwitchOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	prefix := fmt.Sprintf("/dbaas-lease-switch/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	outcome := leaseSwitchOutcome{
		Rounds:                rounds,
		NewValuePreserved:     true,
		NewLeasePreserved:     true,
		OldLeaseGone:          true,
		NewLeaseRevokeDeletes: true,
	}

	for round := 0; round < rounds; round++ {
		leaseA, err := retryLeaseSwitchGrant(ctx, cli, 300)
		require.NoError(t, err)
		leaseB, err := retryLeaseSwitchGrant(ctx, cli, 300)
		require.NoError(t, err)
		key := fmt.Sprintf("%s%02d", prefix, round)
		require.NoError(t, retryLeaseSwitchPut(ctx, cli, key, "lease-a", leaseA.ID))

		if operationClock != nil {
			operationClock.Add(1)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			results <- retryLeaseSwitchPut(ctx, cli, key, "lease-b", leaseB.ID)
		}()
		go func() {
			<-start
			results <- retryLeaseSwitchRevoke(ctx, cli, leaseA.ID)
		}()
		close(start)
		require.NoError(t, <-results)
		require.NoError(t, <-results)
		if operationClock != nil {
			operationClock.Add(1)
		}

		current, err := cli.Get(ctx, key)
		require.NoError(t, err)
		if len(current.Kvs) != 1 {
			outcome.UnexpectedKeyCardinality = true
			outcome.NewValuePreserved = false
			outcome.NewLeasePreserved = false
		} else {
			outcome.NewValuePreserved = outcome.NewValuePreserved &&
				string(current.Kvs[0].Value) == "lease-b"
			outcome.NewLeasePreserved = outcome.NewLeasePreserved &&
				current.Kvs[0].Lease == int64(leaseB.ID)
		}
		oldTTL, err := cli.TimeToLive(ctx, leaseA.ID)
		require.NoError(t, err)
		outcome.OldLeaseGone = outcome.OldLeaseGone && oldTTL.TTL == -1

		require.NoError(t, retryLeaseSwitchRevoke(ctx, cli, leaseB.ID))
		after, err := cli.Get(ctx, key)
		require.NoError(t, err)
		outcome.NewLeaseRevokeDeletes = outcome.NewLeaseRevokeDeletes && len(after.Kvs) == 0
	}
	return outcome
}

func retryLeaseSwitchGrant(ctx context.Context, cli *clientv3.Client, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	for {
		lease, err := cli.Grant(ctx, ttl)
		if err == nil {
			return lease, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func retryLeaseSwitchPut(ctx context.Context, cli *clientv3.Client, key, value string, leaseID clientv3.LeaseID) error {
	for {
		_, err := cli.Put(ctx, key, value, clientv3.WithLease(leaseID))
		if err == nil {
			return nil
		}
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func retryLeaseSwitchRevoke(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID) error {
	for {
		_, err := cli.Revoke(ctx, leaseID)
		if err == nil || errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}
