package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func concurrencyEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run client/v3 concurrency recipes")
	}
	return endpoint
}

func newConcurrencyClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{concurrencyEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	return cli
}

func newConcurrencySession(t *testing.T, cli *clientv3.Client) *concurrency.Session {
	t.Helper()
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestConcurrencyMutexAndSessionRelease(t *testing.T) {
	cli := newConcurrencyClient(t)
	s1 := newConcurrencySession(t, cli)
	s2 := newConcurrencySession(t, cli)
	prefix := fmt.Sprintf("/dbaas-concurrency/mutex/%d/", time.Now().UnixNano())
	m1 := concurrency.NewMutex(s1, prefix)
	m2 := concurrency.NewMutex(s2, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, m1.Lock(ctx))
	require.ErrorIs(t, m2.TryLock(ctx), concurrency.ErrLocked)

	// Closing the owner's session revokes its lease. The next waiter must acquire
	// without an explicit Unlock, which exercises lease-backed crash release.
	require.NoError(t, s1.Close())
	require.NoError(t, m2.Lock(ctx))
	require.NoError(t, m2.Unlock(ctx))
}

func TestConcurrencyElectionObserveProclaimAndHandoff(t *testing.T) {
	cli := newConcurrencyClient(t)
	s1 := newConcurrencySession(t, cli)
	s2 := newConcurrencySession(t, cli)
	prefix := fmt.Sprintf("/dbaas-concurrency/election/%d/", time.Now().UnixNano())
	e1 := concurrency.NewElection(s1, prefix)
	e2 := concurrency.NewElection(s2, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, e1.Campaign(ctx, "candidate-1"))
	observe := e2.Observe(ctx)
	first := <-observe
	require.Len(t, first.Kvs, 1)
	require.Equal(t, "candidate-1", string(first.Kvs[0].Value))
	require.NoError(t, e1.Proclaim(ctx, "candidate-1-updated"))

	won := make(chan error, 1)
	go func() { won <- e2.Campaign(ctx, "candidate-2") }()
	select {
	case err := <-won:
		t.Fatalf("second candidate won before resignation: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, e1.Resign(ctx))
	select {
	case err := <-won:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	leader, err := e2.Leader(ctx)
	require.NoError(t, err)
	require.Len(t, leader.Kvs, 1)
	require.Equal(t, "candidate-2", string(leader.Kvs[0].Value))
	require.NoError(t, e2.Resign(ctx))

	cancel()
}

func TestConcurrencyOrphanedSessionExpiresAndHandsOff(t *testing.T) {
	ownerClient := newConcurrencyClient(t)
	contenderClient := newConcurrencyClient(t)
	owner, err := concurrency.NewSession(ownerClient, concurrency.WithTTL(2))
	require.NoError(t, err)
	contender := newConcurrencySession(t, contenderClient)
	prefix := fmt.Sprintf("/dbaas-concurrency/natural-expiry/%d/", time.Now().UnixNano())
	ownerMutex := concurrency.NewMutex(owner, prefix+"mutex/")
	contenderMutex := concurrency.NewMutex(contender, prefix+"mutex/")
	ownerElection := concurrency.NewElection(owner, prefix+"election/")
	contenderElection := concurrency.NewElection(contender, prefix+"election/")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, ownerMutex.Lock(ctx))
	require.NoError(t, ownerElection.Campaign(ctx, "owner"))
	mutexWon := make(chan error, 1)
	electionWon := make(chan error, 1)
	go func() { mutexWon <- contenderMutex.Lock(ctx) }()
	go func() { electionWon <- contenderElection.Campaign(ctx, "contender") }()
	assertBlocked := func(name string, result <-chan error) {
		t.Helper()
		select {
		case err := <-result:
			t.Fatalf("%s completed before lease expiry: %v", name, err)
		case <-time.After(300 * time.Millisecond):
		}
	}
	assertBlocked("mutex", mutexWon)
	assertBlocked("election", electionWon)

	leaseID := owner.Lease()
	owner.Orphan() // stop keepalive without LeaseRevoke, simulating a crashed process
	assertBlocked("mutex after orphan", mutexWon)
	assertBlocked("election after orphan", electionWon)
	select {
	case err := <-mutexWon:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("mutex was not handed off after natural lease expiry")
	}
	select {
	case err := <-electionWon:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("election was not handed off after natural lease expiry")
	}

	ttl, err := ownerClient.TimeToLive(ctx, leaseID)
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)
	leader, err := contenderElection.Leader(ctx)
	require.NoError(t, err)
	require.Len(t, leader.Kvs, 1)
	require.Equal(t, "contender", string(leader.Kvs[0].Value))
	require.NoError(t, contenderElection.Resign(ctx))
	require.NoError(t, contenderMutex.Unlock(ctx))
}

// TestConcurrencySessionSurvivesKubeBrainFailover is an opt-in live-cluster
// test. The caller names the current leader pod; ordinary unit/CI runs never
// mutate Kubernetes. It verifies the official concurrency recipes across a
// real KubeBrain leadership change, including lease keepalive continuity.
func TestConcurrencySessionSurvivesKubeBrainFailover(t *testing.T) {
	pod := os.Getenv("KUBEBRAIN_FAILOVER_POD")
	if pod == "" {
		t.Skip("set KUBEBRAIN_FAILOVER_POD to delete a live leader pod")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}
	cli := newConcurrencyClient(t)
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(30))
	require.NoError(t, err)
	defer func() { _ = session.Close() }()
	prefix := fmt.Sprintf("/dbaas-concurrency/failover/%d/", time.Now().UnixNano())
	mutex := concurrency.NewMutex(session, prefix+"mutex/")
	election := concurrency.NewElection(session, prefix+"election/")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	require.NoError(t, mutex.Lock(ctx))
	require.NoError(t, election.Campaign(ctx, "survivor"))

	output, err := runCompatKubectlContext(t, ctx, "-n", namespace, "delete", "pod", pod, "--wait=false")
	require.NoErrorf(t, err, "delete leader pod: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	select {
	case <-session.Done():
		t.Fatal("concurrency session lease expired during KubeBrain failover")
	default:
	}
	owner, err := cli.Txn(ctx).If(mutex.IsOwner()).Then(clientv3.OpGet(prefix+"mutex/", clientv3.WithPrefix())).Commit()
	require.NoError(t, err)
	require.True(t, owner.Succeeded, "mutex ownership must survive leader failover")
	leader, err := election.Leader(ctx)
	require.NoError(t, err)
	require.Len(t, leader.Kvs, 1)
	require.Equal(t, "survivor", string(leader.Kvs[0].Value))
	require.NoError(t, election.Resign(ctx))
	require.NoError(t, mutex.Unlock(ctx))
}
