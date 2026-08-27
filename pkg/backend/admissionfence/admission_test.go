package admissionfence

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestSessionAndRestoreAdmissionAreMutuallyExclusive(t *testing.T) {
	cli := testClient(t)
	ctx := t.Context()
	token, err := NewToken("restore-1", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 42, "a1001")
	require.NoError(t, err)

	session, err := StartSession(ctx, cli, "a1001", "replica-1:2380", 6*time.Second)
	require.NoError(t, err)
	require.True(t, session.Fresh())
	_, err = Acquire(ctx, cli, "a1001", token)
	require.ErrorContains(t, err, "active KubeBrain sessions")
	require.NoError(t, session.Close(ctx))

	resumed, err := Acquire(ctx, cli, "a1001", token)
	require.NoError(t, err)
	require.False(t, resumed)
	require.NoError(t, Verify(ctx, cli, "a1001", token))
	_, err = StartSession(ctx, cli, "a1001", "replica-2:2380", 6*time.Second)
	require.ErrorContains(t, err, "closed")

	require.NoError(t, Release(ctx, cli, "a1001", token))
	require.NoError(t, VerifyOpen(ctx, cli, "a1001"))
	session, err = StartSession(ctx, cli, "a1001", "replica-2:2380", 6*time.Second)
	require.NoError(t, err)
	require.NoError(t, session.Close(ctx))
}

func TestConcurrentSessionRegistrationAndAcquireHaveSingleWinner(t *testing.T) {
	cli := testClient(t)
	ctx := t.Context()
	for i := 0; i < 20; i++ {
		keyspace := "race" + string(rune('a'+i))
		token, err := NewToken("restore-1", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 42, keyspace)
		require.NoError(t, err)
		type result struct {
			session *Session
			err     error
		}
		start := make(chan struct{})
		sessionResult, acquireResult := make(chan result, 1), make(chan error, 1)
		go func() {
			<-start
			s, e := StartSession(ctx, cli, keyspace, "replica:2380", 6*time.Second)
			sessionResult <- result{s, e}
		}()
		go func() { <-start; _, e := Acquire(ctx, cli, keyspace, token); acquireResult <- e }()
		close(start)
		sr, acquireErr := <-sessionResult, <-acquireResult
		require.NotEqual(t, sr.err == nil, acquireErr == nil, "exactly one serialized PD transaction must win")
		if sr.session != nil {
			require.NoError(t, sr.session.Close(ctx))
		}
		if acquireErr == nil {
			require.NoError(t, Release(ctx, cli, keyspace, token))
		}
	}
}

func TestConcurrentSessionsInitializeMissingGate(t *testing.T) {
	cli := testClient(t)
	ctx := t.Context()
	const replicas = 16
	start := make(chan struct{})
	type result struct {
		session *Session
		err     error
	}
	results := make(chan result, replicas)
	var ready sync.WaitGroup
	ready.Add(replicas)
	for i := 0; i < replicas; i++ {
		identity := fmt.Sprintf("replica-%d:2380", i)
		go func() {
			ready.Done()
			<-start
			session, err := StartSession(ctx, cli, "cold-start", identity, 6*time.Second)
			results <- result{session: session, err: err}
		}()
	}
	ready.Wait()
	close(start)
	got := make([]result, 0, replicas)
	for i := 0; i < replicas; i++ {
		got = append(got, <-results)
	}
	for _, result := range got {
		if result.session != nil {
			require.NoError(t, result.session.Close(ctx))
		}
	}
	for _, result := range got {
		require.NoError(t, result.err)
		require.NotNil(t, result.session)
	}
}

func TestSessionFreshnessExpiresBeforeLease(t *testing.T) {
	s := &Session{ttl: 6 * time.Second}
	s.lastAck.Store(time.Now().Add(-4 * time.Second).UnixNano())
	require.False(t, s.Fresh())
}

func TestMalformedKeepAliveMakesSessionStale(t *testing.T) {
	cli := testClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	keepalive := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	keepalive <- nil
	close(keepalive)
	s := &Session{cli: cli, ttl: 6 * time.Second, done: make(chan struct{})}
	s.leaseID.Store(123)
	s.lastAck.Store(time.Now().UnixNano())
	s.run(ctx, "malformed", "replica:2380", keepalive, newResponseAdmission(0))
	require.False(t, s.Fresh())
}

func TestSessionReRegistersAfterKeepAliveStreamEnds(t *testing.T) {
	cli := testClient(t)
	ctx := t.Context()
	session, err := StartSession(ctx, cli, "recover", "replica-1:2380", 6*time.Second)
	require.NoError(t, err)
	for cycle := 1; cycle <= 2; cycle++ {
		oldLease := clientv3.LeaseID(session.leaseID.Load())
		_, err = cli.Revoke(ctx, oldLease)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return session.Fresh() && clientv3.LeaseID(session.leaseID.Load()) != oldLease
		}, 5*time.Second, 50*time.Millisecond, "cycle %d must replace a lost keepalive lease in the same process", cycle)
		response, getErr := cli.Get(ctx, SessionKey("recover", "replica-1:2380"))
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, int64(session.leaseID.Load()), response.Kvs[0].Lease)
	}
	require.NoError(t, session.Close(ctx))
}

func TestSessionIdentityHandoffWaitsForOldLeaseRelease(t *testing.T) {
	cli := testClient(t)
	oldCtx, stopOld := context.WithCancel(t.Context())
	oldSession, err := StartSession(oldCtx, cli, "handoff", "replica-1:2380", 6*time.Second)
	require.NoError(t, err)
	oldLease := clientv3.LeaseID(oldSession.leaseID.Load())
	stopOld()
	require.Eventually(t, func() bool {
		select {
		case <-oldSession.done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	released := make(chan struct{})
	go func() {
		time.Sleep(250 * time.Millisecond)
		revokeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = cli.Revoke(revokeCtx, oldLease)
		close(released)
	}()
	started := time.Now()
	replacement, err := StartSessionWithIdentityHandoff(t.Context(), cli, "handoff", "replica-1:2380", 6*time.Second)
	require.NoError(t, err)
	<-released
	require.GreaterOrEqual(t, time.Since(started), 200*time.Millisecond)
	require.NotEqual(t, oldLease, clientv3.LeaseID(replacement.leaseID.Load()))
	require.NoError(t, replacement.Close(t.Context()))
}

func TestSessionIdentityHandoffNeverStealsActiveLease(t *testing.T) {
	cli := testClient(t)
	active, err := StartSession(t.Context(), cli, "handoff-active", "replica-1:2380", 6*time.Second)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 350*time.Millisecond)
	defer cancel()
	_, err = StartSessionWithIdentityHandoff(ctx, cli, "handoff-active", "replica-1:2380", 6*time.Second)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, active.Fresh())
	response, getErr := cli.Get(t.Context(), SessionKey("handoff-active", "replica-1:2380"))
	require.NoError(t, getErr)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, int64(active.leaseID.Load()), response.Kvs[0].Lease)
	leases, leasesErr := cli.Leases(t.Context())
	require.NoError(t, leasesErr)
	require.Len(t, leases.Leases, 1, "handoff polling must not churn temporary leases")
	require.Equal(t, active.leaseID.Load(), int64(leases.Leases[0].ID))
	require.NoError(t, active.Close(t.Context()))
}

func TestSessionIdentityHandoffDoesNotWaitThroughClosedGate(t *testing.T) {
	cli := testClient(t)
	token, err := NewToken("restore-1", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 42, "handoff-closed")
	require.NoError(t, err)
	_, err = Acquire(t.Context(), cli, "handoff-closed", token)
	require.NoError(t, err)
	started := time.Now()
	_, err = StartSessionWithIdentityHandoff(t.Context(), cli, "handoff-closed", "replica-1:2380", 6*time.Second)
	require.ErrorContains(t, err, "restore admission is closed")
	require.Less(t, time.Since(started), time.Second)
	require.NoError(t, Release(t.Context(), cli, "handoff-closed", token))
}

func TestIPv6ProcessIdentityIsAccepted(t *testing.T) {
	cli := testClient(t)
	session, err := StartSession(t.Context(), cli, "a1001", "[2001:db8::1]:2380", 6*time.Second)
	require.NoError(t, err)
	require.NoError(t, session.Close(t.Context()))
}

func testClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	peerURL := freeURL(t)
	clientURL := freeURL(t)
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peerURL}, []url.URL{peerURL}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{clientURL}, []url.URL{clientURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		server.Close()
		t.Fatal("embedded etcd did not become ready")
	}
	t.Cleanup(server.Close)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{clientURL.String()}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	return cli
}

func freeURL(t *testing.T) url.URL {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return url.URL{Scheme: "http", Host: address}
}
