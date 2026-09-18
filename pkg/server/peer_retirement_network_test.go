package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func retirementPublicGRPC(t *testing.T, s Server, pool *x509.CertPool, certificates []tls.Certificate) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	options := append(s.ClientServerOptions(), grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{certificates[0]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})))
	grpcServer := grpc.NewServer(options...)
	s.RegisterClient(grpcServer)
	done := make(chan struct{})
	go func() { defer close(done); _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); <-done })
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{certificates[3]},
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

// Full server startup/cleanup/reload and real public gRPC + peer HTTP2 mTLS.
// The fault rejects new storage-interface calls and commits on the old node;
// it is not a real TiKV network partition or the original availability gate.
func TestPeerRetirementFullServerNetworkLeaseHandoff(t *testing.T) {
	t.Run("enabled", func(t *testing.T) { runRetirementNetworkHandoff(t, true) })
	t.Run("default-disabled", func(t *testing.T) { runRetirementNetworkHandoff(t, false) })
}

func runRetirementNetworkHandoff(t *testing.T, enabled bool) {
	pool, certs := retirementTestCertificates(t)
	var servers [2]atomic.Pointer[server]
	var requests atomic.Int32
	var authenticatedHTTP2 atomic.Bool
	urls := make([]string, 2)
	for i := range urls {
		i := i
		peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s := servers[i].Load()
			if s == nil {
				w.WriteHeader(503)
				return
			}
			h, ok := s.GetPeerHttpHandlers()[r.URL.Path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			if r.URL.Path == peerRetirementPath {
				requests.Add(1)
				if r.ProtoMajor == 2 && r.TLS != nil && len(r.TLS.VerifiedChains) != 0 {
					authenticatedHTTP2.Store(true)
				}
			}
			h.ServeHTTP(w, r)
		}), pool, []tls.Certificate{certs[i]}, true)
		urls[i] = peer.URL
	}
	store := retirementStorageFixture{memkv.NewKvStorage(), 42}
	oldStore := &retirementFaultStorage{base: store}
	metrics := metricmock.NewMinimalMetrics(gomock.NewController(t))
	bases := make([]backend.Backend, 2)
	for i := range bases {
		if i == 0 {
			bases[i] = backend.NewBackend(oldStore, backend.Config{Prefix: "/network-retirement", Keyspace: "tenant", Identity: urls[i], EnableEtcdCompatibility: true}, metrics)
		} else {
			bases[i] = backend.NewBackend(store, backend.Config{Prefix: "/network-retirement", Keyspace: "tenant", Identity: urls[i], EnableEtcdCompatibility: true}, metrics)
		}
		b := bases[i]
		t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	}
	scope := bases[0].GetResourceLock().(election.RetiredOwnershipReleaser).RetirementScope()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		oldStore.failed.Store(false)
		for i := range servers {
			if s := servers[i].Load(); s != nil {
				require.NoError(t, s.Close())
			}
		}
	})
	pins := map[string][]string{urls[0]: {retirementTestPin(certs[0])}, urls[1]: {retirementTestPin(certs[1])}}
	start := func(i int, b backend.Backend) *server {
		config := PeerRetirementConfig{Scope: scope, HolderPins: pins, PeerURLs: []string{urls[1-i]},
			TLS: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[i]}}, ReadBudget: time.Second,
			OperationBudget: time.Second, SendBudget: time.Second, Concurrency: 2, RequestsPerSecond: 10}
		serverConfig := Config{LeaseDuration: 30 * time.Second, RenewDeadline: 500 * time.Millisecond, RetryPeriod: 50 * time.Millisecond}
		var s Server
		if enabled {
			var err error
			s, err = NewServerWithPeerRetirement(ctx, b, metrics, serverConfig, config)
			require.NoError(t, err)
		} else {
			s = NewServer(ctx, b, metrics, serverConfig)
		}
		concrete := s.(*server)
		servers[i].Store(concrete)
		return concrete
	}
	old := start(0, bases[0])
	require.Eventually(t, old.requestPathReady, 5*time.Second, 10*time.Millisecond)
	oldClient := retirementPublicGRPC(t, old, pool, certs)
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rpcCancel()
	leaseClient := etcdserverpb.NewLeaseClient(oldClient)
	grant, err := leaseClient.LeaseGrant(rpcCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60, ID: 889361})
	require.NoError(t, err)
	key := []byte("/registry/network-handoff")
	_, err = etcdserverpb.NewKVClient(oldClient).Put(rpcCtx, &etcdserverpb.PutRequest{Key: key, Value: []byte("kept"), Lease: grant.ID})
	require.NoError(t, err)
	streamCtx, stopStream := context.WithCancel(rpcCtx)
	defer stopStream()
	stream, err := leaseClient.LeaseKeepAlive(streamCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: grant.ID}))
	ack, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grant.TTL, ack.TTL)
	stopStream()
	oldTerm := old.leaderElection.CurrentLeadershipTerm()
	next := start(1, bases[1])
	require.Eventually(t, func() bool { return next.leaderElection.GetLeaderInfo() == urls[0] }, 3*time.Second, 10*time.Millisecond)
	oldStore.failed.Store(true)
	if !enabled {
		require.Never(t, next.requestPathReady, 2*time.Second, 10*time.Millisecond,
			"ordinary constructor must not bypass the still-valid 30-second record")
		require.Zero(t, requests.Load(), "default must not send retirement requests")
		require.Positive(t, oldStore.rejected.Load())
		record, _, err := bases[1].GetResourceLock().Get(rpcCtx)
		require.NoError(t, err)
		require.Equal(t, urls[0], record.HolderIdentity)
		return
	}
	require.Eventually(t, next.requestPathReady, 5*time.Second, 10*time.Millisecond)
	require.Positive(t, requests.Load())
	require.Positive(t, oldStore.rejected.Load(), "storage fault must actually reject old-node calls")
	require.True(t, oldStore.failed.Load(), "handoff must not depend on restoring old storage access")
	require.True(t, authenticatedHTTP2.Load(), "release request must actually traverse verified HTTP/2 mTLS")
	require.False(t, old.leaderElection.IsLeader())
	require.Greater(t, next.leaderElection.CurrentLeadershipTerm(), oldTerm)
	nextClient := retirementPublicGRPC(t, next, pool, certs)
	ttl, err := etcdserverpb.NewLeaseClient(nextClient).LeaseTimeToLive(rpcCtx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID, Keys: true})
	require.NoError(t, err)
	require.GreaterOrEqual(t, ttl.TTL, ack.TTL)
	require.Contains(t, ttl.Keys, key)
	value, err := etcdserverpb.NewKVClient(nextClient).Range(rpcCtx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, value.Kvs, 1)
	require.Equal(t, grant.ID, value.Kvs[0].Lease)
	require.NotNil(t, ack.Header)
	require.NotNil(t, value.Header)
	require.Equal(t, ack.Header.ClusterId, value.Header.ClusterId)
	require.NotEqual(t, ack.Header.MemberId, value.Header.MemberId)
	require.Greater(t, value.Header.RaftTerm, ack.Header.RaftTerm, "public RPC must advertise the new term")
}
