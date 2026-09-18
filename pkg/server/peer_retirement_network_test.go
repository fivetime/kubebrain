package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func retirementPublicGRPC(t *testing.T, s Server, pool *x509.CertPool, certificates []tls.Certificate, extra ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	options := append(s.ClientServerOptions(), grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{certificates[0]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})))
	grpcServer := grpc.NewServer(append(options, extra...)...)
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
	t.Run("enabled", func(t *testing.T) { runRetirementNetworkHandoff(t, true, false) })
	t.Run("default-disabled", func(t *testing.T) { runRetirementNetworkHandoff(t, false, false) })
}

func TestPeerRetirementPendingExpiredStreamDuringStorageFailure(t *testing.T) {
	runRetirementNetworkHandoff(t, true, true)
}

func TestPeerRetirementPendingStreamWithReloadedProxyCredentials(t *testing.T) {
	runRetirementNetworkHandoff(t, true, true, true)
}

type networkCredentialSource func(context.Context) (transportidentity.ClientCredentialMaterial, error)

func (f networkCredentialSource) LoadClientCredentialMaterial(ctx context.Context) (transportidentity.ClientCredentialMaterial, error) {
	return f(ctx)
}

type retirementCountedStream struct {
	grpc.ServerStream
	messages *atomic.Int32
}

func (s retirementCountedStream) RecvMsg(message any) error {
	err := s.ServerStream.RecvMsg(message)
	if err == nil {
		s.messages.Add(1)
	}
	return err
}

func runRetirementNetworkHandoff(t *testing.T, enabled, pending bool, reload ...bool) {
	pool, certs := retirementTestCertificates(t)
	var credentialLoads atomic.Int32
	if len(reload) > 0 && reload[0] {
		defer func() { require.Positive(t, credentialLoads.Load(), "proxy must actually use the material source") }()
	}
	var servers [2]atomic.Pointer[server]
	var peerRPC [2]atomic.Pointer[grpc.Server]
	var peerRenewals [2]atomic.Int32
	var discoveryRequests [2]atomic.Int32
	var requests atomic.Int32
	var authenticatedHTTP2 atomic.Bool
	urls := make([]string, 2)
	for i := range urls {
		i := i
		peer := retirementHandlerServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == peerSuccessorPath {
				discoveryRequests[i].Add(1)
			}
			if rpc := peerRPC[i].Load(); rpc != nil && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
				rpc.ServeHTTP(w, r)
				return
			}
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
		defer func() {
			for i := range peerRPC {
				if rpc := peerRPC[i].Load(); rpc != nil {
					rpc.Stop()
				}
			}
		}()
		cancel()
		oldStore.failed.Store(false)
		for i := range servers {
			if s := servers[i].Load(); s != nil {
				err := s.Close()
				if pending && errors.Is(err, context.DeadlineExceeded) {
					// Both campaigns are canceled: a proxy-enabled member cannot
					// find a live successor during whole-fixture shutdown.
					t.Logf("whole-fixture close exhausted successor wait: %v", err)
				} else {
					require.NoError(t, err)
				}
			}
		}
	})
	pins := map[string][]string{urls[0]: {retirementTestPin(certs[0])}, urls[1]: {retirementTestPin(certs[1])}}
	start := func(i int, b backend.Backend) *server {
		config := PeerRetirementConfig{Scope: scope, HolderPins: pins, PeerURLs: []string{urls[1-i]},
			TLS: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[i]}}, ReadBudget: time.Second,
			OperationBudget: time.Second, SendBudget: time.Second, Concurrency: 2, RequestsPerSecond: 10}
		serverConfig := Config{LeaseDuration: 30 * time.Second, RenewDeadline: 500 * time.Millisecond, RetryPeriod: 50 * time.Millisecond}
		if pending {
			config.SuccessorHolders = map[string]string{urls[1-i]: urls[1-i]}
			serverConfig.RenewDeadline = 4 * time.Second
			serverConfig.EnableEtcdProxy = true
			serverConfig.ProxyTLS = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{certs[i]}}
			if len(reload) > 0 && reload[0] {
				config.ProxyCredentialSource = networkCredentialSource(func(context.Context) (transportidentity.ClientCredentialMaterial, error) {
					credentialLoads.Add(1)
					return transportidentity.ClientCredentialMaterial{Roots: pool.Clone(), Certificate: certs[i]}, nil
				})
			}
		}
		var s Server
		if enabled {
			var err error
			s, err = NewServerWithPeerRetirement(ctx, b, metrics, serverConfig, config)
			require.NoError(t, err)
		} else {
			s = NewServer(ctx, b, metrics, serverConfig)
		}
		concrete := s.(*server)
		_, exposesDiscovery := s.GetPeerHttpHandlers()[peerSuccessorPath]
		require.Equal(t, pending, exposesDiscovery)
		require.NotContains(t, s.GetClientHttpHandlers(), peerSuccessorPath)
		require.NotContains(t, s.GetInfoHttpHandlers(), peerSuccessorPath)
		servers[i].Store(concrete)
		if pending {
			rpc := grpc.NewServer(append(s.PeerServerOptions(), grpc.ChainStreamInterceptor(
				func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					if info.FullMethod == "/etcdserverpb.Lease/LeaseKeepAlive" {
						stream = retirementCountedStream{ServerStream: stream, messages: &peerRenewals[i]}
					}
					return handler(srv, stream)
				}))...)
			s.RegisterPeer(rpc)
			peerRPC[i].Store(rpc)
		}
		return concrete
	}
	old := start(0, bases[0])
	require.Eventually(t, old.requestPathReady, 5*time.Second, 10*time.Millisecond)
	var streamCount, messageCount atomic.Int32
	oldClient := retirementPublicGRPC(t, old, pool, certs, grpc.ChainStreamInterceptor(
		func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if info.FullMethod == "/etcdserverpb.Lease/LeaseKeepAlive" {
				streamCount.Add(1)
				stream = retirementCountedStream{ServerStream: stream, messages: &messageCount}
			}
			return handler(srv, stream)
		}))
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rpcCancel()
	leaseClient := etcdserverpb.NewLeaseClient(oldClient)
	leaseTTL := int64(60)
	if pending {
		leaseTTL = 1
	}
	grant, err := leaseClient.LeaseGrant(rpcCtx, &etcdserverpb.LeaseGrantRequest{TTL: leaseTTL, ID: 889361})
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
	if !pending {
		stopStream()
	}
	oldTerm := old.leaderElection.CurrentLeadershipTerm()
	next := start(1, bases[1])
	require.Eventually(t, func() bool { return next.leaderElection.GetLeaderInfo() == urls[0] }, 3*time.Second, 10*time.Millisecond)
	oldStore.failed.Store(true)
	type renewalResult struct {
		response *etcdserverpb.LeaseKeepAliveResponse
		err      error
	}
	var renewalDone chan renewalResult
	if pending {
		// Actual wall-clock expiry, not mutation of the lease manager's state.
		// The failed storage prevents durable revocation before term retirement.
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-rpcCtx.Done():
			t.Fatal(rpcCtx.Err())
		}
		require.True(t, old.leaderElection.IsLeader(), "request must precede old term retirement")
		require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: grant.ID}))
		require.Eventually(t, func() bool { return messageCount.Load() == 2 }, 500*time.Millisecond, time.Millisecond,
			"old server must consume the original request before term retirement")
		require.True(t, old.leaderElection.IsLeader())
		renewalDone = make(chan renewalResult, 1)
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			response, err := stream.Recv()
			renewalDone <- renewalResult{response, err}
		}()
		defer func() { stopStream(); <-joined }()
		select {
		case got := <-renewalDone:
			t.Fatalf("expired request must wait for retirement, got response=%v error=%v", got.response, got.err)
		case <-time.After(100 * time.Millisecond):
		}
	}
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
	require.Eventually(t, func() bool {
		return next.leaderElection.IsLeader() && next.requestPathReady()
	}, 5*time.Second, 10*time.Millisecond)
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
	if pending {
		require.Equal(t, int32(1), streamCount.Load())
		require.Equal(t, int32(2), messageCount.Load(), "one initial ACK and one pending renewal; no resend")
		t.Logf("successor ready with lease and key intact; original stream consumed once; old storage still failed=%v, cached leader=%q, successor=%q",
			oldStore.failed.Load(), old.leaderElection.GetLeaderInfo(), urls[1])
		select {
		case got := <-renewalDone:
			require.NoError(t, got.err)
			require.NotNil(t, got.response)
			require.Equal(t, grant.ID, got.response.ID)
			require.Positive(t, got.response.TTL)
			require.NotNil(t, got.response.Header)
			require.Equal(t, value.Header.ClusterId, got.response.Header.ClusterId)
			require.Equal(t, ack.Header.MemberId, got.response.Header.MemberId, "public header identifies the original ingress member, even when forwarded")
			require.GreaterOrEqual(t, got.response.Header.RaftTerm, value.Header.RaftTerm,
				"isolated ingress must retain the verified successor term, not merely the old term")
			require.Positive(t, peerRenewals[1].Load(), "successor peer must actually consume the forwarded renewal")
			require.True(t, oldStore.failed.Load())
			// Keep the original stream alive beyond the two-second routing hint
			// lifetime. Repeated renewal must survive revalidation while the old
			// ingress still cannot refresh the authoritative election record.
			continued := 0
			initialDiscovery := discoveryRequests[1].Load()
			until := time.Now().Add(3 * time.Second)
			for time.Now().Before(until) {
				timer := time.NewTimer(200 * time.Millisecond)
				select {
				case <-timer.C:
				case <-rpcCtx.Done():
					timer.Stop()
					t.Fatal("original stream deadline expired during hint revalidation")
				}
				require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: grant.ID}))
				response, err := stream.Recv()
				require.NoError(t, err)
				require.Equal(t, grant.ID, response.ID)
				require.Positive(t, response.TTL, "hint expiry must not lose the short lease")
				require.NotNil(t, response.Header)
				require.GreaterOrEqual(t, response.Header.RaftTerm, value.Header.RaftTerm,
					"successor term must survive routing hint revalidation on the original stream")
				continued++
			}
			require.Equal(t, int32(1), streamCount.Load())
			require.Equal(t, int32(2+continued), messageCount.Load())
			require.GreaterOrEqual(t, peerRenewals[1].Load(), int32(1+continued))
			require.Greater(t, discoveryRequests[1].Load(), initialDiscovery, "expired hint must trigger another authenticated discovery")
			require.True(t, oldStore.failed.Load())
		case <-rpcCtx.Done():
			t.Fatal("original expired keepalive stream did not recover while old storage remained unavailable")
		}
	}
}
