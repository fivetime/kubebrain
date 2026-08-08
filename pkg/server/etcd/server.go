// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"go.etcd.io/etcd/server/v3/proxy/grpcproxy/adapter"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service"
)

var (
	_ etcdserverpb.KVServer          = (*RPCServer)(nil)
	_ etcdserverpb.WatchServer       = (*RPCServer)(nil)
	_ etcdserverpb.MaintenanceServer = (*RPCServer)(nil)
	_ etcdserverpb.AuthServer        = (*RPCServer)(nil)
)

const (
	// countProxyTimeout bounds one proxied count round-trip to the leader's
	// index (hot answers are ~1s at 10M keys); the remaining request deadline
	// stays available for the local fallback.
	countProxyTimeout = 5 * time.Second
	// countProxyFailureQuiet skips the proxy after a failure so follow-up
	// lookups (same or subsequent requests) fail over locally at once instead
	// of re-burning the timeout during an election gap or leader outage.
	countProxyFailureQuiet = 3 * time.Second
)

// RPCServer only support limited method of etcd grpc server.
//
// etcd 3.7's generated code (standard google.golang.org/protobuf, grpc
// require-unimplemented mode) forces every service handler to embed its
// UnimplementedXxxServer for forward compatibility — the mustEmbedUnimplementedXxxServer
// methods are unexported, so embedding is the only way to satisfy the interfaces.
// KubeBrain still explicitly implements every method it actually serves; the
// embedded defaults are overridden and never called. The one exception is a
// default we deliberately keep: RangeStream is overridden by a real streaming
// implementation (kv.go) — the embed provides only its mustEmbed shim.
type RPCServer struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	etcdserverpb.UnimplementedMaintenanceServer
	etcdserverpb.UnimplementedClusterServer
	etcdserverpb.UnimplementedAuthServer
	// UnimplementedLeaseServer is embedded on leaseManager, not here, to avoid an
	// ambiguous selector with the promoted *leaseManager lease handlers.

	backend BackendShim
	auth    *authManager
	tokens  *authTokenManager

	metricCli metrics.Metrics
	peers     service.PeerService

	alarmMetricMu     sync.Mutex
	knownAlarmMetrics map[alarmMetricKey]struct{}

	// Advertised client endpoint shape for MemberList (see SetAdvertiseClientInfo):
	// the election identity is host:PEER-port and says nothing about the client
	// port or TLS, so ClientURLs built from it alone are wrong on both counts.
	advertiseClientPort  int
	advertiseClientHTTPS bool
	advertiseClientURLs  []string
	staticMembers        []*etcdserverpb.Member
	clientCertAuth       bool
	maxTxnOps            int
	maxRequestBytes      uint
	maxRequestsInFlight  uint32
	admissionMu          sync.Mutex
	requestsInFlight     int64
	requestRateLimiter   *rate.Limiter
	priorityRateLimiter  *rate.Limiter
	maxDeleteRangeKeys   uint32
	maxWatches           uint32
	watchQuotaMu         sync.Mutex
	activeWatches        int64
	activeWatchStreams   atomic.Int64
	// Serializes the runtime etcd auth transition with legacy native unary
	// calls on the public listener. AuthEnable drains already-admitted calls
	// before committing, then new calls observe enabled auth and fail closed.
	nativeAuthBoundary sync.RWMutex

	concurrencyClient *clientv3.Client

	// The lease subsystem: its state and logic live in leaseManager (lease.go /
	// lease_manager.go). Embedded so the lease gRPC handlers and the write-path
	// bind/unbind/IDForKey helpers are promoted onto RPCServer.
	*leaseManager
}

// SetClientCertAuth enables etcd-compatible authentication by the CommonName
// of a client certificate already verified by the gRPC TLS transport.
func (s *RPCServer) SetClientCertAuth(enabled bool) {
	s.clientCertAuth = enabled
}

// SetMaxRequestsInFlight sets the process-wide public client RPC limit. A
// bounded revoke-only reserve is derived from it. Configuration happens before
// serving starts, so readers need no additional lock.
func (s *RPCServer) SetMaxRequestsInFlight(limit uint32) {
	s.maxRequestsInFlight = limit
}

// SetRequestRateLimit configures the process-wide public client request-message
// token bucket and its bounded revoke-only reserve. Validation guarantees rate
// and burst are both zero or positive.
func (s *RPCServer) SetRequestRateLimit(requestsPerSecond, burst uint32) {
	if requestsPerSecond == 0 {
		s.requestRateLimiter = nil
		s.priorityRateLimiter = nil
		return
	}
	s.requestRateLimiter = rate.NewLimiter(rate.Limit(requestsPerSecond), int(burst))
	priorityRate := priorityAdmissionReserve(requestsPerSecond)
	priorityBurst := priorityAdmissionReserve(burst)
	s.priorityRateLimiter = rate.NewLimiter(rate.Limit(priorityRate), int(priorityBurst))
}

// SetMaxDeleteRangeKeys bounds keys materialized into one atomic DeleteRange.
// Zero preserves etcd's unlimited behavior.
func (s *RPCServer) SetMaxDeleteRangeKeys(limit uint32) {
	s.maxDeleteRangeKeys = limit
}

func withUnaryRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, unaryRpcTimeout)
}

// SetMaxWatches sets the process-wide logical watch limit. It is configured
// before serving starts, so the limit itself is immutable on request paths.
func (s *RPCServer) SetMaxWatches(limit uint32) {
	s.maxWatches = limit
}

type leaseState struct {
	id              int64
	ttl             int64
	remainingTTL    int64
	deadline        time.Time
	keys            map[string]struct{}
	timer           *time.Timer
	checkpointTimer *time.Timer
	revoked         chan struct{}
}

// New returns the etcd rpc server
func New(backend b.Backend, metricCli metrics.Metrics, peers service.PeerService) *RPCServer {
	server := &RPCServer{
		backend:           NewBackendShim(backend, metricCli),
		metricCli:         metricCli,
		peers:             peers,
		knownAlarmMetrics: make(map[alarmMetricKey]struct{}),
		maxTxnOps:         defaultMaxTxnOps,
		maxRequestBytes:   defaultMaxRequestBytes,
	}
	server.auth = newAuthManager(server.backend)
	server.tokens = newAuthTokenManager(server.backend)
	// The lease subsystem borrows its deps from server (backend/peers/metrics),
	// so it is wired after server exists and reads them live through server.
	server.leaseManager = newLeaseManager(server, time.Now().UnixNano())
	server.concurrencyClient = newConcurrencyClient(server)
	// Wire read/watch KeyValues to carry the lease attached to each key (etcd
	// parity). Safe to set before serving: New runs single-threaded.
	server.backend.SetLeaseLookup(server.leaseIDForKey)
	// Wire follower counts to the leader's count index (#41): the index is
	// leader-only, so a follower's count fallback was a full range scan — a
	// guaranteed request-deadline timeout at 10M keys. Forward a CountOnly
	// Range preserving the caller's revision, so a paginated sequence's counts
	// stay exact; the leader answers from its index (or its own bounded
	// fallback). Any failure falls back to the local path. No recursion: the
	// leader never proxies (IsLeader guard).
	// The proxy gets its own bounded budget, NOT the caller's deadline: a wedged
	// leader connection (TCP black hole, overload) would otherwise burn the whole
	// request deadline before the local fallback even starts — reproducing the
	// #41 timeout with extra steps. On failure, a short quiet window skips the
	// proxy entirely (a request may consult it twice — first-page rolling count,
	// then the rev=0 count path — and election gaps affect every request at once).
	var proxyQuietUntil atomic.Int64
	server.backend.SetCountProxy(func(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, bool) {
		if peers.IsLeader() || !peers.EtcdProxyEnabled() {
			return 0, false
		}
		if time.Now().UnixNano() < proxyQuietUntil.Load() {
			return 0, false
		}
		req := proto.Clone(r).(*etcdserverpb.RangeRequest)
		req.CountOnly = true
		req.Limit = 0
		req.KeysOnly = false
		pctx, cancel := context.WithTimeout(ctx, countProxyTimeout)
		defer cancel()
		// Mark the forward so the leader can fast-reject it (rather than full-scan)
		// while its count index is rebuilding; on that reject the failure branch
		// below opens the quiet window and this node falls back to a local scan.
		pctx = metadata.AppendToOutgoingContext(pctx, countProxyMarkerKey, "1")
		resp, err := peers.Range(pctx, req)
		if err != nil || resp == nil {
			proxyQuietUntil.Store(time.Now().Add(countProxyFailureQuiet).UnixNano())
			server.metricCli.EmitCounter("count.proxy.err", 1)
			return 0, false
		}
		return resp.Count, true
	})
	if err := server.restoreLeases(context.Background()); err != nil {
		klog.ErrorS(err, "restore leases failed")
	}
	emitVersionMetrics(metricCli)
	initEtcdMVCCOperationCounters(metricCli)
	initEtcdMVCCWatchEventCounter(metricCli)
	initEtcdLeaseExpiredCounter(metricCli)
	return server
}

// Close stops lease timers and waits for every lease-owned background task.
// Endpoint invokes it after listeners drain and before the backend closes TiKV.
func (s *RPCServer) Close() {
	if s.concurrencyClient != nil {
		_ = s.concurrencyClient.Close()
	}
	s.leaseManager.close()
}

// SetRequestLimits configures etcd-compatible admission limits. Zero keeps the
// defaults for programmatic embedders that predate these fields.
func (s *RPCServer) SetRequestLimits(maxTxnOps, maxRequestBytes uint) {
	if maxTxnOps > 0 {
		s.maxTxnOps = int(maxTxnOps)
	}
	if maxRequestBytes > 0 {
		s.maxRequestBytes = maxRequestBytes
	}
}

// SetAuthConfiguration applies etcd's token-provider, TTL, and bcrypt policy.
// endpoint.Config.Validate has already parsed provider keys; a failure here is a
// startup invariant violation and must remain fail-loud.
func (s *RPCServer) SetAuthConfiguration(authToken string, bcryptCost, tokenTTLSeconds uint) {
	cost := int(bcryptCost)
	if bcryptCost < uint(bcrypt.MinCost) || bcryptCost > uint(bcrypt.MaxCost) {
		cost = bcrypt.DefaultCost
	}
	s.auth.bcryptCost = cost
	if tokenTTLSeconds == 0 {
		tokenTTLSeconds = uint(authTokenTTL / time.Second)
	}
	ttl := time.Duration(tokenTTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = authTokenTTL
	}
	s.tokens.ttl = ttl
	if err := s.tokens.configureProvider(authToken); err != nil {
		panic(err)
	}
}

// SetAdvertiseClientInfo tells MemberList how to shape ClientURLs. Explicit
// URLs take precedence; otherwise deployments derive each URL from the member
// host, homogeneous clientPort, and TLS mode. Called once before serving.
func (s *RPCServer) SetAdvertiseClientInfo(clientPort int, https bool, urls ...string) {
	s.advertiseClientPort = clientPort
	s.advertiseClientHTTPS = https
	s.advertiseClientURLs = append([]string(nil), urls...)
}

// SetStaticMembers installs the DBaaS control-plane supplied KubeBrain service
// membership used by MemberList. TiKV/PD membership is deliberately unrelated.
func (s *RPCServer) SetStaticMembers(members []*etcdserverpb.Member) {
	s.staticMembers = make([]*etcdserverpb.Member, len(members))
	for i := range members {
		s.staticMembers[i] = proto.Clone(members[i]).(*etcdserverpb.Member)
		if len(s.advertiseClientURLs) > 0 {
			s.staticMembers[i].ClientURLs = append([]string(nil), s.advertiseClientURLs...)
		}
	}
	localID := s.memberIDForPeerIdentity(s.backend.GetResourceLock().Identity())
	emitServerIDMetric(s.metricCli, localID)
	emitKnownPeersMetric(s.metricCli, localID, s.staticMembers)
}

// Register register etcd grpc service
func (s *RPCServer) Register(server *grpc.Server) {
	etcdserverpb.RegisterLeaseServer(server, s)
	etcdserverpb.RegisterWatchServer(server, s)
	etcdserverpb.RegisterKVServer(server, s)
	etcdserverpb.RegisterClusterServer(server, s)
	etcdserverpb.RegisterMaintenanceServer(server, s)
	etcdserverpb.RegisterAuthServer(server, s)
	v3lockpb.RegisterLockServer(server, newLockServer(s.concurrencyClient))
	v3electionpb.RegisterElectionServer(server, newElectionServer(s.concurrencyClient))
}

func newConcurrencyClient(server *RPCServer) *clientv3.Client {
	client := clientv3.NewCtxClient(context.Background())
	client.KV = clientv3.NewKVFromKVClient(adapter.KvServerToKvClient(server), client)
	client.Lease = clientv3.NewLeaseFromLeaseClient(adapter.LeaseServerToLeaseClient(server), client, time.Second)
	client.Watcher = clientv3.NewWatchFromWatchClient(adapter.WatchServerToWatchClient(server), client)
	return client
}
