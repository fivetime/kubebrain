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
	"errors"
	"sort"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// A leader that has delivered terminal RangeStream metadata or a Snapshot
	// checksum has no application payload left to produce. Its next Recv must
	// promptly carry EOF or the authoritative trailing gRPC status. Bound that
	// drain independently so a broken peer cannot retain public admission
	// forever when the caller supplied no deadline.
	defaultProxyStreamTrailingStatusTimeout = unaryRpcTimeout
)

var errProxyStreamTrailingStatusTimeout = status.Error(codes.Unavailable, "forwarded stream timed out waiting for trailing status")

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
	// Response metadata only: observing a peer term must never refresh election
	// ownership, lease freshness, routing, or write fences.
	forwardedResponseTerm atomic.Uint64
	// Tests shorten this internal reliability boundary; zero uses the production
	// default. It is immutable after the RPC server starts serving.
	proxyStreamTrailingStatusTimeout time.Duration

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
	// One local online Snapshot owns almost a full logical-quota-sized anonymous
	// bbolt until its final checksum frame is sent. The production workspace is
	// sized for one such artifact, so the generic request admission limit is not
	// a safe concurrency bound for this KubeBrain-specific reconstruction path.
	snapshotAdmission  snapshotAdmission
	snapshotActive     atomic.Bool
	snapshotRejections snapshotRejectionDiagnostics
	maxDeleteRangeKeys uint32
	maxWatches         uint32
	watchQuotaMu       sync.Mutex
	activeWatches      int64
	activeWatchStreams atomic.Int64
	mvccPutSizeBytes   atomic.Int64
	leaderReady        atomic.Bool
	// Serializes the runtime etcd auth transition with legacy native unary
	// calls on the public listener. AuthEnable drains already-admitted calls
	// before committing, then new calls observe enabled auth and fail closed.
	nativeAuthBoundary sync.RWMutex
	// leadershipDrainBoundary prevents a voluntary leader release from cutting
	// across an already-admitted unary RPC. DrainLeadership publishes
	// leadershipDrainInProgress before taking the write side. New calls use
	// TryRLock and fail before handler admission instead of waiting through the
	// potentially multi-second release/successor window; calls that already hold
	// the read side still finish before release. Public admission reopens after
	// release, while peer calls remain fenced from the retiring member. Endpoint
	// may close public transports after a bounded GOAWAY window while this
	// boundary is held; peer transports and leadership streams remain until
	// durable handoff finishes.
	leadershipDrainBoundary   sync.RWMutex
	leadershipDrainInProgress atomic.Bool
	peerLeadershipDrained     atomic.Bool
	leadershipStreamDrainMu   sync.Mutex
	leadershipStreamDrain     chan struct{}
	leadershipStreamsDrained  bool

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

// AuthEnabled returns the current durable authentication state. The public
// HTTP access controller uses it to mirror etcd's client-certificate gateway
// rejection without trusting a potentially stale process-local flag.
func (s *RPCServer) AuthEnabled(ctx context.Context) (bool, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return false, err
	}
	return snapshot.Config.Enabled, nil
}

// PrepareLeadershipDrain runs release only after every admitted unary RPC has
// completed and holds later calls until the release is visible locally. New
// public unary calls may then proxy through the successor while new peer calls
// remain fenced from this retiring member. Streams deliberately remain active:
// Endpoint must first start HTTP/2 GOAWAY while the connection is non-idle, or
// net/http Shutdown can close it before the graceful signal reaches clientv3.
func (s *RPCServer) PrepareLeadershipDrain(release func() bool) bool {
	// Publish before waiting for the write lock. A unary request that races this
	// transition either already owns the read side (and must finish), or observes
	// the fence before entering its handler. It must never queue on the RWMutex
	// for the whole durable handoff and consume the external client's retry SLO.
	s.leadershipDrainInProgress.Store(true)
	s.leadershipDrainBoundary.Lock()
	succeeded := release()
	if succeeded {
		s.peerLeadershipDrained.Store(true)
	}
	s.leadershipDrainBoundary.Unlock()
	// Clear only after the write side is released. Calls in the narrow gap are
	// conservatively rejected before admission; later public calls may proxy to
	// the successor, and a failed drain restores both public and peer admission.
	s.leadershipDrainInProgress.Store(false)
	return succeeded
}

// RetireLeadershipStreams ends public and peer streams after transport GOAWAY
// has started. Public streams receive etcd's retryable stopped status; peer
// streams receive their distinct internal retry signal.
func (s *RPCServer) RetireLeadershipStreams() {
	s.retireLeadershipStreams()
}

// DrainLeadership preserves the single-call contract used by focused RPCServer
// tests and non-Endpoint callers. The production server uses the split prepare /
// transport / retire phases so GOAWAY cannot race an already-idle connection.
func (s *RPCServer) DrainLeadership(release func() bool) {
	if s.PrepareLeadershipDrain(release) {
		s.retireLeadershipStreams()
	}
}

func (s *RPCServer) leadershipStreamDrainState() (<-chan struct{}, bool) {
	s.leadershipStreamDrainMu.Lock()
	defer s.leadershipStreamDrainMu.Unlock()
	if s.leadershipStreamDrain == nil {
		s.leadershipStreamDrain = make(chan struct{})
	}
	return s.leadershipStreamDrain, s.leadershipStreamsDrained
}

func (s *RPCServer) retireLeadershipStreams() {
	s.leadershipStreamDrainMu.Lock()
	defer s.leadershipStreamDrainMu.Unlock()
	if s.leadershipStreamsDrained {
		return
	}
	if s.leadershipStreamDrain == nil {
		s.leadershipStreamDrain = make(chan struct{})
	}
	s.leadershipStreamsDrained = true
	close(s.leadershipStreamDrain)
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
	// Match etcd's processInternalRaftRequestOnce: every server attempt is
	// bounded by the server request timeout, while context.WithTimeout still
	// preserves any shorter client deadline. A long client deadline belongs to
	// the client's retry budget; it must not turn one TiKV attempt into an
	// unbounded in-flight request.
	return context.WithTimeout(ctx, unaryRpcTimeout)
}

func receiveProxyStreamResult[T any](
	ctx context.Context,
	results <-chan T,
	terminalSeen bool,
	trailingStatusTimeout time.Duration,
) (T, bool, error) {
	// The peer adapters normally close their result channels when the forwarded
	// gRPC context ends. Keep the public handler independently cancellation-aware:
	// a broken adapter must not retain stream admission after its caller left.
	// Reading until close is still intentional because the receive after a final
	// response carries the authoritative trailing gRPC status.
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, false, err
	}
	if !terminalSeen {
		select {
		case <-ctx.Done():
			var zero T
			return zero, false, ctx.Err()
		case result, ok := <-results:
			return result, ok, nil
		}
	}
	if trailingStatusTimeout <= 0 {
		trailingStatusTimeout = defaultProxyStreamTrailingStatusTimeout
	}
	timer := time.NewTimer(trailingStatusTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		var zero T
		return zero, false, ctx.Err()
	case <-timer.C:
		var zero T
		if err := ctx.Err(); err != nil {
			return zero, false, err
		}
		return zero, false, errProxyStreamTrailingStatusTimeout
	case result, ok := <-results:
		return result, ok, nil
	}
}

// SetMaxWatches sets the process-wide logical watch limit. It is configured
// before serving starts, so the limit itself is immutable on request paths.
func (s *RPCServer) SetMaxWatches(limit uint32) {
	s.maxWatches = limit
}

// SetLeasePromotionExtension configures the election window added to every
// recovered lease when this replica becomes leader, matching etcd's
// lessor.Promote(ElectionTimeout). It is configured before Campaign starts.
func (s *RPCServer) SetLeasePromotionExtension(extension time.Duration) {
	if extension < 0 {
		extension = 0
	}
	s.leasePromotionExtension = extension
}

type leaseState struct {
	id              int64
	incarnation     string
	ttl             int64
	remainingTTL    int64
	deadline        time.Time
	keys            map[string]struct{}
	timer           *time.Timer
	checkpointTimer *time.Timer
	revoked         chan struct{}
	revokePending   bool
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
	// Direct-constructed and single-node servers have no asynchronous leader
	// startup lifecycle. The production server toggles this around each term.
	server.leaderReady.Store(true)
	server.auth = newAuthManager(server.backend)
	server.tokens = newAuthTokenManager(server.backend)
	server.auth.repo.afterMutation = server.tokens.snapshots.invalidate
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
	server.backend.SetCountProxy(func(ctx context.Context, r *etcdserverpb.RangeRequest) (count int64, headerRevision int64, served bool) {
		if peers == nil || peers.IsLeader() || !peers.EtcdProxyEnabled() {
			return 0, 0, false
		}
		if time.Now().UnixNano() < proxyQuietUntil.Load() {
			emitCountProxyOutcome(server.metricCli, countProxyOutcomeQuietSkip)
			return 0, 0, false
		}
		req := proto.Clone(r).(*etcdserverpb.RangeRequest)
		req.CountOnly = true
		req.Limit = 0
		req.KeysOnly = false
		pctx, cancel := context.WithTimeout(ctx, countProxyTimeout)
		defer cancel()
		// A serializable follower Range is authorized locally before Count runs,
		// but the leader must still receive the original identity when this
		// optimization crosses the peer hop. Normalize it exactly like the other
		// pre-apply forwarding paths: preserve a raw client credential for
		// authoritative leader verification, translate verified client-certificate
		// identity, and scrub any inherited aliases or internal capabilities.
		var err error
		pctx, err = server.forwardWriteAuthContext(pctx)
		if err != nil {
			proxyQuietUntil.Store(time.Now().Add(countProxyFailureQuiet).UnixNano())
			server.metricCli.EmitCounter("count.proxy.err", 1)
			emitCountProxyOutcome(server.metricCli, countProxyOutcomeFailure)
			return 0, 0, false
		}
		// Mark the forward so the leader can fast-reject it (rather than full-scan)
		// while its count index is rebuilding; on that reject the failure branch
		// below opens the quiet window and this node falls back to a local scan. Add
		// this only after identity normalization, which deliberately removes every
		// inherited internal capability.
		pctx = withCanonicalOutgoingMetadata(pctx, countProxyMarkerKey, "1")
		resp, err := peers.Range(pctx, req)
		// This optimization returns only the count, but it crosses the same trusted
		// peer boundary as every other Range proxy. Do not let a missing/foreign
		// header or malformed CountOnly payload become a public count merely because
		// the local fallback interface intentionally collapses proxy errors to false.
		resp, err = validateKVProxyResult(server.metricCli, server.expectedProxyResponseIdentity(), kvProxyRPCRange, resp, err)
		resp, err = validateRangeProxyPayload(server.metricCli, req, resp, err)
		if err != nil {
			proxyQuietUntil.Store(time.Now().Add(countProxyFailureQuiet).UnixNano())
			server.metricCli.EmitCounter("count.proxy.err", 1)
			emitCountProxyOutcome(server.metricCli, countProxyOutcomeFailure)
			return 0, 0, false
		}
		server.observeForwardedRevision(resp.GetHeader(), nil)
		emitCountProxyOutcome(server.metricCli, countProxyOutcomeHit)
		return resp.Count, resp.GetHeader().GetRevision(), true
	})
	// Upstream recovers leases synchronously from local bbolt. Our equivalent
	// crosses PD/TiKV, so a network black hole must not prevent the process from
	// ever reaching its serving and shutdown lifecycle. A later leadership reload
	// remains authoritative and reconstructs the complete lease state.
	initLeaseStartupRestoreFailureMetric(metricCli)
	if err := server.restoreLeasesAtStartup(context.Background(), unaryRpcTimeout); err != nil {
		klog.ErrorS(err, "restore leases failed")
	}
	emitVersionMetrics(metricCli)
	initEtcdMVCCOperationCounters(metricCli)
	initEtcdMVCCPutSizeGauge(metricCli)
	initEtcdMVCCWatchEventCounter(metricCli)
	initEtcdMVCCWatchPendingEventGauge(metricCli)
	initWatchBackendIntegrityMetrics(metricCli)
	initWatchGenerationRecoveryMetrics(metricCli)
	initEtcdWatchSendLoopDurationMetrics(metricCli)
	initEtcdLeaseLifecycleMetrics(metricCli)
	initEtcdLeaseExpiredCounter(metricCli)
	initLeaseUncertainReconcileMetrics(metricCli)
	initLeaseBackgroundFailureMetrics(metricCli)
	initLeaseOrphanSweepFailureMetrics(metricCli)
	initLeaseGrantCleanupMetrics(metricCli)
	initLeaseRevokeReconcileMetrics(metricCli)
	initEtcdClientRequestCounters(metricCli)
	initEtcdClientGRPCBytesCounters(metricCli)
	initEtcdServerStreamFailureCounters(metricCli)
	initRangeStreamFailureMetrics(metricCli)
	initSnapshotFailureMetrics(metricCli)
	initMaintenanceProxyIntegrityMetrics(metricCli)
	initKVProxyIntegrityMetrics(metricCli)
	initAuthProxyIntegrityMetrics(metricCli)
	initLeaseProxyIntegrityMetrics(metricCli)
	initClusterProxyIntegrityMetrics(metricCli)
	initClientAdmissionMetrics(metricCli)
	initDeleteRangeAdmissionMetrics(metricCli)
	initCountProxyMetrics(metricCli)
	initEtcdMVCCKeysGauge(metricCli)
	initEtcdMVCCHashDurationMetrics(metricCli)
	initEtcdBackendSnapshotDuration(metricCli)
	initEtcdBackendDefragMetrics(metricCli)
	initEtcdBackendBboltCommitPhaseMetrics(metricCli)
	initEtcdWALMetrics(metricCli)
	initEtcdRaftSnapshotMetrics(metricCli)
	initAlarmStateMetrics(metricCli)
	initAuthRevisionMetrics(metricCli)
	return server
}

func (s *RPCServer) SetLeaderReady(ready bool) {
	s.leaderReady.Store(ready)
}

func (s *RPCServer) waitLeaderReady(ctx context.Context) error {
	if s.leaderReady.Load() {
		return nil
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if s.leaderReady.Load() {
				return nil
			}
		}
	}
}

// waitLeaderReadyEpoch admits a mutation only after durable leader startup and
// captures the term that actually owns the published state. A caller may have
// entered while an earlier term was leading and remained parked across a full
// demotion/re-election cycle, so an epoch sampled before the wait is stale.
func (s *RPCServer) waitLeaderReadyEpoch(ctx context.Context) (uint64, error) {
	if err := s.waitLeaderReady(ctx); err != nil {
		return 0, err
	}
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		return 0, status.Error(codes.Unavailable, "etcdserver: leadership changed during startup wait")
	}
	return epoch, nil
}

// Close stops lease timers and waits for every lease-owned background task.
// Endpoint invokes it after listeners drain and before the backend closes TiKV.

func (s *RPCServer) Close() error {
	var closeErr error
	if s.concurrencyClient != nil {
		closeErr = withoutContextCanceled(s.concurrencyClient.Close())
	}
	// The concrete production shim owns shared Watch PrevKV lookups. Cancel
	// those before the lease manager and raw TiKV backend start shutting down;
	// test/embedding BackendShim implementations are intentionally not required
	// to expose lifecycle management.
	if closer, ok := s.backend.(interface{ Close() }); ok {
		closer.Close()
	}
	s.leaseManager.close()
	return closeErr
}

func withoutContextCanceled(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var result error
		for _, child := range joined.Unwrap() {
			result = errors.Join(result, withoutContextCanceled(child))
		}
		return result
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
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
	// Upstream seeds every automatic lease ID with a stable per-member prefix.
	// A wall-clock-only process counter can collide when multiple ingress
	// replicas start from the same/cloned clock. Static DBaaS membership gives us
	// a collision-free ordinal without truncating the 32-bit public member ID.
	if prefix, ok := automaticLeaseMemberPrefix(s.staticMembers, localID); ok {
		s.leaseManager.configureAutomaticLeaseIDs(prefix, time.Now())
	}
	emitServerIDMetric(s.metricCli, localID)
	emitKnownPeersMetric(s.metricCli, localID, s.staticMembers)
}

func automaticLeaseMemberPrefix(members []*etcdserverpb.Member, localID uint64) (uint16, bool) {
	// LeaseGrant masks generator output to positive int64, so bit 15 of the
	// upstream uint16 prefix is discarded. Restrict prefixes to the remaining
	// collision-free 15-bit domain.
	if localID == 0 || len(members) == 0 || len(members) > 1<<15-1 {
		return 0, false
	}
	ids := make([]uint64, 0, len(members))
	for _, member := range members {
		if member.GetID() != 0 {
			ids = append(ids, member.GetID())
		}
	}
	if len(ids) != len(members) {
		return 0, false
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for index, id := range ids[1:] {
		if ids[index] == id {
			return 0, false
		}
	}
	for index, id := range ids {
		if id == localID {
			// Prefix zero is valid upstream, but reserving it for the unconfigured
			// fallback makes configured IDs visibly member-scoped.
			return uint16(index + 1), true
		}
	}
	return 0, false
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
