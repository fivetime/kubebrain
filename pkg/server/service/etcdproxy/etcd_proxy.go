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

package etcdproxy

import (
	"context"
	"crypto/tls"
	stderrors "errors"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/util"
)

const (
	proxyConnectTimeout = 5 * time.Second
	// Keep the retry delay aligned with the authoritative refresh throttle. A
	// one-second delay was visible in rollout traces after an exact peer-drain
	// sentinel and compounded clientv3's safe mutable retries. The connector is
	// serialized and refreshes are already rate-limited, so a shorter delay does
	// not create concurrent dial or election-record storms.
	proxyFailedLeaderRetryInterval = 250 * time.Millisecond
	// A transport failure can precede the follower election loop's next shared
	// record read. Refresh that record from the single background connector at a
	// bounded rate so a published successor is not hidden behind the local
	// election retry period. This is deliberately shorter than the production
	// 500ms election poll but long enough to prevent request-driven refresh
	// storms across followers.
	proxyFailedLeaderRefreshInterval = 250 * time.Millisecond
)

func loggedProxyKey(key []byte) string {
	return util.LoggedKey(key)
}

var leaseKeepAliveForwardTimeout = 5 * time.Second

type etcdProxy struct {
	// allowInsecure permits TLS-to-plaintext fallback only for an endpoint
	// explicitly configured to serve both modes.
	allowInsecure bool
	// dialTimeout is injectable for deterministic tests; zero uses the
	// production proxyConnectTimeout.
	dialTimeout          time.Duration
	callOptions          []grpc.CallOption
	coreUnaryCallOptions []grpc.CallOption

	election  leader.LeaderElection
	tlsConfig *tls.Config
	metricCli metrics.Metrics

	closed chan struct{}
	// updateCh lets request handlers wake the single background connector
	// without waiting behind updateMu or performing a multi-second peer health
	// check themselves. This keeps the public request lifetime bounded by its
	// own context while preserving one dial/check flight per proxy.
	updateCh  chan struct{}
	client    *clientv3.Client
	err       error
	curLeader string
	// failedLeader/retryAfter bound reconnect pressure when long-lived Watch
	// generations call waitReady every 100ms while the cached election holder is
	// still the same unavailable endpoint. A newly observed leader identity
	// bypasses this delay immediately.
	failedLeader string
	retryAfter   time.Time
	// refreshAfter rate-limits shared election-record reads while the cached
	// holder is the same transport identity that just failed. It is guarded by
	// lock and consumed only by the serialized background connector; request
	// goroutines merely wake that connector through updateCh.
	refreshAfter time.Time
	lock         sync.RWMutex
	// updateMu serializes updateClient so at most one goroutine builds/swaps the
	// forwarding client at a time. Without it the 1s checkLeaderLoop and the RPC
	// goroutines that call updateClient via waitReady run concurrently: each
	// releases `lock` during the slow clientv3.New/checkClientConn and then does
	// e.client = client, so a later winner overwrites an earlier client without
	// closing it (leaked connection, #47) and every caller stampedes the new
	// leader with its own dial (#41). Held for the whole function; acquired before
	// `lock` so the lock order is always updateMu -> lock. A separately guarded
	// attempt context lets waitReady preempt this single flight when election
	// publishes a different leader; it never starts a concurrent dial.
	updateMu  sync.Mutex
	attemptMu sync.Mutex
	// attemptGeneration distinguishes completion of an older tracked context
	// from a later one without comparing context.CancelFunc values.
	attemptGeneration uint64
	attemptLeader     string
	attemptCancel     context.CancelFunc

	cancel    context.CancelFunc
	loopDone  chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (e *etcdProxy) EtcdProxyEnabled() bool {
	return true
}

func proxyCallOptions(maxRequestBytes uint) []grpc.CallOption {
	if maxRequestBytes == 0 {
		maxRequestBytes = 1572864
	}
	return []grpc.CallOption{
		grpc.WaitForReady(true),
		grpc.MaxCallSendMsgSize(int(maxRequestBytes + 512*1024)),
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
	}
}

func proxyCoreUnaryCallOptions(maxRequestBytes uint) []grpc.CallOption {
	options := proxyCallOptions(maxRequestBytes)
	// readyClient already proves the published peer transport before admission.
	// If that transport changes state after the proof, fail the core unary call
	// immediately so the public client can perform its operation-specific
	// ambiguity reconciliation. Waiting for the old transport here compounds the
	// reconnect Range and successor write while providing no additional safety.
	return append(options, grpc.WaitForReady(false))
}

// A voluntary handoff publishes the successor before every follower has
// necessarily completed its own health-check/dial loop. Keep unary requests
// parked briefly through that propagation window, but leave room for clientv3's
// bounded safe retries plus the caller's reconciliation Range and Watch inside
// the five-second rollout SLO. The connector runs independently, so a request
// timing out here has not reached a leader RPC and can use the exact
// before-admission retry sentinel without waiting for a peer dial itself.
const proxyReadyWaitTimeout = 200 * time.Millisecond

// NewEtcdProxy return an ETCD proxy for forward request to leader.
// The election identity is the leader's peer endpoint. That listener registers
// the complete RPC surface without public admission, so forwarding there counts
// each external request exactly once at its ingress replica.
func NewEtcdProxy(ctx context.Context, leaderElection leader.LeaderElection, tlsConfig *tls.Config, allowInsecure bool, maxRequestBytes uint) EtcdProxy {
	return NewEtcdProxyWithMetrics(ctx, leaderElection, tlsConfig, allowInsecure, maxRequestBytes, nil)
}

// NewEtcdProxyWithMetrics constructs the forwarding proxy and publishes fixed,
// low-cardinality telemetry for the core KV unary path. Keep NewEtcdProxy as a
// compatibility wrapper for embedders that do not supply a metrics backend.
func NewEtcdProxyWithMetrics(ctx context.Context, leaderElection leader.LeaderElection, tlsConfig *tls.Config, allowInsecure bool,
	maxRequestBytes uint, metricCli metrics.Metrics,
) EtcdProxy {
	runCtx, cancel := context.WithCancel(ctx)
	proxy := &etcdProxy{
		election: leaderElection, tlsConfig: tlsConfig,
		allowInsecure: allowInsecure, callOptions: proxyCallOptions(maxRequestBytes),
		coreUnaryCallOptions: proxyCoreUnaryCallOptions(maxRequestBytes),
		metricCli:            metricCli,
		cancel:               cancel, loopDone: make(chan struct{}), updateCh: make(chan struct{}, 1),
	}
	initUnaryForwardMetrics(metricCli)
	go func() {
		defer util.Recover()
		defer close(proxy.loopDone)
		proxy.checkLeaderLoop(runCtx)
	}()

	return proxy
}

func (e *etcdProxy) Close() error {
	e.closeOnce.Do(func() {
		e.cancel()
		<-e.loopDone
		e.updateMu.Lock()
		e.lock.Lock()
		if e.client != nil {
			e.closeClient(e.client)
			e.client = nil
		}
		e.curLeader = ""
		e.failedLeader = ""
		e.retryAfter = time.Time{}
		e.refreshAfter = time.Time{}
		e.lock.Unlock()
		e.updateMu.Unlock()
	})
	return e.closeErr
}

func (e *etcdProxy) checkLeaderLoop(ctx context.Context) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		e.updateClientContext(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.updateCh:
		case <-timer.C:
		}
		timer.Reset(time.Second)
	}
}

func (e *etcdProxy) requestClientUpdate() {
	if e.updateCh == nil {
		return
	}
	select {
	case e.updateCh <- struct{}{}:
	default:
	}
}

func (e *etcdProxy) trackLeaderConnectionAttempt(parent context.Context, leaderIdentity string) (context.Context, func()) {
	attemptCtx, cancel := context.WithCancel(parent)
	e.attemptMu.Lock()
	e.attemptGeneration++
	generation := e.attemptGeneration
	e.attemptLeader = leaderIdentity
	e.attemptCancel = cancel
	e.attemptMu.Unlock()

	// The election may have changed immediately before publication of this
	// attempt. Keep observing it even without request-side waitReady callers:
	// existing forwarded RPCs and local requests on a newly elected leader do
	// not enter that readiness loop. Otherwise a stale health check can retain
	// the old shared connection for its entire five-second timeout.
	e.cancelStaleLeaderConnectionAttempt()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case <-ticker.C:
				e.cancelStaleLeaderConnectionAttempt()
			}
		}
	}()
	return attemptCtx, func() {
		cancel()
		<-monitorDone
		e.attemptMu.Lock()
		if e.attemptGeneration == generation {
			e.attemptLeader = ""
			e.attemptCancel = nil
		}
		e.attemptMu.Unlock()
	}
}

func (e *etcdProxy) cancelStaleLeaderConnectionAttempt() {
	observedLeader := e.election.GetLeaderInfo()
	if !election.IsLeaderKnown(observedLeader) {
		return
	}
	e.attemptMu.Lock()
	defer e.attemptMu.Unlock()
	if e.attemptCancel != nil && observedLeader != e.attemptLeader {
		e.attemptCancel()
	}
}

func (e *etcdProxy) resetClient() (reset bool) {
	reset = e.client != nil

	if e.client != nil {
		e.closeClient(e.client)
		e.client = nil
	}

	if e.closed != nil {
		close(e.closed)
	}
	e.closed = make(chan struct{})
	return
}

// closeClient runs only while updateMu is held, so hot-swap cleanup failures
// can be accumulated without racing final shutdown. A replacement client may
// still become healthy, but the leaked/failed transport remains part of this
// proxy instance's lifecycle result.
func (e *etcdProxy) closeClient(client *clientv3.Client) {
	if err := client.Close(); err != nil {
		e.closeErr = stderrors.Join(e.closeErr, err)
		klog.ErrorS(err, "failed to close etcd proxy client")
	}
}

func (e *etcdProxy) updateClient() {
	e.updateClientContext(context.Background())
}

func (e *etcdProxy) updateClientContext(ctx context.Context) {
	// Serialize the whole build/swap: concurrent callers otherwise leak clients
	// and stampede the leader (#41/#47). Once the winner has a healthy client, the
	// queued callers fall through the hasClient()+checkConnContext() fast path and return
	// without redialing.
	e.updateMu.Lock()
	defer e.updateMu.Unlock()

	if e.hasClient() {
		e.lock.RLock()
		leaderIdentity := e.curLeader
		e.lock.RUnlock()
		attemptCtx, finishAttempt := e.trackLeaderConnectionAttempt(ctx, leaderIdentity)
		err := e.checkConnContext(attemptCtx)
		finishAttempt()
		if err != nil {
			e.lock.Lock()
			failedLeader := e.curLeader
			e.curLeader = ""
			e.err = err
			e.deferLeaderRetryLocked(failedLeader)
			if e.resetClient() {
				klog.InfoS("reset client caused by checking conn", "err", e.err)
			}
			e.lock.Unlock()
			e.requestClientUpdate()
			return
		}
	}

	if e.election.IsLeader() {
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.resetClient() {
			klog.InfoS("reset client caused by becoming leader")
		}
		// A leader does not forward, so the cached forwarding identity is no
		// longer backed by a client. Clear it before a later demotion: if the
		// successor is the same address we previously followed, retaining this
		// value would make the sameLeader fast path skip redial forever.
		e.curLeader = ""
		e.failedLeader = ""
		e.retryAfter = time.Time{}
		e.refreshAfter = time.Time{}
		e.err = nil
		return
	}

	curLeader := e.election.GetLeaderInfo()
	if !election.IsLeaderKnown(curLeader) {
		refreshCtx, cancel := context.WithTimeout(ctx, e.connectionTimeout())
		err := e.election.RefreshLeaderInfo(refreshCtx)
		cancel()
		if err != nil {
			e.lock.Lock()
			e.err = err
			e.lock.Unlock()
			return
		}
		curLeader = e.election.GetLeaderInfo()
	}
	curLeader = e.refreshFailedLeader(ctx, curLeader)
	e.lock.RLock()
	sameLeader := e.client != nil && curLeader == e.curLeader
	retryDeferred := curLeader == e.failedLeader && time.Now().Before(e.retryAfter)
	e.lock.RUnlock()
	if sameLeader || retryDeferred || !election.IsLeaderKnown(curLeader) {
		return
	}

	e.lock.Lock()
	oldLeader := e.curLeader
	// close prev client
	if e.resetClient() {
		klog.InfoS("reset client caused by changing leader")
	}
	e.lock.Unlock()

	klog.InfoS("try to conn to new leader", "leaderIdentity", curLeader)
	tlsConfigs := e.dialTLSConfigs()

	// The election identity is already the leader's peer endpoint.
	dialEndpoint := curLeader
	for _, tlsConfig := range tlsConfigs {
		dialTimeout := e.connectionTimeout()
		var dialOptions []grpc.DialOption
		dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(peerUnaryForwardErrorInterceptor))
		if tlsConfig != nil && tlsConfig.ServerName != "" {
			// grpc-go derives TLS ServerName from the resolver authority and
			// overwrites tls.Config.ServerName. The proxy connects to a leader Pod
			// IP, so preserve the configured stable service DNS identity explicitly.
			dialOptions = append(dialOptions, grpc.WithAuthority(tlsConfig.ServerName))
		}
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{dialEndpoint},
			TLS:         tlsConfig,
			DialTimeout: dialTimeout,
			DialOptions: dialOptions,
		})
		if err != nil {
			klog.ErrorS(err, "failed to create new client")
			e.lock.Lock()
			e.err = status.Error(codes.Internal, err.Error())
			e.deferLeaderRetryLocked(curLeader)
			e.lock.Unlock()
			e.requestClientUpdate()
			return
		}

		klog.InfoS("check conn to new leader", "leaderIdentity", curLeader, "dialEndpoint", dialEndpoint, "secure", tlsConfig != nil)

		attemptCtx, finishAttempt := e.trackLeaderConnectionAttempt(ctx, curLeader)
		err = checkClientConnContext(attemptCtx, client, nil, dialTimeout)
		finishAttempt()
		if err != nil {
			e.closeClient(client)
			klog.InfoS("leader connection not ready", "err", err, "leaderIdentity", curLeader, "dialEndpoint", dialEndpoint, "secure", tlsConfig != nil)
			e.lock.Lock()
			e.err = err
			e.lock.Unlock()
			if errors.Is(err, context.DeadlineExceeded) {
				// maybe tls config error, just change config and retry
				continue
			}
			break
		}

		e.lock.Lock()
		e.client = client
		e.err = nil
		e.curLeader = curLeader
		e.failedLeader = ""
		e.retryAfter = time.Time{}
		e.refreshAfter = time.Time{}
		e.lock.Unlock()
		klog.InfoS("conn to new leader", "oldLeader", oldLeader, "curLeader", curLeader)
		return
	}

	e.lock.Lock()
	klog.InfoS("leader connection not ready", "err", e.err, "leaderIdentity", curLeader, "dialEndpoint", dialEndpoint)
	e.curLeader = ""
	e.deferLeaderRetryLocked(curLeader)
	if e.client != nil {
		e.closeClient(e.client)
		e.client = nil
	}
	e.lock.Unlock()
	e.requestClientUpdate()
}

func (e *etcdProxy) deferLeaderRetryLocked(leader string) {
	if !election.IsLeaderKnown(leader) {
		return
	}
	e.failedLeader = leader
	e.retryAfter = time.Now().Add(proxyFailedLeaderRetryInterval)
	// Permit one immediate authoritative refresh after each newly observed
	// transport failure. refreshFailedLeader reserves subsequent reads before
	// doing I/O, so concurrent wakeups still cannot stampede the backend.
	e.refreshAfter = time.Time{}
}

func (e *etcdProxy) refreshFailedLeader(ctx context.Context, cachedLeader string) string {
	if !election.IsLeaderKnown(cachedLeader) {
		return cachedLeader
	}
	now := time.Now()
	e.lock.Lock()
	if cachedLeader != e.failedLeader || now.Before(e.refreshAfter) {
		e.lock.Unlock()
		return cachedLeader
	}
	e.refreshAfter = now.Add(proxyFailedLeaderRefreshInterval)
	e.lock.Unlock()

	timeout := proxyFailedLeaderRefreshInterval
	if connectionTimeout := e.connectionTimeout(); connectionTimeout < timeout {
		timeout = connectionTimeout
	}
	refreshCtx, cancel := context.WithTimeout(ctx, timeout)
	err := e.election.RefreshLeaderInfo(refreshCtx)
	cancel()
	if err != nil {
		klog.V(2).InfoS("failed to refresh election record after leader transport failure", "leaderIdentity", cachedLeader, "err", err)
		return cachedLeader
	}
	refreshedLeader := e.election.GetLeaderInfo()
	if election.IsLeaderKnown(refreshedLeader) && refreshedLeader != cachedLeader {
		klog.InfoS("observed successor after leader transport failure", "oldLeader", cachedLeader, "curLeader", refreshedLeader)
	}
	return refreshedLeader
}

func (e *etcdProxy) connectionTimeout() time.Duration {
	if e.dialTimeout > 0 {
		return e.dialTimeout
	}
	return proxyConnectTimeout
}

func (e *etcdProxy) dialTLSConfigs() []*tls.Config {
	configs := []*tls.Config{e.tlsConfig}
	if e.tlsConfig != nil && e.allowInsecure {
		configs = append(configs, nil)
	}
	return configs
}

func (e *etcdProxy) hasClient() bool {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.client != nil
}

func (e *etcdProxy) checkConnContext(ctx context.Context) error {
	e.lock.RLock()
	client := e.client
	err := e.err
	e.lock.RUnlock()
	return checkExistingClientConnContext(ctx, client, err, e.connectionTimeout())
}

// checkExistingClientConn verifies that an already-published peer transport is
// alive without treating backend readiness as transport failure. A data-plane
// leader can report NOT_SERVING while a TiKV Region elects a successor and still
// safely serve storage-free operations such as ordinary lease keepalives. New
// connections continue to require SERVING in checkClientConn below.
func checkExistingClientConn(client *clientv3.Client, clientErr error, timeout time.Duration) error {
	return checkExistingClientConnContext(context.Background(), client, clientErr, timeout)
}

func checkExistingClientConnContext(parent context.Context, client *clientv3.Client, clientErr error, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if client == nil {
		return clientErr
	}
	_, err := healthpb.NewHealthClient(client.ActiveConnection()).Check(
		ctx, &healthpb.HealthCheckRequest{},
	)
	return err
}

func checkClientConn(client *clientv3.Client, clientErr error, timeout time.Duration) error {
	return checkClientConnContext(context.Background(), client, clientErr, timeout)
}

func checkClientConnContext(parent context.Context, client *clientv3.Client, clientErr error, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if client == nil {
		return clientErr
	}

	// grpc.NewClient is lazy and ignores WithBlock. The peer listener exposes
	// the standard health service outside etcd Auth, so this proves TCP, mTLS,
	// HTTP/2, service routing, and completed server initialization without an
	// internal privileged token or an unauthenticated Maintenance.Status call.
	response, err := healthpb.NewHealthClient(client.ActiveConnection()).Check(
		ctx, &healthpb.HealthCheckRequest{},
	)
	if err != nil {
		return err
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return status.Errorf(codes.Unavailable, "leader health status is %s", response.GetStatus())
	}
	return nil
}

func (e *etcdProxy) readyClient(ctx context.Context) (*clientv3.Client, string, <-chan struct{}, error) {
	if err := e.waitReady(ctx); err != nil {
		return nil, "", nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	if err := e.readyLocked(); err != nil {
		return nil, "", nil, err
	}
	return e.client, e.curLeader, e.closed, nil
}

func (e *etcdProxy) markForwardError(ctx context.Context, client *clientv3.Client, err error) {
	if err == nil || !shouldResetForwardClient(client, err) {
		return
	}
	// A cancelled or expired CLIENT context is not a leader-connection failure --
	// the caller went away. Resetting the shared forwarding client here would tear
	// down the connection every other in-flight follower request depends on (#23).
	// Only a genuine connection error with a still-live caller context (e.g. the
	// leader is unreachable) should reset it; a truly dead leader is also caught by
	// the periodic checkLeaderLoop.
	if ctx.Err() != nil {
		return
	}
	e.lock.Lock()
	if e.client != client {
		e.lock.Unlock()
		return
	}
	failedLeader := e.curLeader
	e.err = err
	e.curLeader = ""
	e.deferLeaderRetryLocked(failedLeader)
	if e.resetClient() {
		klog.InfoS("reset client caused by forward error", "err", err)
	}
	e.lock.Unlock()
	e.requestClientUpdate()
}

func isForwardConnectionError(err error) bool {
	if proxyprotocol.IsPeerDrainedBeforeAdmission(err) || proxyprotocol.IsPeerStreamDrained(err) {
		return true
	}
	// The leader uses this application-level decline to make follower count
	// requests fall back locally while its count index rebuilds. Resetting the
	// healthy shared transport here strands unrelated writes during rollout.
	if proxyprotocol.IsCountIndexNotReady(err) {
		return false
	}
	for _, topologyErr := range []error{
		rpctypes.ErrGRPCNoLeader,
		rpctypes.ErrGRPCNotLeader,
		rpctypes.ErrGRPCLeaderChanged,
		rpctypes.ErrGRPCStopped,
	} {
		if sameGRPCStatus(err, topologyErr) {
			return true
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func sameGRPCStatus(err, target error) bool {
	return status.Code(err) == status.Code(target) &&
		status.Convert(err).Message() == status.Convert(target).Message()
}

// shouldResetForwardClient separates topology/transport failures from an
// application-level Unavailable returned over a live peer connection. TiKV
// admission and leadership initialization can transiently return Unavailable
// while a large Snapshot is scanned. Closing the shared READY client in that
// case strands every unrelated follower RPC and turns one backend error into a
// proxy-wide availability gap. A genuine transport failure moves the gRPC
// connection out of READY and remains resettable; exact drain/leader sentinels
// are reset regardless of the sampled connectivity state.
func shouldResetForwardClient(client *clientv3.Client, err error) bool {
	if proxyprotocol.IsCountIndexNotReady(err) {
		return false
	}
	if isForwardConnectionError(err) {
		return true
	}
	return status.Code(err) == codes.Unavailable &&
		(client == nil || client.ActiveConnection().GetState() != connectivity.Ready)
}

func forwardUnaryWithDrainRetry[T any](
	e *etcdProxy,
	ctx context.Context,
	rpc string,
	call func(*clientv3.Client, string) (T, error),
) (T, error) {
	var zero T
	var waitReadyDuration time.Duration
	var forwardDuration time.Duration
	var drainRetries int
	defer func() {
		// Direct unit calls do not carry a server transport stream; SetTrailer's
		// error is deliberately ignored because observability must never change
		// the etcd result. Real public/peer handlers expose these bounded values.
		_ = grpc.SetTrailer(ctx, metadata.Pairs(
			proxyprotocol.CoreUnaryProxyRouteTrailer, "proxy",
			proxyprotocol.CoreUnaryProxyWaitMicrosTrailer, strconv.FormatInt(max(0, waitReadyDuration.Microseconds()), 10),
			proxyprotocol.CoreUnaryProxyForwardMicrosTrailer, strconv.FormatInt(max(0, forwardDuration.Microseconds()), 10),
			proxyprotocol.CoreUnaryProxyDrainRetriesTrailer, strconv.Itoa(drainRetries),
		))
	}()
	for attempt := 0; attempt < 2; attempt++ {
		readyStarted := time.Now()
		client, leader, _, err := e.readyClient(ctx)
		waitReadyDuration += time.Since(readyStarted)
		emitUnaryForwardDuration(e.metricCli, rpc, unaryForwardStageWaitReady, readyStarted, ctx, client, err)
		if err != nil {
			return zero, err
		}
		forwardStarted := time.Now()
		response, err := call(client, leader)
		forwardDuration += time.Since(forwardStarted)
		emitUnaryForwardDuration(e.metricCli, rpc, unaryForwardStageForward, forwardStarted, ctx, client, err)
		e.markForwardError(ctx, client, err)
		if err == nil || !proxyprotocol.IsPeerDrainedBeforeAdmission(err) || attempt == 1 {
			return response, err
		}
		emitUnaryForwardDrainRetry(e.metricCli, rpc)
		drainRetries++
	}
	return zero, status.Error(codes.Internal, "kubebrain: exhausted peer drain retry")
}

func getKeyFromTxn(txn *etcdserverpb.TxnRequest) (string, int64) {
	if len(txn.GetCompare()) <= 0 {
		return "", 0
	}
	return string(txn.GetCompare()[0].GetKey()), txn.GetCompare()[0].GetModRevision()
}

func (e *etcdProxy) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	key, rev := getKeyFromTxn(txn)
	resp, err := forwardUnaryWithDrainRetry(e, ctx, unaryForwardRPCTxn, func(client *clientv3.Client, leader string) (*etcdserverpb.TxnResponse, error) {
		klog.InfoS("forward txn", "leader", leader, "key", key, "rev", rev)
		return etcdserverpb.NewKVClient(client.ActiveConnection()).Txn(ctx, txn, e.coreUnaryCallOptions...)
	})
	if err != nil {
		klog.InfoS("forward txn failed", "key", loggedProxyKey([]byte(key)), "err", err.Error())
		return nil, err
	}
	if !resp.GetSucceeded() {
		var respRev int64

		if len(resp.Responses) == 1 {
			getResp := (*clientv3.GetResponse)(resp.Responses[0].GetResponseRange())
			if getResp != nil && getResp.Kvs != nil && len(getResp.Kvs) == 1 {
				respRev = getResp.Kvs[0].ModRevision
			}
		}

		klog.InfoS("forward txn cas failed",
			"key", key,
			"result", resp.GetSucceeded(),
			"rev", rev,
			"respRev", resp.GetHeader().GetRevision(),
			"kvRev", respRev,
		)
	}

	return resp, err
}

func (e *etcdProxy) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return forwardUnaryWithDrainRetry(e, ctx, unaryForwardRPCRange, func(client *clientv3.Client, leader string) (*etcdserverpb.RangeResponse, error) {
		klog.InfoS("forward range", "leader", leader, "key", loggedProxyKey(req.Key), "rangeEnd", loggedProxyKey(req.RangeEnd), "revision", req.Revision)
		return etcdserverpb.NewKVClient(client.ActiveConnection()).Range(ctx, req, e.coreUnaryCallOptions...)
	})
}

func peerUnaryForwardErrorInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	return normalizePeerUnaryForwardError(ctx, err)
}

func normalizePeerUnaryForwardError(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	// Retiring the shared peer connection after a leader loss can make any
	// in-flight unary forward return Canceled even though the downstream caller
	// is still live. Classify that topology failure as retryable at the internal
	// client boundary; preserve genuine caller cancellation and application
	// statuses. Streaming RPCs keep their operation-specific resume/integrity
	// contracts and are intentionally outside this interceptor.
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return rpctypes.ErrGRPCLeaderChanged
	}
	return err
}

func (e *etcdProxy) RangeStream(ctx context.Context, req *etcdserverpb.RangeRequest) (<-chan RangeStreamResult, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward range stream", "leader", leader, "key", loggedProxyKey(req.Key), "rangeEnd", loggedProxyKey(req.RangeEnd), "revision", req.Revision)
	stream, err := etcdserverpb.NewKVClient(client.ActiveConnection()).RangeStream(ctx, req, e.callOptions...)
	if err != nil {
		e.markForwardError(ctx, client, err)
		return nil, err
	}
	out := make(chan RangeStreamResult)
	go func() {
		defer close(out)
		for {
			response, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				return
			}
			if recvErr != nil {
				e.markForwardError(ctx, client, recvErr)
				select {
				case out <- RangeStreamResult{Err: recvErr}:
				case <-ctx.Done():
				}
				return
			}
			select {
			case out <- RangeStreamResult{Response: response}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (e *etcdProxy) MemberList(ctx context.Context, req *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward member list", "leader", leader, "linearizable", req.Linearizable)
	response, err := etcdserverpb.NewClusterClient(client.ActiveConnection()).MemberList(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return response, err
}

func (e *etcdProxy) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return forwardUnaryWithDrainRetry(e, ctx, unaryForwardRPCPut, func(client *clientv3.Client, leader string) (*etcdserverpb.PutResponse, error) {
		klog.InfoS("forward put", "leader", leader, "key", loggedProxyKey(req.Key), "lease", req.Lease)
		return etcdserverpb.NewKVClient(client.ActiveConnection()).Put(ctx, req, e.coreUnaryCallOptions...)
	})
}

func (e *etcdProxy) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return forwardUnaryWithDrainRetry(e, ctx, unaryForwardRPCDeleteRange, func(client *clientv3.Client, leader string) (*etcdserverpb.DeleteRangeResponse, error) {
		klog.InfoS("forward delete range", "leader", leader, "key", loggedProxyKey(req.Key), "rangeEnd", loggedProxyKey(req.RangeEnd))
		return etcdserverpb.NewKVClient(client.ActiveConnection()).DeleteRange(ctx, req, e.coreUnaryCallOptions...)
	})
}

func (e *etcdProxy) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward compact", "leader", leader, "revision", req.Revision, "physical", req.Physical)
	resp, err := etcdserverpb.NewKVClient(client.ActiveConnection()).Compact(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Alarm(ctx context.Context, req *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward alarm", "leader", leader, "action", req.GetAction(), "alarm", req.GetAlarm(), "memberID", req.GetMemberID())
	resp, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Alarm(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Defragment(ctx context.Context, req *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward defragment", "leader", leader)
	resp, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Defragment(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Status(ctx context.Context, req *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward status", "leader", leader)
	resp, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Status(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Hash(ctx context.Context, req *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward hash", "leader", leader)
	resp, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Hash(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) HashKV(ctx context.Context, req *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward hash kv", "leader", leader, "revision", req.GetRevision())
	resp, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).HashKV(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Downgrade(ctx context.Context, req *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward downgrade", "leader", leader, "action", req.GetAction(), "version", req.GetVersion())
	resp, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Downgrade(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) AuthStatus(ctx context.Context, req *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth status", "leader", leader)
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).AuthStatus(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Authenticate(ctx context.Context, req *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward authenticate", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).Authenticate(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) AuthEnable(ctx context.Context, req *etcdserverpb.AuthEnableRequest) (*etcdserverpb.AuthEnableResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth enable", "leader", leader)
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).AuthEnable(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) AuthDisable(ctx context.Context, req *etcdserverpb.AuthDisableRequest) (*etcdserverpb.AuthDisableResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth disable", "leader", leader)
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).AuthDisable(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserAdd(ctx context.Context, req *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user add", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserAdd(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserDelete(ctx context.Context, req *etcdserverpb.AuthUserDeleteRequest) (*etcdserverpb.AuthUserDeleteResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user delete", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserDelete(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserChangePassword(ctx context.Context, req *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user change password", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserChangePassword(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserGrantRole(ctx context.Context, req *etcdserverpb.AuthUserGrantRoleRequest) (*etcdserverpb.AuthUserGrantRoleResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user grant role", "leader", leader, "user", req.GetUser(), "role", req.GetRole())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserGrantRole(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserRevokeRole(ctx context.Context, req *etcdserverpb.AuthUserRevokeRoleRequest) (*etcdserverpb.AuthUserRevokeRoleResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user revoke role", "leader", leader, "name", req.GetName(), "role", req.GetRole())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserRevokeRole(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) RoleAdd(ctx context.Context, req *etcdserverpb.AuthRoleAddRequest) (*etcdserverpb.AuthRoleAddResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth role add", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).RoleAdd(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) RoleDelete(ctx context.Context, req *etcdserverpb.AuthRoleDeleteRequest) (*etcdserverpb.AuthRoleDeleteResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth role delete", "leader", leader, "role", req.GetRole())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).RoleDelete(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) RoleGrantPermission(ctx context.Context, req *etcdserverpb.AuthRoleGrantPermissionRequest) (*etcdserverpb.AuthRoleGrantPermissionResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth role grant permission", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).RoleGrantPermission(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) RoleRevokePermission(ctx context.Context, req *etcdserverpb.AuthRoleRevokePermissionRequest) (*etcdserverpb.AuthRoleRevokePermissionResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth role revoke permission", "leader", leader, "role", req.GetRole())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).RoleRevokePermission(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserGet(ctx context.Context, req *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user get", "leader", leader, "name", req.GetName())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserGet(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) UserList(ctx context.Context, req *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth user list", "leader", leader)
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).UserList(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) RoleGet(ctx context.Context, req *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth role get", "leader", leader, "role", req.GetRole())
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).RoleGet(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) RoleList(ctx context.Context, req *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward auth role list", "leader", leader)
	resp, err := etcdserverpb.NewAuthClient(client.ActiveConnection()).RoleList(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease grant", "leader", leader, "id", req.ID, "ttl", req.TTL)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseGrant(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease revoke", "leader", leader, "id", req.ID)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseRevoke(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease keepalive", "leader", leader, "id", req.ID)
	// The follower forwards one keepalive message and returns one response, but
	// LeaseKeepAlive is a bidi stream. Derive a per-call cancelable context and
	// cancel it on return so the leader-side stream is fully torn down instead of
	// left half-open (CloseSend only closes the send direction, leaking the
	// receive side until the long-lived caller context ends) (#62).
	callCtx, cancel := context.WithTimeout(ctx, leaseKeepAliveForwardTimeout)
	defer cancel()
	stream, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseKeepAlive(callCtx, e.callOptions...)
	if err != nil {
		if mapped := mapLeaseKeepAliveForwardError(ctx, callCtx, err); mapped != err {
			return nil, mapped
		}
		e.markForwardError(ctx, client, err)
		return nil, err
	}
	if err := stream.Send(req); err != nil {
		if mapped := mapLeaseKeepAliveForwardError(ctx, callCtx, err); mapped != err {
			return nil, mapped
		}
		e.markForwardError(ctx, client, err)
		return nil, err
	}
	resp, err := stream.Recv()
	if mapped := mapLeaseKeepAliveForwardError(ctx, callCtx, err); mapped != err {
		return nil, mapped
	}
	e.markForwardError(ctx, client, err)
	return resp, err
}

func mapLeaseKeepAliveForwardError(parentCtx, callCtx context.Context, err error) error {
	// A voluntary handoff may retire this internal peer stream after consuming
	// the message. Ask the outer keepalive loop to replay it against the successor.
	if err != nil && parentCtx.Err() == nil && proxyprotocol.IsPeerStreamDrained(err) {
		return rpctypes.ErrGRPCLeaderChanged
	}
	// grpc may surface codes.DeadlineExceeded just before callCtx.Err becomes
	// observable to this goroutine. The parent still being live distinguishes
	// the proxy's bounded per-message forwarding deadline from caller
	// cancellation. The downstream bidi stream already consumed this request, so
	// make the outer keepalive loop retry it after leader recovery instead of
	// terminating the public stream with an internal timeout.
	if err != nil && parentCtx.Err() == nil &&
		(callCtx.Err() == context.DeadlineExceeded ||
			errors.Is(err, context.DeadlineExceeded) ||
			status.Code(err) == codes.DeadlineExceeded) {
		return rpctypes.ErrGRPCLeaderChanged
	}
	// The shared leader ClientConn is retired on a topology change. Its in-flight
	// bidi stream can report Canceled even though the downstream KeepAlive stream
	// is still live; classify it as retryable so the already-consumed message is
	// retried by leaseKeepAlive's Unavailable loop. Preserve caller cancellation.
	if err != nil && parentCtx.Err() == nil &&
		(errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled) {
		return rpctypes.ErrGRPCLeaderChanged
	}
	return err
}

func (e *etcdProxy) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease ttl", "leader", leader, "id", req.ID, "keys", req.Keys)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseTimeToLive(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	client, leader, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	klog.InfoS("forward lease leases", "leader", leader)
	resp, err := etcdserverpb.NewLeaseClient(client.ActiveConnection()).LeaseLeases(ctx, req, e.callOptions...)
	e.markForwardError(ctx, client, err)
	return resp, err
}

func (e *etcdProxy) Snapshot(ctx context.Context, req *etcdserverpb.SnapshotRequest) (<-chan SnapshotResult, error) {
	client, _, _, err := e.readyClient(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Snapshot(ctx, req, e.callOptions...)
	if err != nil {
		e.markForwardError(ctx, client, err)
		return nil, err
	}
	out := make(chan SnapshotResult)
	go func() {
		defer close(out)
		for {
			response, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				return
			}
			if recvErr != nil {
				e.markForwardError(ctx, client, recvErr)
				select {
				case out <- SnapshotResult{Err: recvErr}:
				case <-ctx.Done():
				}
				return
			}
			select {
			case out <- SnapshotResult{Response: response}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (e *etcdProxy) Ready() error {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.readyLocked()
}

func (e *etcdProxy) readyLocked() error {
	if e.client == nil {
		return status.Errorf(codes.Unavailable, "no ready right now")
	}
	currentLeader := e.election.GetLeaderInfo()
	if !election.IsLeaderKnown(currentLeader) {
		return status.Errorf(codes.Unavailable, "leader is not elected")
	}
	if e.curLeader != currentLeader {
		return status.Errorf(codes.Unavailable, "proxy leader %q is stale, current leader %q", e.curLeader, currentLeader)
	}
	conn := e.client.ActiveConnection()
	if conn == nil {
		return status.Errorf(codes.Unavailable, "proxy connection to leader %q is not ready", currentLeader)
	}
	if conn.GetState() == connectivity.Idle {
		conn.Connect()
	}
	if conn.GetState() != connectivity.Ready {
		return status.Errorf(codes.Unavailable, "proxy connection to leader %q is not ready", currentLeader)
	}
	return nil
}

// notReadyErr renders the terminal error when the wait deadline fires. A live
// caller has not reached any leader RPC yet, so use clientv3's exact mutable-RPC
// retry sentinel. The caller's own cancellation still takes precedence.
func notReadyErr(ctx context.Context, lastErr error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if lastErr != nil {
		klog.V(2).InfoS("proxy remained unavailable before request admission", "wait", proxyReadyWaitTimeout, "err", lastErr)
	}
	return proxyprotocol.ErrClientDrainedBeforeAdmission
}

func (e *etcdProxy) waitReady(ctx context.Context) error {
	if err := e.Ready(); err == nil {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, proxyReadyWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		select {
		case <-waitCtx.Done():
			return notReadyErr(ctx, lastErr)
		default:
		}

		// A peer dial/health check can legitimately consume the full five-second
		// connection timeout. Never run it, or wait for updateMu, in this request
		// goroutine. If election already published a successor, cancel only the
		// stale tracked attempt; then wake the single connector and keep polling
		// readiness under the caller-derived waitCtx instead.
		e.cancelStaleLeaderConnectionAttempt()
		e.requestClientUpdate()
		if err := e.Ready(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-waitCtx.Done():
			return notReadyErr(ctx, lastErr)
		case <-ticker.C:
		}
	}
}

func (e *etcdProxy) Watch(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan WatchResult, error) {
	outputCh := make(chan WatchResult, 100)
	go func() {
		defer util.Recover()
		defer close(outputCh)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		outgoingMetadata, _ := metadata.FromOutgoingContext(ctx)
		authorizedValues := outgoingMetadata.Get(AuthorizedWatchProxyMetadataKey)
		authorizedContinuation := len(authorizedValues) == 1 && authorizedValues[0] == "1"
		if len(authorizedValues) != 0 && !authorizedContinuation {
			// The marker is an internal capability, not an arbitrary truthy header.
			// Strip malformed or duplicated values before the first generation so
			// the leader performs normal create-time authorization. A successful
			// Created response installs the canonical singleton marker below.
			sanitized := outgoingMetadata.Copy()
			sanitized.Delete(AuthorizedWatchProxyMetadataKey)
			ctx = metadata.NewOutgoingContext(ctx, sanitized)
		}
		watchRevision := revision
		for {
			// The ingress replica may itself win the next term. A proxy generation
			// cannot make progress in that role because leaders intentionally have no
			// forwarding client. Close this generation so the outer Watch pipeline can
			// reopen the same logical Watch against its local backend.
			if _, leadingFresh := e.election.EpochAndLeadingFresh(); leadingFresh {
				klog.InfoS("etcd proxy watch yielding to local leader", "key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd), "rev", watchRevision)
				return
			}
			client, leader, closed, err := e.readyClient(ctx)
			if err != nil {
				klog.InfoS("etcd proxy watch ready failed", "key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd), "rev", watchRevision, "error", err)
				// A watch is long-lived and a short no-leader window is part of a
				// normal failover. Unary forwarding may return Unavailable after its
				// bounded readiness wait, but terminating this output stream turns
				// that transient into a terminal Watch cancel at the ingress replica.
				// Keep the explicit resume revision and retry until a successor is
				// ready or the caller closes the watch.
				if !waitProxyWatchReconnect(ctx) {
					return
				}
				continue
			}

			klog.InfoS("etcd proxy start watching", "leader", leader, "key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd), "rev", watchRevision)
			// Always request PrevKV from the leader. The outer etcd watch server
			// still strips PrevKv when the original client did not request it.
			// The logical Watch outlives individual backend generations. Cancel
			// each abandoned subscription even when the shared client stays open;
			// otherwise it can keep receiving and buffering events without a reader.
			generationCtx, cancelGeneration := context.WithCancel(ctx)
			inputCh := client.Watch(generationCtx, string(key), watchOptionsForRange(rangeEnd, watchRevision)...)
			generationCreated := false
			reconnect := false
			for !reconnect {
				select {
				case <-closed:
					klog.InfoS("etcd proxy watch leader changed", "key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd), "rev", watchRevision)
					reconnect = true
				case <-ctx.Done():
					klog.InfoS("etcd proxy watch ctx done")
					cancelGeneration()
					return
				case wresp, ok := <-inputCh:
					if !ok {
						klog.InfoS("etcd proxy watch closed", "key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd), "rev", watchRevision, "channel", outputCh)
						reconnect = true
						break
					}
					err := wresp.Err()
					if err != nil {
						klog.InfoS("etcd proxy watch error", "key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd), "rev", watchRevision, "channel", outputCh, "error", err)
						e.markForwardError(ctx, client, err)
						if isForwardConnectionError(err) {
							reconnect = true
							break
						}
						select {
						case outputCh <- watchErrorResultFromResponse(wresp, err):
						case <-ctx.Done():
						}
						cancelGeneration()
						return
					}
					if watchResponsePrecedesGenerationCreate(generationCreated, wresp) {
						// CreatedNotify makes the leader's Created response the only
						// authoritative proof that this backend generation exists. The
						// logical authorization marker survives reconnects, but it must
						// never stand in for the next generation's create acknowledgement.
						// clientv3 can surface an empty/progress/event response, or an
						// empty canceled envelope, while rebuilding its transport. Reopen
						// from the unchanged explicit revision so any event observed by
						// the abandoned generation is replayed.
						klog.InfoS("etcd proxy watch response preceded authoritative create; reconnecting",
							"key", loggedProxyKey(key), "rangeEnd", loggedProxyKey(rangeEnd),
							"rev", watchRevision, "headerRevision", wresp.Header.GetRevision(),
							"events", len(wresp.Events), "canceled", wresp.Canceled)
						reconnect = true
						break
					}
					if wresp.Created {
						generationCreated = true
					}
					if !authorizedContinuation && watchResponseAuthorizesContinuation(wresp) {
						// Created proves the leader accepted the initial create-time
						// credentials. Only reconnects after this point may
						// use the trusted continuation marker and preserve etcd's rule
						// that permission changes do not cancel an established Watch.
						ctx = metadata.AppendToOutgoingContext(ctx, AuthorizedWatchProxyMetadataKey, "1")
						authorizedContinuation = true
					}
					// Advance the resume revision to the store revision this
					// response covers, so a reconnect after a leader change resumes
					// from a concrete point instead of restarting at the new
					// leader's "current" and silently dropping the gap (#63). The
					// header revision is >= every event's ModRevision (subsuming
					// per-event advancement) and, thanks to WithProgressNotify,
					// also advances on idle progress notifications that carry no
					// events — the case where the old code left watchRevision at
					// its initial value (0 for a from-now watch). A positive Created
					// header also establishes the resume floor immediately.
					watchRevision = nextWatchRevision(watchRevision, wresp.Header.Revision)
					select {
					case outputCh <- watchResultFromResponse(wresp):
					case <-ctx.Done():
						cancelGeneration()
						return
					}
				}
			}
			cancelGeneration()

			select {
			case <-ctx.Done():
				klog.InfoS("etcd proxy watch ctx done")
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return outputCh, nil
}

func watchResponseAuthorizesContinuation(wresp clientv3.WatchResponse) bool {
	return wresp.Created
}

func watchResponsePrecedesGenerationCreate(generationCreated bool, wresp clientv3.WatchResponse) bool {
	return !generationCreated && !wresp.Created
}

func waitProxyWatchReconnect(ctx context.Context) bool {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func watchOptionsForRange(rangeEnd []byte, revision uint64) []clientv3.OpOption {
	// WithProgressNotify makes the leader advertise its current revision even
	// when the watched range is idle, so the proxy can advance its resume point
	// and not lose the gap on a leader-change reconnect (#63).
	opts := []clientv3.OpOption{
		clientv3.WithRev(int64(revision)),
		clientv3.WithPrevKV(),
		clientv3.WithProgressNotify(),
		// The ingress follower must not expose a successful create before the
		// leader has authoritatively authenticated and accepted it.
		clientv3.WithCreatedNotify(),
	}
	if rangeEnd == nil {
		return opts
	}
	if len(rangeEnd) == 0 {
		return append(opts, clientv3.WithFromKey())
	}
	return append(opts, clientv3.WithRange(string(rangeEnd)))
}

// nextWatchRevision returns the resume revision after a watch response whose
// header covers store revision headerRev. It never moves backwards and ignores a
// zero header revision, so an older server cannot rewind a from-now watch to the
// beginning of history.
func nextWatchRevision(current uint64, headerRev int64) uint64 {
	if headerRev <= 0 {
		return current
	}
	if next := uint64(headerRev) + 1; next > current {
		return next
	}
	return current
}

// watchResultFromResponse maps one leader watch response to a WatchResult. An
// idle progress notification (no events, carrying only the leader's current
// revision) becomes a ProgressRevision result so the follower's watch can advance
// a quiet watch's progress; the leader now makes that header revision a safe
// "all events <= R delivered on this stream" value, and clientv3 preserves FIFO,
// so the proxy's FIFO copy preserves the guarantee. Any other response carries
// converted events.
func watchResultFromResponse(wresp clientv3.WatchResponse) WatchResult {
	header := cloneWatchResponseHeader(wresp.Header)
	if wresp.Created {
		return WatchResult{Header: header, Created: true, Revision: uint64(wresp.Header.Revision)}
	}
	if wresp.IsProgressNotify() {
		return WatchResult{Header: header, ProgressRevision: uint64(wresp.Header.Revision)}
	}
	return WatchResult{
		Header:   header,
		Events:   convertEvents(wresp.Events),
		Revision: uint64(wresp.Header.Revision),
	}
}

func watchErrorResultFromResponse(wresp clientv3.WatchResponse, err error) WatchResult {
	return WatchResult{
		Header:          cloneWatchResponseHeader(wresp.Header),
		Err:             err,
		Created:         wresp.Created,
		Revision:        uint64(wresp.Header.Revision),
		CompactRevision: wresp.CompactRevision,
	}
}

func cloneWatchResponseHeader(header *etcdserverpb.ResponseHeader) *etcdserverpb.ResponseHeader {
	if header == nil {
		return nil
	}
	return proto.Clone(header).(*etcdserverpb.ResponseHeader)
}

func convertEvents(events []*clientv3.Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, len(events))
	for i, event := range events {
		ret[i] = (*mvccpb.Event)(event)
	}
	return ret
}
