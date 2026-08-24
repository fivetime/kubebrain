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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/util"
)

var (
	// one watcher is one grpc stream
	watcherID        int64
	errWatchInactive = errors.New("watch is no longer active")
)

const (
	watchCompactionProbeTimeout = 250 * time.Millisecond
	progressRequestSyncWait     = 100 * time.Millisecond
)

func loggedWatchKey(key []byte) string {
	return util.LoggedKey(key)
}

const watchQuotaCancelReason = "etcdserver: too many requests"

const watchControlBuffer = 16

func canceledWatchCreateResponse(revision uint64, reason string) *etcdserverpb.WatchResponse {
	return &etcdserverpb.WatchResponse{
		Header:       txnHeader(int64(revision)),
		WatchId:      -1,
		Created:      true,
		Canceled:     true,
		CancelReason: reason,
	}
}

type watchControlResponse struct {
	resp *etcdserverpb.WatchResponse
	done chan error
	// ready gates a deferred control response whose final shape depends on an
	// authoritative leader result. cancel lets stream teardown discard it.
	ready      <-chan struct{}
	cancel     <-chan struct{}
	generation *watch
	barrier    *watchControlBarrier
}

type watchControlBarrier struct {
	done chan struct{}
	err  error
}

type watchReceiveResult struct {
	request *etcdserverpb.WatchRequest
	err     error
}

type watchProgressRequest struct {
	target      uint64
	generations map[int64]*watch
}

// watcher correspond to one stream, one watcher has many watches
type watcher struct {
	sync.Mutex
	sendMu sync.Mutex

	directControlMu      sync.Mutex
	directControlCh      chan watchControlResponse
	directControlBarrier *watchControlBarrier

	wg         sync.WaitGroup
	controlWG  sync.WaitGroup
	progressWG sync.WaitGroup
	backend    BackendShim
	// stream server
	watchServer etcdserverpb.Watch_WatchServer
	// gRPC server
	grpcServer *RPCServer

	// hold watch request info in this stream
	watches     map[int64]*watch
	nextWatchID int64
	id          int64
	// controlRev is the latest leader revision synchronized for this stream.
	// Followers use it for created/client-cancel headers and as the R fence for
	// from-now proxy watches that subscribe at R+1.
	controlRev uint64

	controlCh  chan watchControlResponse
	progressCh chan watchProgressRequest

	metricCli metrics.Metrics

	// Upstream samples one 0-10% progress interval jitter per Watch stream so
	// clients that reconnect together do not retain a synchronized ticker phase.
	progressInterval time.Duration
}

func (w *watcher) responseRevision() uint64 {
	rev := w.backend.GetPublishedRevision()
	if control := atomic.LoadUint64(&w.controlRev); control > rev {
		rev = control
	}
	return rev
}

// clientCancelResponseRevision reports the latest committed revision. Unlike a
// created or progress response, a canceled watch is terminal and cannot cause
// a client to skip an undelivered event, so etcd exposes the current revision
// even while the watch publication pipeline is catching up.
func (w *watcher) clientCancelResponseRevision() uint64 {
	rev := w.backend.GetCurrentRevision()
	if control := atomic.LoadUint64(&w.controlRev); control > rev {
		rev = control
	}
	return rev
}

func (w *watcher) syncControlRevision(ctx context.Context) error {
	revision := w.backend.GetPublishedRevision()
	if _, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh(); !leadingFresh {
		if err := w.grpcServer.peers.SyncReadRevision(ctx); err != nil {
			return readBarrierStatusErr(err)
		}
		revision = w.grpcServer.backend.GetCurrentRevision()
	}
	util.StoreMaxUint64(&w.controlRev, revision)
	return nil
}

// watch correspond to one watch request
type watch struct {
	cancel     func()
	start, end string
	quotaHeld  bool
	// closing is set before terminal cancellation work begins. The WatchId stays
	// reserved in watches until its Canceled control has completed the required
	// send ordering, preventing a new explicit create from overtaking a slow
	// compact/error cancellation.
	closing atomic.Bool
	// progressStartRevision is the client's requested progress floor. It normally
	// equals WatchCreateRequest.StartRevision, except when a follower rewrites a
	// from-now request to R+1 internally to close the proxy registration gap.
	// That backend resume point must not turn the original revision-zero request
	// into a client-visible future watch.
	progressStartRevision uint64
	// syncedRev is the highest revision this watch has actually delivered to the
	// client (or the caught-up revision captured at creation). Progress
	// notifications must never advertise a revision beyond syncedRev: the global
	// current revision is advanced (SetCurrentRevision) before the corresponding
	// events are published to the watch pipeline, so reporting it here would tell
	// a client (e.g. the kube-apiserver watch cache) that it is synced through a
	// revision whose events it has not yet received, causing it to skip them.
	// Access atomically.
	syncedRev uint64
	// sourceRev is the highest watermark actually consumed from this watch's
	// backend/proxy result stream. Unlike syncedRev, it is not initialized to
	// StartRevision-1 for a future watch. Access atomically.
	sourceRev uint64
	// awaitAuthoritativeCreate is set only for a public Watch first received by
	// a follower. Its first proxy generation must prove that the leader accepted
	// the caller's credentials before this replica exposes Created.
	awaitAuthoritativeCreate bool
	createdRevision          uint64
	authoritativeControl     *etcdserverpb.WatchResponse
	authoritativeReady       chan struct{}
	authoritativeDone        chan error
	authoritativeSent        chan struct{}
	cancelPending            atomic.Bool
}

type periodicProgressState struct {
	eligible bool
}

func invalidWatchResultShape(result etcdproxy.WatchResult) error {
	if result.ProgressRevision == 0 && result.Revision == 0 && len(result.Events) == 0 {
		return errors.New("watch backend returned an empty watch result")
	}
	if result.ProgressRevision > 0 && len(result.Events) > 0 {
		return fmt.Errorf("watch backend returned mixed progress and events at progress revision %d", result.ProgressRevision)
	}
	if result.ProgressRevision > 0 && result.Revision > 0 {
		return fmt.Errorf("watch backend returned mixed progress revision %d and batch revision %d", result.ProgressRevision, result.Revision)
	}
	for i, event := range result.Events {
		if event == nil || event.Kv == nil {
			return fmt.Errorf("watch backend returned invalid nil event at index %d", i)
		}
		if len(event.Kv.Key) == 0 {
			return fmt.Errorf("watch backend returned an empty event key at index %d", i)
		}
		if event.PrevKv != nil && !bytes.Equal(event.Kv.Key, event.PrevKv.Key) {
			return fmt.Errorf("watch backend returned a previous key that differs from the event key at index %d", i)
		}
		switch event.Type {
		case mvccpb.PUT:
		case mvccpb.DELETE:
			kv := event.Kv
			if len(kv.Value) != 0 {
				return fmt.Errorf("watch backend returned DELETE event with a non-empty value at index %d", i)
			}
			if kv.CreateRevision != 0 {
				return fmt.Errorf("watch backend returned DELETE event with create revision %d at index %d", kv.CreateRevision, i)
			}
			if kv.Version != 0 {
				return fmt.Errorf("watch backend returned DELETE event with version %d at index %d", kv.Version, i)
			}
			if kv.Lease != 0 {
				return fmt.Errorf("watch backend returned DELETE event with lease %d at index %d", kv.Lease, i)
			}
		default:
			return fmt.Errorf("watch backend returned unsupported event type %s at index %d", event.Type, i)
		}
	}
	return nil
}

func validatedWatchBatchRevision(result etcdproxy.WatchResult, sourceRevision uint64) (uint64, error) {
	batchRevision := result.Revision
	if batchRevision == 0 {
		for _, event := range result.Events {
			if revision := event.GetKv().GetModRevision(); revision > 0 && uint64(revision) > batchRevision {
				batchRevision = uint64(revision)
			}
		}
	}
	if batchRevision > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("watch backend returned batch revision %d exceeds MaxInt64", batchRevision)
	}
	if batchRevision < sourceRevision {
		return 0, fmt.Errorf("watch backend returned batch revision %d below source revision %d", batchRevision, sourceRevision)
	}
	var precedingRevision int64
	for i, event := range result.Events {
		kv := event.GetKv()
		eventRevision := kv.GetModRevision()
		if eventRevision <= 0 {
			return 0, fmt.Errorf("watch backend returned invalid event revision %d at index %d", eventRevision, i)
		}
		if createRevision := kv.GetCreateRevision(); createRevision < 0 || createRevision > eventRevision {
			return 0, fmt.Errorf("watch backend returned invalid event create revision %d for mod revision %d at index %d", createRevision, eventRevision, i)
		}
		if version := kv.GetVersion(); version < 0 {
			return 0, fmt.Errorf("watch backend returned invalid event version %d at index %d", version, i)
		}
		if event.GetType() == mvccpb.PUT && kv.GetCreateRevision() == 0 {
			return 0, fmt.Errorf("watch backend returned PUT event without a create revision at index %d", i)
		}
		if event.GetType() == mvccpb.PUT && kv.GetVersion() == 0 {
			return 0, fmt.Errorf("watch backend returned PUT event without a version at index %d", i)
		}
		if event.GetType() == mvccpb.PUT {
			createRevision := kv.GetCreateRevision()
			version := kv.GetVersion()
			if version == 1 && createRevision != eventRevision {
				return 0, fmt.Errorf("watch backend returned PUT version 1 with create revision %d differing from mod revision %d at index %d", createRevision, eventRevision, i)
			}
			if maximumVersion := eventRevision - createRevision + 1; version > maximumVersion {
				return 0, fmt.Errorf("watch backend returned PUT version %d exceeding maximum %d for create revision %d and mod revision %d at index %d", version, maximumVersion, createRevision, eventRevision, i)
			}
		}
		if prevKV := event.GetPrevKv(); prevKV != nil {
			prevModRevision := prevKV.GetModRevision()
			if prevModRevision <= 0 || prevModRevision >= eventRevision {
				return 0, fmt.Errorf("watch backend returned invalid previous mod revision %d for event revision %d at index %d", prevModRevision, eventRevision, i)
			}
			if createRevision := prevKV.GetCreateRevision(); createRevision <= 0 || createRevision > prevModRevision {
				return 0, fmt.Errorf("watch backend returned invalid previous create revision %d for mod revision %d at index %d", createRevision, prevModRevision, i)
			}
			if version := prevKV.GetVersion(); version <= 0 {
				return 0, fmt.Errorf("watch backend returned invalid previous version %d at index %d", version, i)
			}
			prevCreateRevision := prevKV.GetCreateRevision()
			prevVersion := prevKV.GetVersion()
			if prevVersion == 1 && prevCreateRevision != prevModRevision {
				return 0, fmt.Errorf("watch backend returned previous version 1 with create revision %d differing from mod revision %d at index %d", prevCreateRevision, prevModRevision, i)
			}
			if maximumVersion := prevModRevision - prevCreateRevision + 1; prevVersion > maximumVersion {
				return 0, fmt.Errorf("watch backend returned previous version %d exceeding maximum %d for create revision %d and mod revision %d at index %d", prevVersion, maximumVersion, prevCreateRevision, prevModRevision, i)
			}
			if event.GetType() == mvccpb.PUT && prevCreateRevision != kv.GetCreateRevision() {
				return 0, fmt.Errorf("watch backend returned PUT and previous values from different create revisions %d and %d at index %d", kv.GetCreateRevision(), prevCreateRevision, i)
			}
			if event.GetType() == mvccpb.PUT && prevVersion != kv.GetVersion()-1 {
				return 0, fmt.Errorf("watch backend returned PUT version %d not following previous version %d at index %d", kv.GetVersion(), prevVersion, i)
			}
		}
		if sourceRevision > 0 && uint64(eventRevision) <= sourceRevision {
			return 0, fmt.Errorf("watch backend returned event revision %d at index %d does not advance source revision %d", eventRevision, i, sourceRevision)
		}
		if i > 0 && eventRevision < precedingRevision {
			return 0, fmt.Errorf("watch backend returned event revision %d at index %d below preceding revision %d", eventRevision, i, precedingRevision)
		}
		if uint64(eventRevision) > batchRevision {
			return 0, fmt.Errorf("watch backend returned batch revision %d below event revision %d at index %d", batchRevision, eventRevision, i)
		}
		precedingRevision = eventRevision
	}
	return batchRevision, nil
}

func newPeriodicProgressState() periodicProgressState {
	return periodicProgressState{eligible: true}
}

func watchProgressIntervalWithJitter(interval time.Duration, jitterN func(int64) int64) time.Duration {
	if interval <= 0 {
		interval = time.Second
	}
	jitterLimit := interval / 10
	if jitterLimit <= 0 {
		return interval
	}
	return interval + time.Duration(jitterN(int64(jitterLimit)))
}

func (s *periodicProgressState) eventSent() {
	s.eligible = false
}

func (s *periodicProgressState) tick() bool {
	send := s.eligible
	s.eligible = true
	return send
}

// progressSyncedRevSnapshot returns only watches whose own delivered watermark
// has reached the requested start revision. allEligible is false when a
// stream-wide progress response would be unsafe because any active watch is
// still waiting for its first event or in-band progress marker.
func (w *watcher) progressSyncedRevSnapshot() (snapshot map[int64]uint64, allEligible bool) {
	w.Lock()
	defer w.Unlock()
	snapshot = make(map[int64]uint64, len(w.watches))
	active := 0
	for id, wt := range w.watches {
		// Cancellation retains the map entry until its terminal control is
		// ordered, so a create cannot reuse the ID too early. It is no longer an
		// active watcher, however, and etcd's RequestProgressAll excludes a watch
		// as soon as cancellation begins. In particular, do not let a slow compact
		// revision lookup manufacture a WatchId=-1 progress response for a watch
		// that can no longer deliver events.
		if wt.closing.Load() || wt.cancelPending.Load() {
			continue
		}
		active++
		syncedRev := atomic.LoadUint64(&wt.syncedRev)
		if wt.progressStartRevision > syncedRev {
			continue
		}
		snapshot[id] = syncedRev
	}
	allEligible = active > 0 && len(snapshot) == active
	return snapshot, allEligible
}

func (w *watcher) captureProgressRequest(target uint64) watchProgressRequest {
	w.Lock()
	defer w.Unlock()
	generations := make(map[int64]*watch, len(w.watches))
	for id, generation := range w.watches {
		if !generation.closing.Load() && !generation.cancelPending.Load() {
			generations[id] = generation
		}
	}
	return watchProgressRequest{target: target, generations: generations}
}

func (w *watcher) progressRequestRevision(request watchProgressRequest) (revision uint64, synced, retry bool) {
	if request.target == 0 || len(request.generations) == 0 {
		return 0, false, false
	}
	w.Lock()
	defer w.Unlock()
	minRevision := uint64(0)
	for id, generation := range request.generations {
		if current := w.watches[id]; current != generation || generation.closing.Load() || generation.cancelPending.Load() {
			return 0, false, false
		}
		// Match upstream progressIfSync's request-time revision check. A watch
		// starting after this request's target cannot become eligible for this
		// particular progress request, even if its event stream catches up later.
		// Treat it as terminal instead of occupying the bounded worker for 100ms.
		if generation.progressStartRevision > request.target {
			return 0, false, false
		}
		deliveredRevision := atomic.LoadUint64(&generation.syncedRev)
		if generation.progressStartRevision > deliveredRevision {
			return 0, false, true
		}
		if deliveredRevision < request.target {
			return 0, false, true
		}
		if minRevision == 0 || deliveredRevision < minRevision {
			minRevision = deliveredRevision
		}
	}
	return minRevision, true, false
}

func (w *watcher) waitProgressRequest(request watchProgressRequest) (uint64, bool) {
	deadline := time.NewTimer(progressRequestSyncWait)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		revision, synced, retry := w.progressRequestRevision(request)
		if synced {
			return revision, true
		}
		if !retry {
			return 0, false
		}
		select {
		case <-w.watchServer.Context().Done():
			return 0, false
		case <-deadline.C:
			return 0, false
		case <-ticker.C:
		}
	}
}

// minSyncedRevision returns the minimum revision delivered across all active
// watches on the stream, and whether any watch is active.
func (w *watcher) minSyncedRevision() (uint64, bool) {
	w.Lock()
	defer w.Unlock()
	var minRev uint64
	found := false
	for _, wt := range w.watches {
		if wt.closing.Load() || wt.cancelPending.Load() {
			continue
		}
		rev := atomic.LoadUint64(&wt.syncedRev)
		if !found || rev < minRev {
			minRev = rev
			found = true
		}
	}
	return minRev, found
}

func (s *RPCServer) Watch(ws etcdserverpb.Watch_WatchServer) (err error) {
	s.activeWatchStreams.Add(1)
	w := &watcher{
		watchServer: ws,
		grpcServer:  s,
		backend:     s.backend,
		watches:     make(map[int64]*watch),
		controlCh:   make(chan watchControlResponse, watchControlBuffer),
		metricCli:   s.metricCli,
	}
	w.id = atomic.AddInt64(&watcherID, 1)
	w.controlWG.Add(1)
	go w.sendControls()
	w.startDirectControls()
	w.startProgressRequests()
	receiveNext := make(chan struct{})
	receiveResults := make(chan watchReceiveResult)
	receiveStop := make(chan struct{})
	go pumpWatchRequests(ws, receiveNext, receiveResults, receiveStop)
	klog.InfoS("new watcher", "id", w.id)
	defer func() {
		close(receiveStop)
		s.activeWatchStreams.Add(-1)
		w.Close()
		if errors.Is(err, context.Canceled) {
			err = rpctypes.ErrGRPCWatchCanceled
		}
	}()

	for {
		var msg *etcdserverpb.WatchRequest
		select {
		case receiveNext <- struct{}{}:
		case <-ws.Context().Done():
			return ws.Context().Err()
		}
		select {
		case result := <-receiveResults:
			msg, err = result.request, result.err
		case <-ws.Context().Done():
			return ws.Context().Err()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				// CloseSend only half-closes a bidirectional watch stream. Keep the
				// active watches and response side alive until the client also closes
				// the stream context. The generated HTTP gateway reaches this path as
				// soon as it finishes decoding the finite JSON request body.
				<-ws.Context().Done()
				return ws.Context().Err()
			}
			// log watch infos on stream when stream err occurs
			w.Lock()
			watchInfo := make([]string, 0, len(w.watches))
			for watchID, i := range w.watches {
				watchInfo = append(watchInfo, fmt.Sprintf("watch id: %d start: %s end: %s", watchID, i.start, i.end))
			}
			w.Unlock()
			s.metricCli.EmitCounter("watcher.receive.cancel", 1)
			if shouldCountServerStreamFailure(ws.Context(), err) {
				emitEtcdServerStreamFailureCounter(s.metricCli, "receive", "watch", 1)
			}
			if isExpectedWatchCloseError(err) {
				klog.V(4).InfoS("watcher receive closed", "id", w.id, "err", err, "info", strings.Join(watchInfo, "\n"))
			} else {
				klog.ErrorS(err, "watcher receive err", "id", w.id, "info", strings.Join(watchInfo, "\n"))
			}
			return err
		}

		if r := msg.GetCreateRequest(); r != nil {
			if r.ProgressNotify && w.progressInterval <= 0 {
				// Sample lazily: streams that fail in transport before creating a
				// watch must not acquire a backend dependency merely for jitter.
				// Every progress-enabled logical watch on this stream then reuses
				// the same upstream-compatible interval.
				w.progressInterval = watchProgressIntervalWithJitter(
					s.backend.WatchProgressNotifyInterval(), rand.Int64N,
				)
			}
			// Authorize the wire RangeEnd before normalizing its single-zero
			// from-key sentinel to the internal empty-slice representation. The auth
			// interval code distinguishes {0} (open-ended range) from nil/empty
			// (exact key); normalizing first lets exact-key permission authorize a
			// watch over [key,+inf), matching upstream's 7cf71ec9e security bug.
			authKey := r.Key
			if len(authKey) == 0 {
				authKey = []byte{0}
			}
			authRangeEnd := r.RangeEnd
			r = normalizeWatchCreateRequest(r)
			if r.StartRevision < 0 {
				// etcd treats a negative start revision as an immediately canceled
				// create, while keeping the multiplexed stream usable for later watches.
				if err := w.SendControl(canceledWatchCreateResponse(
					w.responseRevision(), rpctypes.ErrCompacted.Error(),
				)); err != nil {
					return err
				}
				continue
			}
			_, leadingFresh := s.peers.EpochAndLeadingFresh()
			authorizedContinuation := authorizedPeerWatchContinuation(ws.Context())
			followerProxy := !leadingFresh && s.peers.EtcdProxyEnabled() && !authorizedContinuation
			// Unlike raft-based etcd, a KubeBrain follower does not apply every auth
			// mutation into its process-local snapshot. A non-nil cache can therefore
			// be complete but stale (for example, immediately after RoleGrantPermission).
			// Always let the current leader authorize a newly created proxied Watch;
			// only a successful leader response upgrades later generations to the
			// trusted continuation path.
			proxyInitialAuthorization := followerProxy
			var caller *authCaller
			var authErr error
			if !authorizedContinuation {
				if !followerProxy {
					caller, authErr = s.authCallerFromContext(ws.Context())
					if authErr == nil {
						authErr = caller.require(authKey, authRangeEnd, authpb.READ)
					}
				}
			}
			if authErr != nil {
				if err := w.SendControl(canceledWatchCreateResponse(
					w.responseRevision(), watchAuthCancelReason(authErr),
				)); err != nil {
					return err
				}
				continue
			}
			// Upstream rejects invalid ranges locally before entering the watch
			// stream. A follower must not turn this deterministic control response
			// into Unavailable merely because its leader read barrier is down.
			if len(r.RangeEnd) != 0 && bytes.Compare(r.Key, r.RangeEnd) >= 0 {
				if err := w.SendControl(canceledWatchCreateResponse(
					w.responseRevision(), "mvcc: watcher range is empty",
				)); err != nil {
					return err
				}
				continue
			}
			if r.WatchId != 0 && w.hasWatchID(r.WatchId) {
				if err := w.SendControl(canceledWatchCreateResponse(
					w.responseRevision(), "mvcc: duplicate watch ID provided on the WatchStream",
				)); err != nil {
					return err
				}
				continue
			}
			quotaReserved := s.maxWatches != 0
			if !s.acquireWatch() {
				if err := w.SendControl(canceledWatchCreateResponse(
					w.responseRevision(), watchQuotaCancelReason,
				)); err != nil {
					return err
				}
				continue
			}
			releaseReservedQuota := func() {
				if quotaReserved {
					s.releaseWatch()
					quotaReserved = false
				}
			}
			if err := w.syncControlRevision(ws.Context()); err != nil {
				releaseReservedQuota()
				return err
			}
			watchCtx := ws.Context()
			if proxyInitialAuthorization {
				// A follower can remain reachable after losing every TiKV/PD path. Pass
				// the raw credential to the leader without consulting local auth state;
				// the first authoritative Watch generation performs create-time auth.
				// The proxy marks later reconnects as continuations only after that
				// generation has returned a successful response.
				watchCtx, authErr = s.forwardWriteAuthContext(watchCtx)
				if authErr != nil {
					releaseReservedQuota()
					return authErr
				}
			} else if s.peers.EtcdProxyEnabled() {
				// Prepare forwarding credentials even when this node is the leader at
				// creation time. A long-lived locally authorized watch can outlive that
				// leadership term and resume through a successor without reauthorization.
				watchCtx, authErr = s.forwardAuthToken(watchCtx, caller)
				if authErr != nil {
					releaseReservedQuota()
					return authErr
				}
				// This logical Watch was authorized at the public ingress. Mark every
				// internal generation so a successor reached through the peer listener
				// preserves that create-time decision across auth revision changes.
				watchCtx = metadata.AppendToOutgoingContext(
					watchCtx, etcdproxy.AuthorizedWatchProxyMetadataKey, "1",
				)
			}
			// A local watch generation is leader-only. Use the same lease-freshness
			// boundary as linearizable reads; client-go's leader flag can remain true
			// briefly after this replica has already self-fenced.
			if !leadingFresh && !s.peers.EtcdProxyEnabled() {
				releaseReservedQuota()
				s.metricCli.EmitCounter("watch.follower", 1)
				leaderInfo := s.peers.GetLeaderInfo()
				klog.InfoS("watch follower", "revision", r.StartRevision, "addr", s.backend.GetResourceLock().Identity(), "leader", leaderInfo)
				return status.Errorf(codes.Unavailable, "watch error addr is %s leader %s", s.backend.GetResourceLock().Identity(), leaderInfo)
			}
			progressStartRevision := r.StartRevision
			if !leadingFresh && r.StartRevision == 0 {
				r.StartRevision = int64(w.responseRevision()) + 1
			}

			w.start(watchCtx, r, uint64(progressStartRevision), quotaReserved, false, proxyInitialAuthorization)
		} else if cancelRequest := msg.GetCancelRequest(); cancelRequest != nil {
			// Match etcd's stream-local cancellation: removing an existing watch
			// does not require a leader read barrier. Its terminal response reports
			// the local current revision, so a just-committed write is visible even
			// when event publication has not caught up.
			s.metricCli.EmitCounter("watch.client.cancel", 1)
			klog.InfoS("receive watch cancel request", "id", w.id, "watchID", cancelRequest.GetWatchId())
			w.CancelRequest(msg.GetCancelRequest().WatchId)
		} else if msg.GetProgressRequest() != nil {
			s.metricCli.EmitCounter("watch.progress.request", 1)
			targetRevision := s.backend.GetCurrentRevision()
			// TiKV-backed progress markers traverse each subscriber FIFO instead of
			// updating a local MVCC watcher set synchronously as upstream does. Freeze
			// the request's generations now, kick a marker, and let the bounded worker
			// wait for their watermarks without blocking later Cancel/Create requests.
			s.backend.KickWatchProgress()
			request := w.captureProgressRequest(targetRevision)
			if len(request.generations) == 0 {
				continue
			}
			// Upstream evaluates future-start watches synchronously and emits no
			// response. Do the same before enqueueing so a burst of requests that
			// is already impossible at capture time cannot fill the worker queue
			// and stall later requests on this multiplexed stream.
			if _, synced, retry := w.progressRequestRevision(request); !synced && !retry {
				continue
			}
			if !w.queueProgressRequest(request) {
				return ws.Context().Err()
			}
			// Match etcd's progressAll/progressIfSync contract: RequestProgress
			// is stream-wide. If any captured watch has not caught up through the
			// target before the bounded wait expires, emit nothing; a later request may return one
			// WatchId=-1 response after every watch is synchronized. Per-watch
			// header-only fallbacks are not protocol-compatible because clientv3
			// treats those frames as notifications requested by that watch.
		} else {
			s.metricCli.EmitCounter("watch.request.unsupported", 1)
			klog.Info("watch receive message unsupported type")
		}
	}
}

// pumpWatchRequests keeps the transport Recv outside the handler goroutine.
// Some ServerStream implementations do not unblock an in-flight Recv when a
// wrapped stream context is canceled. Matching upstream's recvLoop isolation
// lets Watch still unwind; at most this pump remains blocked in transport code,
// and a late result is discarded after receiveStop without touching watcher
// state that the handler has already closed.
func pumpWatchRequests(ws etcdserverpb.Watch_WatchServer, next <-chan struct{}, results chan<- watchReceiveResult, receiveStop <-chan struct{}) {
	for {
		select {
		case <-next:
		case <-receiveStop:
			return
		}
		request, err := ws.Recv()
		select {
		case results <- watchReceiveResult{request: request, err: err}:
		case <-receiveStop:
			return
		}
		if err != nil {
			return
		}
	}
}

func authorizedPeerWatchContinuation(ctx context.Context) bool {
	if !isPeerRequest(ctx) {
		return false
	}
	values := metadata.ValueFromIncomingContext(ctx, etcdproxy.AuthorizedWatchProxyMetadataKey)
	return len(values) > 0 && values[0] == "1"
}

func (w *watcher) hasWatchID(id int64) bool {
	w.Lock()
	defer w.Unlock()
	_, exists := w.watches[id]
	return exists
}

func watchAuthCancelReason(err error) string {
	switch {
	case errors.Is(err, rpctypes.ErrInvalidAuthToken):
		return rpctypes.ErrGRPCInvalidAuthToken.Error()
	case errors.Is(err, rpctypes.ErrAuthOldRevision):
		return rpctypes.ErrGRPCAuthOldRevision.Error()
	case errors.Is(err, rpctypes.ErrUserEmpty):
		return rpctypes.ErrGRPCUserEmpty.Error()
	default:
		return rpctypes.ErrGRPCPermissionDenied.Error()
	}
}

func (w *watcher) Start(c context.Context, r *etcdserverpb.WatchCreateRequest) {
	w.start(c, r, uint64(r.StartRevision), false, true, false)
}

func (w *watcher) start(c context.Context, r *etcdserverpb.WatchCreateRequest, progressStartRevision uint64, quotaReserved, waitForCreated, awaitAuthoritativeCreate bool) {
	w.Lock()
	ctx, cancel := context.WithCancel(c)
	releaseReservedQuota := func() {
		if quotaReserved {
			w.grpcServer.releaseWatch()
			quotaReserved = false
		}
	}
	// Match etcd watchStream.Watch validation order: an empty range wins over
	// duplicate-ID detection when both fields are invalid.
	if len(r.RangeEnd) != 0 && bytes.Compare(r.Key, r.RangeEnd) >= 0 {
		w.Unlock()
		cancel()
		releaseReservedQuota()
		_ = w.SendControl(canceledWatchCreateResponse(
			w.responseRevision(), "mvcc: watcher range is empty",
		))
		return
	}
	if r.WatchId != 0 {
		if _, duplicate := w.watches[r.WatchId]; duplicate {
			w.Unlock()
			cancel()
			releaseReservedQuota()
			_ = w.SendControl(canceledWatchCreateResponse(
				w.responseRevision(), "mvcc: duplicate watch ID provided on the WatchStream",
			))
			return
		}
	}
	if !quotaReserved && !w.grpcServer.acquireWatch() {
		w.Unlock()
		cancel()
		_ = w.SendControl(canceledWatchCreateResponse(
			w.responseRevision(), watchQuotaCancelReason,
		))
		return
	}
	if !quotaReserved {
		quotaReserved = w.grpcServer.maxWatches != 0
	}
	id, _ := w.allocateWatchIDLocked(r.WatchId)

	// Seed syncedRev with the revision the watch is guaranteed to be caught up
	// through before any event is delivered: StartRevision-1 for a historical
	// watch (it will deliver events >= StartRevision), or the published revision
	// for a watch that starts from "now" (StartRevision == 0). Negative start
	// revisions are rejected upstream, so they never reach here.
	//
	// For the from-now case seed from the published revision, not the current one:
	// GetCurrentRevision is advanced (SetCurrentRevision) before the corresponding
	// events are published to the watch pipeline, so a freshly-registered
	// subscriber can still receive an event whose revision <= the current
	// revision — seeding at current would over-report it. GetPublishedRevision is
	// by construction below every such still-in-flight event, so any event the new
	// sub receives has revision > seed and cannot be skipped. Follower from-now
	// requests are rewritten to the synchronized leader fence R+1 before Start,
	// so both leader and follower watches use a published registration floor.
	var initSyncedRev uint64
	if r.StartRevision > 0 {
		initSyncedRev = uint64(r.StartRevision) - 1
	} else if r.StartRevision == 0 {
		initSyncedRev = w.backend.GetPublishedRevision()
	}
	generation := &watch{
		cancel: cancel,
		// These fields exist only for diagnostics. Keep their retained size and
		// eventual log output bounded instead of holding another full copy of
		// client-controlled watch keys for the lifetime of the stream.
		start:                    loggedWatchKey(r.Key),
		end:                      loggedWatchKey(r.RangeEnd),
		quotaHeld:                quotaReserved,
		progressStartRevision:    progressStartRevision,
		syncedRev:                initSyncedRev,
		awaitAuthoritativeCreate: awaitAuthoritativeCreate,
	}
	w.watches[id] = generation
	watchCount := len(w.watches)
	if watchCount > 1 {
		klog.InfoS("watcher reuse", "id", w.id, "size", watchCount)
	}
	w.Unlock()
	w.metricCli.EmitGauge("watch.watch_id", id)

	// Report the store's current published revision in the created response header
	// (previously 0). etcd clientv3 records the created header revision as the
	// resume point for a from-now (StartRevision == 0) watch, so a disconnect
	// after "created" but before the first event would otherwise resume from 0 and
	// could skip events. The published revision is at/below every still-in-flight
	// event (see initSyncedRev above), so it is a safe, non-skipping resume floor.
	createdRev := w.responseRevision()
	generation.createdRevision = createdRev
	if r.StartRevision == 0 {
		// Register from-now watches as an explicit historical watch from the
		// created watermark forward. Send(Created) happens before the backend
		// goroutine subscribes, so leaving revision zero here creates a gap where
		// a committed event can land before AddWatcher and be skipped forever.
		// Replaying from createdRev+1 closes that gap; backend.Watch installs its
		// live subscriber before scanning history, so the handoff is lossless.
		r.StartRevision = int64(createdRev) + 1
	}
	if awaitAuthoritativeCreate {
		generation.authoritativeControl = &etcdserverpb.WatchResponse{}
		generation.authoritativeReady = make(chan struct{})
		generation.authoritativeDone = make(chan error, 1)
		generation.authoritativeSent = make(chan struct{})
		select {
		case w.controlCh <- watchControlResponse{
			resp:       generation.authoritativeControl,
			done:       generation.authoritativeDone,
			ready:      generation.authoritativeReady,
			cancel:     ctx.Done(),
			generation: generation,
		}:
		case <-ctx.Done():
			w.rejectAuthoritativeCreate(id, generation, ctx.Err(), 0)
			return
		}
		w.wg.Add(1)
		w.metricCli.EmitCounter("watch.watch", 1)
		go w.watchGeneration(ctx, id, r, generation)
		klog.InfoS("watch awaiting authoritative create", "id", id, "count", watchCount, "key", loggedWatchKey(r.Key), "revision", r.StartRevision)
		return
	}
	createdDone, err := w.sendControlWithCompletion(&etcdserverpb.WatchResponse{
		Header:  txnHeader(int64(createdRev)),
		Created: true,
		WatchId: id,
	})
	if err != nil {
		klog.ErrorS(err, "watch send create watch response err", "wacher", w.id, "watch", id)
		w.CancelGeneration(id, generation, err, false)
		return
	}

	w.wg.Add(1)
	key := string(r.Key)
	w.metricCli.EmitCounter("watch.watch", 1)
	waitAndStartGeneration := func() {
		if sendErr := <-createdDone; sendErr != nil {
			klog.ErrorS(sendErr, "watch send create watch response err", "watcher", w.id, "watch", id)
			w.CancelGeneration(id, generation, sendErr, false)
			w.wg.Done()
			return
		}
		w.watchGeneration(ctx, id, r, generation)
	}
	if waitForCreated {
		if sendErr := <-createdDone; sendErr != nil {
			klog.ErrorS(sendErr, "watch send create watch response err", "watcher", w.id, "watch", id)
			w.CancelGeneration(id, generation, sendErr, false)
			w.wg.Done()
			return
		}
		go w.watchGeneration(ctx, id, r, generation)
	} else {
		go waitAndStartGeneration()
	}
	klog.InfoS("watch start", "id", id, "count", watchCount, "key", loggedWatchKey([]byte(key)), "revision", r.StartRevision)
}

func (w *watcher) rejectAuthoritativeCreate(id int64, generation *watch, err error, compactRevision int64) {
	if generation == nil || !generation.closing.CompareAndSwap(false, true) {
		return
	}
	w.Lock()
	if w.watches[id] == generation {
		delete(w.watches, id)
	}
	quotaHeld := generation.quotaHeld
	generation.quotaHeld = false
	w.Unlock()
	if quotaHeld {
		w.grpcServer.releaseWatch()
	}
	reason := err.Error()
	if compactRevision > 0 {
		reason = ""
	}
	if errors.Is(err, rpctypes.ErrInvalidAuthToken) || errors.Is(err, rpctypes.ErrAuthOldRevision) ||
		errors.Is(err, rpctypes.ErrUserEmpty) || errors.Is(err, rpctypes.ErrPermissionDenied) {
		reason = watchAuthCancelReason(err)
	}
	response := canceledWatchCreateResponse(w.responseRevision(), reason)
	response.CompactRevision = compactRevision
	if generation.authoritativeControl != nil {
		// Preserve the placeholder identity already queued in controlCh without
		// copying protobuf's embedded MessageState (which contains a mutex).
		proto.Reset(generation.authoritativeControl)
		proto.Merge(generation.authoritativeControl, response)
		close(generation.authoritativeReady)
		if generation.cancel != nil {
			generation.cancel()
		}
		return
	}
	if generation.cancel != nil {
		generation.cancel()
	}
	_ = w.SendControl(response)
}

// allocateWatchIDLocked selects an ID while w is locked.
func (w *watcher) allocateWatchIDLocked(requested int64) (id int64, duplicate bool) {
	if requested != 0 {
		_, duplicate = w.watches[requested]
		return requested, duplicate
	}
	// Watch IDs are scoped to one stream. Match etcd's monotonic, zero-indexed
	// allocator and skip IDs explicitly reserved by earlier create requests
	// without recycling canceled IDs.
	for {
		id = w.nextWatchID
		w.nextWatchID++
		if _, exists := w.watches[id]; !exists {
			return id, false
		}
	}
}

func (w *watcher) Cancel(id int64, err error, compact bool) {
	w.cancel(id, nil, err, compact, false)
}

func (w *watcher) CancelGeneration(id int64, generation *watch, err error, compact bool) {
	w.cancel(id, generation, err, compact, false)
}

func (w *watcher) CancelRequest(id int64) {
	w.cancel(id, nil, nil, false, true)
}

func (w *watcher) cancel(id int64, expected *watch, err error, compact, clientRequest bool) {
	klog.InfoS("watch cancel", "watcher", w.id, "watch", id, "err", err, "compact", compact)
	var tags []metrics.T
	tags = append(tags, metrics.Tag("compact", strconv.FormatBool(compact)))
	w.metricCli.EmitCounter("watch.cancel", 1, tags...)
	w.Lock()
	if clientRequest && expected == nil {
		if generation := w.watches[id]; generation != nil && generation.authoritativeSent != nil &&
			generation.cancelPending.CompareAndSwap(false, true) {
			w.Unlock()
			response := &etcdserverpb.WatchResponse{
				Header: txnHeader(int64(w.clientCancelResponseRevision())), Canceled: true, WatchId: id,
			}
			if w.queueDeferredDirectControl(response, generation) {
				return
			}
			select {
			case <-generation.authoritativeSent:
			case <-w.watchServer.Context().Done():
				return
			}
			if w.beginDeferredClientCancel(id, generation) {
				w.finishCancel(id, generation, w.Send(response))
			}
			return
		}
	}
	found := false
	quotaHeld := false
	var generation *watch
	if c, ok := w.watches[id]; ok && (expected == nil || c == expected) {
		if c.closing.CompareAndSwap(false, true) {
			found = true
			generation = c
			quotaHeld = c.quotaHeld
			// The logical watch stops consuming admission quota immediately even
			// though its ID remains reserved for terminal-control ordering. Clear
			// ownership under watcherMu so concurrent Close cannot double-release.
			c.quotaHeld = false
			klog.InfoS("cancel context", "watcher", w.id, "watch", id, "start", c.start, "end", c.end)
			if c.cancel != nil {
				c.cancel()
			}
		}
	}
	w.Unlock()
	if quotaHeld {
		w.grpcServer.releaseWatch()
	}
	// etcd emits at most one cancellation response for a watch. The closing CAS
	// suppresses the backend goroutine's later close response as well as requests
	// for unknown IDs, while retaining the ID through terminal-control ordering.
	if !found {
		return
	}
	// if compact is true, apiserver reflector watch will return with err, which will trigger re-list & re-watch (detail in etcd/clientv3/watch.go watchGrpcStream.run)
	// else, apiserver reflector watch will return nil, which will trigger re-watch
	var compactRevision int64
	if compact {
		compactRevision = 1
		// Fresh read (bypasses the TTL cache): this value tells the client where
		// to re-list from. On a follower the cache can lag a just-proxied Compact
		// by up to the TTL, and a stale-low CompactRevision sends the client into
		// another compacted round-trip (#33). Unlike upstream's local MVCC lookup,
		// this can cross the TiKV data-plane boundary, so do not let an unavailable
		// backend indefinitely withhold the terminal frame or reserve the watch ID.
		probeCtx, cancel := context.WithTimeout(w.watchServer.Context(), watchCompactionProbeTimeout)
		defer cancel()
		if rev, revErr := w.backend.GetCompactRevisionFresh(probeCtx); revErr == nil && rev > 0 {
			compactRevision = int64(rev)
		}
	}
	cancelReason := "watch closed"
	if clientRequest || compact {
		cancelReason = ""
	} else if err != nil {
		cancelReason = err.Error()
	}
	header := &etcdserverpb.ResponseHeader{}
	if clientRequest {
		header = txnHeader(int64(w.clientCancelResponseRevision()))
	}
	response := &etcdserverpb.WatchResponse{
		Header:          header,
		Canceled:        true,
		CancelReason:    cancelReason,
		WatchId:         id,
		CompactRevision: compactRevision,
	}
	if clientRequest && generation.authoritativeControl != nil {
		// Another follower create awaiting the leader's authoritative
		// acknowledgement can occupy a deferred slot in controlCh. Do not let it
		// withhold cancellation of this logical watch indefinitely.
		// The transport Send still serializes on sendMu, but it must not run in the request loop:
		// a client that stops reading can otherwise prevent the server from
		// receiving another cancellation on the same multiplexed stream. A single
		// stream-owned sender drains these bypass controls in request order; using
		// one goroutine per cancellation would turn a non-reading client into an
		// admission-limit-sized goroutine leak. Retain the generation in watches
		// until Send finishes so an explicit ID cannot be reused ahead of its
		// terminal frame.
		if w.queueDirectControl(response, generation) {
			return
		}
		// Direct-constructed watcher adapters do not own stream send loops. Keep
		// their cancellation behavior synchronous.
		w.finishCancel(id, generation, w.Send(response))
		return
	}
	w.finishCancel(id, generation, w.SendControl(response))
}

func (w *watcher) beginDeferredClientCancel(id int64, generation *watch) bool {
	w.Lock()
	if w.watches[id] != generation || !generation.closing.CompareAndSwap(false, true) {
		w.Unlock()
		return false
	}
	quotaHeld := generation.quotaHeld
	generation.quotaHeld = false
	if generation.cancel != nil {
		generation.cancel()
	}
	w.Unlock()
	if quotaHeld {
		w.grpcServer.releaseWatch()
	}
	return true
}

func (w *watcher) finishCancel(id int64, generation *watch, serr error) {
	// Reaching here means the terminal response is either already sent or ordered
	// in controlCh. Only now may a create reuse this explicit ID.
	w.Lock()
	if w.watches[id] == generation {
		delete(w.watches, id)
	}
	w.Unlock()
	if serr != nil {
		if isExpectedWatchCloseError(serr) {
			klog.V(4).InfoS("cancel response skipped because watch stream is closed", "watcher", w.id, "watch", id, "err", serr)
		} else {
			klog.ErrorS(serr, "failed to send cancel response", "watcher", w.id, "watch", id)
		}
	}
}

func (s *RPCServer) acquireWatch() bool {
	if s.maxWatches == 0 {
		return true
	}
	s.watchQuotaMu.Lock()
	defer s.watchQuotaMu.Unlock()
	if s.activeWatches >= int64(s.maxWatches) {
		s.metricCli.EmitCounter("watch.admission.rejected", 1)
		emitClientAdmissionRejection(s.metricCli, clientAdmissionGuardWatch)
		return false
	}
	s.activeWatches++
	s.metricCli.EmitGauge("watch.admission.active", s.activeWatches)
	return true
}

func (s *RPCServer) releaseWatch() {
	if s.maxWatches == 0 {
		return
	}
	s.watchQuotaMu.Lock()
	defer s.watchQuotaMu.Unlock()
	s.activeWatches--
	s.metricCli.EmitGauge("watch.admission.active", s.activeWatches)
}

func (w *watcher) Send(resp *etcdserverpb.WatchResponse) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	err := w.watchServer.Send(resp)
	if shouldCountServerStreamFailure(w.watchServer.Context(), err) {
		emitEtcdServerStreamFailureCounter(w.metricCli, "send", "watch", 1)
	}
	return err
}

// SendWatch orders the watch-generation check with every wire send. Cancel
// marks the generation closing before queuing its control response; holding
// sendMu here means an event either commits to the wire before that transition,
// or observes closing/removal/reuse and is suppressed. A canceled generation can
// therefore never reappear after its terminal response, including when the
// client has reused the same WatchId or between response fragments.
func (w *watcher) SendWatch(id int64, generation *watch, resp *etcdserverpb.WatchResponse) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	w.Lock()
	current, active := w.watches[id]
	w.Unlock()
	if !active || generation == nil || current != generation || generation.closing.Load() {
		return errWatchInactive
	}
	err := w.watchServer.Send(resp)
	if shouldCountServerStreamFailure(w.watchServer.Context(), err) {
		emitEtcdServerStreamFailureCounter(w.metricCli, "send", "watch", 1)
	}
	return err
}

func (w *watcher) SendControl(resp *etcdserverpb.WatchResponse) error {
	if w.controlCh == nil {
		return w.Send(resp)
	}
	_, err := w.sendControlWithCompletion(resp)
	return err
}

func (w *watcher) sendControlWithCompletion(resp *etcdserverpb.WatchResponse) (<-chan error, error) {
	if err := w.waitDirectControlBarrier(); err != nil {
		return nil, err
	}
	if w.controlCh == nil {
		done := make(chan error, 1)
		done <- w.Send(resp)
		return done, nil
	}
	done := make(chan error, 1)
	select {
	case w.controlCh <- watchControlResponse{resp: resp, done: done}:
		return done, nil
	case <-w.watchServer.Context().Done():
		return nil, w.watchServer.Context().Err()
	}
}

func (w *watcher) waitDirectControlBarrier() error {
	w.directControlMu.Lock()
	barrier := w.directControlBarrier
	w.directControlMu.Unlock()
	if barrier == nil {
		return nil
	}
	select {
	case <-barrier.done:
		return barrier.err
	case <-w.watchServer.Context().Done():
		return w.watchServer.Context().Err()
	}
}

func (w *watcher) sendControls() {
	defer w.controlWG.Done()
	for control := range w.controlCh {
		if control.ready != nil {
			select {
			case <-control.ready:
			case <-control.cancel:
				if control.done != nil {
					control.done <- context.Canceled
				}
				if control.generation != nil && control.generation.authoritativeSent != nil {
					close(control.generation.authoritativeSent)
				}
				continue
			}
			if control.generation != nil && !control.resp.Canceled && control.generation.closing.Load() {
				if control.done != nil {
					control.done <- context.Canceled
				}
				if control.generation.authoritativeSent != nil {
					close(control.generation.authoritativeSent)
				}
				continue
			}
		}
		start := time.Now()
		var err error
		if control.generation != nil && !control.resp.Canceled {
			err = w.SendWatch(control.resp.WatchId, control.generation, control.resp)
			if errors.Is(err, errWatchInactive) {
				err = context.Canceled
			}
		} else {
			err = w.Send(control.resp)
		}
		emitWatchSendLoopControlStreamDuration(w.metricCli, time.Since(start))
		if control.done != nil {
			control.done <- err
		}
		if control.ready != nil && control.generation != nil && control.generation.authoritativeSent != nil {
			close(control.generation.authoritativeSent)
		}
		if err != nil {
			if !isExpectedWatchCloseError(err) {
				w.metricCli.EmitCounter("watch.control.send.err", 1)
				klog.ErrorS(err, "watch control response send failed", "watcher", w.id)
			}
		}
	}
}

func (w *watcher) startDirectControls() {
	w.directControlCh = make(chan watchControlResponse, watchControlBuffer)
	w.controlWG.Add(1)
	go w.sendDirectControls()
}

func (w *watcher) queueDirectControl(resp *etcdserverpb.WatchResponse, generation *watch) bool {
	return w.queueDirectControlAfter(resp, generation, nil)
}

func (w *watcher) queueDeferredDirectControl(resp *etcdserverpb.WatchResponse, generation *watch) bool {
	return w.queueDirectControlAfter(resp, generation, generation.authoritativeSent)
}

func (w *watcher) queueDirectControlAfter(resp *etcdserverpb.WatchResponse, generation *watch, ready <-chan struct{}) bool {
	if w.directControlCh == nil {
		return false
	}
	barrier := &watchControlBarrier{done: make(chan struct{})}
	w.directControlMu.Lock()
	defer w.directControlMu.Unlock()
	select {
	case w.directControlCh <- watchControlResponse{resp: resp, generation: generation, barrier: barrier, ready: ready}:
		// Publishing the barrier while holding directControlMu makes every later
		// ordinary control observe this cancellation before it can enter controlCh.
		w.directControlBarrier = barrier
		return true
	case <-w.watchServer.Context().Done():
		return false
	}
}

func (w *watcher) sendDirectControls() {
	defer w.controlWG.Done()
	for control := range w.directControlCh {
		if control.ready != nil {
			select {
			case <-control.ready:
			case <-w.watchServer.Context().Done():
				control.barrier.err = w.watchServer.Context().Err()
				close(control.barrier.done)
				continue
			}
			if !w.beginDeferredClientCancel(control.resp.WatchId, control.generation) {
				close(control.barrier.done)
				continue
			}
		}
		err := w.Send(control.resp)
		w.finishCancel(control.resp.WatchId, control.generation, err)
		control.barrier.err = err
		close(control.barrier.done)
	}
}

func (w *watcher) closeDirectControls() {
	if w.directControlCh != nil {
		close(w.directControlCh)
	}
}

func (w *watcher) startProgressRequests() {
	w.progressCh = make(chan watchProgressRequest, watchControlBuffer)
	w.progressWG.Add(1)
	go w.sendProgressRequests()
}

func (w *watcher) queueProgressRequest(request watchProgressRequest) bool {
	select {
	case w.progressCh <- request:
		return true
	case <-w.watchServer.Context().Done():
		return false
	}
}

func (w *watcher) sendProgressRequests() {
	defer w.progressWG.Done()
	for request := range w.progressCh {
		if revision, synced := w.waitProgressRequest(request); synced {
			if err := w.SendControl(&etcdserverpb.WatchResponse{
				Header: txnHeader(int64(revision)), WatchId: -1,
			}); err != nil && !isExpectedWatchCloseError(err) {
				klog.ErrorS(err, "watch send stream progress response err", "watcher", w.id)
			}
		}
	}
}

func (w *watcher) Close() {
	w.metricCli.EmitCounter("watch.close", 1)
	w.Lock()
	quotaHeld := 0
	for id, v := range w.watches {
		if v.cancel != nil {
			v.cancel()
		}
		if v.quotaHeld {
			quotaHeld++
		}
		delete(w.watches, id)
	}
	w.Unlock()
	for i := 0; i < quotaHeld; i++ {
		w.grpcServer.releaseWatch()
	}
	w.wg.Wait()
	if w.progressCh != nil {
		close(w.progressCh)
		w.progressWG.Wait()
	}
	if w.controlCh != nil {
		close(w.controlCh)
	}
	w.closeDirectControls()
	w.controlWG.Wait()
}

func (w *watcher) Watch(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	wt := w.watches[id]
	w.Unlock()
	w.watchGeneration(ctx, id, r, wt)
}

func (w *watcher) watchGeneration(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest, wt *watch) {
	defer w.wg.Done()
	if wt == nil {
		return
	}
	// etcd keys are arbitrary byte strings — do NOT police their shape here. A
	// leading-'/' heuristic used to gate this path (a relic of the retired
	// StartRevision<0 range-stream overload, #67), which silently rejected every
	// watch from consumers with slash-less key layouts (Cilium's kvstore uses
	// "cilium/..."; found by the cilium-as-consumer suite, #78). The one shape
	// that IS invalid — a negative start revision — is rejected at request
	// decode in the stream loop above.
	if _, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh(); leadingFresh {
		if compacted, err := w.isCompactedWatchRevision(ctx, r.StartRevision); err != nil {
			w.CancelGeneration(id, wt, err, isWatchCompactedError(err))
			return
		} else if compacted {
			w.CancelGeneration(id, wt, compactedRevisionError(), true)
			return
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	backendPrefix := watchBackendPrefix(r.Key, r.RangeEnd)
	watchRevision := uint64(r.StartRevision)
	generationCtx, cancelGeneration := context.WithCancel(ctx)
	defer func() { cancelGeneration() }()
	ch, localGeneration, generationEpoch, err := w.openWatchChannel(generationCtx, r, backendPrefix, watchRevision)
	klog.InfoS("[watch stream] watch", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key), "end", loggedWatchKey(r.RangeEnd), "backendPrefix", loggedWatchKey([]byte(backendPrefix)), "rev", r.StartRevision)
	if err != nil {
		w.metricCli.EmitCounter("watch.backend.err", 1)
		klog.ErrorS(err, "[watch stream] cancel due to backend watch err", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key), "end", loggedWatchKey(r.RangeEnd), "rev", r.StartRevision)
		if wt.awaitAuthoritativeCreate {
			w.rejectAuthoritativeCreate(id, wt, err, 0)
		} else {
			w.CancelGeneration(id, wt, err, isWatchCompactedError(err))
		}
		return
	}
	if wt.awaitAuthoritativeCreate {
		select {
		case result, ok := <-ch:
			if !ok {
				w.rejectAuthoritativeCreate(id, wt, errors.New("watch closed before authoritative create"), 0)
				return
			}
			if result.Err != nil {
				w.rejectAuthoritativeCreate(id, wt, result.Err, result.CompactRevision)
				return
			}
			if !result.Created || result.ProgressRevision != 0 || len(result.Events) != 0 {
				w.rejectAuthoritativeCreate(id, wt, errors.New("watch backend omitted authoritative create acknowledgement"), 0)
				return
			}
		case <-ctx.Done():
			return
		}
		*wt.authoritativeControl = etcdserverpb.WatchResponse{
			Header: txnHeader(int64(wt.createdRevision)), Created: true, WatchId: id,
		}
		close(wt.authoritativeReady)
		select {
		case sendCreateErr := <-wt.authoritativeDone:
			if sendCreateErr != nil {
				w.CancelGeneration(id, wt, sendCreateErr, false)
				return
			}
		case <-ctx.Done():
			return
		}
		wt.awaitAuthoritativeCreate = false
	}

	var sendErr error
	var progressTicker *time.Ticker
	var progressC <-chan time.Time
	progressState := newPeriodicProgressState()
	if r.ProgressNotify {
		interval := w.progressInterval
		if interval <= 0 {
			// Keep direct-constructed watchers (tests and internal adapters) safe;
			// production Watch streams always carry the sampled stream interval.
			interval = w.backend.WatchProgressNotifyInterval()
			if interval <= 0 {
				interval = time.Second
			}
		}
		progressTicker = time.NewTicker(interval)
		defer progressTicker.Stop()
		progressC = progressTicker.C
	}
	resumeGeneration := func() bool {
		_, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh()
		if (!leadingFresh && !w.grpcServer.peers.EtcdProxyEnabled()) || w.nextWatchRevisionCompacted(ctx, id) {
			return false
		}
		// Backend and proxy channels are generation-scoped. Resume from the
		// first revision not actually delivered to the client. syncedRev is
		// advanced only after Send succeeds, so discarding an unread result from
		// an obsolete generation remains both gap-free and duplicate-free.
		watchRevision = nextUndeliveredWatchRevision(wt, watchRevision)
		cancelGeneration()
		nextGenerationCtx, nextCancelGeneration := context.WithCancel(ctx)
		var reopenErr error
		ch, localGeneration, generationEpoch, reopenErr = w.reopenWatchChannel(nextGenerationCtx, r, backendPrefix, watchRevision)
		if reopenErr == nil {
			cancelGeneration = nextCancelGeneration
			klog.InfoS("[watch stream] watch resumed", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key), "rev", watchRevision, "local", localGeneration)
			return true
		}
		nextCancelGeneration()
		if ctx.Err() == nil {
			klog.ErrorS(reopenErr, "[watch stream] watch resume failed", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key), "rev", watchRevision)
		}
		return false
	}
	fenceLocalGeneration := func() (current, resumed bool) {
		if !localGeneration {
			return true, false
		}
		currentEpoch, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh()
		if leadingFresh && currentEpoch == generationEpoch {
			return true, false
		}
		// OnStoppedLeading may lag the renew-deadline self fence. Do not
		// publish anything newly observed from that obsolete local generation;
		// the authoritative generation must replay it from syncedRev+1.
		klog.InfoS("[watch stream] local generation leadership fence changed", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key), "generationEpoch", generationEpoch, "currentEpoch", currentEpoch, "leadingFresh", leadingFresh)
		if ctx.Err() == nil && resumeGeneration() {
			return false, true
		}
		if ctx.Err() != nil {
			return false, false
		}
		compacted := w.nextWatchRevisionCompacted(ctx, id)
		var staleErr error
		if compacted {
			staleErr = compactedRevisionError()
		}
		w.CancelGeneration(id, wt, staleErr, compacted)
		return false, false
	}

watchLoop:
	for {
		select {
		case result, ok := <-ch:
			if !ok {
				klog.InfoS("[watch stream] watch channel closed", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key))
				_, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh()
				roleTransition := localGeneration || leadingFresh
				if ctx.Err() == nil && roleTransition && resumeGeneration() {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				compacted := w.nextWatchRevisionCompacted(ctx, id)
				var closeErr error
				if compacted {
					closeErr = compactedRevisionError()
				}
				w.CancelGeneration(id, wt, closeErr, compacted)
				klog.InfoS("[watch stream] watch canceled", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key))
				return
			}
			if current, resumed := fenceLocalGeneration(); !current {
				if resumed {
					continue watchLoop
				}
				return
			}
			if result.Err != nil {
				klog.InfoS("[watch stream] watch channel error", "watcher", w.id, "watch", id, "key", loggedWatchKey(r.Key), "err", result.Err)
				w.CancelGeneration(id, wt, result.Err, isWatchCompactedError(result.Err))
				return
			}
			if result.Created {
				if localGeneration || result.ProgressRevision != 0 || len(result.Events) != 0 {
					resultErr := errors.New("watch backend returned invalid create acknowledgement")
					emitWatchBackendIntegrityFailure(w.metricCli, "invalid_result")
					w.CancelGeneration(id, wt, resultErr, false)
					return
				}
				// Every proxy reconnect requests CreatedNotify. The logical watch was
				// already exposed after its first authoritative acknowledgement, so
				// later generation-local acknowledgements are internal-only.
				continue
			}
			if resultErr := invalidWatchResultShape(result); resultErr != nil {
				emitWatchBackendIntegrityFailure(w.metricCli, "invalid_result")
				klog.ErrorS(resultErr, "[watch stream] cancel due to invalid backend result", "watcher", w.id, "watch", id)
				w.CancelGeneration(id, wt, resultErr, false)
				cancel()
				return
			}
			if !localGeneration {
				if rangeErr := validateForwardedWatchEventRange(result.Events, r.Key, r.RangeEnd); rangeErr != nil {
					emitWatchBackendIntegrityFailure(w.metricCli, "invalid_result")
					klog.ErrorS(rangeErr, "[watch stream] cancel due to out-of-range forwarded event", "watcher", w.id, "watch", id)
					w.CancelGeneration(id, wt, rangeErr, false)
					cancel()
					return
				}
			}
			if result.ProgressRevision > uint64(math.MaxInt64) {
				revisionErr := fmt.Errorf("watch backend returned progress revision %d exceeds MaxInt64", result.ProgressRevision)
				emitWatchBackendIntegrityFailure(w.metricCli, "invalid_revision")
				klog.ErrorS(revisionErr, "[watch stream] cancel due to unrepresentable progress revision", "watcher", w.id, "watch", id)
				w.CancelGeneration(id, wt, revisionErr, false)
				cancel()
				return
			}
			if result.Revision > uint64(math.MaxInt64) {
				revisionErr := fmt.Errorf("watch backend returned batch revision %d exceeds MaxInt64", result.Revision)
				emitWatchBackendIntegrityFailure(w.metricCli, "invalid_revision")
				klog.ErrorS(revisionErr, "[watch stream] cancel due to unrepresentable batch revision", "watcher", w.id, "watch", id)
				w.CancelGeneration(id, wt, revisionErr, false)
				cancel()
				return
			}
			if result.ProgressRevision > 0 {
				if wt != nil {
					sourceRevision := atomic.LoadUint64(&wt.sourceRev)
					if result.ProgressRevision < sourceRevision {
						revisionErr := fmt.Errorf("watch backend returned progress revision %d below source revision %d", result.ProgressRevision, sourceRevision)
						emitWatchBackendIntegrityFailure(w.metricCli, "invalid_revision")
						klog.ErrorS(revisionErr, "[watch stream] cancel due to regressing progress revision", "watcher", w.id, "watch", id)
						w.CancelGeneration(id, wt, revisionErr, false)
						cancel()
						return
					}
				}
				if current, resumed := fenceLocalGeneration(); !current {
					if resumed {
						continue watchLoop
					}
					return
				}
				// In-band progress marker: it FIFO-guarantees it sits behind every
				// matching event <= its revision (all such events were read and Sent
				// above, on this same goroutine, before this marker), so folding it
				// into syncedRev can never claim an undelivered event. This is the
				// sole liveness source for a quiet watch, whose prefix matches no
				// event batch. Reporting (progressC / on-demand) is unchanged.
				if wt != nil {
					util.StoreMaxUint64(&wt.sourceRev, result.ProgressRevision)
					util.StoreMaxUint64(&wt.syncedRev, result.ProgressRevision)
				}
				continue
			}
			if sendErr != nil {
				// drain the channel to ensure producer could exit
				continue
			}
			var sourceRevision uint64
			if wt != nil {
				sourceRevision = atomic.LoadUint64(&wt.sourceRev)
			}
			batchRevision, revisionErr := validatedWatchBatchRevision(result, sourceRevision)
			if revisionErr != nil {
				emitWatchBackendIntegrityFailure(w.metricCli, "invalid_revision")
				klog.ErrorS(revisionErr, "[watch stream] cancel due to invalid backend batch revision", "watcher", w.id, "watch", id)
				w.CancelGeneration(id, wt, revisionErr, false)
				cancel()
				return
			}
			events := filterWatchEventsByRange(result.Events, r.Key, r.RangeEnd)
			events = filterWatchEvents(events, r.Filters)
			if !r.PrevKv {
				events = withoutWatchPrevKvs(events)
			} else if watchEventsNeedPrevKVs(events) {
				compactRevision, compactErr := w.backend.GetCompactRevisionFresh(ctx)
				if compactErr != nil {
					w.metricCli.EmitCounter("watch.prev_kv.compact_revision.err", 1)
					klog.ErrorS(compactErr, "failed to resolve compact revision for watch PrevKV; omitting previous values", "watcher", w.id, "watch", id)
				}
				// Upstream resolves requested PrevKV with a Range at
				// ModRevision-1 while assembling each response. Once compaction
				// reaches the event revision that historical read is below the
				// watermark, so PrevKV must be nil even though KubeBrain retains
				// the internal value needed to reconstruct the DELETE event itself.
				// A failed Range likewise leaves PrevKV unset; conservatively omit
				// all previous values because the unknown watermark may cover them.
				events = watchPrevKVVisibility(events, compactRevision, compactErr)
				if compactErr == nil {
					if prevKVErr := validateRequestedWatchPrevKVs(events, compactRevision); prevKVErr != nil {
						emitWatchBackendIntegrityFailure(w.metricCli, "invalid_result")
						klog.ErrorS(prevKVErr, "[watch stream] cancel due to missing requested PrevKV", "watcher", w.id, "watch", id)
						w.CancelGeneration(id, wt, prevKVErr, false)
						cancel()
						return
					}
				}
			}
			if r.StartRevision > 0 {
				for _, event := range events {
					eventRevision := event.GetKv().GetModRevision()
					if eventRevision < r.StartRevision {
						revisionErr := fmt.Errorf("watch backend returned event revision %d below watch start revision %d", eventRevision, r.StartRevision)
						emitWatchBackendIntegrityFailure(w.metricCli, "invalid_revision")
						klog.ErrorS(revisionErr, "[watch stream] cancel due to invalid backend event revision", "watcher", w.id, "watch", id)
						w.CancelGeneration(id, wt, revisionErr, false)
						cancel()
						return
					}
				}
			}
			if len(events) == 0 {
				if current, resumed := fenceLocalGeneration(); !current {
					if resumed {
						continue watchLoop
					}
					return
				}
				// etcd advances the watcher's min revision even when its filters
				// suppress every event in a batch. Preserve that delivered
				// watermark so the next progress response does not lag behind
				// filtered writes.
				if wt != nil {
					util.StoreMaxUint64(&wt.sourceRev, batchRevision)
					util.StoreMaxUint64(&wt.syncedRev, batchRevision)
				}
				continue
			}
			watchResponse := &etcdserverpb.WatchResponse{
				Header:  txnHeader(int64(batchRevision)),
				WatchId: id,
				Events:  events,
			}
			// The stream interceptor stamps these identity fields immediately before
			// the wire send. Stamp them here as well so fragmentation measures the
			// final protobuf size; otherwise a response just below the limit can grow
			// past it after sendWatchFragments has already elected not to split.
			if headerErr := w.grpcServer.stampWatchResponseHeader(ctx, watchResponse); headerErr != nil {
				sendErr = headerErr
			} else {
				if current, resumed := fenceLocalGeneration(); !current {
					if resumed {
						continue watchLoop
					}
					return
				}
				w.metricCli.EmitGauge("watch.watch_stream.push", watchResponse.Header.Revision)
				w.metricCli.EmitHistogram("watch.watch_stream.push.size", proto.Size(watchResponse))
				start := time.Now()
				sendWatch := func(response *etcdserverpb.WatchResponse) error {
					return w.SendWatch(id, wt, response)
				}
				if r.Fragment {
					sendErr = sendWatchFragments(watchResponse, w.grpcServer.watchFragmentBytes(), sendWatch)
				} else {
					sendErr = sendWatch(watchResponse)
				}
				emitWatchSendLoopWatchStreamDuration(w.metricCli, time.Since(start), len(events))
			}
			if errors.Is(sendErr, errWatchInactive) {
				return
			}
			if sendErr != nil {
				w.metricCli.EmitCounter("watch.watch_stream.push.err", 1)
				klog.ErrorS(sendErr, "[watch stream] watch send err, cancel", "watcher", w.id, "watch", id)
				w.CancelGeneration(id, wt, sendErr, false)
				cancel()
			} else if wt != nil {
				// These events are now delivered; the watch is synced through the
				// highest revision in this batch.
				util.StoreMaxUint64(&wt.sourceRev, uint64(watchResponse.Header.Revision))
				util.StoreMaxUint64(&wt.syncedRev, uint64(watchResponse.Header.Revision))
				progressState.eventSent()
			}
		case <-progressC:
			if sendErr != nil {
				continue
			}
			if current, resumed := fenceLocalGeneration(); !current {
				if resumed {
					continue watchLoop
				}
				return
			}
			if !progressState.tick() {
				// Match etcd: an event proves progress, so suppress the next
				// periodic response and rearm the watch for the following tick.
				continue
			}
			// Report the revision actually delivered to this watch, never the
			// global current revision (which runs ahead of undelivered events).
			var revision uint64
			if wt != nil {
				revision = atomic.LoadUint64(&wt.syncedRev)
			}
			if revision == 0 {
				// Nothing delivered yet (e.g. a from-now follower watch before its
				// first proxied progress marker seeds syncedRev). A WatchResponse
				// with Header.Revision==0 is not a valid progress notification
				// (clientv3 IsProgressNotify requires a non-zero revision), so skip
				// it; the next tick reports the real revision once it advances.
				continue
			}
			if wt != nil && wt.progressStartRevision > revision {
				// A future-revision watch is not synchronized merely because the
				// global store reached its start. Wait for this watch's FIFO event
				// stream to deliver an event or progress marker at that revision.
				continue
			}
			progressResp := &etcdserverpb.WatchResponse{
				Header:  txnHeader(int64(revision)),
				WatchId: id,
			}
			w.metricCli.EmitGauge("watch.watch_stream.progress", progressResp.Header.Revision)
			start := time.Now()
			if sendErr = w.SendWatch(id, wt, progressResp); errors.Is(sendErr, errWatchInactive) {
				return
			} else if sendErr != nil {
				w.metricCli.EmitCounter("watch.watch_stream.progress.err", 1)
				klog.ErrorS(sendErr, "[watch stream] progress send err, cancel", "watcher", w.id, "watch", id)
				w.CancelGeneration(id, wt, sendErr, false)
				cancel()
			}
			emitWatchSendLoopProgressDuration(w.metricCli, time.Since(start))
		}
	}
}

func emitWatchSendLoopWatchStreamDuration(metricCli metrics.Metrics, duration time.Duration, eventCount int) {
	if metricCli == nil {
		return
	}
	metricCli.EmitHistogram("etcd_debugging.server.watch_send_loop.watch_stream.duration.seconds", duration.Seconds())
	if eventCount > 0 {
		metricCli.EmitHistogram("etcd_debugging.server.watch_send_loop.watch_stream.duration_per_event.seconds", duration.Seconds()/float64(eventCount))
	}
}

func emitWatchSendLoopControlStreamDuration(metricCli metrics.Metrics, duration time.Duration) {
	if metricCli == nil {
		return
	}
	metricCli.EmitHistogram("etcd_debugging.server.watch_send_loop.control_stream.duration.seconds", duration.Seconds())
}

func emitWatchSendLoopProgressDuration(metricCli metrics.Metrics, duration time.Duration) {
	if metricCli == nil {
		return
	}
	metricCli.EmitHistogram("etcd_debugging.server.watch_send_loop.progress.duration.seconds", duration.Seconds())
}

// openWatchChannel binds one watch generation to the authoritative source for
// the node's current role. The local backend and follower proxy expose the same
// WatchResult stream, so callers can resume between them at an explicit revision.
var errNilWatchGeneration = errors.New("watch backend returned a nil generation channel")

func (w *watcher) openWatchChannel(ctx context.Context, r *etcdserverpb.WatchCreateRequest, backendPrefix string, revision uint64) (<-chan etcdproxy.WatchResult, bool, uint64, error) {
	epoch, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh()
	if leadingFresh {
		ch, err := w.backend.Watch(ctx, backendPrefix, revision)
		if err == nil && ch == nil {
			emitWatchBackendIntegrityFailure(w.metricCli, "invalid_result")
			err = errNilWatchGeneration
		}
		return ch, true, epoch, err
	}
	ch, err := w.grpcServer.peers.Watch(ctx, r.Key, r.RangeEnd, revision)
	if err == nil && ch == nil {
		emitWatchBackendIntegrityFailure(w.metricCli, "invalid_result")
		err = errNilWatchGeneration
	}
	return ch, false, 0, err
}

// reopenWatchChannel tolerates the short no-leader interval between terms. A
// closed generation is not a terminal watch cancellation; retry until a current
// leader can serve the explicit resume revision or the client goes away.
func (w *watcher) reopenWatchChannel(ctx context.Context, r *etcdserverpb.WatchCreateRequest, backendPrefix string, revision uint64) (<-chan etcdproxy.WatchResult, bool, uint64, error) {
	// Avoid a hot loop when a proxy implementation briefly returns an already-
	// closed generation while its leader cache is converging.
	if err := waitWatchReconnect(ctx); err != nil {
		return nil, false, 0, err
	}
	for {
		if !w.grpcServer.peers.EtcdProxyEnabled() {
			if _, leadingFresh := w.grpcServer.peers.EpochAndLeadingFresh(); !leadingFresh {
				emitWatchGenerationRecovery(w.metricCli, watchGenerationRecoveryFailed)
				return nil, false, 0, errors.New("watch has no fresh local generation and peer proxy is disabled")
			}
		}
		ch, local, epoch, err := w.openWatchChannel(ctx, r, backendPrefix, revision)
		if err == nil {
			emitWatchGenerationRecovery(w.metricCli, watchGenerationRecoveryRecovered)
			return ch, local, epoch, nil
		}
		if isWatchCompactedError(err) {
			emitWatchGenerationRecovery(w.metricCli, watchGenerationRecoveryCompacted)
			return nil, false, 0, err
		}
		emitWatchGenerationRecovery(w.metricCli, watchGenerationRecoveryRetry)
		if err := waitWatchReconnect(ctx); err != nil {
			return nil, false, 0, err
		}
	}
}

func waitWatchReconnect(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextUndeliveredWatchRevision(wt *watch, fallback uint64) uint64 {
	if wt == nil {
		return fallback
	}
	delivered := atomic.LoadUint64(&wt.syncedRev)
	if delivered == math.MaxUint64 {
		return fallback
	}
	if next := delivered + 1; next > fallback {
		return next
	}
	return fallback
}

func (w *watcher) nextWatchRevisionCompacted(ctx context.Context, id int64) bool {
	w.Lock()
	wt := w.watches[id]
	w.Unlock()
	if wt == nil {
		return false
	}
	syncedRevision := atomic.LoadUint64(&wt.syncedRev)
	if syncedRevision == math.MaxUint64 {
		return false
	}
	// A cleanly closed generation has no status from which to distinguish
	// compaction from a transport transition. Preserve the local durable hint,
	// but never let a storage-isolated follower stall a long-lived Watch while
	// probing it; proxied compaction errors normally arrive authoritatively on
	// the leader response before this fallback is needed.
	probeCtx, cancel := context.WithTimeout(ctx, watchCompactionProbeTimeout)
	defer cancel()
	compactRevision, err := w.backend.GetCompactRevisionFresh(probeCtx)
	if err != nil {
		return false
	}
	// etcd keeps the event at exactly the compact revision watchable. Only a
	// strictly older next revision proves that this closed stream lost history
	// and must force the client to re-list.
	return syncedRevision+1 < compactRevision
}

func (s *RPCServer) watchFragmentBytes() int {
	return int(s.maxRequestBytes + grpcOverheadBytes)
}

func (s *RPCServer) stampWatchResponseHeader(ctx context.Context, response *etcdserverpb.WatchResponse) error {
	term, err := s.responseRaftTerm(ctx)
	if err != nil {
		return err
	}
	stampHeader(response, s.backend.ClusterID(), s.localMemberID(), term)
	return nil
}

func sendWatchFragments(response *etcdserverpb.WatchResponse, maxBytes int, send func(*etcdserverpb.WatchResponse) error) error {
	if proto.Size(response) < maxBytes || len(response.Events) < 2 {
		return send(response)
	}
	for offset := 0; offset < len(response.Events); {
		fragment := &etcdserverpb.WatchResponse{
			Header: response.Header, WatchId: response.WatchId, Created: response.Created,
			Canceled: response.Canceled, CompactRevision: response.CompactRevision,
			CancelReason: response.CancelReason, Fragment: true,
		}
		for offset < len(response.Events) {
			fragment.Events = append(fragment.Events, response.Events[offset])
			if len(fragment.Events) > 1 && proto.Size(fragment) >= maxBytes {
				fragment.Events = fragment.Events[:len(fragment.Events)-1]
				break
			}
			offset++
		}
		if offset == len(response.Events) {
			fragment.Fragment = false
		}
		if err := send(fragment); err != nil {
			return err
		}
	}
	return nil
}

func isWatchCompactedError(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.OutOfRange {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "required revision has been compacted") ||
		strings.Contains(msg, "cache event oldest revision")
}

func normalizeWatchCreateRequest(r *etcdserverpb.WatchCreateRequest) *etcdserverpb.WatchCreateRequest {
	normalized := proto.Clone(r).(*etcdserverpb.WatchCreateRequest)
	if len(normalized.Key) == 0 {
		normalized.Key = []byte{0}
	}
	if len(normalized.RangeEnd) == 0 {
		normalized.RangeEnd = nil
	}
	if len(normalized.RangeEnd) == 1 && normalized.RangeEnd[0] == 0 {
		normalized.RangeEnd = []byte{}
	}
	return normalized
}

func (w *watcher) isCompactedWatchRevision(ctx context.Context, revision int64) (bool, error) {
	if revision <= 0 {
		return false, nil
	}
	// Watch creation is an authoritative history boundary. A newly elected
	// replica may still have the previous leader's compact watermark cached for
	// up to the TTL; accepting the watch on that stale-low value can silently
	// skip the compacted prefix before the backend fallback notices anything.
	compactRevision, err := w.backend.GetCompactRevisionFresh(ctx)
	if err != nil {
		return false, err
	}
	return revision < int64(compactRevision), nil
}

func filterWatchEventsByRange(events []*mvccpb.Event, start, end []byte) []*mvccpb.Event {
	if len(events) == 0 {
		return events
	}
	filtered := events[:0]
	for _, event := range events {
		if watchEventInRange(event, start, end) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func validateForwardedWatchEventRange(events []*mvccpb.Event, start, end []byte) error {
	for i, event := range events {
		if !watchEventInRange(event, start, end) {
			return fmt.Errorf("watch leader proxy returned event key outside the requested range at index %d", i)
		}
	}
	return nil
}

func watchEventInRange(event *mvccpb.Event, start, end []byte) bool {
	if event == nil || event.Kv == nil {
		return false
	}
	key := event.Kv.Key
	if end == nil {
		return bytes.Equal(key, start)
	}
	if bytes.Compare(key, start) < 0 {
		return false
	}
	return len(end) == 0 || bytes.Compare(key, end) < 0
}

func watchBackendPrefix(start, end []byte) string {
	if end == nil || bytes.Equal(end, prefixEnd(start)) {
		return string(start)
	}
	return ""
}

func filterWatchEvents(events []*mvccpb.Event, filters []etcdserverpb.WatchCreateRequest_FilterType) []*mvccpb.Event {
	if len(events) == 0 || len(filters) == 0 {
		return events
	}
	dropPut := false
	dropDelete := false
	for _, filter := range filters {
		switch filter {
		case etcdserverpb.WatchCreateRequest_NOPUT:
			dropPut = true
		case etcdserverpb.WatchCreateRequest_NODELETE:
			dropDelete = true
		default:
		}
	}
	if !dropPut && !dropDelete {
		return events
	}
	filtered := events[:0]
	for _, event := range events {
		if event == nil {
			filtered = append(filtered, event)
			continue
		}
		if dropPut && event.Type == mvccpb.PUT {
			continue
		}
		if dropDelete && event.Type == mvccpb.DELETE {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func withoutWatchPrevKvs(events []*mvccpb.Event) []*mvccpb.Event {
	if len(events) == 0 {
		return events
	}
	withoutPrev := make([]*mvccpb.Event, 0, len(events))
	for _, event := range events {
		if event == nil {
			withoutPrev = append(withoutPrev, nil)
			continue
		}
		// Field-level shallow copy (not proto.Clone/value-copy): standard
		// protobuf messages embed a non-copyable MessageState, and this runs per
		// event on the watch hot path. Keep Type/Kv, drop PrevKv.
		withoutPrev = append(withoutPrev, &mvccpb.Event{Type: event.Type, Kv: event.Kv})
	}
	return withoutPrev
}

func watchEventsNeedPrevKVs(events []*mvccpb.Event) bool {
	for _, event := range events {
		if event == nil || event.Kv == nil {
			continue
		}
		if event.Type == mvccpb.DELETE || (event.Type == mvccpb.PUT && event.Kv.CreateRevision != event.Kv.ModRevision) {
			return true
		}
	}
	return false
}

func validateRequestedWatchPrevKVs(events []*mvccpb.Event, compactRevision uint64) error {
	for i, event := range events {
		if event == nil || event.Kv == nil {
			continue
		}
		modRevision := event.Kv.ModRevision
		if modRevision <= 0 || uint64(modRevision) <= compactRevision {
			continue
		}
		needsPrevKV := event.Type == mvccpb.DELETE || (event.Type == mvccpb.PUT && event.Kv.CreateRevision != modRevision)
		if needsPrevKV && event.PrevKv == nil {
			return fmt.Errorf("watch backend omitted requested previous value for %s event at revision %d and index %d", event.Type, modRevision, i)
		}
	}
	return nil
}

func watchPrevKVVisibility(events []*mvccpb.Event, compactRevision uint64, compactErr error) []*mvccpb.Event {
	if compactErr != nil {
		return withoutWatchPrevKvs(events)
	}
	return withoutCompactedWatchPrevKvs(events, compactRevision)
}

// withoutCompactedWatchPrevKvs mirrors upstream's lazy Range at
// event.ModRevision-1: revision == compactRevision remains watchable, but its
// previous revision does not. Clone only affected Event envelopes because the
// source batch is shared by watches with different PrevKv settings.
func withoutCompactedWatchPrevKvs(events []*mvccpb.Event, compactRevision uint64) []*mvccpb.Event {
	if compactRevision == 0 {
		return events
	}
	var out []*mvccpb.Event
	for i, event := range events {
		if event == nil || event.PrevKv == nil {
			continue
		}
		modRevision := event.GetKv().GetModRevision()
		if modRevision <= 0 || uint64(modRevision) > compactRevision {
			continue
		}
		if out == nil {
			out = append([]*mvccpb.Event(nil), events...)
		}
		// Do not value-copy protobuf MessageState; only Type/Kv are needed and
		// match withoutWatchPrevKvs's allocation-safe field copy above.
		out[i] = &mvccpb.Event{Type: event.Type, Kv: event.Kv}
	}
	if out == nil {
		return events
	}
	return out
}

func isExpectedWatchCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
		return true
	}
	code := status.Code(err)
	if code == codes.Canceled || code == codes.InvalidArgument || code == codes.ResourceExhausted {
		return true
	}
	if code == codes.Unavailable && strings.Contains(err.Error(), "transport is closing") {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "context canceled") || strings.Contains(msg, "transport is closing")
}
