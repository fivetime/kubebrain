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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/util"
)

var (
	// one watcher is one grpc stream
	watcherID int64
)

const onDemandProgressSyncWait = 100 * time.Millisecond

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
}

// watcher correspond to one stream, one watcher has many watches
type watcher struct {
	sync.Mutex
	sendMu sync.Mutex

	wg        sync.WaitGroup
	controlWG sync.WaitGroup
	backend   BackendShim
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

	controlCh chan watchControlResponse

	metricCli metrics.Metrics
}

func (w *watcher) responseRevision() uint64 {
	rev := w.backend.GetPublishedRevision()
	if control := atomic.LoadUint64(&w.controlRev); control > rev {
		rev = control
	}
	return rev
}

func (w *watcher) syncControlRevision(ctx context.Context) error {
	revision := w.backend.GetPublishedRevision()
	if !w.grpcServer.peers.IsLeader() {
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
}

type periodicProgressState struct {
	eligible bool
}

func newPeriodicProgressState() periodicProgressState {
	return periodicProgressState{eligible: true}
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
	allEligible = len(w.watches) > 0
	for id, wt := range w.watches {
		syncedRev := atomic.LoadUint64(&wt.syncedRev)
		if wt.progressStartRevision > syncedRev {
			allEligible = false
			continue
		}
		snapshot[id] = syncedRev
	}
	return snapshot, allEligible
}

// minSyncedRevision returns the minimum revision delivered across all active
// watches on the stream, and whether any watch is active.
func (w *watcher) minSyncedRevision() (uint64, bool) {
	w.Lock()
	defer w.Unlock()
	var minRev uint64
	found := false
	for _, wt := range w.watches {
		rev := atomic.LoadUint64(&wt.syncedRev)
		if !found || rev < minRev {
			minRev = rev
			found = true
		}
	}
	return minRev, found
}

// waitStreamProgressRevision returns the minimum delivered revision once every
// active watch has caught up through target. A stream-wide WatchId=-1 progress
// response is safe only at that floor; otherwise clientv3 would broadcast a
// revision that a slower watch has not delivered yet.
func (w *watcher) waitStreamProgressRevision(ctx context.Context, target uint64, timeout time.Duration) (uint64, bool) {
	if target == 0 {
		return 0, false
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, allEligible := w.progressSyncedRevSnapshot()
		var minRev uint64
		allSynced := allEligible
		for _, rev := range snapshot {
			if minRev == 0 || rev < minRev {
				minRev = rev
			}
			if rev < target {
				allSynced = false
			}
		}
		if allSynced {
			return minRev, true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-deadline.C:
			return 0, false
		case <-ticker.C:
		}
	}
}

func (s *RPCServer) Watch(ws etcdserverpb.Watch_WatchServer) error {
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
	klog.InfoS("new watcher", "id", w.id)
	defer func() {
		w.Close()
	}()

	for {
		msg, err := ws.Recv()
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
			if isExpectedWatchCloseError(err) {
				klog.V(4).InfoS("watcher receive closed", "id", w.id, "err", err, "info", strings.Join(watchInfo, "\n"))
			} else {
				klog.ErrorS(err, "watcher receive err", "id", w.id, "info", strings.Join(watchInfo, "\n"))
			}
			return err
		}

		if r := msg.GetCreateRequest(); r != nil {
			r = normalizeWatchCreateRequest(r)
			if r.StartRevision < 0 {
				// etcd treats a negative start revision as an immediately canceled
				// create, while keeping the multiplexed stream usable for later watches.
				if err := w.SendControlAndWait(canceledWatchCreateResponse(
					w.responseRevision(), rpctypes.ErrCompacted.Error(),
				)); err != nil {
					return err
				}
				continue
			}
			caller, authErr := s.authCallerFromContext(ws.Context())
			if authErr == nil {
				authErr = caller.require(r.Key, r.RangeEnd, authpb.READ)
			}
			if authErr != nil {
				if err := w.SendControlAndWait(canceledWatchCreateResponse(
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
				if err := w.SendControlAndWait(canceledWatchCreateResponse(
					w.responseRevision(), "mvcc: watcher range is empty",
				)); err != nil {
					return err
				}
				continue
			}
			if r.WatchId != 0 && w.hasWatchID(r.WatchId) {
				if err := w.SendControlAndWait(canceledWatchCreateResponse(
					w.responseRevision(), "mvcc: duplicate watch ID provided on the WatchStream",
				)); err != nil {
					return err
				}
				continue
			}
			if err := w.syncControlRevision(ws.Context()); err != nil {
				return err
			}
			watchCtx := ws.Context()
			if !s.peers.IsLeader() && s.peers.EtcdProxyEnabled() {
				watchCtx, authErr = s.forwardAuthToken(watchCtx, caller)
				if authErr != nil {
					return authErr
				}
			}
			// normal watch request can only be handled by leader
			if !s.peers.IsLeader() && !s.peers.EtcdProxyEnabled() {
				s.metricCli.EmitCounter("watch.follower", 1)
				leaderInfo := s.peers.GetLeaderInfo()
				klog.InfoS("watch follower", "revision", r.StartRevision, "addr", s.backend.GetResourceLock().Identity(), "leader", leaderInfo)
				return status.Errorf(codes.Unavailable, "watch error addr is %s leader %s", s.backend.GetResourceLock().Identity(), leaderInfo)
			}
			progressStartRevision := r.StartRevision
			if !s.peers.IsLeader() && r.StartRevision == 0 {
				r.StartRevision = int64(w.responseRevision()) + 1
			}

			w.start(watchCtx, r, uint64(progressStartRevision))
		} else if cancelRequest := msg.GetCancelRequest(); cancelRequest != nil {
			if err := w.syncControlRevision(ws.Context()); err != nil {
				return err
			}
			s.metricCli.EmitCounter("watch.client.cancel", 1)
			klog.InfoS("receive watch cancel request", "id", w.id, "watchID", cancelRequest.GetWatchId())
			w.CancelRequest(msg.GetCancelRequest().WatchId)
		} else if msg.GetProgressRequest() != nil {
			s.metricCli.EmitCounter("watch.progress.request", 1)
			targetRevision := s.backend.GetPublishedRevision()
			// Kick one immediate marker fan-out so the watermark converges NOW
			// rather than on the next ticker beat. The snapshot answered below is
			// still the current (possibly one-interval-stale) value — markers ride
			// the FIFO subscriber channels — but the kicked marker advances
			// syncedRev for the follow-up RequestProgress ~100ms later, keeping
			// k8s 1.37 ConsistentListFromCache convergence at poll granularity
			// instead of ticker granularity (and off its 3s LIST-fallback cliff).
			s.backend.KickWatchProgress()
			snapshot, allEligible := w.progressSyncedRevSnapshot()
			if allEligible && len(snapshot) == 1 {
				var singleRev uint64
				for _, rev := range snapshot {
					singleRev = rev
				}
				if singleRev > 0 {
					if err := w.SendControl(&etcdserverpb.WatchResponse{
						Header: txnHeader(int64(singleRev)), WatchId: -1,
					}); err != nil {
						klog.ErrorS(err, "watch send stream progress response err", "watcher", w.id)
						return err
					}
					continue
				}
			}
			if rev, synced := w.waitStreamProgressRevision(ws.Context(), targetRevision, onDemandProgressSyncWait); synced {
				if err := w.SendControl(&etcdserverpb.WatchResponse{
					Header: txnHeader(int64(rev)), WatchId: -1,
				}); err != nil {
					klog.ErrorS(err, "watch send stream progress response err", "watcher", w.id)
					return err
				}
				continue
			}
			// Per-watch progress (#39): answer RequestProgress with one
			// header-only response PER WATCH, each carrying that watch's own
			// delivered watermark (syncedRev). clientv3 routes responses by
			// WatchId, so every consumer gets its own truthful progress.
			//
			// A single stream-wide (WatchId=-1) response capped at the slowest
			// watch — the previous behavior — deadlocks large keyspaces: the
			// kube-apiserver multiplexes EVERY resource cacher's watch onto a
			// few shared etcd streams, and 1.36's ConsistentListFromCache +
			// WatchList gate cacher readiness on progress. One cacher doing a
			// multi-minute initial sync (3M namespaces) then pins the stream
			// minimum, so NO cacher ever observes progress past its start
			// revision, storage-readiness never turns, the service-ip-repair
			// PostStartHook hits its hard-coded 1-minute deadline, and the
			// apiserver crash-loops forever. Per-watch progress is also what
			// the periodic notify path already emits, and syncedRev never
			// over-reports (it advances only via delivered events/markers).
			snapshot, _ = w.progressSyncedRevSnapshot()
			for id, rev := range snapshot {
				if rev == 0 {
					// Range-stream pseudo-watches never emit progress.
					continue
				}
				if err := w.SendControl(&etcdserverpb.WatchResponse{
					Header:  txnHeader(int64(rev)),
					WatchId: id,
				}); err != nil {
					klog.ErrorS(err, "watch send progress response err", "watcher", w.id, "watch", id)
					return err
				}
			}
			// With no active watches etcd's progressAll has nothing to send.
			// Likewise, do not synthesize a stream response here.
		} else {
			s.metricCli.EmitCounter("watch.request.unsupported", 1)
			klog.Info("watch receive message unsupported type")
		}
	}
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
	w.start(c, r, uint64(r.StartRevision))
}

func (w *watcher) start(c context.Context, r *etcdserverpb.WatchCreateRequest, progressStartRevision uint64) {
	w.Lock()
	ctx, cancel := context.WithCancel(c)
	// Match etcd watchStream.Watch validation order: an empty range wins over
	// duplicate-ID detection when both fields are invalid.
	if len(r.RangeEnd) != 0 && bytes.Compare(r.Key, r.RangeEnd) >= 0 {
		w.Unlock()
		cancel()
		_ = w.SendControl(canceledWatchCreateResponse(
			w.responseRevision(), "mvcc: watcher range is empty",
		))
		return
	}
	if r.WatchId != 0 {
		if _, duplicate := w.watches[r.WatchId]; duplicate {
			w.Unlock()
			cancel()
			_ = w.SendControl(canceledWatchCreateResponse(
				w.responseRevision(), "mvcc: duplicate watch ID provided on the WatchStream",
			))
			return
		}
	}
	if !w.grpcServer.acquireWatch() {
		w.Unlock()
		cancel()
		_ = w.SendControl(canceledWatchCreateResponse(
			w.responseRevision(), watchQuotaCancelReason,
		))
		return
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
	w.watches[id] = &watch{
		cancel:                cancel,
		start:                 string(r.Key),
		end:                   string(r.RangeEnd),
		quotaHeld:             w.grpcServer.maxWatches != 0,
		progressStartRevision: progressStartRevision,
		syncedRev:             initSyncedRev,
	}
	if len(w.watches) > 1 {
		klog.InfoS("watcher reuse", "id", w.id, "size", len(w.watches))
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
	if r.StartRevision == 0 {
		// Register from-now watches as an explicit historical watch from the
		// created watermark forward. Send(Created) happens before the backend
		// goroutine subscribes, so leaving revision zero here creates a gap where
		// a committed event can land before AddWatcher and be skipped forever.
		// Replaying from createdRev+1 closes that gap; backend.Watch installs its
		// live subscriber before scanning history, so the handoff is lossless.
		r.StartRevision = int64(createdRev) + 1
	}
	if err := w.SendControlAndWait(&etcdserverpb.WatchResponse{
		Header:  txnHeader(int64(createdRev)),
		Created: true,
		WatchId: id,
	}); err != nil {
		klog.ErrorS(err, "watch send create watch response err", "wacher", w.id, "watch", id)
		w.Cancel(id, err, false)
		return
	}

	w.wg.Add(1)
	key := string(r.Key)
	w.metricCli.EmitCounter("watch.watch", 1)
	go w.Watch(ctx, id, r)
	klog.InfoS("watch start", "id", id, "count", len(w.watches), "key", key, "revision", r.StartRevision)
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
	w.cancel(id, err, compact, false)
}

func (w *watcher) CancelRequest(id int64) {
	w.cancel(id, nil, false, true)
}

func (w *watcher) cancel(id int64, err error, compact, clientRequest bool) {
	klog.InfoS("watch cancel", "watcher", w.id, "watch", id, "err", err, "compact", compact)
	var tags []metrics.T
	tags = append(tags, metrics.Tag("compact", strconv.FormatBool(compact)))
	w.metricCli.EmitCounter("watch.cancel", 1, tags...)
	w.Lock()
	found := false
	quotaHeld := false
	if c, ok := w.watches[id]; ok {
		found = true
		quotaHeld = c.quotaHeld
		klog.InfoS("cancel context", "watcher", w.id, "watch", id, "start", c.start, "end", c.end)
		if c.cancel != nil {
			c.cancel()
		}
		delete(w.watches, id)
	}
	w.Unlock()
	if quotaHeld {
		w.grpcServer.releaseWatch()
	}
	// etcd emits at most one cancellation response for a watch. A client cancel
	// removes the watch before its backend goroutine observes the canceled
	// context, so suppress that goroutine's later close response as well as
	// requests for unknown IDs.
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
		// another compacted round-trip (#33). Cancels are rare — not a hot path.
		if rev, revErr := w.backend.GetCompactRevisionFresh(context.Background()); revErr == nil && rev > 0 {
			compactRevision = int64(rev)
		}
	}
	cancelReason := "watch closed"
	if clientRequest {
		cancelReason = ""
	}
	if err != nil {
		cancelReason = err.Error()
	}
	header := &etcdserverpb.ResponseHeader{}
	if clientRequest {
		header = txnHeader(int64(w.responseRevision()))
	}
	serr := w.SendControl(&etcdserverpb.WatchResponse{
		Header:          header,
		Canceled:        true,
		CancelReason:    cancelReason,
		WatchId:         id,
		CompactRevision: compactRevision,
	})
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
	return w.watchServer.Send(resp)
}

func (w *watcher) SendControl(resp *etcdserverpb.WatchResponse) error {
	if w.controlCh == nil {
		return w.Send(resp)
	}
	select {
	case w.controlCh <- watchControlResponse{resp: resp}:
		return nil
	case <-w.watchServer.Context().Done():
		return w.watchServer.Context().Err()
	}
}

func (w *watcher) SendControlAndWait(resp *etcdserverpb.WatchResponse) error {
	if w.controlCh == nil {
		return w.Send(resp)
	}
	done := make(chan error, 1)
	select {
	case w.controlCh <- watchControlResponse{resp: resp, done: done}:
	case <-w.watchServer.Context().Done():
		return w.watchServer.Context().Err()
	}
	select {
	case err := <-done:
		return err
	case <-w.watchServer.Context().Done():
		return w.watchServer.Context().Err()
	}
}

func (w *watcher) sendControls() {
	defer w.controlWG.Done()
	for control := range w.controlCh {
		err := w.Send(control.resp)
		if control.done != nil {
			control.done <- err
		}
		if err != nil {
			if !isExpectedWatchCloseError(err) {
				w.metricCli.EmitCounter("watch.control.send.err", 1)
				klog.ErrorS(err, "watch control response send failed", "watcher", w.id)
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
	if w.controlCh != nil {
		close(w.controlCh)
		w.controlWG.Wait()
	}
}

func (w *watcher) Watch(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest) {
	defer w.wg.Done()
	// etcd keys are arbitrary byte strings — do NOT police their shape here. A
	// leading-'/' heuristic used to gate this path (a relic of the retired
	// StartRevision<0 range-stream overload, #67), which silently rejected every
	// watch from consumers with slash-less key layouts (Cilium's kvstore uses
	// "cilium/..."; found by the cilium-as-consumer suite, #78). The one shape
	// that IS invalid — a negative start revision — is rejected at request
	// decode in the stream loop above.
	if compacted, err := w.isCompactedWatchRevision(ctx, r.StartRevision); err != nil {
		w.Cancel(id, err, isWatchCompactedError(err))
		return
	} else if compacted {
		w.Cancel(id, compactedRevisionError(), true)
		return
	}

	var ch <-chan etcdproxy.WatchResult
	var err error

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	backendPrefix := watchBackendPrefix(r.Key, r.RangeEnd)
	if w.grpcServer.peers.IsLeader() {
		// Leader: read directly from the local backend, which now returns the same
		// WatchResult type as the follower/proxy branch (events plus in-band
		// progress markers), so the two branches are symmetric downstream.
		ch, err = w.backend.Watch(ctx, backendPrefix, uint64(r.StartRevision))
	} else {
		ch, err = w.grpcServer.peers.Watch(ctx, r.Key, r.RangeEnd, uint64(r.StartRevision))
	}
	klog.InfoS("[watch stream] watch", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "backendPrefix", backendPrefix, "rev", r.StartRevision)
	if err != nil {
		w.metricCli.EmitCounter("watch.backend.err", 1)
		klog.ErrorS(err, "[watch stream] cancel due to backend watch err", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision)
		w.Cancel(id, err, isWatchCompactedError(err))
		return
	}

	// Hold the *watch so this goroutine can advance syncedRev as events are sent
	// and the progress ticker can read it (the stream Recv goroutine reads it too,
	// hence atomic access on the field).
	w.Lock()
	wt := w.watches[id]
	w.Unlock()

	var sendErr error
	var progressTicker *time.Ticker
	var progressC <-chan time.Time
	progressState := newPeriodicProgressState()
	if r.ProgressNotify {
		interval := w.backend.WatchProgressNotifyInterval()
		if interval <= 0 {
			interval = time.Second
		}
		progressTicker = time.NewTicker(interval)
		defer progressTicker.Stop()
		progressC = progressTicker.C
	}
	for {
		select {
		case result, ok := <-ch:
			if !ok {
				klog.InfoS("[watch stream] watch channel closed", "watcher", w.id, "watch", id, "key", string(r.Key))
				w.Cancel(id, nil, false)
				klog.InfoS("[watch stream] watch canceled", "watcher", w.id, "watch", id, "key", string(r.Key))
				return
			}
			if result.Err != nil {
				klog.InfoS("[watch stream] watch channel error", "watcher", w.id, "watch", id, "key", string(r.Key), "err", result.Err)
				w.Cancel(id, result.Err, isWatchCompactedError(result.Err))
				return
			}
			if result.ProgressRevision > 0 {
				// In-band progress marker: it FIFO-guarantees it sits behind every
				// matching event <= its revision (all such events were read and Sent
				// above, on this same goroutine, before this marker), so folding it
				// into syncedRev can never claim an undelivered event. This is the
				// sole liveness source for a quiet watch, whose prefix matches no
				// event batch. Reporting (progressC / on-demand) is unchanged.
				if wt != nil {
					util.StoreMaxUint64(&wt.syncedRev, result.ProgressRevision)
				}
				continue
			}
			if sendErr != nil {
				// drain the channel to ensure producer could exit
				continue
			}
			events := filterWatchEventsByRange(result.Events, r.Key, r.RangeEnd)
			events = filterWatchEvents(events, r.Filters)
			if !r.PrevKv {
				events = withoutWatchPrevKvs(events)
			}
			batchRevision := result.Revision
			if batchRevision == 0 {
				// Compatibility fallback for older/internal WatchResult producers.
				for _, event := range result.Events {
					if revision := uint64(event.GetKv().GetModRevision()); revision > batchRevision {
						batchRevision = revision
					}
				}
			}
			if len(events) == 0 {
				// etcd advances the watcher's min revision even when its filters
				// suppress every event in a batch. Preserve that delivered
				// watermark so the next progress response does not lag behind
				// filtered writes.
				if wt != nil {
					util.StoreMaxUint64(&wt.syncedRev, batchRevision)
				}
				continue
			}
			watchResponse := &etcdserverpb.WatchResponse{
				Header: &etcdserverpb.ResponseHeader{
					Revision: int64(batchRevision),
				},
				WatchId: id,
				Events:  events,
			}
			w.metricCli.EmitGauge("watch.watch_stream.push", watchResponse.Header.Revision)
			w.metricCli.EmitHistogram("watch.watch_stream.push.size", proto.Size(watchResponse))
			if r.Fragment {
				sendErr = sendWatchFragments(watchResponse, w.grpcServer.watchFragmentBytes(), w.Send)
			} else {
				sendErr = w.Send(watchResponse)
			}
			if sendErr != nil {
				w.metricCli.EmitCounter("watch.watch_stream.push.err", 1)
				klog.ErrorS(sendErr, "[watch stream] watch send err, cancel", "watcher", w.id, "watch", id)
				w.Cancel(id, sendErr, false)
				cancel()
			} else if wt != nil {
				// These events are now delivered; the watch is synced through the
				// highest revision in this batch.
				util.StoreMaxUint64(&wt.syncedRev, uint64(watchResponse.Header.Revision))
				progressState.eventSent()
			}
		case <-progressC:
			if sendErr != nil {
				continue
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
			if sendErr = w.Send(progressResp); sendErr != nil {
				w.metricCli.EmitCounter("watch.watch_stream.progress.err", 1)
				klog.ErrorS(sendErr, "[watch stream] progress send err, cancel", "watcher", w.id, "watch", id)
				w.Cancel(id, sendErr, false)
				cancel()
			}
		}
	}
}

func (s *RPCServer) watchFragmentBytes() int {
	return int(s.maxRequestBytes + grpcOverheadBytes)
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
	compactRevision, err := w.backend.GetCompactRevision(ctx)
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
