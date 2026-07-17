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

const defaultWatchFragmentBytes = 2 * 1024 * 1024
const onDemandProgressSyncWait = 100 * time.Millisecond

// watcher correspond to one stream, one watcher has many watches
type watcher struct {
	sync.Mutex
	sendMu sync.Mutex

	wg      sync.WaitGroup
	backend BackendShim
	// stream server
	watchServer etcdserverpb.Watch_WatchServer
	// gRPC server
	grpcServer *RPCServer

	// hold watch request info in this stream
	watches map[int64]*watch
	id      int64
	// controlRev is the latest leader revision synchronized for this stream.
	// Followers use it for created/client-cancel headers and as the R fence for
	// from-now proxy watches that subscribe at R+1.
	controlRev uint64

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
			return err
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

// syncedRevSnapshot returns a copy of every active watch's delivered
// watermark, for per-watch progress responses (#39).
func (w *watcher) syncedRevSnapshot() map[int64]uint64 {
	w.Lock()
	defer w.Unlock()
	m := make(map[int64]uint64, len(w.watches))
	for id, wt := range w.watches {
		m[id] = atomic.LoadUint64(&wt.syncedRev)
	}
	return m
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
		snapshot := w.syncedRevSnapshot()
		var minRev uint64
		allSynced := len(snapshot) > 0
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
		metricCli:   s.metricCli,
	}
	w.id = atomic.AddInt64(&watcherID, 1)
	klog.InfoS("new watcher", "id", w.id)
	defer func() {
		w.Close()
	}()

	for {
		msg, err := ws.Recv()
		if err != nil {
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
			if err := w.syncControlRevision(ws.Context()); err != nil {
				return err
			}
			if r.StartRevision < 0 {
				// etcd treats a negative start revision as an immediately canceled
				// create, while keeping the multiplexed stream usable for later watches.
				if err := w.Send(&etcdserverpb.WatchResponse{
					Header: txnHeader(int64(w.responseRevision())), WatchId: -1,
					Created: true, Canceled: true, CancelReason: rpctypes.ErrCompacted.Error(),
				}); err != nil {
					return err
				}
				continue
			}
			caller, authErr := s.authCallerFromContext(ws.Context())
			if authErr == nil {
				authErr = caller.require(r.Key, r.RangeEnd, authpb.READ)
			}
			if authErr != nil {
				if err := w.Send(&etcdserverpb.WatchResponse{
					Header: txnHeader(int64(w.responseRevision())), WatchId: -1,
					Created: true, Canceled: true, CancelReason: watchAuthCancelReason(authErr),
				}); err != nil {
					return err
				}
				continue
			}
			// normal watch request can only be handled by leader
			if !s.peers.IsLeader() && !s.peers.EtcdProxyEnabled() {
				s.metricCli.EmitCounter("watch.follower", 1)
				leaderInfo := s.peers.GetLeaderInfo()
				klog.InfoS("watch follower", "revision", r.StartRevision, "addr", s.backend.GetResourceLock().Identity(), "leader", leaderInfo)
				return status.Errorf(codes.Unavailable, "watch error addr is %s leader %s", s.backend.GetResourceLock().Identity(), leaderInfo)
			}
			if !s.peers.IsLeader() && r.StartRevision == 0 {
				r.StartRevision = int64(w.responseRevision()) + 1
			}

			w.Start(ws.Context(), r)
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
			snapshot := w.syncedRevSnapshot()
			if len(snapshot) == 1 {
				var singleRev uint64
				for _, rev := range snapshot {
					singleRev = rev
				}
				if singleRev > 0 {
					if err := w.Send(&etcdserverpb.WatchResponse{
						Header: txnHeader(int64(singleRev)), WatchId: -1,
					}); err != nil {
						klog.ErrorS(err, "watch send stream progress response err", "watcher", w.id)
						return err
					}
					continue
				}
			}
			if rev, synced := w.waitStreamProgressRevision(ws.Context(), targetRevision, onDemandProgressSyncWait); synced {
				if err := w.Send(&etcdserverpb.WatchResponse{
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
			for id, rev := range w.syncedRevSnapshot() {
				if rev == 0 {
					// Range-stream pseudo-watches never emit progress.
					continue
				}
				if err := w.Send(&etcdserverpb.WatchResponse{
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
	w.Lock()
	ctx, cancel := context.WithCancel(c)
	id := r.WatchId
	if id == 0 {
		// Watch IDs are scoped to one stream. Match etcd's zero-indexed allocator
		// and skip IDs explicitly reserved by earlier create requests.
		for id = 0; ; id++ {
			if _, exists := w.watches[id]; !exists {
				break
			}
		}
	} else if _, exists := w.watches[id]; exists {
		w.Unlock()
		cancel()
		_ = w.Send(&etcdserverpb.WatchResponse{
			Header: txnHeader(int64(w.responseRevision())), WatchId: -1,
			Created: true, Canceled: true, CancelReason: "mvcc: duplicate watch ID provided on the WatchStream",
		})
		return
	}
	if len(r.RangeEnd) != 0 && bytes.Compare(r.Key, r.RangeEnd) >= 0 {
		w.Unlock()
		cancel()
		_ = w.Send(&etcdserverpb.WatchResponse{
			Header: txnHeader(int64(w.responseRevision())), WatchId: -1,
			Created: true, Canceled: true, CancelReason: "mvcc: watcher range is empty",
		})
		return
	}

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
		cancel:    cancel,
		start:     string(r.Key),
		end:       string(r.RangeEnd),
		syncedRev: initSyncedRev,
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
	if err := w.Send(&etcdserverpb.WatchResponse{
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
	if c, ok := w.watches[id]; ok {
		found = true
		klog.InfoS("cancel context", "watcher", w.id, "watch", id, "start", c.start, "end", c.end)
		if c.cancel != nil {
			c.cancel()
		}
		delete(w.watches, id)
	}
	w.Unlock()
	// etcd silently ignores a client cancellation for an unknown watch ID. An
	// internal cancellation always names a live watch (or carries an error), so
	// retaining its response behavior is safe.
	if !found && clientRequest {
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
	serr := w.Send(&etcdserverpb.WatchResponse{
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

func (w *watcher) Send(resp *etcdserverpb.WatchResponse) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.watchServer.Send(resp)
}

func (w *watcher) Close() {
	w.metricCli.EmitCounter("watch.close", 1)
	w.Lock()
	for _, v := range w.watches {
		if v.cancel != nil {
			v.cancel()
		}
	}
	w.Unlock()
	w.wg.Wait()
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
			if len(events) == 0 {
				continue
			}
			watchResponse := &etcdserverpb.WatchResponse{
				Header: &etcdserverpb.ResponseHeader{
					Revision: events[len(events)-1].Kv.ModRevision,
				},
				WatchId: id,
				Events:  events,
			}
			w.metricCli.EmitGauge("watch.watch_stream.push", watchResponse.Header.Revision)
			w.metricCli.EmitHistogram("watch.watch_stream.push.size", proto.Size(watchResponse))
			if r.Fragment {
				sendErr = sendWatchFragments(watchResponse, defaultWatchFragmentBytes, w.Send)
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
			}
		case <-progressC:
			if sendErr != nil {
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
	if errors.Is(err, context.Canceled) {
		return true
	}
	code := status.Code(err)
	if code == codes.Canceled {
		return true
	}
	if code == codes.Unavailable && strings.Contains(err.Error(), "transport is closing") {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "context canceled") || strings.Contains(msg, "transport is closing")
}
