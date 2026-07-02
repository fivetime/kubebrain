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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
)

var (
	// one watcher is one grpc stream
	watcherID int64
)

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

	metricCli metrics.Metrics
}

var (
	// one watch is one watch request
	watchID int64
)

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

// storeMaxUint64 atomically advances *addr to val, never moving it backwards.
func storeMaxUint64(addr *uint64, val uint64) {
	for {
		old := atomic.LoadUint64(addr)
		if val <= old {
			return
		}
		if atomic.CompareAndSwapUint64(addr, old, val) {
			return
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
		rev := atomic.LoadUint64(&wt.syncedRev)
		if !found || rev < minRev {
			minRev = rev
			found = true
		}
	}
	return minRev, found
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
			// normal watch request can only be handled by leader
			// magic logic: when StartRevision < 0, the request is a RangeStream Request.
			if r.StartRevision >= 0 && isPureWatchRequest(r) && !s.peers.IsLeader() && !s.peers.EtcdProxyEnabled() {
				s.metricCli.EmitCounter("watch.follower", 1)
				leaderInfo := s.peers.GetLeaderInfo()
				klog.InfoS("watch follower", "revision", r.StartRevision, "addr", s.backend.GetResourceLock().Identity(), "leader", leaderInfo)
				return status.Errorf(codes.Unavailable, "watch error addr is %s leader %s", s.backend.GetResourceLock().Identity(), leaderInfo)
			}

			w.Start(ws.Context(), r)
		} else if cancelRequest := msg.GetCancelRequest(); cancelRequest != nil {
			s.metricCli.EmitCounter("watch.client.cancel", 1)
			klog.InfoS("receive watch cancel request", "id", w.id, "watchID", cancelRequest.GetWatchId())
			w.Cancel(msg.GetCancelRequest().WatchId, nil, false)
		} else if msg.GetProgressRequest() != nil {
			s.metricCli.EmitCounter("watch.progress.request", 1)
			// A stream-wide progress notification must not exceed the slowest
			// watch: report the minimum revision delivered across all active
			// watches. With no active watch, nothing can be behind, so fall back
			// to the backend current revision.
			revision, ok := w.minSyncedRevision()
			if !ok {
				backendRev, err := safeBackendRevision(ws.Context(), s.backend)
				if err != nil {
					return err
				}
				revision = backendRev
			}
			if err := w.Send(&etcdserverpb.WatchResponse{
				Header:  txnHeader(int64(revision)),
				WatchId: -1,
			}); err != nil {
				klog.ErrorS(err, "watch send progress response err", "watcher", w.id)
				return err
			}
		} else {
			s.metricCli.EmitCounter("watch.request.unsupported", 1)
			klog.Info("watch receive message unsupported type")
		}
	}
}

func (w *watcher) Start(c context.Context, r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	ctx, cancel := context.WithCancel(c)
	id := atomic.AddInt64(&watchID, 1)

	// Seed syncedRev with the revision the watch is guaranteed to be caught up
	// through before any event is delivered: StartRevision-1 for a historical
	// watch (it will deliver events >= StartRevision), or the current revision for
	// a watch that starts from "now" (StartRevision == 0). Range-stream requests
	// (StartRevision < 0) never emit progress notifications, so leave it at 0.
	var initSyncedRev uint64
	if r.StartRevision > 0 {
		initSyncedRev = uint64(r.StartRevision) - 1
	} else if r.StartRevision == 0 {
		initSyncedRev = w.backend.GetCurrentRevision()
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

	if err := w.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		Created: true,
		WatchId: id,
	}); err != nil {
		klog.ErrorS(err, "watch send create watch response err", "wacher", w.id, "watch", id)
		w.Cancel(id, err, false)
		return
	}

	w.wg.Add(1)
	key := string(r.Key)

	// use watch to simulate list
	if r.StartRevision < 0 {
		w.metricCli.EmitCounter("watch.range", 1)
		go w.List(ctx, id, r)
	} else {
		w.metricCli.EmitCounter("watch.watch", 1)
		go w.Watch(ctx, id, r)
		klog.InfoS("watch start", "id", id, "count", len(w.watches), "key", key, "revision", r.StartRevision)
	}
}

func (w *watcher) Cancel(id int64, err error, compact bool) {
	klog.InfoS("watch cancel", "watcher", w.id, "watch", id, "err", err, "compact", compact)
	var tags []metrics.T
	tags = append(tags, metrics.Tag("compact", strconv.FormatBool(compact)))
	w.metricCli.EmitCounter("watch.cancel", 1, tags...)
	w.Lock()
	if c, ok := w.watches[id]; ok {
		klog.InfoS("cancel context", "watcher", w.id, "watch", id, "start", c.start, "end", c.end)
		if c.cancel != nil {
			c.cancel()
		}
		delete(w.watches, id)
	}
	w.Unlock()
	// if compact is true, apiserver reflector watch will return with err, which will trigger re-list & re-watch (detail in etcd/clientv3/watch.go watchGrpcStream.run)
	// else, apiserver reflector watch will return nil, which will trigger re-watch
	var compactRevision int64
	if compact {
		compactRevision = 1
		if rev, revErr := w.backend.GetCompactRevision(context.Background()); revErr == nil && rev > 0 {
			compactRevision = int64(rev)
		}
	}
	cancelReason := "watch closed"
	if err != nil {
		cancelReason = err.Error()
	}
	serr := w.Send(&etcdserverpb.WatchResponse{
		Header:          &etcdserverpb.ResponseHeader{},
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

func (w *watcher) List(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest) {
	defer w.wg.Done()
	if err := w.grpcServer.peers.SyncReadRevision(ctx); err != nil {
		w.Cancel(id, err, true)
		return
	}
	startTime := time.Now()
	klog.InfoS("RANGE STREAM", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision)
	ch, err := w.backend.ListByStream(ctx, r.Key, r.RangeEnd, uint64(r.StartRevision*-1))
	if err != nil {
		klog.ErrorS(err, "list by stream failed", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision*-1)
		w.metricCli.EmitCounter("watch.backend.list_stream.err", 1)
		w.Cancel(id, err, true)
		return
	}

	for watchResponse := range ch {
		revision := r.StartRevision * -1
		// range stream eof, tell client range ends
		// if has err, CancelReason is not nil
		if watchResponse.Canceled == true {
			klog.InfoS("receive cancel message", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, r.StartRevision*-1)
			// indicates eof
			revision = -1
			// set err info in events
			watchResponse.Events = []*mvccpb.Event{
				{
					Kv: &mvccpb.KeyValue{
						Key:         []byte("eof"),
						Value:       []byte(watchResponse.CancelReason),
						ModRevision: GetPartitionMagic,
					},
				},
			}
			w.metricCli.EmitCounter("watch.list_stream.eof", 1)
			w.metricCli.EmitHistogram("watch.list_stream.latency", time.Since(startTime).Seconds())
		}

		response := &etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{
				Revision: revision,
			},
			WatchId: id,
			Events:  watchResponse.Events,
		}
		w.metricCli.EmitCounter("watch.list_stream.push", len(response.Events))
		w.metricCli.EmitHistogram("watch.list_stream.push.size", response.Size())
		if err := w.Send(response); err != nil {
			klog.ErrorS(err, "[range stream] send response with header failed",
				"watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd, "rev", r.StartRevision*-1, "respRev", revision)
			w.metricCli.EmitCounter("watch.list_stream.push.err", 1)
			// send failed, should break
			// TODO retry refer to etcd victims
			w.Cancel(id, err, true)
			continue
		}
	}
	// for range stream, don't send cancel in watchServer, but wait client to cancel. to prevent message receive order confusion
	// delete watch info in watches map to avoid resource leak
	w.Lock()
	if c, ok := w.watches[id]; ok {
		klog.InfoS("[range stream] begin to cancel context", "watcher", w.id, "watch", id)
		if c.cancel != nil {
			c.cancel()
		}
		delete(w.watches, id)
	}
	w.Unlock()
	klog.InfoS("[range stream] range closed", "watcher", w.id, "watch", id, "key", r.Key, "end", r.RangeEnd)
}

func (w *watcher) Watch(ctx context.Context, id int64, r *etcdserverpb.WatchCreateRequest) {
	defer w.wg.Done()
	// when connection error, in etcd client v3 newWatchClient function,
	// range stream retry with newest revision may fall into this logic, which results
	// range stream hang

	// watchServer send message(including normal message and eof) has err transport closing, so the client can't receive cancel message
	// it will get connection err and trigger newWatchClient, newWatchClient will retry all not completed substreams with latest event revision.
	// once come into watch, it either hangs or come up with resource version too old err
	if !isPureWatchRequest(r) {
		w.Cancel(id, fmt.Errorf("invliad watch %s revision %d", r.Key, r.StartRevision), true)
		w.metricCli.EmitCounter("invalid.watch.key", 1)
		return
	}
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
		var eventCh <-chan []*mvccpb.Event
		eventCh, err = w.backend.Watch(ctx, backendPrefix, uint64(r.StartRevision))
		if err == nil {
			ch = watchResultsFromEvents(eventCh)
		}
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
		progressTicker = time.NewTicker(time.Second)
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
			w.metricCli.EmitHistogram("watch.watch_stream.push.size", watchResponse.Size())
			if sendErr = w.Send(watchResponse); sendErr != nil {
				w.metricCli.EmitCounter("watch.watch_stream.push.err", 1)
				klog.ErrorS(sendErr, "[watch stream] watch send err, cancel", "watcher", w.id, "watch", id)
				w.Cancel(id, sendErr, false)
				cancel()
			} else if wt != nil {
				// These events are now delivered; the watch is synced through the
				// highest revision in this batch.
				storeMaxUint64(&wt.syncedRev, uint64(watchResponse.Header.Revision))
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

func watchResultsFromEvents(events <-chan []*mvccpb.Event) <-chan etcdproxy.WatchResult {
	results := make(chan etcdproxy.WatchResult, 100)
	if events == nil {
		close(results)
		return results
	}
	go func() {
		defer close(results)
		for eventBatch := range events {
			results <- etcdproxy.WatchResult{Events: eventBatch}
		}
	}()
	return results
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

func isPureWatchRequest(r *etcdserverpb.WatchCreateRequest) bool {
	if string(r.Key) == compactRevKey {
		return true
	}
	// if starts with /, it is a normal watch request, not a range stream request
	if strings.HasPrefix(string(r.Key), "/") {
		return true
	}
	return false
}

func normalizeWatchCreateRequest(r *etcdserverpb.WatchCreateRequest) *etcdserverpb.WatchCreateRequest {
	normalized := *r
	if len(normalized.Key) == 0 {
		normalized.Key = []byte{0}
	}
	if len(normalized.RangeEnd) == 0 {
		normalized.RangeEnd = nil
	}
	if len(normalized.RangeEnd) == 1 && normalized.RangeEnd[0] == 0 {
		normalized.RangeEnd = []byte{}
	}
	return &normalized
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
		clone := *event
		clone.PrevKv = nil
		withoutPrev = append(withoutPrev, &clone)
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
