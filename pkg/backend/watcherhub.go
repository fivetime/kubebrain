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

package backend

import (
	"context"
	"sync"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	// TODO: from start options or self-adaptive
	watchBuffer = 10000
)

// WatcherHub maintain registry of Watcher
type WatcherHub struct {
	sync.RWMutex
	subs      map[chan []*proto.Event]struct{}
	metricCli metrics.Metrics
	// bufSize is the per-subscriber channel buffer; 0 means watchBuffer.
	bufSize int
}

func (w *WatcherHub) subBufferSize() int {
	if w.bufSize > 0 {
		return w.bufSize
	}
	return watchBuffer
}

// AddWatcher add watcher, filter by prefix and revision is processed in upper server layer
func (w *WatcherHub) AddWatcher(ctx context.Context) (<-chan []*proto.Event, error) {
	w.metricCli.EmitCounter("watcher_hub.add_watcher", 1)
	w.Lock()
	defer w.Unlock()

	// set watch buffer
	sub := make(chan []*proto.Event, w.subBufferSize())
	if w.subs == nil {
		w.subs = map[chan []*proto.Event]struct{}{}
	}
	w.subs[sub] = struct{}{}
	go func() {
		<-ctx.Done()
		klog.InfoS("ctx done, delete watcher %v", "chan", sub)
		w.DeleteWatcher(sub, true)
	}()

	return sub, nil
}

// DeleteWatcher delete watcher
func (w *WatcherHub) DeleteWatcher(sub chan []*proto.Event, lock bool) {
	w.metricCli.EmitCounter("watcher_hub.delete_watcher", 1)
	if lock {
		w.Lock()
	}
	if _, ok := w.subs[sub]; ok {
		klog.InfoS("close event chan in watcher hub", "chan", sub)
		close(sub)
		delete(w.subs, sub)
	}
	if lock {
		w.Unlock()
	}
}

// CloseAll closes all watchers and forces clients to re-list before watching again.
func (w *WatcherHub) CloseAll() {
	w.metricCli.EmitCounter("watcher_hub.close_all", 1)
	w.Lock()
	defer w.Unlock()
	for sub := range w.subs {
		w.DeleteWatcher(sub, false)
	}
}

// Stream push events to watchers.
func (w *WatcherHub) Stream(input chan []*proto.Event) {
	for item := range input {
		w.broadcast(item)
	}

	w.Lock()
	klog.Info("[watcher hub] input channel from heap closed, delete all watchers")
	for sub := range w.subs {
		w.DeleteWatcher(sub, false)
	}
	w.Unlock()
}

// broadcast delivers one event batch to every subscriber, then synchronously
// evicts any subscriber whose buffer was full.
//
// Eviction must happen here, before the next batch: sending a slow subscriber a
// later batch after it missed one would deliver a gap (…N-1, N+1 with N
// dropped) that a kube-apiserver reflector never recovers from. It also must
// happen outside the RLock — DeleteWatcher takes the write lock, so evicting
// inline would deadlock. Since Stream is the sole sender, a sub removed here
// receives no further batch: the client sees a contiguous prefix then a clean
// cancel and re-watches/re-lists cleanly.
func (w *WatcherHub) broadcast(item []*proto.Event) {
	var slow []chan []*proto.Event
	w.RLock()
	for sub := range w.subs {
		select {
		case sub <- item:
		default:
			slow = append(slow, sub)
		}
	}
	w.RUnlock()
	for _, sub := range slow {
		klog.InfoS("drop slow consumer", "chan", sub, "bufSize", w.subBufferSize())
		w.metricCli.EmitCounter("drop.slow.watcher", 1)
		w.DeleteWatcher(sub, true)
	}
}
