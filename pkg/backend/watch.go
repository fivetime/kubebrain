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
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	resultChanLength = 100
)

// Watch return a channel, every event‘s ModRevision >= revision
// and has specify prefix will be read from channel
// if revision < 0, invalid revision
// if revision == 0, start reading events from read channel
func (b *backend) Watch(ctx context.Context, prefix string, revision uint64) (<-chan []*proto.Event, error) {

	klog.InfoS("WATCH", "prefix", prefix, "revision", revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan, err := b.watcherHub.AddWatcher(ctx)
	if err != nil {
		cancel()
		klog.ErrorS(err, "add watcher failed", "chan", readChan)
		return nil, err
	}

	result := make(chan []*proto.Event, resultChanLength)

	// include the current revision in list
	if revision == 0 {
		go b.processEvents(cancel, result, readChan, prefix, revision)
		return result, nil
	}

	ret := b.watchCache.FindEvents(revision)

	if ret.empty {
		currentRevision, currentErr := b.safeCurrentRevision(ctx)
		if currentErr != nil {
			cancel()
			return nil, currentErr
		}
		if revision > currentRevision {
			// watch revision is latest, no need to fetch history
			go b.processEvents(cancel, result, readChan, prefix, revision)
			return result, nil
		}
		events, historyErr := b.historyWatchEvents(ctx, prefix, revision, currentRevision)
		if historyErr == nil {
			klog.InfoS("watch history fallback", "prefix", prefix, "revision", revision, "events", len(events))
			if len(events) > 0 {
				b.catchUpEvents(result, events)
				revision = events[len(events)-1].Revision + 1
			}
			go b.processEvents(cancel, result, readChan, prefix, revision)
			return result, nil
		}
		klog.ErrorS(historyErr, "watch history fallback failed", "prefix", prefix, "revision", revision)
		// event cache is empty
		cancel()
		klog.Errorf("empty cache event, close chan %v", readChan)
		return nil, fmt.Errorf(" empty cache event, current revision is %d", currentRevision)
	}

	if ret.high {
		go b.processEvents(cancel, result, readChan, prefix, revision)
		return result, nil
	}

	if ret.low {
		currentRevision, currentErr := b.safeCurrentRevision(ctx)
		if currentErr != nil {
			cancel()
			return nil, currentErr
		}
		events, historyErr := b.historyWatchEvents(ctx, prefix, revision, currentRevision)
		if historyErr == nil {
			klog.InfoS("watch history fallback from low cache", "prefix", prefix, "revision", revision, "oldestRev", ret.oldest.Revision, "events", len(events))
			lastRevision := revision
			if len(events) > 0 {
				b.catchUpEvents(result, events)
				lastRevision = events[len(events)-1].Revision + 1
			}
			go b.processEvents(cancel, result, readChan, prefix, lastRevision)
			return result, nil
		}
		cancel()
		klog.ErrorS(historyErr, "ret low history fallback failed", "prefix", prefix, "revision", revision, "oldestRev", ret.oldest.Revision)
		return nil, fmt.Errorf("cache event oldest revision is %d newer than requested revision %d: %w", ret.oldest.Revision, revision+1, historyErr)
	}

	events := filterByPrefix(ret.events, []byte(prefix))

	klog.InfoS("watch list", "prefix", prefix, "revision", revision, "latestRev", ret.newest.Revision, "cachedEvents", len(events))

	lastRevision := revision
	if len(events) > 0 {
		lastRevision = events[len(events)-1].Revision + 1
		b.catchUpEvents(result, events)
	}
	go b.processEvents(cancel, result, readChan, prefix, lastRevision)

	return result, nil
}

// catchUpEvents send stacked events to channel
func (b *backend) catchUpEvents(out chan<- []*proto.Event, events []*proto.Event) {
	batchSize := eventBatchSize
	// to avoid result chan full and hang
	if len(events) > resultChanLength*eventBatchSize {
		batchSize = len(events) / (resultChanLength - 1)
	}
	for {
		if len(events) > batchSize {
			out <- events[0:batchSize]
			events = events[batchSize:]
		} else {
			out <- events
			break
		}
	}
}

func (b *backend) historyWatchEvents(ctx context.Context, prefix string, fromRevision, currentRevision uint64) ([]*proto.Event, error) {
	compactRevision, err := b.GetCompactRevision(ctx)
	if err != nil {
		return nil, err
	}
	if compactRevision > 0 && fromRevision < compactRevision {
		return nil, fmt.Errorf("cache event oldest revision is compacted at %d newer than requested revision %d", compactRevision, fromRevision)
	}

	start := b.coder.EncodeObjectKey([]byte(prefix), 0)
	end := b.coder.EncodeObjectKey(PrefixEnd([]byte(prefix)), 0)
	iter, err := b.kv.Iter(ctx, start, end, 0, 0)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	events := make([]*proto.Event, 0)
	for {
		if err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		key, rev, err := b.coder.Decode(iter.Key())
		if err != nil {
			return nil, err
		}
		if rev == 0 || rev < fromRevision || rev > currentRevision || bytes.HasPrefix(key, etcdMetadataPrefix) {
			continue
		}
		val := append([]byte(nil), iter.Val()...)
		event := &proto.Event{
			Type:     proto.Event_PUT,
			Revision: rev,
			Kv: &proto.KeyValue{
				Key:      append([]byte(nil), key...),
				Value:    val,
				Revision: rev,
			},
		}
		if bytes.Equal(val, tombStoneBytes) {
			event.Type = proto.Event_DELETE
			prev, err := b.Get(ctx, &proto.GetRequest{Key: key, Revision: rev - 1})
			if err == nil && prev.Kv != nil {
				event.Kv.Value = append([]byte(nil), prev.Kv.Value...)
				event.Kv.Revision = prev.Kv.Revision
			} else {
				event.Kv.Value = nil
				event.Kv.Revision = rev
			}
		} else {
			meta, err := b.GetEtcdMetadata(ctx, key, rev)
			if err != nil {
				return nil, err
			}
			if meta.CreateRevision == rev && meta.Version == 1 {
				event.Type = proto.Event_CREATE
			}
		}
		events = append(events, event)
	}

	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Revision == events[j].Revision {
			return bytes.Compare(events[i].Kv.Key, events[j].Kv.Key) < 0
		}
		return events[i].Revision < events[j].Revision
	})
	return events, nil
}

func (b *backend) processEvents(cancel context.CancelFunc, out chan<- []*proto.Event, in <-chan []*proto.Event,
	prefix string, revision uint64) {
	prefixBytes := []byte(prefix)
	klog.InfoS("start process events chan", "prefix", prefix, "revision", revision)

	// always ensure we fully read the channel
	for events := range in {
		evs := filterByPrefix(filterByRevision(events, revision), prefixBytes)
		if len(evs) > 0 {
			out <- evs
		}
	}
	// channel closed by watcher hub due to slow process or ctx done
	klog.InfoS("events chan closed", "chan", in, "prefix", prefix)
	b.metricCli.EmitCounter("watcherhub.events_chan.closed", 1, metrics.Tag("prefix", prefix))

	close(out)
	klog.InfoS("watch channel closed", "prefix", prefix)
	cancel()
}

func filterByPrefix(events []*proto.Event, prefix []byte) []*proto.Event {
	filteredEventList := make([]*proto.Event, 0, len(events))

	for _, event := range events {
		if bytes.HasPrefix(event.Kv.Key, prefix) {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList
}

// filter event's ModRevision start from(inclusive) rev
func filterByRevision(events []*proto.Event, rev uint64) []*proto.Event {
	for len(events) > 0 && events[0].Revision < rev {
		events = events[1:]
	}

	return events
}
