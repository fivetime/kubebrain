// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const slowConsumerWatchID int64 = 1

type rawSlowWatch struct {
	streamMu          sync.Mutex
	stream            etcdserverpb.Watch_WatchClient
	streamCancel      context.CancelFunc
	ctx               context.Context
	cancel            context.CancelFunc
	openStream        func(context.Context) (etcdserverpb.Watch_WatchClient, error)
	prefix            string
	eventCount        int
	clusterID         uint64
	memberID          uint64
	minimumReconnects int
	reconnects        int
}

// requireDirectLeader prevents a follower proxy (and the client library behind
// it) from absorbing the intended backpressure before it reaches the local
// WatcherHub. A single-member reference etcd also satisfies this contract.
func requireDirectLeader(ctx context.Context, client *clientv3.Client, endpoint string) error {
	status, err := client.Status(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("read direct endpoint status before slow-consumer watch: %w", err)
	}
	return validateDirectLeaderStatus(status)
}

func validateDirectLeaderStatus(status *clientv3.StatusResponse) error {
	if status == nil || status.Header == nil || status.Header.ClusterId == 0 || status.Header.MemberId == 0 || status.Leader == 0 {
		return fmt.Errorf("direct endpoint returned incomplete leader identity: %+v", status)
	}
	if status.Header.MemberId != status.Leader {
		return fmt.Errorf("slow-consumer endpoint must directly serve the leader: member=%x leader=%x",
			status.Header.MemberId, status.Leader)
	}
	return nil
}

func startRawSlowWatch(ctx context.Context, client *clientv3.Client, prefix string, startRevision int64, eventCount,
	minimumReconnects int,
) (*rawSlowWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)
	watch := &rawSlowWatch{
		ctx: watchCtx, cancel: cancel, prefix: prefix, eventCount: eventCount,
		minimumReconnects: minimumReconnects,
		openStream: func(streamCtx context.Context) (etcdserverpb.Watch_WatchClient, error) {
			return etcdserverpb.NewWatchClient(client.ActiveConnection()).Watch(streamCtx)
		},
	}
	if err := watch.open(startRevision, true); err != nil {
		watch.close()
		return nil, err
	}
	return watch, nil
}

func (watch *rawSlowWatch) open(startRevision int64, initial bool) error {
	if watch.openStream == nil || watch.ctx == nil {
		return errors.New("raw watch stream factory and context are required")
	}
	streamCtx, streamCancel := context.WithCancel(watch.ctx)
	stream, err := watch.openStream(streamCtx)
	if err != nil {
		streamCancel()
		return fmt.Errorf("open raw watch stream: %w", err)
	}
	closeOnError := func(err error) error {
		streamCancel()
		_ = stream.CloseSend()
		return err
	}
	request := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key:            []byte(watch.prefix),
			RangeEnd:       []byte(clientv3.GetPrefixRangeEnd(watch.prefix)),
			StartRevision:  startRevision,
			ProgressNotify: true,
			WatchId:        slowConsumerWatchID,
		},
	}}
	if err := stream.Send(request); err != nil {
		return closeOnError(fmt.Errorf("send raw watch create: %w", err))
	}
	created, err := stream.Recv()
	if err != nil {
		return closeOnError(fmt.Errorf("receive raw watch Created response: %w", err))
	}
	if created == nil || !created.Created || created.Canceled || created.WatchId != slowConsumerWatchID ||
		created.Header == nil || created.Header.ClusterId == 0 || created.Header.MemberId == 0 ||
		created.Header.Revision < startRevision-1 || len(created.Events) != 0 {
		return closeOnError(fmt.Errorf("invalid raw watch Created response: %+v", created))
	}
	if !initial && (created.Header.ClusterId != watch.clusterID || created.Header.MemberId != watch.memberID) {
		return closeOnError(fmt.Errorf("raw watch reconnect identity mismatch: cluster=%d member=%d want_cluster=%d want_member=%d",
			created.Header.ClusterId, created.Header.MemberId, watch.clusterID, watch.memberID))
	}
	if initial {
		watch.clusterID, watch.memberID = created.Header.ClusterId, created.Header.MemberId
	}
	watch.replaceStream(stream, streamCancel)
	return nil
}

func (watch *rawSlowWatch) close() {
	if watch == nil {
		return
	}
	if watch.cancel != nil {
		watch.cancel()
	}
	watch.closeStream()
}

func (watch *rawSlowWatch) closeStream() {
	if watch == nil {
		return
	}
	watch.streamMu.Lock()
	stream, streamCancel := watch.stream, watch.streamCancel
	watch.stream, watch.streamCancel = nil, nil
	watch.streamMu.Unlock()
	if streamCancel != nil {
		streamCancel()
	}
	if stream != nil {
		_ = stream.CloseSend()
	}
}

func (watch *rawSlowWatch) replaceStream(stream etcdserverpb.Watch_WatchClient, streamCancel context.CancelFunc) {
	watch.closeStream()
	watch.streamMu.Lock()
	watch.stream, watch.streamCancel = stream, streamCancel
	watch.streamMu.Unlock()
}

func (watch *rawSlowWatch) currentStream() etcdserverpb.Watch_WatchClient {
	watch.streamMu.Lock()
	defer watch.streamMu.Unlock()
	return watch.stream
}

func retryableRawSlowWatchError(err error) bool {
	return errors.Is(err, io.EOF) || status.Code(err) == codes.Unavailable
}

func (watch *rawSlowWatch) reconnect(startRevision int64) error {
	watch.closeStream()
	backoff := 10 * time.Millisecond
	for {
		if err := watch.ctx.Err(); err != nil {
			return err
		}
		err := watch.open(startRevision, false)
		if err == nil {
			watch.reconnects++
			return nil
		}
		if !retryableRawSlowWatchError(err) {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-watch.ctx.Done():
			timer.Stop()
			return watch.ctx.Err()
		case <-timer.C:
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
}

type rawSlowWatchResult struct {
	id  int
	err error
}

// consumeRawSlowWatches releases every independently connected raw watcher at
// the same barrier. The first failure cancels all peer streams, but the helper
// still joins every goroutine before returning so no credential-bearing client
// or Recv remains live beyond the run lifecycle.
func consumeRawSlowWatches(watches []*rawSlowWatch, written []eventObservation, expectedOutcome string) error {
	if len(watches) == 0 {
		return nil
	}
	results := make(chan rawSlowWatchResult, len(watches))
	for id, watch := range watches {
		go func() {
			var err error
			if expectedOutcome == slowConsumerExpectedCompacted {
				err = watch.consumeCompacted(written)
			} else {
				var observed []eventObservation
				observed, err = watch.consume(written)
				if err == nil {
					err = validateObservedEvents(observed, written)
				}
			}
			results <- rawSlowWatchResult{id: id, err: err}
		}()
	}
	var firstErr error
	for range watches {
		result := <-results
		if result.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("raw slow-consumer watcher %d: %w", result.id, result.err)
			for _, watch := range watches {
				watch.close()
			}
		}
	}
	return firstErr
}

func rawSlowWatchReconnectSummary(watches []*rawSlowWatch) (int, int) {
	if len(watches) == 0 {
		return 0, 0
	}
	minimum, total := int(^uint(0)>>1), 0
	for _, watch := range watches {
		if watch == nil {
			return 0, total
		}
		total += watch.reconnects
		minimum = min(minimum, watch.reconnects)
	}
	return minimum, total
}

// consumeCompacted requires an exact, ordered prefix of the written events
// followed by etcd's compacted terminal response. A raw stream may already have
// queued some pre-compaction events, but it must neither skip/reorder them nor
// claim the entire history survived once its generation resume point was
// compacted.
func (watch *rawSlowWatch) consumeCompacted(written []eventObservation) error {
	if len(written) == 0 {
		return errors.New("compacted raw watch requires written events")
	}
	target := written[len(written)-1].revision
	observations := make([]eventObservation, 0, min(watch.eventCount, len(written)))
	for {
		stream := watch.currentStream()
		if stream == nil {
			return fmt.Errorf("compacted raw watch stream is unavailable after %d/%d events", len(observations), watch.eventCount)
		}
		response, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("raw watch stream closed without compacted response after %d/%d events", len(observations), watch.eventCount)
			}
			return fmt.Errorf("receive compacted raw watch response after %d/%d events: %w", len(observations), watch.eventCount, err)
		}
		if response == nil || response.Header == nil || response.Header.ClusterId != watch.clusterID ||
			response.Header.MemberId != watch.memberID || response.WatchId != slowConsumerWatchID {
			return fmt.Errorf("compacted raw watch response identity mismatch after %d events: %+v", len(observations), response)
		}
		if response.Created {
			return fmt.Errorf("compacted raw watch returned duplicate Created response after %d events", len(observations))
		}
		if response.Canceled {
			if len(response.Events) != 0 || response.CancelReason != "" || response.CompactRevision != target {
				return fmt.Errorf("invalid compacted raw watch terminal after %d events: reason=%q compact_revision=%d events=%d want_revision=%d",
					len(observations), response.CancelReason, response.CompactRevision, len(response.Events), target)
			}
			if len(observations) >= len(written) {
				return fmt.Errorf("compacted raw watch delivered all %d events before cancellation", len(observations))
			}
			return validateObservedEventPrefix(observations, written)
		}
		for _, event := range response.Events {
			position := len(observations)
			if position >= len(written) || position >= watch.eventCount {
				return fmt.Errorf("compacted raw watch received extra event at position %d", position)
			}
			observation, observeErr := observeEvent(watch.prefix, (*clientv3.Event)(event))
			if observeErr != nil {
				return fmt.Errorf("compacted raw event position %d: %w", position, observeErr)
			}
			observations = append(observations, observation)
		}
	}
}

func validateObservedEventPrefix(observed, written []eventObservation) error {
	if len(observed) > len(written) {
		return fmt.Errorf("observed event prefix count %d exceeds written count %d", len(observed), len(written))
	}
	for position := range observed {
		if observed[position] != written[position] {
			return fmt.Errorf("event position %d mismatch: got=%+v want=%+v", position, observed[position], written[position])
		}
	}
	return nil
}

// consume begins only after every write and (when requested) the exact catch_up
// pressure checkpoint has been observed. Until this call the application
// performs no Recv, so HTTP/2 flow control propagates real backpressure into
// the server send loop.
func (watch *rawSlowWatch) consume(written []eventObservation) ([]eventObservation, error) {
	if len(written) != watch.eventCount || len(written) == 0 {
		return nil, fmt.Errorf("raw watch requires %d written event observations, got %d", watch.eventCount, len(written))
	}
	observations := make([]eventObservation, 0, watch.eventCount)
	for len(observations) < watch.eventCount {
		stream := watch.currentStream()
		if stream == nil {
			return nil, fmt.Errorf("raw watch stream is unavailable after %d/%d events", len(observations), watch.eventCount)
		}
		response, err := stream.Recv()
		if err != nil {
			if watch.minimumReconnects > 0 && retryableRawSlowWatchError(err) {
				if reconnectErr := watch.reconnect(written[len(observations)].revision); reconnectErr != nil {
					return nil, fmt.Errorf("reopen raw watch after %d/%d events: %w", len(observations), watch.eventCount, reconnectErr)
				}
				continue
			}
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("raw watch stream closed after %d/%d events", len(observations), watch.eventCount)
			}
			return nil, fmt.Errorf("receive raw watch response after %d/%d events: %w", len(observations), watch.eventCount, err)
		}
		if response == nil || response.Header == nil || response.Header.ClusterId != watch.clusterID ||
			response.Header.MemberId != watch.memberID || response.WatchId != slowConsumerWatchID {
			return nil, fmt.Errorf("raw watch response identity mismatch after %d events: %+v", len(observations), response)
		}
		if response.Created {
			return nil, fmt.Errorf("raw watch returned duplicate Created response after %d events", len(observations))
		}
		if response.Canceled {
			return nil, fmt.Errorf("raw watch canceled after %d/%d events: reason=%q compact_revision=%d",
				len(observations), watch.eventCount, response.CancelReason, response.CompactRevision)
		}
		for _, event := range response.Events {
			position := len(observations)
			if position >= watch.eventCount {
				return nil, fmt.Errorf("raw watch received extra event at position %d", position)
			}
			observation, err := observeEvent(watch.prefix, (*clientv3.Event)(event))
			if err != nil {
				return nil, fmt.Errorf("raw event position %d: %w", position, err)
			}
			observations = append(observations, observation)
		}
	}
	if watch.reconnects < watch.minimumReconnects {
		return nil, fmt.Errorf("raw watch observed %d reconnects, require at least %d", watch.reconnects, watch.minimumReconnects)
	}
	return observations, nil
}
