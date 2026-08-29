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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const slowConsumerWatchID int64 = 1

type rawSlowWatch struct {
	stream     etcdserverpb.Watch_WatchClient
	prefix     string
	eventCount int
	clusterID  uint64
	memberID   uint64
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

func startRawSlowWatch(ctx context.Context, client *clientv3.Client, prefix string, startRevision int64, eventCount int) (*rawSlowWatch, error) {
	stream, err := etcdserverpb.NewWatchClient(client.ActiveConnection()).Watch(ctx)
	if err != nil {
		return nil, err
	}
	closeOnError := func(err error) (*rawSlowWatch, error) {
		_ = stream.CloseSend()
		return nil, err
	}
	request := &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte(clientv3.GetPrefixRangeEnd(prefix)),
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
	return &rawSlowWatch{
		stream: stream, prefix: prefix, eventCount: eventCount,
		clusterID: created.Header.ClusterId, memberID: created.Header.MemberId,
	}, nil
}

func (watch *rawSlowWatch) close() {
	if watch != nil && watch.stream != nil {
		_ = watch.stream.CloseSend()
	}
}

// consume begins only after every write and (when requested) the catch_up
// counter have been observed. Until this call the application performs no Recv,
// so HTTP/2 flow control propagates real backpressure into the server send loop.
func (watch *rawSlowWatch) consume() ([]int64, error) {
	revisions := make([]int64, 0, watch.eventCount)
	for len(revisions) < watch.eventCount {
		response, err := watch.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("raw watch stream closed after %d/%d events", len(revisions), watch.eventCount)
			}
			return nil, fmt.Errorf("receive raw watch response after %d/%d events: %w", len(revisions), watch.eventCount, err)
		}
		if response == nil || response.Header == nil || response.Header.ClusterId != watch.clusterID ||
			response.Header.MemberId != watch.memberID || response.WatchId != slowConsumerWatchID {
			return nil, fmt.Errorf("raw watch response identity mismatch after %d events: %+v", len(revisions), response)
		}
		if response.Created {
			return nil, fmt.Errorf("raw watch returned duplicate Created response after %d events", len(revisions))
		}
		if response.Canceled {
			return nil, fmt.Errorf("raw watch canceled after %d/%d events: reason=%q compact_revision=%d",
				len(revisions), watch.eventCount, response.CancelReason, response.CompactRevision)
		}
		for _, event := range response.Events {
			index := len(revisions)
			if index >= watch.eventCount {
				return nil, fmt.Errorf("raw watch received extra event at index %d", index)
			}
			if err := validateEvent(watch.prefix, index, event); err != nil {
				return nil, err
			}
			revisions = append(revisions, event.Kv.ModRevision)
		}
	}
	return revisions, nil
}
