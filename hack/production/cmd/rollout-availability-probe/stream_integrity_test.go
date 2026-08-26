// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rangeReceiveStep struct {
	response *etcdserverpb.RangeStreamResponse
	err      error
}

type fakeRangeReceiver struct {
	steps []rangeReceiveStep
	next  int
}

func (f *fakeRangeReceiver) Recv() (*etcdserverpb.RangeStreamResponse, error) {
	if f.next >= len(f.steps) {
		return nil, io.EOF
	}
	step := f.steps[f.next]
	f.next++
	return step.response, step.err
}

type snapshotReceiveStep struct {
	response *etcdserverpb.SnapshotResponse
	err      error
}

type fakeSnapshotReceiver struct {
	steps []snapshotReceiveStep
	next  int
}

func (f *fakeSnapshotReceiver) Recv() (*etcdserverpb.SnapshotResponse, error) {
	if f.next >= len(f.steps) {
		return nil, io.EOF
	}
	step := f.steps[f.next]
	f.next++
	return step.response, step.err
}

func TestConsumeRangeStreamValidatesEveryFrameAndSeed(t *testing.T) {
	expected := newStreamProbeExpectations("/probe/")
	keys := sortedExpectedKeys(expected)
	toKV := func(key string, revision int64) *mvccpb.KeyValue {
		for _, item := range expected {
			if item.key == key {
				return &mvccpb.KeyValue{Key: []byte(key), Value: []byte(item.value), CreateRevision: revision, ModRevision: revision, Version: 1}
			}
		}
		t.Fatal("missing expectation", key)
		return nil
	}
	first := make([]*mvccpb.KeyValue, 0, 10)
	second := make([]*mvccpb.KeyValue, 0, len(keys)-10)
	for index, key := range keys {
		kv := toKV(key, int64(index+1))
		if index < 10 {
			first = append(first, kv)
		} else {
			second = append(second, kv)
		}
	}
	receiver := &fakeRangeReceiver{steps: []rangeReceiveStep{
		{response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Kvs: first}}},
		{response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{
			Header: &etcdserverpb.ResponseHeader{ClusterId: 7, MemberId: 9, Revision: 20},
			Kvs:    second, Count: int64(len(expected)),
		}}},
	}}
	partial, err := consumeRangeStream(receiver, "/probe/", expected, 7)
	require.NoError(t, err)
	require.True(t, partial)
}

func TestConsumeRangeStreamRejectsPartialAndMalformedResults(t *testing.T) {
	expected := newStreamProbeExpectations("/probe/")
	first := expected[0]
	validKV := &mvccpb.KeyValue{Key: []byte(first.key), Value: []byte(first.value), CreateRevision: 1, ModRevision: 1, Version: 1}
	partial, err := consumeRangeStream(&fakeRangeReceiver{steps: []rangeReceiveStep{
		{response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{Kvs: []*mvccpb.KeyValue{validKV}}}},
		{err: status.Error(codes.Unavailable, "rolled")},
	}}, "/probe/", expected, 7)
	require.Error(t, err)
	require.True(t, partial)
	require.Equal(t, codes.Unavailable, status.Code(err))

	badOrder := &fakeRangeReceiver{steps: []rangeReceiveStep{{response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{
		Kvs: []*mvccpb.KeyValue{validKV, validKV},
	}}}}}
	_, err = consumeRangeStream(badOrder, "/probe/", expected, 7)
	require.ErrorContains(t, err, "strictly increasing")

	wrongTerminal := &fakeRangeReceiver{steps: []rangeReceiveStep{{response: &etcdserverpb.RangeStreamResponse{RangeResponse: &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 8, MemberId: 9, Revision: 1}, Count: 0,
	}}}}}
	_, err = consumeRangeStream(wrongTerminal, "/probe/", nil, 7)
	require.ErrorContains(t, err, "invalid terminal")
}

func TestConsumeSnapshotValidatesRemainingBytesVersionAndChecksum(t *testing.T) {
	first := []byte("abc")
	second := []byte("defg")
	digest := sha256.Sum256(append(append([]byte(nil), first...), second...))
	receiver := &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: first, RemainingBytes: uint64(len(second)), Version: "3.6.0"}},
		{response: &etcdserverpb.SnapshotResponse{Blob: second, RemainingBytes: 0, Version: "3.6.0"}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: "3.6.0"}},
	}}
	partial, err := consumeSnapshot(receiver)
	require.NoError(t, err)
	require.True(t, partial)

	emptyVersionDigest := sha256.Sum256(first)
	_, err = consumeSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: first, RemainingBytes: 0}},
		{response: &etcdserverpb.SnapshotResponse{Blob: emptyVersionDigest[:], RemainingBytes: 0}},
	}})
	require.NoError(t, err, "upstream permits an empty storage version before schema initialization")
}

func TestConsumeSnapshotRejectsPartialAndCorruptResults(t *testing.T) {
	partial, err := consumeSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: []byte("abc"), RemainingBytes: 0, Version: "3.6.0"}},
		{err: status.Error(codes.Unavailable, "rolled")},
	}})
	require.Error(t, err)
	require.True(t, partial)
	require.Equal(t, codes.Unavailable, status.Code(err))

	_, err = consumeSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: []byte("abc"), RemainingBytes: 2, Version: "3.6.0"}},
		{response: &etcdserverpb.SnapshotResponse{Blob: []byte("d"), RemainingBytes: 0, Version: "3.6.0"}},
	}})
	require.ErrorContains(t, err, "discontinuous")

	_, err = consumeSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: []byte("abc"), RemainingBytes: 0, Version: "3.6.0"}},
		{response: &etcdserverpb.SnapshotResponse{Blob: make([]byte, sha256.Size), RemainingBytes: 0, Version: "3.6.0"}},
	}})
	require.ErrorContains(t, err, "checksum")

	digest := sha256.Sum256([]byte("abc"))
	_, err = consumeSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: []byte("abc"), RemainingBytes: 0}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: "3.6.0"}},
	}})
	require.ErrorContains(t, err, "changed storage version")
}

func TestRunStreamWorkerRetriesWithBackoffAndDiscardsPartialAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int64
	var success atomic.Int64
	counters := &streamProbeCounters{}
	err := runStreamWorker(ctx, streamWorkerConfig{
		interval: time.Millisecond, attemptTimeout: time.Second,
		retryBackoff: time.Millisecond, maxBackoff: 2 * time.Millisecond, successLimit: 1,
	}, &success, counters, func(context.Context) (bool, error) {
		switch attempts.Add(1) {
		case 1:
			return false, status.Error(codes.Unavailable, "before first frame")
		case 2:
			return true, status.Error(codes.Unavailable, "after first frame")
		default:
			return false, nil
		}
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), attempts.Load())
	require.Equal(t, int64(1), success.Load())
	require.Equal(t, int64(2), counters.retries.Load())
	require.Equal(t, int64(1), counters.partialRetries.Load())
}

func TestRunStreamWorkerDoesNotRetryProtocolFailureOrCallerCancellation(t *testing.T) {
	counters := &streamProbeCounters{}
	var success atomic.Int64
	want := errors.New("checksum mismatch")
	err := runStreamWorker(context.Background(), streamWorkerConfig{
		interval: time.Millisecond, attemptTimeout: time.Second,
		retryBackoff: time.Millisecond, maxBackoff: time.Millisecond,
	}, &success, counters, func(context.Context) (bool, error) {
		return true, want
	})
	require.ErrorIs(t, err, want)
	require.Zero(t, counters.retries.Load())

	ctx, cancel := context.WithCancel(context.Background())
	err = runStreamWorker(ctx, streamWorkerConfig{
		interval: time.Millisecond, attemptTimeout: time.Second,
		retryBackoff: time.Millisecond, maxBackoff: time.Millisecond,
	}, &success, counters, func(context.Context) (bool, error) {
		cancel()
		return false, context.Canceled
	})
	require.NoError(t, err)
	require.Zero(t, counters.retries.Load())
}

func TestStreamProbeWaitForMinimumIsBoundedAndSurfacesFatalError(t *testing.T) {
	group := &streamProbeGroup{}
	go func() {
		time.Sleep(2 * time.Millisecond)
		group.counters.rangeOK.Store(1)
		group.counters.snapshotOK.Store(1)
	}()
	require.NoError(t, group.waitForMinimum(context.Background(), time.Second))

	empty := &streamProbeGroup{}
	err := empty.waitForMinimum(context.Background(), time.Millisecond)
	require.ErrorContains(t, err, "range=0 snapshot=0")

	failing := &streamProbeGroup{fatalErr: errors.New("corrupt stream")}
	require.ErrorContains(t, failing.waitForMinimum(context.Background(), time.Second), "corrupt stream")
}
