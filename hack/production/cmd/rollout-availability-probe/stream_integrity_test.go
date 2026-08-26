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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/etcdsnapshot"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdutlsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
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

type partialRestoreSnapshotManager struct {
	restoreConfig etcdutlsnapshot.RestoreConfig
}

func (*partialRestoreSnapshotManager) Status(string) (etcdutlsnapshot.Status, error) {
	return etcdutlsnapshot.Status{Revision: 7, TotalSize: 4096, Version: etcdsnapshot.StorageVersion}, nil
}

func (m *partialRestoreSnapshotManager) Restore(cfg etcdutlsnapshot.RestoreConfig) error {
	m.restoreConfig = cfg
	if err := os.MkdirAll(cfg.OutputDataDir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(cfg.OutputDataDir, "partial"), []byte("incomplete restore"), 0o600); err != nil {
		return err
	}
	return errors.New("restore rejected snapshot schema")
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

func TestConsumeAndValidateSnapshotRejectsSelfConsistentNonEtcdArtifact(t *testing.T) {
	data := []byte("self-consistent but not a bbolt snapshot")
	digest := sha256.Sum256(data)
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir)
	require.ErrorContains(t, err, "official etcdutl rejected Snapshot artifact")
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a rejected artifact must always be removed")
}

func TestConsumeAndValidateSnapshotAcceptsOfficialRestorableArtifact(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, etcdsnapshot.State{Revision: 7, Records: []etcdsnapshot.Record{{
		Key: []byte("key"), Value: []byte("value"), CreateRevision: 7, ModRevision: 7, Version: 1,
	}}}))
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data[:len(data)/2], RemainingBytes: uint64(len(data) - len(data)/2), Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: data[len(data)/2:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir)
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated artifact must always be removed")

	partial, err = consumeAndValidateSnapshot(&fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: "3.6.0"}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: "3.6.0"}},
	}}, dir)
	require.ErrorContains(t, err, "official etcdutl returned invalid Snapshot status")
	require.True(t, partial)
}

func TestValidateSnapshotArtifactRejectsRestoreFailureAndRemovesPartialOutput(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "artifact.db")
	require.NoError(t, os.WriteFile(artifactPath, []byte("managed by fake status"), 0o600))
	manager := &partialRestoreSnapshotManager{}

	err := validateSnapshotArtifact(manager, artifactPath, etcdsnapshot.StorageVersion, dir)
	require.ErrorContains(t, err, "official etcdutl failed to restore Snapshot artifact")
	require.Equal(t, artifactPath, manager.restoreConfig.SnapshotPath)
	require.False(t, manager.restoreConfig.SkipHashCheck)
	require.NotEmpty(t, manager.restoreConfig.OutputDataDir)
	require.NoDirExists(t, filepath.Dir(manager.restoreConfig.OutputDataDir), "partial restore output must always be removed")
	require.FileExists(t, artifactPath, "the caller owns the source artifact lifecycle")
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
