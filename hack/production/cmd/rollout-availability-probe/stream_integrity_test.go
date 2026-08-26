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
	"crypto/x509"
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
	"go.etcd.io/etcd/client/pkg/v3/transport"
	etcdutlsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"
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

type completeRestoreSnapshotManager struct {
	restoreConfig etcdutlsnapshot.RestoreConfig
}

type recordingRestoreSnapshotManager struct {
	restoreConfigs []etcdutlsnapshot.RestoreConfig
	failAt         int
}

func (*completeRestoreSnapshotManager) Status(string) (etcdutlsnapshot.Status, error) {
	return etcdutlsnapshot.Status{Revision: 7, TotalSize: 4096, Version: etcdsnapshot.StorageVersion}, nil
}

func (m *completeRestoreSnapshotManager) Restore(cfg etcdutlsnapshot.RestoreConfig) error {
	m.restoreConfig = cfg
	dbPath := filepath.Join(cfg.OutputDataDir, "member", "snap", "db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return err
	}
	return os.WriteFile(dbPath, []byte("non-empty restored db"), 0o600)
}

func (*recordingRestoreSnapshotManager) Status(string) (etcdutlsnapshot.Status, error) {
	return etcdutlsnapshot.Status{Revision: 7, TotalSize: 4096, Version: etcdsnapshot.StorageVersion}, nil
}

func (m *recordingRestoreSnapshotManager) Restore(cfg etcdutlsnapshot.RestoreConfig) error {
	m.restoreConfigs = append(m.restoreConfigs, cfg)
	dbPath := filepath.Join(cfg.OutputDataDir, "member", "snap", "db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return err
	}
	if m.failAt == len(m.restoreConfigs) {
		return errors.New("injected member restore failure")
	}
	return os.WriteFile(dbPath, []byte("non-empty restored db"), 0o600)
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
	partial, err := consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir, nil, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "official etcdutl rejected Snapshot artifact")
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a rejected artifact must always be removed")
}

func TestConsumeAndValidateSnapshotStartsOfficialServerAndValidatesSeedData(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, etcdsnapshot.State{Revision: 9, PreserveHistory: true, Records: []etcdsnapshot.Record{{
		Key: []byte("key"), Value: []byte("value"), CreateRevision: 7, ModRevision: 7, Version: 1,
	}}}))
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	expected := []streamProbeExpectation{{key: "key", value: "value", hash: sha256.Sum256([]byte("value")), revision: 7}}
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data[:len(data)/2], RemainingBytes: uint64(len(data) - len(data)/2), Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: data[len(data)/2:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir, expected, restoredSnapshotTLSConfig{})
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated artifact must always be removed")

	identity, err := transport.SelfCert(zap.NewNop(), t.TempDir(), []string{"restored.example:443"}, 1, x509.ExtKeyUsageClientAuth)
	require.NoError(t, err)
	partial, err = consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir, expected, restoredSnapshotTLSConfig{
		caFile: identity.CertFile, certFile: identity.CertFile, keyFile: identity.KeyFile, serverName: "restored.example",
	})
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a mutually authenticated validation must always be removed")

	wrongExpected := append([]streamProbeExpectation(nil), expected...)
	wrongExpected[0].hash = sha256.Sum256([]byte("wrong value"))
	partial, err = consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir, wrongExpected, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "officially restored etcd returned invalid historical seed data")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a semantic validation failure must always be removed")

	wrongExpected = append([]streamProbeExpectation(nil), expected...)
	wrongExpected[0].revision = 8
	partial, err = consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir, wrongExpected, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "historical seed")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a historical validation failure must always be removed")

	partial, err = consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: "3.6.0"}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: "3.6.0"}},
	}}, dir, expected, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "official etcdutl returned invalid Snapshot status")
	require.True(t, partial)
}

func TestConsumeAndValidateSnapshotRejectsUnexpectedHistoricalWatchEvent(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, etcdsnapshot.State{
		Revision: 10, PreserveHistory: true,
		Records: []etcdsnapshot.Record{
			{Key: []byte("probe/a"), Value: []byte("a"), CreateRevision: 7, ModRevision: 7, Version: 1},
			{Key: []byte("probe/b"), Value: []byte("b"), CreateRevision: 8, ModRevision: 8, Version: 1},
			{Key: []byte("probe/b"), ModRevision: 9, Tombstone: true},
			{Key: []byte("probe/c"), Value: []byte("c"), CreateRevision: 10, ModRevision: 10, Version: 1},
		},
	}))
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	expected := []streamProbeExpectation{
		{key: "probe/a", value: "a", hash: sha256.Sum256([]byte("a")), revision: 7},
		{key: "probe/c", value: "c", hash: sha256.Sum256([]byte("c")), revision: 10},
	}
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(t.Context(), &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}, dir, expected, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "historical seed Watch returned unexpected key")
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a historical Watch validation failure must always be removed")
}

func TestConsumeAndValidateSnapshotValidatesUpdateDeleteRecreateHistory(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, etcdsnapshot.State{
		Revision: 10, PreserveHistory: true,
		Records: []etcdsnapshot.Record{
			{Key: []byte("probe/history"), Value: []byte("v1"), CreateRevision: 7, ModRevision: 7, Version: 1},
			{Key: []byte("probe/history"), Value: []byte("v2"), CreateRevision: 7, ModRevision: 8, Version: 2},
			{Key: []byte("probe/history"), ModRevision: 9, Tombstone: true},
			{Key: []byte("probe/history"), Value: []byte("v3"), CreateRevision: 10, ModRevision: 10, Version: 1},
		},
	}))
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	expected := []streamProbeExpectation{{
		key: "probe/history", value: "v3", hash: sha256.Sum256([]byte("v3")), revision: 10,
		events: []streamProbeEventExpectation{
			{eventType: mvccpb.PUT, value: "v1", hash: sha256.Sum256([]byte("v1")), revision: 7, createRevision: 7, version: 1},
			{eventType: mvccpb.PUT, value: "v2", hash: sha256.Sum256([]byte("v2")), revision: 8, createRevision: 7, version: 2},
			{eventType: mvccpb.DELETE, revision: 9},
			{eventType: mvccpb.PUT, value: "v3", hash: sha256.Sum256([]byte("v3")), revision: 10, createRevision: 10, version: 1},
		},
	}}
	receiver := func() *fakeSnapshotReceiver {
		return &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
			{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
			{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		}}
	}
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(t.Context(), receiver(), dir, expected, restoredSnapshotTLSConfig{})
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated historical artifact must always be removed")

	expected[0].events[1].hash = sha256.Sum256([]byte("wrong v2"))
	partial, err = consumeAndValidateSnapshot(t.Context(), receiver(), dir, expected, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "historical seed")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a rejected historical artifact must always be removed")
}

func TestConsumeAndValidateSnapshotValidatesMultipleHistoricalLeaseStates(t *testing.T) {
	expected := []streamProbeExpectation{{
		key: "probe/lease-history", value: "v4", hash: sha256.Sum256([]byte("v4")), revision: 11,
		events: []streamProbeEventExpectation{
			{eventType: mvccpb.PUT, value: "v1", hash: sha256.Sum256([]byte("v1")), revision: 7, createRevision: 7, version: 1},
			{eventType: mvccpb.PUT, value: "v2", hash: sha256.Sum256([]byte("v2")), revision: 8, createRevision: 7, version: 2, lease: 17, leaseGrantedTTL: 60},
			{eventType: mvccpb.PUT, value: "v3", hash: sha256.Sum256([]byte("v3")), revision: 9, createRevision: 7, version: 3, lease: 23, leaseGrantedTTL: 90},
			{eventType: mvccpb.DELETE, revision: 10},
			{eventType: mvccpb.PUT, value: "v4", hash: sha256.Sum256([]byte("v4")), revision: 11, createRevision: 11, version: 1},
		},
	}}
	state := etcdsnapshot.State{
		Revision: 11, PreserveHistory: true,
		Records: []etcdsnapshot.Record{
			{Key: []byte("probe/lease-history"), Value: []byte("v1"), CreateRevision: 7, ModRevision: 7, Version: 1},
			{Key: []byte("probe/lease-history"), Value: []byte("v2"), CreateRevision: 7, ModRevision: 8, Version: 2, Lease: 17},
			{Key: []byte("probe/lease-history"), Value: []byte("v3"), CreateRevision: 7, ModRevision: 9, Version: 3, Lease: 23},
			{Key: []byte("probe/lease-history"), ModRevision: 10, Tombstone: true},
			{Key: []byte("probe/lease-history"), Value: []byte("v4"), CreateRevision: 11, ModRevision: 11, Version: 1},
		},
		Leases: []etcdsnapshot.Lease{
			{ID: 17, GrantedTTL: 60, RemainingTTL: 29},
			{ID: 23, GrantedTTL: 90, RemainingTTL: 47},
		},
	}
	receiver := func(t *testing.T, state etcdsnapshot.State) *fakeSnapshotReceiver {
		t.Helper()
		sourcePath := filepath.Join(t.TempDir(), "source.db")
		require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, state))
		data, err := os.ReadFile(sourcePath)
		require.NoError(t, err)
		digest := sha256.Sum256(data)
		return &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
			{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
			{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		}}
	}
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(t.Context(), receiver(t, state), dir, expected, restoredSnapshotTLSConfig{})
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated lease artifact must always be removed")

	currentlyLeasedState := state
	currentlyLeasedState.Revision = 9
	currentlyLeasedState.Records = append([]etcdsnapshot.Record(nil), state.Records[:3]...)
	currentlyLeasedExpected := append([]streamProbeExpectation(nil), expected...)
	currentlyLeasedExpected[0].value = "v3"
	currentlyLeasedExpected[0].hash = sha256.Sum256([]byte("v3"))
	currentlyLeasedExpected[0].revision = 9
	currentlyLeasedExpected[0].events = append([]streamProbeEventExpectation(nil), expected[0].events[:3]...)
	partial, err = consumeAndValidateSnapshot(
		t.Context(), receiver(t, currentlyLeasedState), dir, currentlyLeasedExpected, restoredSnapshotTLSConfig{},
	)
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated currently leased artifact must always be removed")

	for name, testCase := range map[string]struct {
		state    etcdsnapshot.State
		expected []streamProbeExpectation
		message  string
	}{
		"missing second lease": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Leases = append([]etcdsnapshot.Lease(nil), state.Leases[:1]...)
				return copy
			}(),
			expected: expected, message: "historical lease",
		},
		"wrong second granted ttl": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Leases = append([]etcdsnapshot.Lease(nil), state.Leases...)
				copy.Leases[1].GrantedTTL++
				return copy
			}(),
			expected: expected, message: "invalid historical lease state",
		},
		"unexpected current attachment": {
			state: func() etcdsnapshot.State {
				copy := state
				copy.Revision = 12
				copy.Records = append(append([]etcdsnapshot.Record(nil), state.Records...), etcdsnapshot.Record{
					Key: []byte("probe/unexpected"), Value: []byte("attached"), CreateRevision: 12, ModRevision: 12, Version: 1, Lease: 17,
				})
				return copy
			}(),
			expected: expected, message: "invalid attached key count",
		},
		"missing granted ttl expectation": {
			state: state,
			expected: func() []streamProbeExpectation {
				copy := append([]streamProbeExpectation(nil), expected...)
				copy[0].events = append([]streamProbeEventExpectation(nil), expected[0].events...)
				copy[0].events[1].leaseGrantedTTL = 0
				return copy
			}(),
			message: "missing a granted TTL",
		},
		"inconsistent granted ttl expectation": {
			state: state,
			expected: func() []streamProbeExpectation {
				copy := append([]streamProbeExpectation(nil), expected...)
				copy[0].events = append([]streamProbeEventExpectation(nil), expected[0].events...)
				copy[0].events[2].lease = 17
				return copy
			}(),
			message: "inconsistent restored historical lease granted TTL expectation",
		},
	} {
		t.Run(name, func(t *testing.T) {
			partial, err := consumeAndValidateSnapshot(t.Context(), receiver(t, testCase.state), dir, testCase.expected, restoredSnapshotTLSConfig{})
			require.ErrorContains(t, err, testCase.message)
			require.True(t, partial)
			entries, readErr := os.ReadDir(dir)
			require.NoError(t, readErr)
			require.Empty(t, entries, "a rejected multiple-lease artifact must always be removed")
		})
	}
}

func TestConsumeAndValidateSnapshotValidatesTxnSubrevisionOrder(t *testing.T) {
	state := etcdsnapshot.State{
		Revision: 8, PreserveHistory: true,
		Records: []etcdsnapshot.Record{
			{Key: []byte("probe/z"), Value: []byte("z1"), CreateRevision: 4, ModRevision: 4, Version: 1},
			{Key: []byte("probe/a"), Value: []byte("a1"), CreateRevision: 5, ModRevision: 5, Version: 1},
			{Key: []byte("probe/m"), Value: []byte("m1"), CreateRevision: 6, ModRevision: 6, Version: 1},
			{Key: []byte("probe/z"), Value: []byte("z2"), CreateRevision: 4, ModRevision: 7, Version: 2,
				SubRevision: 0, TotalChanges: 3, Ordered: true},
			{Key: []byte("probe/a"), ModRevision: 7, Tombstone: true,
				SubRevision: 1, TotalChanges: 3, Ordered: true},
			{Key: []byte("probe/m"), Value: []byte("m2"), CreateRevision: 6, ModRevision: 7, Version: 2,
				SubRevision: 2, TotalChanges: 3, Ordered: true},
			{Key: []byte("probe/a"), Value: []byte("a3"), CreateRevision: 8, ModRevision: 8, Version: 1},
		},
	}
	expected := []streamProbeExpectation{
		{key: "probe/z", value: "z2", hash: sha256.Sum256([]byte("z2")), revision: 7, events: []streamProbeEventExpectation{
			{eventType: mvccpb.PUT, value: "z1", hash: sha256.Sum256([]byte("z1")), revision: 4, createRevision: 4, version: 1},
			{eventType: mvccpb.PUT, value: "z2", hash: sha256.Sum256([]byte("z2")), revision: 7, subRevision: 0, totalChanges: 3, createRevision: 4, version: 2},
		}},
		{key: "probe/a", value: "a3", hash: sha256.Sum256([]byte("a3")), revision: 8, events: []streamProbeEventExpectation{
			{eventType: mvccpb.PUT, value: "a1", hash: sha256.Sum256([]byte("a1")), revision: 5, createRevision: 5, version: 1},
			{eventType: mvccpb.DELETE, revision: 7, subRevision: 1, totalChanges: 3},
			{eventType: mvccpb.PUT, value: "a3", hash: sha256.Sum256([]byte("a3")), revision: 8, createRevision: 8, version: 1},
		}},
		{key: "probe/m", value: "m2", hash: sha256.Sum256([]byte("m2")), revision: 7, events: []streamProbeEventExpectation{
			{eventType: mvccpb.PUT, value: "m1", hash: sha256.Sum256([]byte("m1")), revision: 6, createRevision: 6, version: 1},
			{eventType: mvccpb.PUT, value: "m2", hash: sha256.Sum256([]byte("m2")), revision: 7, subRevision: 2, totalChanges: 3, createRevision: 6, version: 2},
		}},
	}
	receiver := func() *fakeSnapshotReceiver {
		sourcePath := filepath.Join(t.TempDir(), "source.db")
		require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, state))
		data, err := os.ReadFile(sourcePath)
		require.NoError(t, err)
		digest := sha256.Sum256(data)
		return &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
			{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
			{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		}}
	}
	dir := t.TempDir()
	partial, err := consumeAndValidateSnapshot(t.Context(), receiver(), dir, expected, restoredSnapshotTLSConfig{})
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a validated subrevision artifact must always be removed")

	wrongOrder := append([]streamProbeExpectation(nil), expected...)
	for index := range wrongOrder {
		wrongOrder[index].events = append([]streamProbeEventExpectation(nil), expected[index].events...)
	}
	wrongOrder[0].events[1].subRevision = 1
	wrongOrder[1].events[1].subRevision = 0
	partial, err = consumeAndValidateSnapshot(t.Context(), receiver(), dir, wrongOrder, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "historical seed Watch returned unexpected key")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a rejected subrevision artifact must always be removed")

	incomplete := append([]streamProbeExpectation(nil), expected...)
	for index := range incomplete {
		incomplete[index].events = append([]streamProbeEventExpectation(nil), expected[index].events...)
	}
	incomplete[2].events[1].totalChanges = 4
	partial, err = consumeAndValidateSnapshot(t.Context(), receiver(), dir, incomplete, restoredSnapshotTLSConfig{})
	require.ErrorContains(t, err, "incomplete restored seed event order expectation")
	require.True(t, partial)
	entries, readErr = os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "an artifact with incomplete subrevision expectations must always be removed")
}

func TestStreamProbeTxnSeedsUseNonLexicographicOrder(t *testing.T) {
	expected := newStreamProbeExpectations("probe/")
	seeds, err := streamProbeTxnSeeds(expected)
	require.NoError(t, err)
	require.Equal(t, []string{"probe/stream/0004", "probe/stream/0002", "probe/stream/0003"},
		[]string{seeds[0].key, seeds[1].key, seeds[2].key})
	require.NotEqual(t, []string{"probe/stream/0002", "probe/stream/0003", "probe/stream/0004"},
		[]string{seeds[0].key, seeds[1].key, seeds[2].key}, "operation order must differ from key order")

	_, err = streamProbeTxnSeeds(expected[:4])
	require.ErrorContains(t, err, "requires at least 5 stream seeds")
}

func TestStreamProbeNestedTxnSeedsUseDistinctNonLexicographicWrites(t *testing.T) {
	expected := newStreamProbeExpectations("probe/")
	seeds, err := streamProbeNestedTxnSeeds(expected)
	require.NoError(t, err)
	require.Equal(t, "probe/stream/0006", seeds.compare.key)
	require.Equal(t, []string{"probe/stream/0009", "probe/stream/0007", "probe/stream/0008"},
		[]string{seeds.outerPut.key, seeds.deleted.key, seeds.innerPut.key})
	require.NotEqual(t, []string{"probe/stream/0007", "probe/stream/0008", "probe/stream/0009"},
		[]string{seeds.outerPut.key, seeds.deleted.key, seeds.innerPut.key})

	_, err = streamProbeNestedTxnSeeds(expected[:9])
	require.ErrorContains(t, err, "requires at least 10 stream seeds")
}

func TestStreamProbeMultilevelTxnSeedsUseDistinctNonLexicographicWrites(t *testing.T) {
	expected := newStreamProbeExpectations("probe/")
	seeds, err := streamProbeMultilevelTxnSeeds(expected)
	require.NoError(t, err)
	require.Equal(t, []string{"probe/stream/0010", "probe/stream/0011"},
		[]string{seeds.outerCompare.key, seeds.innerCompare.key})
	require.Equal(t, []string{"probe/stream/0015", "probe/stream/0012", "probe/stream/0014", "probe/stream/0013"},
		[]string{seeds.outerPut.key, seeds.middlePut.key, seeds.deleted.key, seeds.innerPut.key})
	require.NotEqual(t, []string{"probe/stream/0012", "probe/stream/0013", "probe/stream/0014", "probe/stream/0015"},
		[]string{seeds.outerPut.key, seeds.middlePut.key, seeds.deleted.key, seeds.innerPut.key})

	_, err = streamProbeMultilevelTxnSeeds(expected[:15])
	require.ErrorContains(t, err, "requires at least 16 stream seeds")
}

func TestValidateSnapshotArtifactRejectsRestoreFailureAndRemovesPartialOutput(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "artifact.db")
	require.NoError(t, os.WriteFile(artifactPath, []byte("managed by fake status"), 0o600))
	manager := &partialRestoreSnapshotManager{}

	err := validateSnapshotArtifactWithVerifier(t.Context(), manager, artifactPath, etcdsnapshot.StorageVersion, dir, nil, restoredSnapshotTLSConfig{}, verifyRestoredSnapshot)
	require.ErrorContains(t, err, "official etcdutl failed to restore Snapshot artifact")
	require.Equal(t, artifactPath, manager.restoreConfig.SnapshotPath)
	require.False(t, manager.restoreConfig.SkipHashCheck)
	require.NotEmpty(t, manager.restoreConfig.OutputDataDir)
	require.NoDirExists(t, filepath.Dir(manager.restoreConfig.OutputDataDir), "partial restore output must always be removed")
	require.FileExists(t, artifactPath, "the caller owns the source artifact lifecycle")
}

func TestValidateSnapshotArtifactRejectsRestoredServerFailureAndRemovesOutput(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "artifact.db")
	require.NoError(t, os.WriteFile(artifactPath, []byte("managed by fake status"), 0o600))
	manager := &completeRestoreSnapshotManager{}
	expected := newStreamProbeExpectations("/probe/")[:1]
	verified := false

	err := validateSnapshotArtifactWithVerifier(t.Context(), manager, artifactPath, etcdsnapshot.StorageVersion, dir, expected, restoredSnapshotTLSConfig{},
		func(_ context.Context, cfg restoredSnapshotConfig, got []streamProbeExpectation, revision int64) error {
			verified = true
			require.Len(t, cfg.members, 1)
			require.Equal(t, manager.restoreConfig.OutputDataDir, cfg.members[0].dataDir)
			require.Equal(t, expected, got)
			require.Equal(t, int64(7), revision)
			return errors.New("restored etcd failed to start")
		})
	require.ErrorContains(t, err, "official restored etcd validation failed")
	require.True(t, verified)
	require.NoDirExists(t, filepath.Dir(manager.restoreConfig.OutputDataDir), "failed server validation output must always be removed")
	require.FileExists(t, artifactPath, "the caller owns the source artifact lifecycle")
}

func TestValidateSnapshotArtifactRestoresThreeMemberClusterContract(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "artifact.db")
	require.NoError(t, os.WriteFile(artifactPath, []byte("managed by fake status"), 0o600))
	manager := &recordingRestoreSnapshotManager{}
	var restoredCfg restoredSnapshotConfig

	err := validateSnapshotArtifactWithClusterAuthVerifier(t.Context(), manager, artifactPath, etcdsnapshot.StorageVersion,
		dir, nil, restoredSnapshotTLSConfig{}, nil, 3,
		func(_ context.Context, cfg restoredSnapshotConfig, _ []streamProbeExpectation, revision int64) error {
			restoredCfg = cfg
			require.Equal(t, int64(7), revision)
			for _, member := range cfg.members {
				require.FileExists(t, filepath.Join(member.dataDir, "member", "snap", "db"))
			}
			return nil
		})
	require.NoError(t, err)
	require.Len(t, restoredCfg.members, 3)
	require.Len(t, manager.restoreConfigs, 3)
	names := make(map[string]struct{}, 3)
	dataDirs := make(map[string]struct{}, 3)
	peerURLs := make(map[string]struct{}, 3)
	for index, restoreCfg := range manager.restoreConfigs {
		require.Equal(t, artifactPath, restoreCfg.SnapshotPath)
		require.Equal(t, restoredCfg.initialCluster, restoreCfg.InitialCluster)
		require.Equal(t, restoredCfg.initialClusterToken, restoreCfg.InitialClusterToken)
		require.False(t, restoreCfg.SkipHashCheck)
		require.Equal(t, restoredCfg.members[index].name, restoreCfg.Name)
		require.Equal(t, restoredCfg.members[index].dataDir, restoreCfg.OutputDataDir)
		require.Equal(t, []string{restoredCfg.members[index].peerURL.String()}, restoreCfg.PeerURLs)
		names[restoreCfg.Name] = struct{}{}
		dataDirs[restoreCfg.OutputDataDir] = struct{}{}
		peerURLs[restoreCfg.PeerURLs[0]] = struct{}{}
	}
	require.Len(t, names, 3)
	require.Len(t, dataDirs, 3)
	require.Len(t, peerURLs, 3)
	require.NoDirExists(t, filepath.Dir(manager.restoreConfigs[0].OutputDataDir), "all restored member data must be removed")
	require.FileExists(t, artifactPath, "the caller owns the source artifact lifecycle")
}

func TestValidateSnapshotArtifactRemovesAllMembersAfterLaterRestoreFailure(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "artifact.db")
	require.NoError(t, os.WriteFile(artifactPath, []byte("managed by fake status"), 0o600))
	manager := &recordingRestoreSnapshotManager{failAt: 2}
	verified := false

	err := validateSnapshotArtifactWithClusterAuthVerifier(t.Context(), manager, artifactPath, etcdsnapshot.StorageVersion,
		dir, nil, restoredSnapshotTLSConfig{}, nil, 3,
		func(context.Context, restoredSnapshotConfig, []streamProbeExpectation, int64) error {
			verified = true
			return nil
		})
	require.ErrorContains(t, err, "injected member restore failure")
	require.Len(t, manager.restoreConfigs, 2)
	require.False(t, verified)
	require.NoDirExists(t, filepath.Dir(manager.restoreConfigs[0].OutputDataDir), "partial multi-member restore must be removed")
	require.FileExists(t, artifactPath, "the caller owns the source artifact lifecycle")
}

func TestConsumeAndValidateSnapshotValidatesThreeMemberRestoreReplicationAndQuorum(t *testing.T) {
	const (
		key      = "probe/three-member"
		value    = "restored-through-official-etcd"
		revision = int64(7)
	)
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	require.NoError(t, etcdsnapshot.WriteBackend(sourcePath, etcdsnapshot.State{
		Revision: revision, PreserveHistory: true,
		Records: []etcdsnapshot.Record{{Key: []byte(key), Value: []byte(value), CreateRevision: revision, ModRevision: revision, Version: 1}},
	}))
	data, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	digest := sha256.Sum256(data)
	receiver := &fakeSnapshotReceiver{steps: []snapshotReceiveStep{
		{response: &etcdserverpb.SnapshotResponse{Blob: data, RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
		{response: &etcdserverpb.SnapshotResponse{Blob: digest[:], RemainingBytes: 0, Version: etcdsnapshot.StorageVersion}},
	}}
	expected := []streamProbeExpectation{{key: key, value: value, hash: sha256.Sum256([]byte(value)), revision: revision}}
	dir := t.TempDir()
	identity, err := transport.SelfCert(zap.NewNop(), t.TempDir(), []string{"restored-three-member.example:443"}, 1, x509.ExtKeyUsageClientAuth)
	require.NoError(t, err)
	tlsCfg := restoredSnapshotTLSConfig{
		caFile: identity.CertFile, certFile: identity.CertFile, keyFile: identity.KeyFile, serverName: "restored-three-member.example",
	}

	partial, err := consumeAndValidateSnapshotWithClusterAuth(t.Context(), receiver, dir, expected, tlsCfg, nil, 3)
	require.NoError(t, err)
	require.True(t, partial)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "validated artifact and all three restored members must be removed")
}

func TestRestoredSnapshotConfigRejectsInvalidClusterIdentity(t *testing.T) {
	_, err := newRestoredSnapshotConfig(t.TempDir(), 2, restoredSnapshotTLSConfig{}, nil)
	require.ErrorContains(t, err, "one or at least three")
	base, err := newRestoredSnapshotConfig(t.TempDir(), 3, restoredSnapshotTLSConfig{}, nil)
	require.NoError(t, err)
	for name, mutate := range map[string]func(*restoredSnapshotConfig){
		"missing members": func(cfg *restoredSnapshotConfig) { cfg.members = nil },
		"duplicate name":  func(cfg *restoredSnapshotConfig) { cfg.members[1].name = cfg.members[0].name },
		"duplicate data":  func(cfg *restoredSnapshotConfig) { cfg.members[1].dataDir = cfg.members[0].dataDir },
		"duplicate client": func(cfg *restoredSnapshotConfig) {
			cfg.members[1].clientURL = cfg.members[0].clientURL
		},
		"duplicate peer":   func(cfg *restoredSnapshotConfig) { cfg.members[1].peerURL = cfg.members[0].peerURL },
		"cluster mismatch": func(cfg *restoredSnapshotConfig) { cfg.initialCluster += ",unexpected=http://127.0.0.1:1" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.members = append([]restoredSnapshotMemberConfig(nil), base.members...)
			mutate(&cfg)
			require.Error(t, cfg.validate())
		})
	}
}

func TestRestoredSnapshotTLSConfigRejectsPartialIdentity(t *testing.T) {
	require.NoError(t, (restoredSnapshotTLSConfig{}).validate())
	require.NoError(t, (restoredSnapshotTLSConfig{caFile: "ca", certFile: "cert", keyFile: "key", serverName: "server"}).validate())
	for name, cfg := range map[string]restoredSnapshotTLSConfig{
		"ca only":          {caFile: "ca"},
		"certificate only": {certFile: "cert"},
		"key only":         {keyFile: "key"},
		"server name only": {serverName: "server"},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, cfg.validate(), "requires CA, certificate, key, and server name")
		})
	}
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
