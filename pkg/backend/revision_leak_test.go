// Copyright 2026 ByteDance and/or its affiliates
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// These tests pin the invariant that every revision consumed from the TSO
// eventually reaches the event collector, even when the write fails. A dealt
// revision without a ring-buffer event permanently stalls
// collectStorageWriteEvents (it consumes revisions strictly one at a time),
// freezing the committed revision and with it every list and watch.

// waitCommitted asserts the committed revision catches up to rev, i.e. the
// event pipeline is still advancing.
func waitCommitted(t *testing.T, b Backend, rev uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= rev
	}, 5*time.Second, 2*time.Millisecond,
		"committed revision stuck at %d, want >= %d: a dealt revision leaked out of the event pipeline", b.GetCurrentRevision(), rev)
}

func TestUpdateWithHugeClientRevisionDoesNotStallPipeline(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()

	key := []byte(prefix + "/leak/drift/a")
	createResp, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, createResp.Succeeded)
	waitCommitted(t, s.backend, createResp.Header.Revision)

	// A client-supplied mod revision far beyond the TSO makes deal() consume a
	// revision and fail with ErrRevisionDriftBack. The request must fail
	// without freezing the pipeline.
	_, err = s.backend.Update(s.ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key:      key,
		Value:    []byte("v2"),
		Revision: uint64(1) << 62,
	}})
	require.Error(t, err)

	// The pipeline must advance past the leaked revision.
	after, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: []byte(prefix + "/leak/drift/b"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, s.backend, after.Header.Revision)
}

func TestDeleteWithHugeClientRevisionDoesNotStallPipeline(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()

	key := []byte(prefix + "/leak/deldrift/a")
	createResp, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	waitCommitted(t, s.backend, createResp.Header.Revision)

	_, err = s.backend.Delete(s.ctx, &proto.DeleteRequest{
		Key:      key,
		Revision: uint64(1) << 62,
	})
	require.Error(t, err)

	after, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: []byte(prefix + "/leak/deldrift/b"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, s.backend, after.Header.Revision)
}

func TestDeleteRangeCommitConflictDoesNotStallPipeline(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()

	key1 := []byte(prefix + "/leak/dr/a")
	key2 := []byte(prefix + "/leak/dr/b")
	c1, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: key1, Value: []byte("v1")})
	require.NoError(t, err)
	c2, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: key2, Value: []byte("v2")})
	require.NoError(t, err)
	waitCommitted(t, s.backend, c2.Header.Revision)

	// A stale oldRevision for key2 fails the batch CAS: the commit errors after
	// the range's shared revision was already dealt. This is exactly what
	// happens when any key in the range is modified concurrently between the
	// caller's List and the DeleteRange commit.
	_, err = s.backend.DeleteRange(s.ctx, []*proto.KeyValue{
		{Key: key1, Value: []byte("v1"), Revision: c1.Header.Revision},
		{Key: key2, Value: []byte("v2"), Revision: c2.Header.Revision + 12345},
	})
	require.Error(t, err)

	after, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: []byte(prefix + "/leak/dr/c"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, s.backend, after.Header.Revision)
}

// flakyIterKV injects Iter failures for keys containing failSubstr while
// remaining > 0, delegating everything else to the wrapped storage.
type flakyIterKV struct {
	storage.KvStorage
	failSubstr []byte
	remaining  int32
}

func (f *flakyIterKV) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	if atomic.LoadInt32(&f.remaining) > 0 && bytes.Contains(start, f.failSubstr) {
		atomic.AddInt32(&f.remaining, -1)
		return nil, errors.New("injected iter failure")
	}
	return f.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

func TestUpdateMetadataReadFailureDoesNotStallPipeline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	fkv := &flakyIterKV{KvStorage: imemkv.NewKvStorage(), failSubstr: []byte("etcdmeta")}
	b := NewBackend(fkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	defer func() { require.NoError(t, fkv.Close()) }()
	ctx := context.Background()

	key := []byte(prefix + "/leak/meta/a")
	createResp, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	waitCommitted(t, b, createResp.Header.Revision)

	// Fail the etcd-metadata read that update() performs after dealing its
	// revision — a transient storage blip at exactly this point must not stall
	// the pipeline.
	atomic.StoreInt32(&fkv.remaining, 1)
	_, err = b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key:      key,
		Value:    []byte("v2"),
		Revision: createResp.Header.Revision,
	}})
	require.Error(t, err)
	require.Zero(t, atomic.LoadInt32(&fkv.remaining), "injected failure was not consumed by the metadata read")

	after, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/leak/meta/b"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, b, after.Header.Revision)

	// The key must still be updatable once storage recovers.
	updResp, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key:      key,
		Value:    []byte("v3"),
		Revision: createResp.Header.Revision,
	}})
	require.NoError(t, err)
	require.True(t, updResp.Succeeded)
	waitCommitted(t, b, updResp.Header.Revision)
}
