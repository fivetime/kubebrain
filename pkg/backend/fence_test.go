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
	"context"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// newFenceTestBackend builds a memkv-backed *backend for fence unit tests.
func newFenceTestBackend(t *testing.T) (*backend, storage.KvStorage, func()) {
	ctrl := gomock.NewController(t)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	closer := func() {
		ctrl.Finish()
		_ = kv.Close()
	}
	return b, kv, closer
}

// TestFenceAdmitRejectsOnEpochChange verifies that when the leadership epoch at
// commit time differs from the epoch the write was admitted under, fenceAdmit
// rejects with ErrLeadershipFenced (so the caller never opens the batch).
func TestFenceAdmitRejectsOnEpochChange(t *testing.T) {
	ast := assert.New(t)
	b, _, closer := newFenceTestBackend(t)
	defer closer()

	// admitted under epoch 7, but the node is now in epoch 8 (a successor bumped it)
	b.SetLeadershipFence(func() (uint64, bool) { return 8, true })
	ctx := WithLeadershipEpoch(context.Background(), 7)

	ast.ErrorIs(b.fenceAdmit(ctx), ErrLeadershipFenced)
}

// TestFenceAdmitRejectsWhenNotLeadingFresh verifies that even at the same epoch,
// a node that is no longer safely leading (leader flag cleared or lease freshness
// lapsed) is fenced.
func TestFenceAdmitRejectsWhenNotLeadingFresh(t *testing.T) {
	ast := assert.New(t)
	b, _, closer := newFenceTestBackend(t)
	defer closer()

	b.SetLeadershipFence(func() (uint64, bool) { return 7, false })
	ctx := WithLeadershipEpoch(context.Background(), 7)

	ast.ErrorIs(b.fenceAdmit(ctx), ErrLeadershipFenced)
}

// TestFenceAdmitPassesWhenEpochMatches verifies the happy path: the admit-time
// epoch still matches and the node is still leading, so the write is admitted.
func TestFenceAdmitPassesWhenEpochMatches(t *testing.T) {
	ast := assert.New(t)
	b, _, closer := newFenceTestBackend(t)
	defer closer()

	b.SetLeadershipFence(func() (uint64, bool) { return 7, true })
	ctx := WithLeadershipEpoch(context.Background(), 7)

	ast.NoError(b.fenceAdmit(ctx))
}

// TestFenceAdmitFailOpenWithoutFence verifies that a backend with no registered
// fence (single-node / direct-constructed test backend) admits unconditionally,
// so existing single-node behavior is unchanged.
func TestFenceAdmitFailOpenWithoutFence(t *testing.T) {
	ast := assert.New(t)
	b, _, closer := newFenceTestBackend(t)
	defer closer()

	// no SetLeadershipFence call: fenceFn is nil
	ctx := WithLeadershipEpoch(context.Background(), 7)
	ast.NoError(b.fenceAdmit(ctx))
}

// TestFenceAdmitFailOpenWithoutEpoch verifies that a write carrying no admit-time
// epoch (e.g. an internal/background write) is admitted even when a fence is
// registered — the fence only governs writes admitted through a gated write RPC.
func TestFenceAdmitFailOpenWithoutEpoch(t *testing.T) {
	ast := assert.New(t)
	b, _, closer := newFenceTestBackend(t)
	defer closer()

	// fence would reject if it were consulted, but there is no epoch in the ctx
	b.SetLeadershipFence(func() (uint64, bool) { return 8, false })
	ast.NoError(b.fenceAdmit(context.Background()))
}

// TestFencedCreateRejectsAndCollectorAdvances is the end-to-end regression probe
// for FINDING #39 at the backend layer: a fenced Create must (a) return
// ErrLeadershipFenced, (b) leave nothing in storage, and critically (c) still let
// the event collector advance past the revision the fenced write consumed — the
// per-op notify(...,err) fills an invalid ring slot so the collector never stalls.
// A subsequent unfenced Create then commits and its event is emitted normally.
func TestFencedCreateRejectsAndCollectorAdvances(t *testing.T) {
	ast := assert.New(t)
	b, kv, closer := newFenceTestBackend(t)
	defer closer()

	initRevision := uint64(1000)
	b.SetCurrentRevision(initRevision)
	waitUntilRevisionEqualOrTimeout(b, initRevision)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output, err := b.Watch(ctx, prefix+"/", 0)
	ast.NoError(err)

	// Fence rejects: emulate a leadership change between admit and commit.
	b.SetLeadershipFence(func() (uint64, bool) { return 42, false })
	admitCtx := WithLeadershipEpoch(context.Background(), 41)

	fencedKey := path.Join(prefix, "fence/create-rejected")
	_, err = b.Create(admitCtx, newCreateRequest(fencedKey, testVal))
	ast.ErrorIs(err, ErrLeadershipFenced)

	// Nothing persisted for the fenced key.
	_, gerr := kv.Get(context.Background(), b.coder.EncodeRevisionKey([]byte(fencedKey)))
	ast.ErrorIs(gerr, storage.ErrKeyNotFound)

	// The collector must advance past the consumed revision (initRevision+1)
	// rather than stalling on it, so leadership resumes cleanly.
	waitUntilRevisionEqualOrTimeout(b, initRevision+1)
	ast.Equal(initRevision+1, b.GetCurrentRevision())

	// Now leadership is healthy again: an unfenced create commits and emits.
	b.SetLeadershipFence(func() (uint64, bool) { return 42, true })
	okCtx := WithLeadershipEpoch(context.Background(), 42)
	okKey := path.Join(prefix, "fence/create-ok")
	resp, err := b.Create(okCtx, newCreateRequest(okKey, testVal))
	ast.NoError(err)
	ast.True(resp.Succeeded)
	ast.Equal(initRevision+2, resp.Header.Revision)

	select {
	case events := <-output:
		ast.NotEmpty(events)
		ast.Equal(initRevision+2, events[len(events)-1].Revision)
	case <-time.After(timeout):
		ast.FailNow("expected watch event for the committed create")
	}
}
