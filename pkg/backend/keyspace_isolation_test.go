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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestKeyspaceIsolationOnSharedStorage is the #76 acceptance test: two
// KubeBrain backends with different keyspaces share ONE storage cluster; each
// must see only its own data, and one tenant's compaction (physical GC over
// its whole keyspace) must not touch — let alone delete — the other tenant's
// rows or history.
func TestKeyspaceIsolationOnSharedStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage() // the SHARED storage cluster
	defer func() { require.NoError(t, kv.Close()) }()

	newTenant := func(keyspace, lockPrefix string) *backend {
		// Native (non-etcd-envelope) mode keeps stored values raw so the
		// assertions below compare user bytes directly; isolation semantics
		// are identical in both modes.
		b := NewBackend(kv, Config{
			Prefix:   lockPrefix,
			Keyspace: keyspace,
			Identity: getStorageIdentity(),
		}, m).(*backend)
		b.SetCurrentRevision(uint64(time.Now().UnixNano()))
		return b
	}
	a := newTenant("", "/kubebrain-internal")                 // legacy tenant (e.g. k8s)
	b := newTenant("cilium", "/kubebrain-internal/ks-cilium") // named tenant

	ctx := context.Background()
	key := []byte("/registry/pods/shared-name") // deliberately the SAME user key

	// Native Update is a CAS: creating passes expectedRev 0, updating must
	// carry the current revision.
	put := func(be *backend, val string, expectedRev uint64) uint64 {
		r, err := be.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte(val), Revision: expectedRev}})
		require.NoError(t, err)
		require.True(t, r.Succeeded, "update %q must succeed", val)
		return r.Header.Revision
	}
	get := func(be *backend) (string, bool) {
		r, err := be.Get(ctx, &proto.GetRequest{Key: key})
		require.NoError(t, err)
		if r.Kv == nil || len(r.Kv.Value) == 0 {
			return "", false
		}
		return string(r.Kv.Value), true
	}
	waitCommitted := func(be *backend, rev uint64) {
		require.Eventually(t, func() bool { return be.GetCurrentRevision() >= rev }, 5*time.Second, 2*time.Millisecond)
	}

	// Same user key, different tenants: independent values.
	ra := put(a, "value-a", 0)
	rb1 := put(b, "value-b1", 0)
	waitCommitted(b, rb1)
	rb2 := put(b, "value-b2", rb1)
	waitCommitted(a, ra)
	waitCommitted(b, rb2)

	va, ok := get(a)
	require.True(t, ok)
	require.Equal(t, "value-a", va)
	vb, ok := get(b)
	require.True(t, ok)
	require.Equal(t, "value-b2", vb)

	// Each tenant lists exactly its own single key.
	end := PrefixEnd([]byte("/registry/"))
	for name, be := range map[string]*backend{"a": a, "b": b} {
		resp, err := be.List(ctx, &proto.RangeRequest{Key: []byte("/registry/"), End: end, Revision: 0})
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1, "tenant %s must see exactly its own key", name)
	}

	// Tenant A deletes its key and physically compacts its WHOLE keyspace at
	// head. Tenant B's current value AND its superseded history (value-b1 at
	// rb1) must survive: A's GC borders end at A's keyspace edge.
	_, err := a.Delete(ctx, &proto.DeleteRequest{Key: key})
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, ok := get(a); return !ok }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, a.physicalCompact(ctx, a.GetCurrentRevision()))

	vb, ok = get(b)
	require.True(t, ok, "tenant A's compaction must not delete tenant B's live key")
	require.Equal(t, "value-b2", vb)
	hist, err := b.Get(ctx, &proto.GetRequest{Key: key, Revision: rb1})
	require.NoError(t, err)
	require.NotNil(t, hist.Kv)
	require.Equal(t, "value-b1", string(hist.Kv.Value), "tenant A's GC must not reap tenant B's history")

	// And symmetrically: B compacting at its head must not resurrect or touch
	// A's tombstoned key.
	require.NoError(t, b.physicalCompact(ctx, b.GetCurrentRevision()))
	_, ok = get(a)
	require.False(t, ok)
}
