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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newTxnApplyBackend(t *testing.T) (*backend, context.Context) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	return b, context.Background()
}

func liveValue(t *testing.T, b *backend, ctx context.Context, key []byte) (string, uint64) {
	t.Helper()
	resp, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	if resp.Kv == nil {
		return "", 0
	}
	return string(StripInlineValue(resp.Kv.Value)), resp.Kv.Revision
}

// TestTxnApplySingleRevisionAtomic verifies a mixed put/delete txn applies all
// writes at ONE revision (the core #4 Tier 1 fix) and the state is correct.
func TestTxnApplySingleRevisionAtomic(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)

	// seed: existing key to update, existing key to delete
	upd := []byte(prefix + "/reg/upd")
	del := []byte(prefix + "/reg/del")
	cre := []byte(prefix + "/reg/cre")
	seed := func(k []byte, v string) {
		cr, err := b.Create(ctx, &proto.CreateRequest{Key: k, Value: []byte(v)})
		require.NoError(t, err)
		require.True(t, cr.Succeeded)
	}
	seed(upd, "u0")
	seed(del, "d0")
	waitCommitted(t, b, b.GetCurrentRevision())

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: cre, Value: []byte("c1")}, // create
		{Key: upd, Value: []byte("u1")}, // update
		{Delete: true, Key: del},        // delete
	}, nil)
	require.NoError(t, err)
	require.NotZero(t, rev)
	require.Len(t, results, 3)

	// all ops share the single txn revision
	for _, r := range results {
		require.Equal(t, rev, r.Revision, "op %s must be at the single txn revision", r.Key)
	}
	require.True(t, results[0].Created, "create result")
	require.Equal(t, uint64(1), results[0].Meta.Version)
	require.Equal(t, rev, results[0].Meta.CreateRevision)
	require.False(t, results[1].Created, "update result")
	require.EqualValues(t, 2, results[1].Meta.Version, "update bumps version to 2")
	require.True(t, results[2].Deleted, "delete result")
	require.Equal(t, "d0", string(StripInlineValue(results[2].PrevValue)))

	waitCommitted(t, b, rev)

	// state: create + update present at rev, delete gone
	v, r := liveValue(t, b, ctx, cre)
	require.Equal(t, "c1", v)
	require.Equal(t, rev, r)
	v, r = liveValue(t, b, ctx, upd)
	require.Equal(t, "u1", v)
	require.Equal(t, rev, r)
	v, _ = liveValue(t, b, ctx, del)
	require.Equal(t, "", v, "deleted key must be gone")
}

// TestTxnApplyRecreateOverTombstone verifies a put on a previously-deleted key
// creates it fresh (version resets to 1, new create revision).
func TestTxnApplyRecreateOverTombstone(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/reg/phoenix")

	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	dr, err := b.Delete(ctx, &proto.DeleteRequest{Key: key})
	require.NoError(t, err)
	require.True(t, dr.Succeeded)
	_ = cr
	waitCommitted(t, b, b.GetCurrentRevision())

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("reborn")}}, nil)
	require.NoError(t, err)
	require.True(t, results[0].Created, "put over tombstone must be a create")
	require.EqualValues(t, 1, results[0].Meta.Version)
	require.Equal(t, rev, results[0].Meta.CreateRevision)
	waitCommitted(t, b, rev)
	v, r := liveValue(t, b, ctx, key)
	require.Equal(t, "reborn", v)
	require.Equal(t, rev, r)
}

// TestTxnApplyNoOpDeleteConsumesNoRevision verifies deleting absent keys makes
// no write and does not advance the revision.
func TestTxnApplyNoOpDeleteConsumesNoRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	before := b.GetCurrentRevision()
	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Delete: true, Key: []byte(prefix + "/reg/ghost1")},
		{Delete: true, Key: []byte(prefix + "/reg/ghost2")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.False(t, results[0].Deleted)
	require.Equal(t, before, rev, "no-op txn must not advance the revision")
	require.Equal(t, before, b.GetCurrentRevision())
}

func TestTxnApplyCommitsInternalMetadataAtOnlyUserRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	before := b.GetCurrentRevision()
	userKey := []byte(prefix + "/reg/leased")
	internalKey := []byte("leasekeys/" + string(userKey))

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: userKey, Value: []byte("value"), Lease: 42},
		{Internal: true, Key: internalKey, Value: []byte("42")},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, before+1, rev)
	require.Len(t, results, 2)
	require.True(t, results[0].Created)
	require.False(t, results[1].Created)
	value, err := b.InternalGet(ctx, internalKey)
	require.NoError(t, err)
	require.Equal(t, []byte("42"), value)
	require.Equal(t, rev, b.GetCurrentRevision(), "internal op must not allocate a second revision")
}

// TestTxnApplyContendedSameKeyRetries drives concurrent TxnApply updates to the
// SAME key: each must eventually win via the CAS-retry loop (no lost update, no
// error), and the final version equals the number of writers + 1 (the seed).
func TestTxnApplyContendedSameKeyRetries(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/reg/hot")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v0")})
	require.NoError(t, err)
	require.True(t, cr.Succeeded)
	waitCommitted(t, b, b.GetCurrentRevision())

	const writers = 24
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, e := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte(fmt.Sprintf("v%d", i+1))}}, nil)
			errs[i] = e
		}(i)
	}
	wg.Wait()
	for i := 0; i < writers; i++ {
		require.NoError(t, errs[i], "writer %d", i)
	}
	waitCommitted(t, b, b.GetCurrentRevision())
	// version must have advanced exactly writers times over the seed (v1),
	// proving every contended write applied (serialized), none lost.
	meta, err := b.GetEtcdMetadata(ctx, key, b.GetCurrentRevision())
	require.NoError(t, err)
	require.EqualValues(t, writers+1, meta.Version, "every contended write must apply exactly once")
}

// TestTxnApplyGuard verifies the #4 Tier 2 OCC guard: a txn applies when its
// compare guard's key is unchanged, and fails with ErrTxnGuardConflict (writing
// nothing) once the guarded key has been modified.
func TestTxnApplyGuard(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte(prefix + "/reg/guard")
	a := []byte(prefix + "/reg/ga")
	bk := []byte(prefix + "/reg/gb")

	cr, err := b.Create(ctx, &proto.CreateRequest{Key: guardKey, Value: []byte("g")})
	require.NoError(t, err)
	guardRev := cr.Header.Revision
	waitCommitted(t, b, b.GetCurrentRevision())

	// guard unchanged -> applies
	_, rev, err := b.TxnApply(ctx,
		[]TxnWriteOp{{Key: a, Value: []byte("va")}, {Key: bk, Value: []byte("vb")}},
		[]TxnGuard{{Key: guardKey, Revision: guardRev}})
	require.NoError(t, err)
	require.NotZero(t, rev)
	waitCommitted(t, b, rev)
	v, _ := liveValue(t, b, ctx, a)
	require.Equal(t, "va", v)

	// modify the guarded key
	u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: guardKey, Value: []byte("g2"), Revision: guardRev}})
	require.NoError(t, err)
	require.True(t, u.Succeeded)
	waitCommitted(t, b, b.GetCurrentRevision())

	// guard now stale -> conflict, and a/b are NOT overwritten
	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: a, Value: []byte("va2")}, {Key: bk, Value: []byte("vb2")}},
		[]TxnGuard{{Key: guardKey, Revision: guardRev}})
	require.ErrorIs(t, err, ErrTxnGuardConflict)
	va, _ := liveValue(t, b, ctx, a)
	require.Equal(t, "va", va, "a must not be overwritten when the guard conflicts")
	vb, _ := liveValue(t, b, ctx, bk)
	require.Equal(t, "vb", vb, "b must not be overwritten when the guard conflicts")
}

func TestTxnApplyAbsentGuardOverlappingPut(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/reg/absent-overlap")

	results, _, err := b.TxnApply(ctx,
		[]TxnWriteOp{{Key: key, Value: []byte("created")}},
		[]TxnGuard{{Key: key, Absent: true}})
	require.NoError(t, err)
	require.True(t, results[0].Created)

	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: key, Value: []byte("must-not-overwrite")}},
		[]TxnGuard{{Key: key, Absent: true}})
	require.ErrorIs(t, err, ErrTxnGuardConflict)
	value, _ := liveValue(t, b, ctx, key)
	require.Equal(t, "created", value)
}

func TestTxnApplyAbsentGuardDisjointFromWrite(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte(prefix + "/reg/absent-guard")
	writeKey := []byte(prefix + "/reg/absent-result")

	_, _, err := b.TxnApply(ctx,
		[]TxnWriteOp{{Key: writeKey, Value: []byte("allowed")}},
		[]TxnGuard{{Key: guardKey, Absent: true}})
	require.NoError(t, err)

	created, err := b.Create(ctx, &proto.CreateRequest{Key: guardKey, Value: []byte("now-present")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)

	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: writeKey, Value: []byte("must-not-write")}},
		[]TxnGuard{{Key: guardKey, Absent: true}})
	require.ErrorIs(t, err, ErrTxnGuardConflict)
	value, _ := liveValue(t, b, ctx, writeKey)
	require.Equal(t, "allowed", value)
}

func TestTxnApplyAbsentGuardAcceptsTombstone(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte(prefix + "/reg/tombstoned-guard")
	writeKey := []byte(prefix + "/reg/tombstoned-result")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: guardKey, Value: []byte("old")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: guardKey})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)

	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: writeKey, Value: []byte("allowed")}},
		[]TxnGuard{{Key: guardKey, Absent: true}})
	require.NoError(t, err)
	value, _ := liveValue(t, b, ctx, writeKey)
	require.Equal(t, "allowed", value)
}

// TestTxnApplyConcurrentDistinctKeys stress-tests many concurrent single-key
// TxnApply calls: every write must land, at a unique revision, with no lost
// update or stall.
func TestTxnApplyConcurrentDistinctKeys(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const n = 64
	var wg sync.WaitGroup
	revs := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("%s/reg/c%03d", prefix, i))
			_, rev, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v")}}, nil)
			revs[i] = rev
			errs[i] = err
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.NotZero(t, revs[i])
		require.False(t, seen[revs[i]], "revision %d reused across concurrent txns", revs[i])
		seen[revs[i]] = true
	}
	waitCommitted(t, b, b.GetCurrentRevision())
	for i := 0; i < n; i++ {
		v, _ := liveValue(t, b, ctx, []byte(fmt.Sprintf("%s/reg/c%03d", prefix, i)))
		require.Equal(t, "v", v, "key %d must be present", i)
	}
}
