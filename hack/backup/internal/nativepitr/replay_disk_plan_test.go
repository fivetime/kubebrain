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

package nativepitr

import (
	"context"
	"os"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type mutateReplayPlanOnGetStore struct {
	storage.KvStorage
	plan    *ReplayDiskPlan
	mutated bool
	err     error
}

func (s *mutateReplayPlanOnGetStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if !s.mutated {
		s.mutated = true
		s.err = s.plan.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(replayPlanBucket)
			planKey, value := bucket.Cursor().First()
			changed := append([]byte(nil), value...)
			changed[len(changed)-1] ^= 0xff
			return bucket.Put(planKey, changed)
		})
	}
	return s.KvStorage.Get(ctx, key)
}

func TestMaterializeReplayDiskPlanMatchesCanonicalMemoryPlan(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	wantManifest, wantMutations, err := MaterializeReplay(receipt, digest, root, 119, 150)
	require.NoError(t, err)
	scratch := t.TempDir()

	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, scratch, 119, 150)
	require.NoError(t, err)
	require.Equal(t, wantManifest, plan.Manifest)
	var got []ReplayMutation
	require.NoError(t, plan.forEach(func(_ int, mutation ReplayMutation) error {
		got = append(got, mutation)
		return nil
	}))
	require.Equal(t, wantMutations, got)
	planPath := plan.path
	require.NoError(t, plan.Close())
	_, err = os.Stat(planPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	entries, err := os.ReadDir(scratch)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestMaterializeAndApplyEmptyReplayDiskPlanMatchesNilDigestContract(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	wantManifest, wantMutations, err := MaterializeReplay(receipt, digest, root, 119, 125)
	require.NoError(t, err)
	require.Nil(t, wantMutations)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 125)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	require.Equal(t, wantManifest, plan.Manifest)
	inner := memkv.NewKvStorage()
	defer inner.Close()
	target := &countingReplayStore{KvStorage: inner}

	result, err := ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{}, result)
	require.Equal(t, 1, target.commits, "an empty plan must still persist its exact checkpoint")
	result, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{Resumed: true}, result)
	require.Equal(t, 1, target.commits)
}

func TestReplayCandidateStoreRejectsConflictsAndMultipleCommitTS(t *testing.T) {
	store, err := newReplayCandidateStore(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.Put(20, 10, []byte("a"), 'P', []byte("value")))
	require.NoError(t, store.Put(20, 10, []byte("a"), 'P', []byte("value")))
	require.EqualError(t, store.Put(20, 10, []byte("a"), 'P', []byte("changed")), "stream log contains conflicting write-CF entries")
	require.EqualError(t, store.Put(21, 10, []byte("b"), 'P', []byte("value")), "source transaction has multiple commit TSOs")
}

func TestReplayDiskPlanDoesNotExposeMmapMemory(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()

	var first ReplayMutation
	require.NoError(t, plan.forEach(func(index int, mutation ReplayMutation) error {
		if index == 0 {
			first = mutation
		}
		return nil
	}))
	first.Key[0] ^= 0xff
	first.Value[0] ^= 0xff
	require.NoError(t, plan.forEach(func(index int, mutation ReplayMutation) error {
		if index == 0 {
			require.NotEqual(t, first.Key, mutation.Key)
			require.NotEqual(t, first.Value, mutation.Value)
		}
		return nil
	}))
}

func TestReplayDiskPlanRecordChecksumBindsKeyValueAndEmptySemantics(t *testing.T) {
	key := replayPlanKey(ReplayMutation{CommitTS: 20, StartTS: 10, Key: []byte("key")})
	putEmpty := ReplayMutation{CommitTS: 20, StartTS: 10, Key: []byte("key"), Value: []byte{}}
	decoded, err := decodeReplayPlanMutation(key, replayPlanValue(key, putEmpty))
	require.NoError(t, err)
	require.Equal(t, putEmpty, decoded)
	require.NotNil(t, decoded.Value)

	deleteMutation := ReplayMutation{CommitTS: 20, StartTS: 10, Key: []byte("key"), Delete: true}
	decoded, err = decodeReplayPlanMutation(key, replayPlanValue(key, deleteMutation))
	require.NoError(t, err)
	require.Equal(t, deleteMutation, decoded)

	changedKey := append([]byte(nil), key...)
	changedKey[len(changedKey)-1] ^= 0xff
	_, err = decodeReplayPlanMutation(changedKey, replayPlanValue(key, putEmpty))
	require.ErrorContains(t, err, "checksum")
}

func TestApplyReplayDiskPlanBoundsTransactionBeforeTargetAccessAndResumes(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	inner := memkv.NewKvStorage()
	defer inner.Close()
	target := &countingReplayStore{KvStorage: inner}
	maxTransactionBytes := replayMutationResidentOverhead + uint64(len(keyA)+len("long-value"))

	_, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, maxTransactionBytes-1)
	require.ErrorContains(t, err, "source transaction exceeds")
	require.Zero(t, target.commits, "the complete plan must pass its memory gate before target writes")

	result, err := ApplyReplayDiskPlan(t.Context(), target, digest, plan, maxTransactionBytes)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{AppliedMutations: 3, AppliedTransactions: 3, LastCommitTS: 140, LastStartTS: 135}, result)
	require.Equal(t, 3, target.commits)
	_, err = target.Get(t.Context(), keyA)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	value, err := target.Get(t.Context(), keyB)
	require.NoError(t, err)
	require.Equal(t, []byte("short"), value)

	result, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, maxTransactionBytes)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{CheckpointMutationsBefore: 3, CheckpointTransactionsBefore: 3, Resumed: true, LastCommitTS: 140, LastStartTS: 135}, result)
	require.Equal(t, 3, target.commits)
}

func TestApplyReplayDiskPlanRejectsScratchCorruptionBeforeTargetWrite(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	require.NoError(t, plan.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(replayPlanBucket)
		key, value := bucket.Cursor().First()
		changed := append([]byte(nil), value...)
		changed[len(changed)-1] ^= 0xff
		return bucket.Put(key, changed)
	}))
	inner := memkv.NewKvStorage()
	defer inner.Close()
	target := &countingReplayStore{KvStorage: inner}

	_, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1<<20)
	require.ErrorContains(t, err, "checksum")
	require.Zero(t, target.commits)
}

func TestApplyReplayDiskPlanRejectsMutationAfterPreflightBeforeTargetWrite(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	inner := memkv.NewKvStorage()
	defer inner.Close()
	counted := &countingReplayStore{KvStorage: inner}
	target := &mutateReplayPlanOnGetStore{KvStorage: counted, plan: plan}

	_, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1<<20)
	require.NoError(t, target.err)
	require.ErrorContains(t, err, "checksum")
	require.Zero(t, counted.commits, "a post-preflight scratch bit flip must not enter a target transaction")
}

func TestApplyReplayDiskPlanResolvesCommittedUncertainCheckpoint(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	inner := memkv.NewKvStorage()
	defer inner.Close()
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainOnce: true}

	result, err := ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1<<20)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Equal(t, ReplayApplyResult{}, result)
	require.Equal(t, 1, target.commits)
	value, err := target.Get(t.Context(), keyA)
	require.NoError(t, err)
	require.Equal(t, []byte("long-value"), value)

	result, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1<<20)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{CheckpointMutationsBefore: 1, CheckpointTransactionsBefore: 1, AppliedMutations: 2, AppliedTransactions: 2, Resumed: true, LastCommitTS: 140, LastStartTS: 135}, result)
	require.Equal(t, 3, target.commits)
	_, err = target.Get(t.Context(), keyA)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	value, err = target.Get(t.Context(), keyB)
	require.NoError(t, err)
	require.Equal(t, []byte("short"), value)
}
