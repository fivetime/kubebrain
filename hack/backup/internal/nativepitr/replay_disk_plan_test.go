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
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestReplayScratchQuotaAggregatesApparentFileSizes(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	second := filepath.Join(dir, "second")
	require.NoError(t, os.WriteFile(first, []byte("12"), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("345"), 0o600))
	quota, err := newReplayScratchQuota(5)
	require.NoError(t, err)
	require.NoError(t, quota.add(first))
	require.NoError(t, quota.add(second))

	require.NoError(t, os.WriteFile(second, []byte("3456"), 0o600))
	require.ErrorContains(t, quota.check(), "exceed 5-byte limit")
	require.NoError(t, os.Remove(first))
	require.ErrorContains(t, quota.check(), "stat native PITR replay scratch file")
}

func TestReplayScratchQuotaRejectsPendingBytesBeforeFileGrowth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scratch")
	require.NoError(t, os.WriteFile(path, []byte("1234"), 0o600))
	quota, err := newReplayScratchQuota(7)
	require.NoError(t, err)
	require.NoError(t, quota.add(path))
	require.NoError(t, quota.checkAdditional(3))
	require.ErrorContains(t, quota.checkAdditional(4), "plus 4 pending bytes exceed 7-byte limit")
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("1234"), contents)
}

func TestReplayScratchQuotaPreservesFilesystemFreeSpace(t *testing.T) {
	available := uint64(100)
	var probeErr error
	quota, err := newReplayScratchQuotaWithFreeBytes(1<<20, 80, "/scratch", func(path string) (uint64, error) {
		require.Equal(t, "/scratch", path)
		return available, probeErr
	})
	require.NoError(t, err)
	require.NoError(t, quota.checkAdditional(20))
	require.ErrorContains(t, quota.checkAdditional(21), "below 80-byte reserve plus 21 pending bytes")

	probeErr = errors.New("probe failed")
	require.ErrorContains(t, quota.check(), "probe failed")
	probeErr = nil
	quota.minFreeBytes = ^uint64(0)
	require.ErrorContains(t, quota.checkAdditional(1), "reserve overflows uint64")
}

func TestReplayFilesystemFreeBytesReadsScratchFilesystem(t *testing.T) {
	available, err := replayFilesystemFreeBytes(t.TempDir())
	require.NoError(t, err)
	require.Positive(t, available)
	_, err = replayFilesystemFreeBytes(filepath.Join(t.TempDir(), "missing"))
	require.ErrorContains(t, err, "stat native PITR replay scratch filesystem")
}

func TestRestoreReplayScratchErrnoOnlyMapsFixedBboltGrowthErrors(t *testing.T) {
	for _, testCase := range []struct {
		message string
		errno   error
	}{
		{message: "file resize error: truncate /scratch/db: no space left on device", errno: syscall.ENOSPC},
		{message: "file resize error: truncate /scratch/db: file too large", errno: syscall.EFBIG},
		{message: "file sync error: disk quota exceeded", errno: syscall.EDQUOT},
		{message: "file sync error: input/output error", errno: syscall.EIO},
		{message: "file resize error: truncate /scratch/db: read-only file system", errno: syscall.EROFS},
	} {
		t.Run(testCase.errno.Error(), func(t *testing.T) {
			original := errors.New(testCase.message)
			restored := restoreReplayScratchErrno(original)
			require.ErrorIs(t, restored, original)
			require.ErrorIs(t, restored, testCase.errno)
			require.Contains(t, restored.Error(), testCase.message)
		})
	}
	original := errors.New("unrelated subsystem: no space left on device")
	require.Same(t, original, restoreReplayScratchErrno(original))
}

func TestReplayScratchFilesystemReserveRejectsBeforeBboltPut(t *testing.T) {
	available := ^uint64(0)
	quota, err := newReplayScratchQuotaWithFreeBytes(^uint64(0), 100, "/scratch", func(string) (uint64, error) {
		return available, nil
	})
	require.NoError(t, err)
	store, err := newReplayDefaultStoreWithQuota(t.TempDir(), quota)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	info, err := os.Stat(store.path)
	require.NoError(t, err)
	recordBytes := sha256.Size + 4 + uint64(len("join")) + uint64(len("value"))
	available = quota.minFreeBytes + recordBytes - 1

	err = store.Put("join", []byte("value"))
	require.ErrorContains(t, err, "below 100-byte reserve")
	require.Zero(t, store.pendingBytes)
	digest := sha256.Sum256([]byte("join"))
	require.Nil(t, store.bucket.Get(digest[:]))
	after, err := os.Stat(store.path)
	require.NoError(t, err)
	require.Equal(t, info.Size(), after.Size())
}

func TestReplayScratchStoresRejectNewRecordBeforeBboltPut(t *testing.T) {
	t.Run("default CF", func(t *testing.T) {
		quota, err := newReplayScratchQuota(^uint64(0))
		require.NoError(t, err)
		store, err := newReplayDefaultStoreWithQuota(t.TempDir(), quota)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		info, err := os.Stat(store.path)
		require.NoError(t, err)
		quota.maxBytes = uint64(info.Size()) + sha256.Size + 4 + uint64(len("join")) + uint64(len("value")) - 1

		err = store.Put("join", []byte("value"))
		require.ErrorContains(t, err, "pending bytes exceed")
		require.Zero(t, store.pendingBytes)
		digest := sha256.Sum256([]byte("join"))
		require.Nil(t, store.bucket.Get(digest[:]))
		after, err := os.Stat(store.path)
		require.NoError(t, err)
		require.Equal(t, info.Size(), after.Size())
	})

	t.Run("write candidate", func(t *testing.T) {
		quota, err := newReplayScratchQuota(^uint64(0))
		require.NoError(t, err)
		store, err := newReplayCandidateStore(t.TempDir(), quota)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		info, err := os.Stat(store.path)
		require.NoError(t, err)
		quota.maxBytes = uint64(info.Size()) + uint64(len("key")) + uint64(len("short")) + 34 - 1

		err = store.Put(20, 10, []byte("key"), 'P', []byte("short"))
		require.ErrorContains(t, err, "pending bytes exceed")
		require.Zero(t, store.pendingBytes)
		require.Nil(t, store.candidates.Get(replayCandidateKey(20, []byte("key"))))
		after, err := os.Stat(store.path)
		require.NoError(t, err)
		require.Equal(t, info.Size(), after.Size())
	})
}

func TestReplayDiskPlanNumberingReservesBatchBeforeBboltPut(t *testing.T) {
	quota, err := newReplayScratchQuota(^uint64(0))
	require.NoError(t, err)
	plan, err := newReplayDiskPlan(t.TempDir(), quota)
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	mutations := []ReplayMutation{
		{CommitTS: 20, StartTS: 10, Key: []byte("a"), Value: []byte("one")},
		{CommitTS: 30, StartTS: 11, Key: []byte("b"), Value: []byte("two")},
	}
	require.NoError(t, plan.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(replayPlanBucket)
		for _, mutation := range mutations {
			key := replayPlanKey(mutation)
			if err := bucket.Put(key, replayPlanValue(key, mutation, 0)); err != nil {
				return err
			}
		}
		return nil
	}))
	info, err := os.Stat(plan.path)
	require.NoError(t, err)
	firstKey := replayPlanKey(mutations[0])
	firstValue := replayPlanValue(firstKey, mutations[0], 0)
	quota.maxBytes = uint64(info.Size()) + uint64(len(firstKey)+len(firstValue)) - 1

	err = numberReplayDiskPlanWithBatchBytes(plan, replayDefaultStoreBatchBytes)
	require.ErrorContains(t, err, "pending bytes exceed")
	after, err := os.Stat(plan.path)
	require.NoError(t, err)
	require.Equal(t, info.Size(), after.Size())
	require.NoError(t, plan.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(replayPlanBucket)
		for _, mutation := range mutations {
			key := replayPlanKey(mutation)
			_, ordinal, decodeErr := decodeReplayPlanRecord(key, bucket.Get(key))
			require.NoError(t, decodeErr)
			require.Zero(t, ordinal, "failed numbering transaction must not partially persist")
		}
		return nil
	}))
}

func TestReplayScratchDuplicateRecordsDoNotConsumeAnotherReservation(t *testing.T) {
	t.Run("default CF", func(t *testing.T) {
		quota, err := newReplayScratchQuota(^uint64(0))
		require.NoError(t, err)
		store, err := newReplayDefaultStoreWithQuota(t.TempDir(), quota)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		require.NoError(t, store.Put("join", []byte("value")))
		pending := store.pendingBytes
		quota.maxBytes = 1
		require.NoError(t, store.Put("join", []byte("value")))
		require.Equal(t, pending, store.pendingBytes)
	})

	t.Run("write candidate", func(t *testing.T) {
		quota, err := newReplayScratchQuota(^uint64(0))
		require.NoError(t, err)
		store, err := newReplayCandidateStore(t.TempDir(), quota)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		require.NoError(t, store.Put(20, 10, []byte("key"), 'P', []byte("short")))
		pending := store.pendingBytes
		quota.maxBytes = 1
		require.NoError(t, store.Put(20, 10, []byte("key"), 'P', []byte("short")))
		require.Equal(t, pending, store.pendingBytes)
	})
}

func TestMaterializeReplayDiskPlanRejectsScratchQuotaAndCleansFiles(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	scratch := t.TempDir()
	_, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, scratch, 119, 150, 1)
	require.ErrorContains(t, err, "scratch files exceed 1-byte limit")
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestMaterializeReplayDiskPlanRejectsFilesystemReserveBeforeScratchCreation(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	scratch := t.TempDir()
	_, err := MaterializeReplayDiskPlanWithScratchLimits(receipt, digest, root, scratch, 119, 150, ^uint64(0), ^uint64(0))
	require.ErrorContains(t, err, "scratch filesystem has")
	require.ErrorContains(t, err, "below 18446744073709551615-byte reserve")
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestMaterializeReplayDiskPlanCleansAllScratchFilesAfterMidBuildENOSPC(t *testing.T) {
	for _, failPattern := range []string{"-defaults-", "-plan-"} {
		t.Run(failPattern, func(t *testing.T) {
			_, receipt, root, _, _ := replayFixtureWithDefaultValue(t, make([]byte, 1<<20))
			scratch := t.TempDir()
			baseline := make(map[string]uint64)
			observedTargetGrowth := false
			freeBytes := func(path string) (uint64, error) {
				require.Equal(t, scratch, path)
				entries, err := os.ReadDir(path)
				require.NoError(t, err)
				if len(entries) != 3 {
					return ^uint64(0), nil
				}
				for _, entry := range entries {
					info, infoErr := entry.Info()
					require.NoError(t, infoErr)
					require.False(t, info.IsDir())
					current := uint64(info.Size())
					previous, found := baseline[entry.Name()]
					baseline[entry.Name()] = current
					if found && current > previous && strings.Contains(entry.Name(), failPattern) {
						observedTargetGrowth = true
						return 0, syscall.ENOSPC
					}
				}
				return ^uint64(0), nil
			}

			_, err := materializeReplayDiskPlanWithScratchQuotaFactory(receipt, digest, root, scratch, 119, 150, func() (*replayScratchQuota, error) {
				return newReplayScratchQuotaWithFreeBytes(^uint64(0), 1, scratch, freeBytes)
			})
			require.ErrorIs(t, err, syscall.ENOSPC)
			require.ErrorContains(t, err, "no space left on device")
			require.True(t, observedTargetGrowth, "ENOSPC must follow real growth of %s scratch", failPattern)
			entries, readErr := os.ReadDir(scratch)
			require.NoError(t, readErr)
			require.Empty(t, entries, "default, candidate, and plan scratch databases must all be removed")
		})
	}
}

func TestMaterializeReplayDiskPlanValidatesEvidenceBeforeScratchQuotaFactory(t *testing.T) {
	called := false
	_, err := materializeReplayDiskPlanWithScratchQuotaFactory(LogArtifactReceipt{}, digest, t.TempDir(), t.TempDir(), 119, 150, func() (*replayScratchQuota, error) {
		called = true
		return nil, errors.New("scratch quota factory must not be called")
	})
	require.Error(t, err)
	require.False(t, called)
}

func TestMaterializeReplayDiskPlanCleansScratchAfterKernelFileSizeFailure(t *testing.T) {
	const childEnv = "KUBEBRAIN_NATIVE_PITR_RLIMIT_FSIZE_CHILD"
	if os.Getenv(childEnv) == "" {
		command := exec.Command(os.Args[0], "-test.run=^TestMaterializeReplayDiskPlanCleansScratchAfterKernelFileSizeFailure$")
		command.Env = append(os.Environ(), childEnv+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	_, receipt, root, _, _ := replayFixtureWithDefaultValue(t, make([]byte, 1<<20))
	scratch := t.TempDir()
	var original syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original))
	require.GreaterOrEqual(t, original.Max, uint64(64<<10))
	limited := original
	limited.Cur = 64 << 10
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited))
	defer func() { require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original)) }()
	signal.Ignore(syscall.SIGXFSZ)
	defer signal.Reset(syscall.SIGXFSZ)

	_, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, scratch, 119, 150, ^uint64(0))
	require.ErrorIs(t, err, syscall.EFBIG)
	require.ErrorContains(t, err, "file too large")
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries, "a kernel bbolt growth failure must remove every scratch database")
}

type changeReplayPlanOnGetStore struct {
	storage.KvStorage
	plan    *ReplayDiskPlan
	change  func(*bolt.Bucket) error
	mutated bool
	err     error
}

func (s *changeReplayPlanOnGetStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if !s.mutated {
		s.mutated = true
		s.err = s.plan.db.Update(func(tx *bolt.Tx) error {
			return s.change(tx.Bucket(replayPlanBucket))
		})
	}
	return s.KvStorage.Get(ctx, key)
}

func TestMaterializeReplayDiskPlanMatchesCanonicalMemoryPlan(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	wantManifest, wantMutations, err := MaterializeReplay(receipt, digest, root, 119, 150)
	require.NoError(t, err)
	scratch := t.TempDir()

	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, scratch, 119, 150, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, wantManifest, plan.Manifest())
	var got []ReplayMutation
	require.NoError(t, plan.forEach(func(_ int, mutation ReplayMutation) error {
		got = append(got, mutation)
		return nil
	}))
	require.Equal(t, wantMutations, got)
	require.NoError(t, numberReplayDiskPlanWithBatchBytes(plan, 1), "numbering must resume exactly across bounded write transactions")
	require.NoError(t, plan.forEach(func(index int, mutation ReplayMutation) error {
		require.Equal(t, wantMutations[index], mutation)
		return nil
	}))
	planPath := plan.path
	require.NoError(t, plan.Close())
	_, err = os.Stat(planPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	entries, err := os.ReadDir(scratch)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestReplayDiskPlanManifestIsAnImmutableSnapshot(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()

	want := plan.Manifest()
	changed := plan.Manifest()
	changed.Format = "weakened"
	changed.MutationCount++
	changed.MutationsSHA256 = ""

	require.Equal(t, want, plan.Manifest())
	inner := memkv.NewKvStorage()
	defer inner.Close()
	_, err = ApplyReplayDiskPlan(t.Context(), inner, digest, plan, 1<<20)
	require.NoError(t, err)
}

func TestMaterializeAndApplyEmptyReplayDiskPlanMatchesNilDigestContract(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	wantManifest, wantMutations, err := MaterializeReplay(receipt, digest, root, 119, 125)
	require.NoError(t, err)
	require.Nil(t, wantMutations)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 125, ^uint64(0))
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	require.Equal(t, wantManifest, plan.Manifest())
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
	quota, err := newReplayScratchQuota(^uint64(0))
	require.NoError(t, err)
	store, err := newReplayCandidateStore(t.TempDir(), quota)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.Put(20, 10, []byte("a"), 'P', []byte("value")))
	require.NoError(t, store.Put(20, 10, []byte("a"), 'P', []byte("value")))
	require.EqualError(t, store.Put(20, 10, []byte("a"), 'P', []byte("changed")), "stream log contains conflicting write-CF entries")
	require.EqualError(t, store.Put(21, 10, []byte("b"), 'P', []byte("value")), "source transaction has multiple commit TSOs")
}

func TestReplayDiskPlanDoesNotExposeMmapMemory(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
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
	decoded, err := decodeReplayPlanMutation(key, replayPlanValue(key, putEmpty, 7))
	require.NoError(t, err)
	require.Equal(t, putEmpty, decoded)
	require.NotNil(t, decoded.Value)

	deleteMutation := ReplayMutation{CommitTS: 20, StartTS: 10, Key: []byte("key"), Delete: true}
	decoded, err = decodeReplayPlanMutation(key, replayPlanValue(key, deleteMutation, 7))
	require.NoError(t, err)
	require.Equal(t, deleteMutation, decoded)

	changedKey := append([]byte(nil), key...)
	changedKey[len(changedKey)-1] ^= 0xff
	_, err = decodeReplayPlanMutation(changedKey, replayPlanValue(key, putEmpty, 7))
	require.ErrorContains(t, err, "checksum")
}

func TestApplyReplayDiskPlanBoundsTransactionBeforeTargetAccessAndResumes(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
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
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
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
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
	require.NoError(t, err)
	defer func() { require.NoError(t, plan.Close()) }()
	inner := memkv.NewKvStorage()
	defer inner.Close()
	counted := &countingReplayStore{KvStorage: inner}
	target := &changeReplayPlanOnGetStore{KvStorage: counted, plan: plan, change: func(bucket *bolt.Bucket) error {
		planKey, value := bucket.Cursor().First()
		changed := append([]byte(nil), value...)
		changed[len(changed)-1] ^= 0xff
		return bucket.Put(planKey, changed)
	}}

	_, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1<<20)
	require.NoError(t, target.err)
	require.ErrorContains(t, err, "checksum")
	require.Zero(t, counted.commits, "a post-preflight scratch bit flip must not enter a target transaction")
}

func TestApplyReplayDiskPlanRejectsSequenceChangeAfterPreflight(t *testing.T) {
	tests := []struct {
		name   string
		change func(*bolt.Bucket) error
		match  string
	}{
		{name: "middle deletion", match: "ordinal", change: func(bucket *bolt.Bucket) error {
			cursor := bucket.Cursor()
			key, _ := cursor.First()
			key, _ = cursor.Next()
			return bucket.Delete(key)
		}},
		{name: "tail deletion", match: "count", change: func(bucket *bolt.Bucket) error {
			key, _ := bucket.Cursor().Last()
			return bucket.Delete(key)
		}},
		{name: "insertion", match: "ordinal", change: func(bucket *bolt.Bucket) error {
			key, value := bucket.Cursor().First()
			mutation, err := decodeReplayPlanMutation(key, value)
			if err != nil {
				return err
			}
			mutation.Key = append(mutation.Key, 0)
			insertedKey := replayPlanKey(mutation)
			return bucket.Put(insertedKey, replayPlanValue(insertedKey, mutation, 1))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, receipt, root, _, _ := replayFixture(t)
			plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
			require.NoError(t, err)
			defer func() { require.NoError(t, plan.Close()) }()
			inner := memkv.NewKvStorage()
			defer inner.Close()
			counted := &countingReplayStore{KvStorage: inner}
			target := &changeReplayPlanOnGetStore{KvStorage: counted, plan: plan, change: test.change}

			_, err = ApplyReplayDiskPlan(t.Context(), target, digest, plan, 1<<20)
			require.NoError(t, target.err)
			require.ErrorContains(t, err, test.match)
			require.Zero(t, counted.commits, "a post-preflight sequence change must not enter a target transaction")
		})
	}
}

func TestApplyReplayDiskPlanResolvesCommittedUncertainCheckpoint(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	plan, err := MaterializeReplayDiskPlanWithScratchDir(receipt, digest, root, t.TempDir(), 119, 150, ^uint64(0))
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
