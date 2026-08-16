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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestMaterializeReplayResolvesDefaultAndShortValues(t *testing.T) {
	task, receipt, root, keyA, keyB := replayFixture(t)
	manifest, mutations, err := MaterializeReplay(receipt, digest, root, 119, 150)
	require.NoError(t, err)
	require.Equal(t, 3, manifest.MutationCount)
	require.Equal(t, 3, manifest.TransactionCount)
	require.Equal(t, 2, manifest.PutCount)
	require.Equal(t, 1, manifest.DeleteCount)
	require.Equal(t, uint64(130), manifest.FirstCommitTS)
	require.Equal(t, uint64(140), manifest.LastCommitTS)
	require.Equal(t, task.Keyspace, manifest.Keyspace)
	require.Equal(t, ReplayMutation{CommitTS: 130, StartTS: 120, Key: keyA, Value: []byte("long-value")}, mutations[0])
	require.Equal(t, ReplayMutation{CommitTS: 130, StartTS: 121, Key: keyB, Value: []byte("short")}, mutations[1])
	require.Equal(t, ReplayMutation{CommitTS: 140, StartTS: 135, Key: keyA, Delete: true}, mutations[2])
}

func TestMaterializeReplayResidentLimitIncludesCandidatesAndDefaultValues(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	candidateBytes := 3*replayMutationResidentOverhead + uint64(2*len(keyA)+len(keyB)+len("short"))
	_, _, err := MaterializeReplayWithScratchDirAndMemoryLimit(receipt, digest, root, t.TempDir(), 119, 150, 0)
	require.ErrorContains(t, err, "memory limit must be positive")

	_, _, err = MaterializeReplayWithScratchDirAndMemoryLimit(receipt, digest, root, t.TempDir(), 119, 150, candidateBytes+uint64(len("long-value"))-1)
	require.ErrorContains(t, err, "resident memory limit")

	manifest, mutations, err := MaterializeReplayWithScratchDirAndMemoryLimit(receipt, digest, root, t.TempDir(), 119, 150, candidateBytes+uint64(len("long-value")))
	require.NoError(t, err)
	require.Equal(t, 3, manifest.MutationCount)
	require.Equal(t, []byte("long-value"), mutations[0].Value)
}

type boundedReadRequest struct {
	reader      io.Reader
	max         int
	maxObserved int
}

type boundedWriteRequest struct {
	bytes.Buffer
	max         int
	maxObserved int
}

type countingReplayStore struct {
	storage.KvStorage
	commits int
}

func (s *countingReplayStore) BeginBatchWrite() storage.BatchWrite {
	return &countingReplayBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), store: s}
}

type countingReplayBatch struct {
	storage.BatchWrite
	store *countingReplayStore
}

func (b *countingReplayBatch) Commit(ctx context.Context) error {
	err := b.BatchWrite.Commit(ctx)
	if err == nil {
		b.store.commits++
	}
	return err
}

type commitThenUncertainReplayStore struct {
	storage.KvStorage
	commits                   int
	uncertainOnce             bool
	uncertainAtCommit         int
	uncertainBeforeCommitOnce bool
	cancelAfterCommit         context.CancelFunc
	failReadsAfterCommit      bool
	reconciliationReadsFail   bool
}

func (s *commitThenUncertainReplayStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.reconciliationReadsFail {
		return nil, errors.New("reconciliation read unavailable")
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *commitThenUncertainReplayStore) BeginBatchWrite() storage.BatchWrite {
	return &commitThenUncertainReplayBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), store: s}
}

type commitThenUncertainReplayBatch struct {
	storage.BatchWrite
	store *commitThenUncertainReplayStore
}

func (b *commitThenUncertainReplayBatch) Commit(ctx context.Context) error {
	if b.store.uncertainBeforeCommitOnce {
		b.store.uncertainBeforeCommitOnce = false
		abortErr := errors.New("abort simulated pre-commit request")
		b.BatchWrite.Atomic(func(context.Context, storage.AtomicBatch) error { return abortErr })
		if err := b.BatchWrite.Commit(ctx); !errors.Is(err, abortErr) {
			return fmt.Errorf("abort simulated pre-commit request: %w", err)
		}
		return storage.NewErrUncertainResult(errors.New("request lost before commit"))
	}
	if err := b.BatchWrite.Commit(ctx); err != nil {
		return err
	}
	b.store.commits++
	if b.store.uncertainOnce && (b.store.uncertainAtCommit == 0 || b.store.commits == b.store.uncertainAtCommit) {
		b.store.uncertainOnce = false
		b.store.reconciliationReadsFail = b.store.failReadsAfterCommit
		if b.store.cancelAfterCommit != nil {
			b.store.cancelAfterCommit()
		}
		return storage.NewErrUncertainResult(errors.New("response lost after commit"))
	}
	return nil
}

func (w *boundedWriteRequest) Write(data []byte) (int, error) {
	if len(data) > w.max {
		return 0, errors.New("writer received the complete mutation plan")
	}
	if len(data) > w.maxObserved {
		w.maxObserved = len(data)
	}
	return w.Buffer.Write(data)
}

type shortReplayWriter struct{}

func (shortReplayWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return len(data) - 1, nil
}

func TestReplayMutationStreamingJSONMatchesMarshalExactly(t *testing.T) {
	cases := [][]ReplayMutation{
		nil,
		{},
		{{CommitTS: 1, Key: []byte("key"), Value: []byte("value")}},
		{{CommitTS: 2, Key: []byte{0, 0xff}, Delete: true}, {CommitTS: 3, Key: []byte("empty"), Value: []byte{}}},
	}
	for _, mutations := range cases {
		expected, err := json.Marshal(mutations)
		require.NoError(t, err)
		var output bytes.Buffer
		require.NoError(t, writeReplayMutationsJSON(&output, mutations))
		require.Equal(t, expected, output.Bytes())
		digest := sha256.Sum256(expected)
		got, err := digestReplayMutations(mutations)
		require.NoError(t, err)
		require.Equal(t, hex.EncodeToString(digest[:]), got)
	}
}

func TestReplayMutationJSONDoesNotMaterializeCompletePlan(t *testing.T) {
	mutations := make([]ReplayMutation, 4096)
	for i := range mutations {
		mutations[i] = ReplayMutation{CommitTS: uint64(i + 1), Key: []byte("key"), Value: bytes.Repeat([]byte{'v'}, 128)}
	}
	output := &boundedWriteRequest{max: 512}

	require.NoError(t, writeReplayMutationsJSON(output, mutations))
	require.LessOrEqual(t, output.maxObserved, 512)
	expected, err := json.Marshal(mutations)
	require.NoError(t, err)
	require.Equal(t, expected, output.Bytes())
}

func TestReplayMutationJSONPropagatesShortWrite(t *testing.T) {
	require.ErrorIs(t, writeReplayMutationsJSON(shortReplayWriter{}, []ReplayMutation{}), io.ErrShortWrite)
}

func TestCanonicalizeReplayMutationsReusesCandidateBackingArray(t *testing.T) {
	mutations := []ReplayMutation{
		{CommitTS: 3, StartTS: 2, Key: []byte("b"), Value: []byte("short"), sourceWriteKind: 'P'},
		{CommitTS: 2, StartTS: 1, Key: []byte("a"), Delete: true, sourceWriteKind: 'D'},
		{CommitTS: 3, StartTS: 2, Key: []byte("b"), Value: []byte("short"), sourceWriteKind: 'P'},
	}
	backing := &mutations[0]

	canonical, err := canonicalizeReplayMutations(mutations)

	require.NoError(t, err)
	require.Len(t, canonical, 2)
	require.Same(t, backing, &canonical[0], "canonicalization must not allocate a second mutation array")
	require.Equal(t, uint64(2), canonical[0].CommitTS)
	require.Equal(t, uint64(3), canonical[1].CommitTS)
}

func TestCanonicalizeReplayMutationsRejectsConflictingDuplicate(t *testing.T) {
	base := ReplayMutation{CommitTS: 2, StartTS: 1, Key: []byte("key"), Value: []byte("short"), sourceWriteKind: 'P'}
	for _, conflicting := range []ReplayMutation{
		{CommitTS: 2, StartTS: 9, Key: []byte("key"), Value: []byte("short"), sourceWriteKind: 'P'},
		{CommitTS: 2, StartTS: 1, Key: []byte("key"), Value: []byte("short"), sourceWriteKind: 'D', Delete: true},
		{CommitTS: 2, StartTS: 1, Key: []byte("key"), Value: []byte("different"), sourceWriteKind: 'P'},
	} {
		_, err := canonicalizeReplayMutations([]ReplayMutation{base, conflicting})
		require.EqualError(t, err, "stream log contains conflicting write-CF entries")
	}
	_, err := canonicalizeReplayMutations([]ReplayMutation{
		{CommitTS: 2, StartTS: 1, Key: []byte("empty"), sourceWriteKind: 'P'},
		{CommitTS: 2, StartTS: 1, Key: []byte("empty"), Value: []byte{}, sourceWriteKind: 'P'},
	})
	require.EqualError(t, err, "stream log contains conflicting write-CF entries")
}

func TestCanonicalizeReplayMutationsRejectsMultipleCommitsForStartTS(t *testing.T) {
	_, err := canonicalizeReplayMutations([]ReplayMutation{
		{CommitTS: 2, StartTS: 1, Key: []byte("a"), Value: []byte("one"), sourceWriteKind: 'P'},
		{CommitTS: 3, StartTS: 1, Key: []byte("b"), Value: []byte("two"), sourceWriteKind: 'P'},
	})
	require.EqualError(t, err, "source transaction has multiple commit TSOs")
}

func TestDecodeWriteValuePreservesPresentEmptyShortValue(t *testing.T) {
	kind, startTS, short, err := decodeWriteValue(encodeReplayWrite('P', 123, []byte{}))
	require.NoError(t, err)
	require.Equal(t, byte('P'), kind)
	require.Equal(t, uint64(123), startTS)
	require.NotNil(t, short)
	require.Empty(t, short)
}

func TestDecodeWriteValueAcceptsCanonicalMetadataAndUnknownSuffix(t *testing.T) {
	value := encodeReplayWrite('P', 123, []byte("value"))
	value = append(value, 'R', 'F')
	value = append(value, make([]byte, 8)...)
	value = append(value, 'l')
	value = append(value, make([]byte, 8)...)
	value = append(value, 1, 'S', 2)
	value = append(value, "future-field"...)

	kind, startTS, short, err := decodeWriteValue(value)
	require.NoError(t, err)
	require.Equal(t, byte('P'), kind)
	require.Equal(t, uint64(123), startTS)
	require.Equal(t, []byte("value"), short)
}

func TestDecodeWriteValueRejectsNonCanonicalKnownMetadata(t *testing.T) {
	base := encodeReplayWrite('P', 123, []byte("first"))
	for name, suffix := range map[string][]byte{
		"duplicate short value": {'v', 6, 's', 'e', 'c', 'o', 'n', 'd'},
		"duplicate rollback":    {'R', 'R'},
		"out of order":          {'S', 1, 'R'},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := decodeWriteValue(append(append([]byte(nil), base...), suffix...))
			require.EqualError(t, err, "non-canonical write-CF metadata")
		})
	}
}

func TestMaterializeReplayRejectsNonCanonicalWriteMetadata(t *testing.T) {
	task, _ := readyTask(t)
	ready := readyReceiptFor(task)
	ks, err := coder.NewKeyspace(task.Keyspace)
	require.NoError(t, err)
	key := ks.NewCoder().EncodeRevisionKey([]byte("/ambiguous"))
	write := encodeReplayWrite('P', 120, []byte("first"))
	write = append(write, 'v', 6, 's', 'e', 'c', 'o', 'n', 'd')
	writeData := encodeReplayEntry(encodeReplayMVCCKey(key, 130), write)
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/log"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/backupmeta"), 0o700))
	writeFile := replayDataFile(
		"v1/log/write.log", "write", writeData, task.StartTS, ready.GlobalCheckpointTS, 1,
	)
	require.NoError(t, os.WriteFile(filepath.Join(root, writeFile.Path), writeData, 0o600))
	meta := &backuppb.Metadata{
		MetaVersion: backuppb.MetaVersion_V1, StoreId: 1,
		MinTs: task.StartTS, MaxTs: ready.GlobalCheckpointTS, ResolvedTs: ready.GlobalCheckpointTS,
		Files: []*backuppb.DataFileInfo{writeFile},
	}
	metaBytes, err := meta.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "v1/backupmeta/1.meta"), metaBytes, 0o600))
	inventory := inventoryForMirror(t, task.LogStoragePrefix, root)
	receipt, err := VerifyLogArtifacts(task, digest, ready, digest, inventory, digest, root)
	require.NoError(t, err)

	_, _, err = MaterializeReplay(receipt, digest, root, 119, 150)
	require.EqualError(t, err, "decode write log entry: non-canonical write-CF metadata")
}

func TestMaterializeReplayPreservesEmptyEtcdValue(t *testing.T) {
	task, _ := readyTask(t)
	ready := readyReceiptFor(task)
	ks, err := coder.NewKeyspace(task.Keyspace)
	require.NoError(t, err)
	key := ks.NewCoder().EncodeRevisionKey([]byte("/empty"))
	writeData := encodeReplayEntry(
		encodeReplayMVCCKey(key, 130), encodeReplayWrite('P', 120, []byte{}),
	)
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/log"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/backupmeta"), 0o700))
	writeFile := replayDataFile(
		"v1/log/write.log", "write", writeData, task.StartTS, ready.GlobalCheckpointTS, 1,
	)
	require.NoError(t, os.WriteFile(filepath.Join(root, writeFile.Path), writeData, 0o600))
	meta := &backuppb.Metadata{
		MetaVersion: backuppb.MetaVersion_V1, StoreId: 1,
		MinTs: task.StartTS, MaxTs: ready.GlobalCheckpointTS, ResolvedTs: ready.GlobalCheckpointTS,
		Files: []*backuppb.DataFileInfo{writeFile},
	}
	metaBytes, err := meta.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "v1/backupmeta/1.meta"), metaBytes, 0o600))
	inventory := inventoryForMirror(t, task.LogStoragePrefix, root)
	receipt, err := VerifyLogArtifacts(task, digest, ready, digest, inventory, digest, root)
	require.NoError(t, err)

	manifest, mutations, err := MaterializeReplay(receipt, digest, root, 119, 150)

	require.NoError(t, err)
	require.Equal(t, 1, manifest.PutCount)
	require.Equal(t, []ReplayMutation{{CommitTS: 130, StartTS: 120, Key: key, Value: []byte{}}}, mutations)
	require.NotNil(t, mutations[0].Value)
	target := memkv.NewKvStorage()
	defer target.Close()
	result, err := ApplyReplay(t.Context(), target, digest, manifest, mutations)
	require.NoError(t, err)
	require.Equal(t, 1, result.AppliedMutations)
	value, err := target.Get(t.Context(), key)
	require.NoError(t, err)
	require.Empty(t, value)
}

func (r *boundedReadRequest) Read(data []byte) (int, error) {
	if len(data) > r.max {
		return 0, errors.New("reader was asked to materialize the segment")
	}
	if len(data) > r.maxObserved {
		r.maxObserved = len(data)
	}
	return r.reader.Read(data)
}

func TestWalkReplayEntriesDoesNotMaterializeLargeSegment(t *testing.T) {
	const entries = 32 << 10
	entry := encodeReplayEntry([]byte("key"), []byte("value"))
	data := bytes.Repeat(entry, entries)
	source := &boundedReadRequest{reader: bytes.NewReader(data), max: 64}
	decoded := &countingReplayReader{reader: source}
	visited := 0

	err := walkReplayEntries(decoded, uint64(len(data)), func(_, _ []byte) error {
		visited++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, entries, visited)
	require.LessOrEqual(t, source.maxObserved, 64)
}

func TestWalkReplaySegmentDrainsAndVerifiesAfterVisitorFailure(t *testing.T) {
	data := append(encodeReplayEntry([]byte("first"), []byte("one")),
		encodeReplayEntry([]byte("second"), []byte("two"))...)
	root := t.TempDir()
	name := "segment.log"
	require.NoError(t, os.WriteFile(filepath.Join(root, name), data, 0o600))
	digest := sha256.Sum256(data)
	segment := &backuppb.DataFileInfo{Length: uint64(len(data)), Sha256: digest[:]}
	visitorErr := errors.New("visitor rejected entry")
	visited := 0

	err := walkReplaySegment(root, name, segment, func(_, _ []byte) error {
		visited++
		return visitorErr
	})

	require.ErrorIs(t, err, visitorErr)
	require.Equal(t, 1, visited)

	segment.Sha256 = make([]byte, sha256.Size)
	err = walkReplaySegment(root, name, segment, func(_, _ []byte) error { return visitorErr })
	require.EqualError(t, err, "decoded replay segment digest mismatch")

	segment.Sha256 = digest[:]
	require.NoError(t, os.WriteFile(filepath.Join(root, name), append(data, 'x'), 0o600))
	err = walkReplaySegment(root, name, segment, func(_, _ []byte) error { return nil })
	require.EqualError(t, err, "decoded replay segment length mismatch")
}

func TestWalkReplayEntriesRejectsDeclaredLengthShorterThanHeader(t *testing.T) {
	decoded := &countingReplayReader{reader: bytes.NewReader([]byte{0, 0, 0, 0})}
	require.EqualError(t, walkReplayEntries(decoded, 3, func(_, _ []byte) error { return nil }),
		"invalid log key length")
}

func TestWalkReplaySegmentStreamsCompressedMergedRange(t *testing.T) {
	data := bytes.Repeat(encodeReplayEntry([]byte("key"), []byte("value")), 1024)
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(data, nil)
	encoder.Close()
	root := t.TempDir()
	name := "merged.log"
	require.NoError(t, os.WriteFile(filepath.Join(root, name), compressed, 0o600))
	digest := sha256.Sum256(data)
	segment := &backuppb.DataFileInfo{
		Length: uint64(len(data)), Sha256: digest[:],
		RangeLength: uint64(len(compressed)), CompressionType: backuppb.CompressionType_ZSTD,
	}
	visited := 0

	err = walkReplaySegment(root, name, segment, func(_, _ []byte) error {
		visited++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1024, visited)
}

func TestBuildLogReplayExecutionBindsReplayFenceAndRemainsPreSemantic(t *testing.T) {
	plan := validReceiptPlan(t)
	restore := validFullRestoreExecution()
	restore.PlanSHA256 = digest
	manifest := ReplayManifest{Format: ReplayManifestFormat, LogArtifactReceiptSHA256: plan.Log.ArtifactReceiptSHA, ArtifactManifestSHA256: plan.Log.ArtifactManifest, Keyspace: plan.Source.Keyspace, StartExclusiveTS: plan.Full.BackupTS, RestoreTS: plan.RestoreTS, MutationCount: 1, TransactionCount: 1, PutCount: 1, FirstCommitTS: plan.RestoreTS, LastCommitTS: plan.RestoreTS, MutationsSHA256: digest, AllEntriesInTenantRange: true, AllPutsResolved: true, ExactLocalMirrorRechecked: true}
	now := time.Now().Unix()
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", now-1, false)
	require.NoError(t, err)
	handoff := replayAdmissionHandoff(t, plan, restore, fence, now)
	receipt, err := BuildLogReplayExecution(plan, restore, manifest, fence, digest, handoff, digest, LogReplayExecutionReceipt{PlanSHA256: digest, FullRestoreReceiptSHA256: digest, LogArtifactReceiptSHA256: plan.Log.ArtifactReceiptSHA, RestorationFenceReceiptSHA256: digest, AdmissionHandoffReceiptSHA256: digest, AppliedMutations: 1, AppliedTransactions: 1, LastCommitTS: plan.RestoreTS, LastStartTS: plan.RestoreTS - 1, StartedAtUnix: now, CompletedAtUnix: now})
	require.NoError(t, err)
	require.True(t, receipt.LogReplayCompleted)
	require.True(t, receipt.ReplayWriteFenceProven)
	require.True(t, receipt.TargetWriteFenceProven)
	require.True(t, receipt.ContinuousWriterExclusion)
	require.False(t, receipt.PostRestoreSemanticValidated)
	require.False(t, receipt.PITRComplete)
	require.Equal(t, "kubebrain.native-pitr-log-replay.v6", receipt.Format)
	var encoded bytes.Buffer
	require.NoError(t, json.NewEncoder(&encoded).Encode(receipt))
	_, err = DecodeLogReplayExecution(&encoded)
	require.NoError(t, err)
	wrongFormat := receipt
	wrongFormat.Format = "kubebrain.native-pitr-log-replay.v5"
	require.EqualError(t, wrongFormat.Validate(), "native PITR log replay receipt is incomplete")
	missingLastStart := receipt
	missingLastStart.LastStartTS = 0
	require.EqualError(t, missingLastStart.Validate(), "native PITR log replay receipt has invalid bounds")
	nonPrecedingLastStart := receipt
	nonPrecedingLastStart.LastStartTS = nonPrecedingLastStart.LastCommitTS
	require.EqualError(t, nonPrecedingLastStart.Validate(), "native PITR log replay receipt has invalid bounds")

	freshPartial := receipt
	freshPartial.AppliedMutations, freshPartial.AppliedTransactions = 0, 0
	require.EqualError(t, freshPartial.Validate(), "native PITR log replay receipt progress does not complete the manifest")
	resumedFull := receipt
	resumedFull.Resumed = true
	require.EqualError(t, resumedFull.Validate(), "native PITR log replay receipt checkpoint progress contradicts resume state")
	mutationsWithoutTransactions := receipt
	mutationsWithoutTransactions.AppliedTransactions = 0
	require.EqualError(t, mutationsWithoutTransactions.Validate(), "native PITR log replay receipt has invalid applied statistics")
	moreTransactionsThanMutations := receipt
	moreTransactionsThanMutations.AppliedMutations = 0
	require.EqualError(t, moreTransactionsThanMutations.Validate(), "native PITR log replay receipt has invalid applied statistics")
	resumedComplete := receipt
	resumedComplete.Resumed = true
	resumedComplete.AppliedMutations, resumedComplete.AppliedTransactions = 0, 0
	resumedComplete.CheckpointMutationsBefore, resumedComplete.CheckpointTransactionsBefore = 1, 1
	require.NoError(t, resumedComplete.Validate())
	resumedPartial := receipt
	resumedPartial.Resumed = true
	resumedPartial.MutationCount, resumedPartial.TransactionCount = 3, 3
	resumedPartial.CheckpointMutationsBefore, resumedPartial.CheckpointTransactionsBefore = 2, 2
	require.NoError(t, resumedPartial.Validate())
}

func TestBuildLogReplayExecutionRejectsWrongOrLateFence(t *testing.T) {
	plan := validReceiptPlan(t)
	restore := validFullRestoreExecution()
	restore.PlanSHA256 = digest
	manifest := ReplayManifest{Format: ReplayManifestFormat, LogArtifactReceiptSHA256: plan.Log.ArtifactReceiptSHA, ArtifactManifestSHA256: plan.Log.ArtifactManifest, Keyspace: plan.Source.Keyspace, StartExclusiveTS: plan.Full.BackupTS, RestoreTS: plan.RestoreTS, MutationCount: 1, TransactionCount: 1, PutCount: 1, FirstCommitTS: plan.RestoreTS, LastCommitTS: plan.RestoreTS, MutationsSHA256: digest, AllEntriesInTenantRange: true, AllPutsResolved: true, ExactLocalMirrorRechecked: true}
	now := time.Now().Unix()
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", now-1, false)
	require.NoError(t, err)
	handoff := replayAdmissionHandoff(t, plan, restore, fence, now)
	input := LogReplayExecutionReceipt{PlanSHA256: digest, FullRestoreReceiptSHA256: digest, LogArtifactReceiptSHA256: plan.Log.ArtifactReceiptSHA, RestorationFenceReceiptSHA256: digest, AdmissionHandoffReceiptSHA256: digest, AppliedMutations: 1, AppliedTransactions: 1, LastCommitTS: plan.RestoreTS, LastStartTS: plan.RestoreTS - 1, StartedAtUnix: now, CompletedAtUnix: now}

	_, err = BuildLogReplayExecution(plan, restore, manifest, fence, strings.Repeat("b", 64), handoff, digest, input)
	require.ErrorContains(t, err, "does not match")
	fence.VerifiedAtUnix = now + 1
	_, err = BuildLogReplayExecution(plan, restore, manifest, fence, digest, handoff, digest, input)
	require.ErrorContains(t, err, "does not match")
}

func replayAdmissionHandoff(t *testing.T, plan Plan, restore FullRestoreExecutionReceipt, fence RestorationFenceReceipt, startedAt int64) AdmissionHandoffReceipt {
	t.Helper()
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, fence.OperationID, 8, false)
	require.NoError(t, err)
	handoff, err := BuildAdmissionHandoff(plan, digest, admission, digest, restore, digest, fence, digest, startedAt-1)
	require.NoError(t, err)
	return handoff
}

func TestApplyReplayCheckpointsAndResumesExactly(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	manifest, mutations, err := MaterializeReplay(receipt, digest, root, 119, 150)
	require.NoError(t, err)
	target := memkv.NewKvStorage()
	defer target.Close()
	result, err := ApplyReplay(t.Context(), target, digest, manifest, mutations)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{AppliedMutations: 3, AppliedTransactions: 3, LastCommitTS: 140, LastStartTS: 135}, result)
	_, err = target.Get(t.Context(), keyA)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	value, err := target.Get(t.Context(), keyB)
	require.NoError(t, err)
	require.Equal(t, []byte("short"), value)

	result, err = ApplyReplay(t.Context(), target, digest, manifest, mutations)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{CheckpointMutationsBefore: 3, CheckpointTransactionsBefore: 3, Resumed: true, LastCommitTS: 140, LastStartTS: 135}, result)

	changed := append([]ReplayMutation(nil), mutations...)
	changed[0].Value = []byte("tampered")
	_, err = ApplyReplay(t.Context(), target, digest, manifest, changed)
	require.ErrorContains(t, err, "manifest")
}

func TestApplyReplayResolvesCommittedUncertainResultFromCheckpoint(t *testing.T) {
	_, receipt, root, keyA, keyB := replayFixture(t)
	manifest, mutations, err := MaterializeReplay(receipt, digest, root, 119, 150)
	require.NoError(t, err)
	inner := memkv.NewKvStorage()
	defer inner.Close()
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainOnce: true}

	result, err := ApplyReplay(t.Context(), target, digest, manifest, mutations)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Equal(t, ReplayApplyResult{}, result, "an uncertain commit must not be reported as applied")
	require.Equal(t, 1, target.commits)
	value, err := target.Get(t.Context(), keyA)
	require.NoError(t, err)
	require.Equal(t, []byte("long-value"), value, "the first source transaction actually committed")

	result, err = ApplyReplay(t.Context(), target, digest, manifest, mutations)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{CheckpointMutationsBefore: 1, CheckpointTransactionsBefore: 1, AppliedMutations: 2, AppliedTransactions: 2, Resumed: true, LastCommitTS: 140, LastStartTS: 135}, result)
	require.Equal(t, 3, target.commits, "the committed uncertain source transaction must not be replayed")
	_, err = target.Get(t.Context(), keyA)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	value, err = target.Get(t.Context(), keyB)
	require.NoError(t, err)
	require.Equal(t, []byte("short"), value)
}

func TestReleaseReplayFenceRequiresExactCompletedCheckpoint(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt := validHandoffReplay(plan, time.Now().Unix())
	prefix := "/kubebrain-internal/" + plan.Source.Keyspace
	token, err := restorationfence.NewToken("restore-1", receipt.PlanSHA256, receipt.TargetClusterID, receipt.Keyspace)
	require.NoError(t, err)
	ks, err := coder.NewKeyspace(receipt.Keyspace)
	require.NoError(t, err)
	checkpointKey := ks.EncodeInternalKey([]byte("native-pitr/log-replay-checkpoint"))

	for _, tc := range []struct {
		name       string
		checkpoint replayCheckpoint
		wantError  bool
	}{
		{name: "exact", checkpoint: replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: receipt.MutationsSHA256, RestoreTS: receipt.RestoreTS, AppliedMutations: receipt.MutationCount, LastCommitTS: receipt.LastCommitTS, LastStartTS: receipt.LastStartTS}},
		{name: "partial", checkpoint: replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: receipt.MutationsSHA256, RestoreTS: receipt.RestoreTS, AppliedMutations: 0}, wantError: true},
		{name: "foreign digest", checkpoint: replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: strings.Repeat("b", 64), RestoreTS: receipt.RestoreTS, AppliedMutations: receipt.MutationCount, LastCommitTS: receipt.LastCommitTS, LastStartTS: receipt.LastStartTS}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			_, err := restorationfence.Acquire(t.Context(), store, prefix, token)
			require.NoError(t, err)
			checkpointBytes, err := json.Marshal(tc.checkpoint)
			require.NoError(t, err)
			batch := store.BeginBatchWrite()
			batch.Put(checkpointKey, checkpointBytes, 0)
			require.NoError(t, batch.Commit(t.Context()))

			err = ReleaseReplayFence(t.Context(), store, prefix, token, receipt, digest)
			if tc.wantError {
				require.ErrorContains(t, err, "checkpoint")
				require.NoError(t, restorationfence.Verify(t.Context(), store, prefix, token))
			} else {
				require.NoError(t, err)
				require.NoError(t, restorationfence.VerifyOpen(t.Context(), store, prefix))
			}
		})
	}
}

func TestReleaseReplayFenceReconcilesCommittedUncertainResult(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt := validHandoffReplay(plan, time.Now().Unix())
	prefix := "/kubebrain-internal/" + plan.Source.Keyspace
	token, err := restorationfence.NewToken("restore-1", receipt.PlanSHA256, receipt.TargetClusterID, receipt.Keyspace)
	require.NoError(t, err)
	ks, err := coder.NewKeyspace(receipt.Keyspace)
	require.NoError(t, err)
	checkpointBytes, err := json.Marshal(replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: receipt.MutationsSHA256, RestoreTS: receipt.RestoreTS, AppliedMutations: receipt.MutationCount, LastCommitTS: receipt.LastCommitTS, LastStartTS: receipt.LastStartTS})
	require.NoError(t, err)
	inner := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	_, err = restorationfence.Acquire(t.Context(), inner, prefix, token)
	require.NoError(t, err)
	batch := inner.BeginBatchWrite()
	batch.Put(ks.EncodeInternalKey([]byte("native-pitr/log-replay-checkpoint")), checkpointBytes, 0)
	require.NoError(t, batch.Commit(t.Context()))
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainOnce: true}

	require.NoError(t, ReleaseReplayFence(t.Context(), target, prefix, token, receipt, digest))
	require.NoError(t, restorationfence.VerifyOpen(t.Context(), inner, prefix))
	require.NoError(t, ReleaseReplayFence(t.Context(), target, prefix, token, receipt, digest), "exact release retry must be idempotent")
}

func TestReleaseReplayFenceReconcilesAfterCallerCancellation(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt := validHandoffReplay(plan, time.Now().Unix())
	prefix := "/kubebrain-internal/" + plan.Source.Keyspace
	token, err := restorationfence.NewToken("restore-1", receipt.PlanSHA256, receipt.TargetClusterID, receipt.Keyspace)
	require.NoError(t, err)
	ks, err := coder.NewKeyspace(receipt.Keyspace)
	require.NoError(t, err)
	checkpointBytes, err := json.Marshal(replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: receipt.MutationsSHA256, RestoreTS: receipt.RestoreTS, AppliedMutations: receipt.MutationCount, LastCommitTS: receipt.LastCommitTS, LastStartTS: receipt.LastStartTS})
	require.NoError(t, err)
	inner := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	_, err = restorationfence.Acquire(t.Context(), inner, prefix, token)
	require.NoError(t, err)
	batch := inner.BeginBatchWrite()
	batch.Put(ks.EncodeInternalKey([]byte("native-pitr/log-replay-checkpoint")), checkpointBytes, 0)
	require.NoError(t, batch.Commit(t.Context()))
	releaseCtx, cancel := context.WithCancel(t.Context())
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainOnce: true, cancelAfterCommit: cancel}

	require.NoError(t, ReleaseReplayFence(releaseCtx, target, prefix, token, receipt, digest))
	require.ErrorIs(t, releaseCtx.Err(), context.Canceled)
	require.Equal(t, 1, target.commits)
	require.NoError(t, restorationfence.VerifyOpen(t.Context(), inner, prefix))
	require.NoError(t, ReleaseReplayFence(t.Context(), target, prefix, token, receipt, digest), "exact retry after canceled caller must be idempotent")
}

func TestReleaseReplayFenceFailsClosedWhenReconciliationIsUnavailable(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt := validHandoffReplay(plan, time.Now().Unix())
	prefix := "/kubebrain-internal/" + plan.Source.Keyspace
	token, err := restorationfence.NewToken("restore-1", receipt.PlanSHA256, receipt.TargetClusterID, receipt.Keyspace)
	require.NoError(t, err)
	ks, err := coder.NewKeyspace(receipt.Keyspace)
	require.NoError(t, err)
	checkpointBytes, err := json.Marshal(replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: receipt.MutationsSHA256, RestoreTS: receipt.RestoreTS, AppliedMutations: receipt.MutationCount, LastCommitTS: receipt.LastCommitTS, LastStartTS: receipt.LastStartTS})
	require.NoError(t, err)
	inner := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	_, err = restorationfence.Acquire(t.Context(), inner, prefix, token)
	require.NoError(t, err)
	batch := inner.BeginBatchWrite()
	batch.Put(ks.EncodeInternalKey([]byte("native-pitr/log-replay-checkpoint")), checkpointBytes, 0)
	require.NoError(t, batch.Commit(t.Context()))
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainOnce: true, failReadsAfterCommit: true}

	err = releaseReplayFence(t.Context(), target, prefix, token, receipt, digest, 20*time.Millisecond)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.ErrorContains(t, err, "reconciliation read unavailable")
	require.Equal(t, 1, target.commits)
	require.NoError(t, restorationfence.VerifyOpen(t.Context(), inner, prefix), "underlying commit did open the fence even though its result remains unknown to the caller")
	target.reconciliationReadsFail = false
	require.NoError(t, ReleaseReplayFence(t.Context(), target, prefix, token, receipt, digest), "new call must reconcile the durable commit after reads recover")
	require.Equal(t, 1, target.commits, "reconciliation retry must not submit another release transaction")
}

func TestReleaseReplayFenceDoesNotAcceptUncommittedUncertainResult(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt := validHandoffReplay(plan, time.Now().Unix())
	prefix := "/kubebrain-internal/" + plan.Source.Keyspace
	token, err := restorationfence.NewToken("restore-1", receipt.PlanSHA256, receipt.TargetClusterID, receipt.Keyspace)
	require.NoError(t, err)
	ks, err := coder.NewKeyspace(receipt.Keyspace)
	require.NoError(t, err)
	checkpointBytes, err := json.Marshal(replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: receipt.PlanSHA256, MutationsSHA256: receipt.MutationsSHA256, RestoreTS: receipt.RestoreTS, AppliedMutations: receipt.MutationCount, LastCommitTS: receipt.LastCommitTS, LastStartTS: receipt.LastStartTS})
	require.NoError(t, err)
	inner := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	_, err = restorationfence.Acquire(t.Context(), inner, prefix, token)
	require.NoError(t, err)
	batch := inner.BeginBatchWrite()
	batch.Put(ks.EncodeInternalKey([]byte("native-pitr/log-replay-checkpoint")), checkpointBytes, 0)
	require.NoError(t, batch.Commit(t.Context()))
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainBeforeCommitOnce: true}

	err = releaseReplayFence(t.Context(), target, prefix, token, receipt, digest, 20*time.Millisecond)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.ErrorContains(t, err, "release marker was not committed")
	require.Zero(t, target.commits)
	require.NoError(t, restorationfence.Verify(t.Context(), inner, prefix, token), "uncommitted uncertain release must leave every fence shard held")
	require.NoError(t, ReleaseReplayFence(t.Context(), target, prefix, token, receipt, digest), "same evidence must safely retry the release")
	require.Equal(t, 1, target.commits)
	require.NoError(t, restorationfence.VerifyOpen(t.Context(), inner, prefix))
}

func TestApplyReplaySeparatesTransactionsWithCollidingCommitTS(t *testing.T) {
	task, _ := readyTask(t)
	ks, err := coder.NewKeyspace(task.Keyspace)
	require.NoError(t, err)
	encode := ks.NewCoder().EncodeRevisionKey
	mutations := []ReplayMutation{
		{CommitTS: 150, StartTS: 120, Key: encode([]byte("/a")), Value: []byte("one")},
		{CommitTS: 150, StartTS: 120, Key: encode([]byte("/b")), Value: []byte("two")},
		{CommitTS: 150, StartTS: 121, Key: encode([]byte("/c")), Value: []byte("three")},
	}
	mutationDigest, err := digestReplayMutations(mutations)
	require.NoError(t, err)
	manifest := ReplayManifest{
		Format: ReplayManifestFormat, LogArtifactReceiptSHA256: digest, ArtifactManifestSHA256: digest,
		Keyspace: task.Keyspace, StartExclusiveTS: 119, RestoreTS: 150,
		MutationCount: 3, TransactionCount: 2, PutCount: 3,
		FirstCommitTS: 150, LastCommitTS: 150, MutationsSHA256: mutationDigest,
		AllEntriesInTenantRange: true, AllPutsResolved: true, ExactLocalMirrorRechecked: true,
	}
	inner := memkv.NewKvStorage()
	defer inner.Close()
	target := &countingReplayStore{KvStorage: inner}
	badManifest := manifest
	badManifest.TransactionCount = 1
	_, err = ApplyReplay(t.Context(), target, digest, badManifest, mutations)
	require.EqualError(t, err, "replay mutation statistics do not match manifest")

	result, err := ApplyReplay(t.Context(), target, digest, manifest, mutations)

	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{AppliedMutations: 3, AppliedTransactions: 2, LastCommitTS: 150, LastStartTS: 121}, result)
	require.Equal(t, 2, target.commits, "each distinct source start TS must commit separately")
	require.NotEmpty(t, mutations[0].Key, "the compatibility API must preserve caller-owned mutations")
}

func TestApplyReplayAndReleaseClearsOnlyDurablyReconciledTransactions(t *testing.T) {
	task, _ := readyTask(t)
	ks, err := coder.NewKeyspace(task.Keyspace)
	require.NoError(t, err)
	encode := ks.NewCoder().EncodeRevisionKey
	mutations := []ReplayMutation{
		{CommitTS: 150, StartTS: 120, Key: encode([]byte("/a")), Value: bytes.Repeat([]byte{'a'}, 1024)},
		{CommitTS: 151, StartTS: 121, Key: encode([]byte("/b")), Value: bytes.Repeat([]byte{'b'}, 1024)},
		{CommitTS: 152, StartTS: 122, Key: encode([]byte("/c")), Value: bytes.Repeat([]byte{'c'}, 1024)},
	}
	mutationDigest, err := digestReplayMutations(mutations)
	require.NoError(t, err)
	manifest := ReplayManifest{
		Format: ReplayManifestFormat, LogArtifactReceiptSHA256: digest, ArtifactManifestSHA256: digest,
		Keyspace: task.Keyspace, StartExclusiveTS: 119, RestoreTS: 152,
		MutationCount: 3, TransactionCount: 3, PutCount: 3,
		FirstCommitTS: 150, LastCommitTS: 152, MutationsSHA256: mutationDigest,
		AllEntriesInTenantRange: true, AllPutsResolved: true, ExactLocalMirrorRechecked: true,
	}
	retryMutations := append([]ReplayMutation(nil), mutations...)
	inner := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	target := &commitThenUncertainReplayStore{KvStorage: inner, uncertainOnce: true, uncertainAtCommit: 2}

	result, err := ApplyReplayAndRelease(t.Context(), target, digest, manifest, mutations)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Equal(t, ReplayApplyResult{AppliedMutations: 1, AppliedTransactions: 1, LastCommitTS: 150, LastStartTS: 120}, result)
	require.Equal(t, ReplayMutation{}, mutations[0], "a definitely committed transaction must release its payload before a later failure")
	require.NotEmpty(t, mutations[1].Key, "an uncertain transaction must retain its payload until checkpoint reconciliation")
	require.NotEmpty(t, mutations[2].Key, "an unattempted transaction must remain intact")

	result, err = ApplyReplayAndRelease(t.Context(), target, digest, manifest, retryMutations)
	require.NoError(t, err)
	require.Equal(t, ReplayApplyResult{CheckpointMutationsBefore: 2, CheckpointTransactionsBefore: 2, AppliedMutations: 1, AppliedTransactions: 1, LastCommitTS: 152, LastStartTS: 122, Resumed: true}, result)
	require.Equal(t, ReplayMutation{}, retryMutations[0], "checkpoint-reconciled prefix must be released")
	require.Equal(t, ReplayMutation{}, retryMutations[1], "uncertain transaction must release after checkpoint reconciliation")
	require.Equal(t, ReplayMutation{}, retryMutations[2], "newly committed transaction must be released")
	require.Equal(t, 3, target.commits, "each source transaction must commit exactly once across the retry")
}

func TestMaterializeReplayFailsClosed(t *testing.T) {
	_, receipt, root, _, _ := replayFixture(t)
	_, _, err := MaterializeReplay(receipt, digest, root, 119, receipt.GlobalCheckpointTS+1)
	require.ErrorContains(t, err, "TSO window")

	data := receipt.Objects[1]
	if data.Kind != "data" {
		data = receipt.Objects[0]
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, filepath.FromSlash(data.Name)), []byte("changed"), 0o600))
	_, _, err = MaterializeReplay(receipt, digest, root, 119, 150)
	require.ErrorContains(t, err, "recheck")
}

func replayFixture(t *testing.T) (TaskCreateReceipt, LogArtifactReceipt, string, []byte, []byte) {
	t.Helper()
	task, _ := readyTask(t)
	ready := readyReceiptFor(task)
	ks, err := coder.NewKeyspace(task.Keyspace)
	require.NoError(t, err)
	keyA := ks.NewCoder().EncodeRevisionKey([]byte("/a"))
	keyB := ks.NewCoder().EncodeRevisionKey([]byte("/b"))
	defaultData := encodeReplayEntry(encodeReplayMVCCKey(keyA, 120), []byte("long-value"))
	writeData := append(encodeReplayEntry(encodeReplayMVCCKey(keyA, 130), encodeReplayWrite('P', 120, nil)), encodeReplayEntry(encodeReplayMVCCKey(keyB, 130), encodeReplayWrite('P', 121, []byte("short")))...)
	writeData = append(writeData, encodeReplayEntry(encodeReplayMVCCKey(keyA, 140), encodeReplayWrite('D', 135, nil))...)
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/log"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "v1/backupmeta"), 0o700))
	defaultFile := replayDataFile("v1/log/default.log", "default", defaultData, task.StartTS, ready.GlobalCheckpointTS, 1)
	writeFile := replayDataFile("v1/log/write.log", "write", writeData, task.StartTS, ready.GlobalCheckpointTS, 3)
	require.NoError(t, os.WriteFile(filepath.Join(root, defaultFile.Path), defaultData, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, writeFile.Path), writeData, 0o600))
	meta := &backuppb.Metadata{MetaVersion: backuppb.MetaVersion_V1, StoreId: 1, MinTs: task.StartTS, MaxTs: ready.GlobalCheckpointTS, ResolvedTs: ready.GlobalCheckpointTS, Files: []*backuppb.DataFileInfo{defaultFile, writeFile}}
	metaBytes, err := meta.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "v1/backupmeta/1.meta"), metaBytes, 0o600))
	inventory := inventoryForMirror(t, task.LogStoragePrefix, root)
	receipt, err := VerifyLogArtifacts(task, digest, ready, digest, inventory, digest, root)
	require.NoError(t, err)
	return task, receipt, root, keyA, keyB
}

func replayDataFile(path, cf string, data []byte, minTS, resolved uint64, entries int64) *backuppb.DataFileInfo {
	h := sha256.Sum256(data)
	return &backuppb.DataFileInfo{Path: path, Cf: cf, MinTs: minTS, MaxTs: resolved, ResolvedTs: resolved, Length: uint64(len(data)), NumberOfEntries: entries, Sha256: h[:]}
}

func encodeReplayEntry(key, value []byte) []byte {
	result := make([]byte, 4, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(result, uint32(len(key)))
	result = append(result, key...)
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
	result = append(result, length[:]...)
	return append(result, value...)
}

func encodeReplayMVCCKey(raw []byte, ts uint64) []byte {
	result := make([]byte, 0, len(raw)+18)
	for len(raw) >= 8 {
		result = append(result, raw[:8]...)
		result = append(result, 0xff)
		raw = raw[8:]
	}
	pad := 8 - len(raw)
	result = append(result, raw...)
	result = append(result, make([]byte, pad)...)
	result = append(result, byte(0xff-pad))
	var encodedTS [8]byte
	binary.BigEndian.PutUint64(encodedTS[:], ^ts)
	return append(result, encodedTS[:]...)
}

func encodeReplayWrite(kind byte, startTS uint64, short []byte) []byte {
	result := []byte{kind}
	result = binary.AppendUvarint(result, startTS)
	if short != nil {
		result = append(result, 'v', byte(len(short)))
		result = append(result, short...)
	}
	return result
}
