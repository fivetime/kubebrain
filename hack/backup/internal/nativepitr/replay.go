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
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

const ReplayManifestFormat = "kubebrain.native-pitr-log-replay-manifest.v1"
const LogReplayExecutionFormat = "kubebrain.native-pitr-log-replay.v2"

type ReplayMutation struct {
	CommitTS uint64 `json:"commit_ts"`
	Key      []byte `json:"key"`
	Value    []byte `json:"value,omitempty"`
	Delete   bool   `json:"delete,omitempty"`
}

type ReplayManifest struct {
	Format                    string `json:"format"`
	LogArtifactReceiptSHA256  string `json:"log_artifact_receipt_sha256"`
	ArtifactManifestSHA256    string `json:"artifact_manifest_sha256"`
	Keyspace                  string `json:"keyspace"`
	StartExclusiveTS          uint64 `json:"start_exclusive_ts"`
	RestoreTS                 uint64 `json:"restore_ts"`
	MutationCount             int    `json:"mutation_count"`
	TransactionCount          int    `json:"transaction_count"`
	PutCount                  int    `json:"put_count"`
	DeleteCount               int    `json:"delete_count"`
	FirstCommitTS             uint64 `json:"first_commit_ts,omitempty"`
	LastCommitTS              uint64 `json:"last_commit_ts,omitempty"`
	MutationsSHA256           string `json:"mutations_sha256"`
	AllEntriesInTenantRange   bool   `json:"all_entries_in_tenant_range"`
	AllPutsResolved           bool   `json:"all_puts_resolved"`
	ExactLocalMirrorRechecked bool   `json:"exact_local_mirror_rechecked"`
}

type replayWrite struct {
	commitTS, startTS uint64
	key, shortValue   []byte
	kind              byte
}

const replayCheckpointFormat = "kubebrain.native-pitr-log-replay-checkpoint.v1"

type ReplayApplyResult struct {
	AppliedMutations    int
	AppliedTransactions int
	Resumed             bool
	LastCommitTS        uint64
}

type replayCheckpoint struct {
	Format           string `json:"format"`
	PlanSHA256       string `json:"plan_sha256"`
	MutationsSHA256  string `json:"mutations_sha256"`
	RestoreTS        uint64 `json:"restore_ts"`
	AppliedMutations int    `json:"applied_mutations"`
	LastCommitTS     uint64 `json:"last_commit_ts"`
}

type LogReplayExecutionReceipt struct {
	Format                        string `json:"format"`
	PlanSHA256                    string `json:"plan_sha256"`
	FullRestoreReceiptSHA256      string `json:"full_restore_receipt_sha256"`
	LogArtifactReceiptSHA256      string `json:"log_artifact_receipt_sha256"`
	ArtifactManifestSHA256        string `json:"artifact_manifest_sha256"`
	MutationsSHA256               string `json:"mutations_sha256"`
	RestorationFenceReceiptSHA256 string `json:"restoration_fence_receipt_sha256"`
	SourceClusterID               uint64 `json:"source_cluster_id"`
	TargetClusterID               uint64 `json:"target_cluster_id"`
	Keyspace                      string `json:"keyspace"`
	BackupTS                      uint64 `json:"backup_ts"`
	RestoreTS                     uint64 `json:"restore_ts"`
	MutationCount                 int    `json:"mutation_count"`
	TransactionCount              int    `json:"transaction_count"`
	AppliedMutations              int    `json:"applied_mutations_this_run"`
	AppliedTransactions           int    `json:"applied_transactions_this_run"`
	LastCommitTS                  uint64 `json:"last_commit_ts,omitempty"`
	Resumed                       bool   `json:"resumed_from_checkpoint"`
	CheckpointAtomic              bool   `json:"checkpoint_atomic_with_source_transaction"`
	ReplayWriteFenceProven        bool   `json:"replay_write_fence_proven"`
	TargetWriteFenceProven        bool   `json:"target_write_fence_proven"`
	LogReplayCompleted            bool   `json:"log_replay_completed"`
	PostRestoreSemanticValidated  bool   `json:"post_restore_semantic_validated"`
	PITRComplete                  bool   `json:"pitr_complete"`
	StartedAtUnix                 int64  `json:"started_at_unix"`
	CompletedAtUnix               int64  `json:"completed_at_unix"`
}

func BuildLogReplayExecution(plan Plan, restore FullRestoreExecutionReceipt, manifest ReplayManifest, fence RestorationFenceReceipt, fenceSHA string, in LogReplayExecutionReceipt) (LogReplayExecutionReceipt, error) {
	if err := plan.Validate(); err != nil {
		return LogReplayExecutionReceipt{}, err
	}
	if err := restore.Validate(); err != nil {
		return LogReplayExecutionReceipt{}, err
	}
	if err := manifest.Validate(); err != nil {
		return LogReplayExecutionReceipt{}, err
	}
	if err := fence.Validate(); err != nil {
		return LogReplayExecutionReceipt{}, err
	}
	if plan.RestoreTS <= plan.Full.BackupTS || restore.PlanSHA256 != in.PlanSHA256 || fence.PlanSHA256 != in.PlanSHA256 || fence.TargetClusterID != plan.Target.ClusterID || fence.Keyspace != plan.Source.Keyspace || in.LogArtifactReceiptSHA256 != manifest.LogArtifactReceiptSHA256 || in.LastCommitTS != manifest.LastCommitTS || manifest.LogArtifactReceiptSHA256 != plan.Log.ArtifactReceiptSHA || manifest.ArtifactManifestSHA256 != plan.Log.ArtifactManifest || manifest.Keyspace != plan.Source.Keyspace || manifest.StartExclusiveTS != plan.Full.BackupTS || manifest.RestoreTS != plan.RestoreTS || !sha256RE.MatchString(fenceSHA) || in.RestorationFenceReceiptSHA256 != fenceSHA || fence.VerifiedAtUnix > in.StartedAtUnix {
		return LogReplayExecutionReceipt{}, errors.New("log replay evidence does not match restore plan")
	}
	in.Format = LogReplayExecutionFormat
	in.SourceClusterID, in.TargetClusterID, in.Keyspace = plan.Source.ClusterID, plan.Target.ClusterID, plan.Source.Keyspace
	in.BackupTS, in.RestoreTS = plan.Full.BackupTS, plan.RestoreTS
	in.ArtifactManifestSHA256, in.MutationsSHA256 = manifest.ArtifactManifestSHA256, manifest.MutationsSHA256
	in.MutationCount, in.TransactionCount = manifest.MutationCount, manifest.TransactionCount
	in.CheckpointAtomic, in.ReplayWriteFenceProven, in.LogReplayCompleted = true, true, true
	in.TargetWriteFenceProven, in.PostRestoreSemanticValidated, in.PITRComplete = false, false, false
	if err := in.Validate(); err != nil {
		return LogReplayExecutionReceipt{}, err
	}
	return in, nil
}

func (r LogReplayExecutionReceipt) Validate() error {
	if r.Format != LogReplayExecutionFormat || r.SourceClusterID == 0 || r.TargetClusterID == 0 || r.SourceClusterID == r.TargetClusterID || r.Keyspace == "" || r.BackupTS == 0 || r.RestoreTS <= r.BackupTS || r.MutationCount < 0 || r.TransactionCount < 0 || r.AppliedMutations < 0 || r.AppliedTransactions < 0 || r.AppliedMutations > r.MutationCount || r.AppliedTransactions > r.TransactionCount || !r.CheckpointAtomic || !r.ReplayWriteFenceProven || !r.LogReplayCompleted || r.TargetWriteFenceProven || r.PostRestoreSemanticValidated || r.PITRComplete || r.StartedAtUnix <= 0 || r.CompletedAtUnix < r.StartedAtUnix {
		return errors.New("native PITR log replay receipt is incomplete")
	}
	for _, value := range []string{r.PlanSHA256, r.FullRestoreReceiptSHA256, r.LogArtifactReceiptSHA256, r.ArtifactManifestSHA256, r.MutationsSHA256, r.RestorationFenceReceiptSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("native PITR log replay receipt has invalid digest")
		}
	}
	if (r.MutationCount == 0) != (r.TransactionCount == 0) || (r.MutationCount == 0) != (r.LastCommitTS == 0) || (r.LastCommitTS != 0 && (r.LastCommitTS <= r.BackupTS || r.LastCommitTS > r.RestoreTS)) {
		return errors.New("native PITR log replay receipt has invalid bounds")
	}
	return nil
}

func DecodeLogReplayExecution(reader io.Reader) (LogReplayExecutionReceipt, error) {
	var receipt LogReplayExecutionReceipt
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode native PITR log replay receipt: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return receipt, errors.New("native PITR log replay receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}

// MaterializeReplay converts BR stream default/write CF records into raw
// KubeBrain mutations. It performs no target writes.
func MaterializeReplay(logs LogArtifactReceipt, logsSHA, root string, startExclusive, restoreTS uint64) (ReplayManifest, []ReplayMutation, error) {
	if err := logs.Validate(); err != nil {
		return ReplayManifest{}, nil, err
	}
	if !sha256RE.MatchString(logsSHA) || root == "" || startExclusive == 0 || restoreTS <= startExclusive || restoreTS > logs.GlobalCheckpointTS {
		return ReplayManifest{}, nil, errors.New("invalid replay receipt digest, root, or TSO window")
	}
	for _, object := range logs.Objects {
		path := filepath.Join(root, filepath.FromSlash(object.Name))
		if err := verifyReplayObject(path, object); err != nil {
			return ReplayManifest{}, nil, fmt.Errorf("recheck log object %q: %w", object.Name, err)
		}
	}
	expected, err := replaySegments(root, logs.Objects)
	if err != nil {
		return ReplayManifest{}, nil, err
	}
	ks, err := coder.NewKeyspace(logs.Keyspace)
	if err != nil {
		return ReplayManifest{}, nil, err
	}
	start, end := ks.ObjectKeyspaceStart(), ks.ObjectKeyspaceEnd()
	defaults := make(map[string][]byte)
	var writes []replayWrite
	for _, object := range expected {
		for _, segment := range object.segments {
			decoded, err := readReplaySegment(root, object.path, segment)
			if err != nil {
				return ReplayManifest{}, nil, err
			}
			entries := int64(0)
			for len(decoded) != 0 {
				encodedKey, value, rest, err := decodeLogEntry(decoded)
				if err != nil {
					return ReplayManifest{}, nil, fmt.Errorf("decode %s log entry: %w", segment.Cf, err)
				}
				decoded = rest
				entries++
				rawKey, ts, err := decodeMVCCKey(encodedKey)
				if err != nil {
					return ReplayManifest{}, nil, err
				}
				if bytes.Compare(rawKey, start) < 0 || bytes.Compare(rawKey, end) >= 0 {
					return ReplayManifest{}, nil, errors.New("stream log contains a key outside the plan tenant range")
				}
				if len(value) == 0 {
					// TiKV log backup may emit a duplicate prewrite entry without
					// its value; pinned BR v7.5.1 intentionally ignores it too.
					continue
				}
				switch segment.Cf {
				case "default":
					defaultKey := replayValueKey(rawKey, ts)
					if existing, duplicate := defaults[defaultKey]; duplicate {
						if !bytes.Equal(existing, value) {
							return ReplayManifest{}, nil, errors.New("stream log contains conflicting default-CF entries")
						}
						continue
					}
					defaults[defaultKey] = append(make([]byte, 0, len(value)), value...)
				case "write":
					kind, startTS, short, err := decodeWriteValue(value)
					if err != nil {
						return ReplayManifest{}, nil, err
					}
					if startTS >= ts {
						return ReplayManifest{}, nil, errors.New("write-CF commit TSO does not follow start TSO")
					}
					if ts > startExclusive && ts <= restoreTS && (kind == 'P' || kind == 'D') {
						writes = append(writes, replayWrite{commitTS: ts, startTS: startTS, key: append([]byte(nil), rawKey...), shortValue: short, kind: kind})
					}
				default:
					return ReplayManifest{}, nil, fmt.Errorf("unsupported stream column family %q", segment.Cf)
				}
			}
			if entries != segment.NumberOfEntries {
				return ReplayManifest{}, nil, errors.New("decoded log entry count does not match stream metadata")
			}
		}
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].commitTS != writes[j].commitTS {
			return writes[i].commitTS < writes[j].commitTS
		}
		return bytes.Compare(writes[i].key, writes[j].key) < 0
	})
	canonicalWrites := writes[:0]
	for _, write := range writes {
		if len(canonicalWrites) != 0 {
			previous := canonicalWrites[len(canonicalWrites)-1]
			if previous.commitTS == write.commitTS && bytes.Equal(previous.key, write.key) {
				if previous.startTS != write.startTS || previous.kind != write.kind || !bytes.Equal(previous.shortValue, write.shortValue) {
					return ReplayManifest{}, nil, errors.New("stream log contains conflicting write-CF entries")
				}
				continue
			}
		}
		canonicalWrites = append(canonicalWrites, write)
	}
	writes = canonicalWrites
	mutations := make([]ReplayMutation, 0, len(writes))
	putCount, deleteCount, txns := 0, 0, 0
	var lastTS uint64
	for _, write := range writes {
		if write.commitTS != lastTS {
			txns++
			lastTS = write.commitTS
		}
		mutation := ReplayMutation{CommitTS: write.commitTS, Key: write.key}
		if write.kind == 'D' {
			mutation.Delete = true
			deleteCount++
		} else {
			mutation.Value = write.shortValue
			if mutation.Value == nil {
				value, ok := defaults[replayValueKey(write.key, write.startTS)]
				if !ok {
					return ReplayManifest{}, nil, fmt.Errorf("PUT at commit TSO %d lacks its default-CF value", write.commitTS)
				}
				mutation.Value = value
			}
			putCount++
		}
		mutations = append(mutations, mutation)
	}
	encoded, err := json.Marshal(mutations)
	if err != nil {
		return ReplayManifest{}, nil, err
	}
	digest := sha256.Sum256(encoded)
	manifest := ReplayManifest{Format: ReplayManifestFormat, LogArtifactReceiptSHA256: logsSHA, ArtifactManifestSHA256: logs.ManifestSHA256, Keyspace: logs.Keyspace, StartExclusiveTS: startExclusive, RestoreTS: restoreTS, MutationCount: len(mutations), TransactionCount: txns, PutCount: putCount, DeleteCount: deleteCount, MutationsSHA256: hex.EncodeToString(digest[:]), AllEntriesInTenantRange: true, AllPutsResolved: true, ExactLocalMirrorRechecked: true}
	if len(mutations) != 0 {
		manifest.FirstCommitTS, manifest.LastCommitTS = mutations[0].CommitTS, mutations[len(mutations)-1].CommitTS
	}
	if err := validateReplayMutations(manifest, mutations); err != nil {
		return ReplayManifest{}, nil, err
	}
	for _, object := range logs.Objects {
		path := filepath.Join(root, filepath.FromSlash(object.Name))
		if err := verifyReplayObject(path, object); err != nil {
			return ReplayManifest{}, nil, fmt.Errorf("post-materialization log object %q: %w", object.Name, err)
		}
	}
	return manifest, mutations, manifest.Validate()
}

func (m ReplayManifest) Validate() error {
	if m.Format != ReplayManifestFormat || !sha256RE.MatchString(m.LogArtifactReceiptSHA256) || !sha256RE.MatchString(m.ArtifactManifestSHA256) || !sha256RE.MatchString(m.MutationsSHA256) || m.Keyspace == "" || m.StartExclusiveTS == 0 || m.RestoreTS <= m.StartExclusiveTS || m.MutationCount < 0 || m.TransactionCount < 0 || m.PutCount < 0 || m.DeleteCount < 0 || m.MutationCount != m.PutCount+m.DeleteCount || !m.AllEntriesInTenantRange || !m.AllPutsResolved || !m.ExactLocalMirrorRechecked {
		return errors.New("native PITR replay manifest is incomplete")
	}
	if (m.MutationCount == 0) != (m.TransactionCount == 0) || (m.MutationCount == 0) != (m.FirstCommitTS == 0 && m.LastCommitTS == 0) || (m.MutationCount > 0 && (m.FirstCommitTS <= m.StartExclusiveTS || m.LastCommitTS < m.FirstCommitTS || m.LastCommitTS > m.RestoreTS)) {
		return errors.New("native PITR replay manifest has invalid mutation bounds")
	}
	return nil
}

// ApplyReplay commits every original source transaction as one target
// transaction. The mutation group and its durable checkpoint advance
// atomically, making retries exact at source commit-TS boundaries.
func ApplyReplay(ctx context.Context, target storage.KvStorage, planSHA string, manifest ReplayManifest, mutations []ReplayMutation) (ReplayApplyResult, error) {
	if err := manifest.Validate(); err != nil {
		return ReplayApplyResult{}, err
	}
	if !sha256RE.MatchString(planSHA) {
		return ReplayApplyResult{}, errors.New("invalid replay plan digest")
	}
	encoded, err := json.Marshal(mutations)
	if err != nil {
		return ReplayApplyResult{}, err
	}
	digest := sha256.Sum256(encoded)
	if len(mutations) != manifest.MutationCount || hex.EncodeToString(digest[:]) != manifest.MutationsSHA256 {
		return ReplayApplyResult{}, errors.New("replay mutations do not match manifest")
	}
	if err := validateReplayMutations(manifest, mutations); err != nil {
		return ReplayApplyResult{}, err
	}
	ks, err := coder.NewKeyspace(manifest.Keyspace)
	if err != nil {
		return ReplayApplyResult{}, err
	}
	checkpointKey := ks.EncodeInternalKey([]byte("native-pitr/log-replay-checkpoint"))
	currentBytes, err := target.Get(ctx, checkpointKey)
	resumed := err == nil
	var current replayCheckpoint
	if err == nil {
		if json.Unmarshal(currentBytes, &current) != nil || current.Format != replayCheckpointFormat || current.PlanSHA256 != planSHA || current.MutationsSHA256 != manifest.MutationsSHA256 || current.RestoreTS != manifest.RestoreTS || current.AppliedMutations < 0 || current.AppliedMutations > len(mutations) {
			return ReplayApplyResult{}, errors.New("target replay checkpoint does not match this exact plan")
		}
		if current.AppliedMutations > 0 && mutations[current.AppliedMutations-1].CommitTS != current.LastCommitTS {
			return ReplayApplyResult{}, errors.New("target replay checkpoint is not on a transaction boundary")
		}
		if current.AppliedMutations < len(mutations) && mutations[current.AppliedMutations].CommitTS == current.LastCommitTS {
			return ReplayApplyResult{}, errors.New("target replay checkpoint splits a source transaction")
		}
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return ReplayApplyResult{}, err
	}
	result := ReplayApplyResult{Resumed: resumed, LastCommitTS: current.LastCommitTS}
	if len(mutations) == 0 && !resumed {
		next := replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: planSHA, MutationsSHA256: manifest.MutationsSHA256, RestoreTS: manifest.RestoreTS}
		nextBytes, err := json.Marshal(next)
		if err != nil {
			return result, err
		}
		batch := target.BeginBatchWrite()
		batch.PutIfNotExist(checkpointKey, nextBytes, 0)
		if err := batch.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit empty replay checkpoint: %w", err)
		}
	}
	for offset := current.AppliedMutations; offset < len(mutations); {
		end := offset + 1
		for end < len(mutations) && mutations[end].CommitTS == mutations[offset].CommitTS {
			end++
		}
		next := replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: planSHA, MutationsSHA256: manifest.MutationsSHA256, RestoreTS: manifest.RestoreTS, AppliedMutations: end, LastCommitTS: mutations[offset].CommitTS}
		nextBytes, err := json.Marshal(next)
		if err != nil {
			return result, err
		}
		batch := target.BeginBatchWrite()
		for _, mutation := range mutations[offset:end] {
			if mutation.Delete {
				batch.Del(mutation.Key)
			} else {
				batch.Put(mutation.Key, mutation.Value, 0)
			}
		}
		if current.AppliedMutations == 0 && !resumed {
			batch.PutIfNotExist(checkpointKey, nextBytes, 0)
		} else {
			batch.CAS(checkpointKey, nextBytes, currentBytes, 0)
		}
		if err := batch.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit replay source TSO %d: %w", mutations[offset].CommitTS, err)
		}
		current, currentBytes = next, nextBytes
		result.AppliedMutations += end - offset
		result.AppliedTransactions++
		result.LastCommitTS = next.LastCommitTS
		offset = end
	}
	return result, nil
}

func validateReplayMutations(manifest ReplayManifest, mutations []ReplayMutation) error {
	ks, err := coder.NewKeyspace(manifest.Keyspace)
	if err != nil {
		return err
	}
	start, end := ks.ObjectKeyspaceStart(), ks.ObjectKeyspaceEnd()
	for i, mutation := range mutations {
		if mutation.CommitTS <= manifest.StartExclusiveTS || mutation.CommitTS > manifest.RestoreTS || len(mutation.Key) == 0 || bytes.Compare(mutation.Key, start) < 0 || bytes.Compare(mutation.Key, end) >= 0 || (mutation.Delete && mutation.Value != nil) || (!mutation.Delete && mutation.Value == nil) || (i > 0 && (mutation.CommitTS < mutations[i-1].CommitTS || (mutation.CommitTS == mutations[i-1].CommitTS && bytes.Compare(mutation.Key, mutations[i-1].Key) <= 0))) {
			return errors.New("replay mutations are not canonical, bounded, or complete")
		}
	}
	return nil
}

func verifyReplayObject(path string, object LogArtifactObject) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, int64(object.Bytes)+1))
	if err != nil || uint64(n) != object.Bytes || hex.EncodeToString(h.Sum(nil)) != object.SHA256 {
		return errors.New("size or SHA-256 changed")
	}
	return nil
}

func replaySegments(root string, objects []LogArtifactObject) (map[string]*logDataObject, error) {
	expected := make(map[string]*logDataObject)
	for _, object := range objects {
		if object.Kind != "metadata" {
			continue
		}
		content, err := readBoundedPath(filepath.Join(root, filepath.FromSlash(object.Name)), maxMetaIndexBytes)
		if err != nil {
			return nil, err
		}
		var meta backuppb.Metadata
		if err := meta.Unmarshal(content); err != nil {
			return nil, err
		}
		count, resolved := 0, uint64(0)
		if err := collectLogMetadata(&meta, expected, &count, &resolved); err != nil {
			return nil, err
		}
	}
	return expected, nil
}

func readReplaySegment(root, name string, segment *backuppb.DataFileInfo) ([]byte, error) {
	f, err := os.Open(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var input io.Reader = f
	if segment.RangeLength != 0 {
		input = io.NewSectionReader(f, int64(segment.RangeOffset), int64(segment.RangeLength))
	}
	if segment.CompressionType == backuppb.CompressionType_ZSTD {
		decoder, err := zstd.NewReader(input, zstd.WithDecoderMaxMemory(1<<30))
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		input = decoder
	}
	content, err := io.ReadAll(io.LimitReader(input, int64(segment.Length)+1))
	if err != nil || uint64(len(content)) != segment.Length {
		return nil, errors.New("decoded replay segment length mismatch")
	}
	digest := sha256.Sum256(content)
	if !bytes.Equal(digest[:], segment.Sha256) {
		return nil, errors.New("decoded replay segment digest mismatch")
	}
	return content, nil
}

func decodeLogEntry(input []byte) (key, value, rest []byte, err error) {
	if len(input) < 8 {
		return nil, nil, nil, errors.New("truncated log entry")
	}
	keyLen := uint64(binary.LittleEndian.Uint32(input))
	if keyLen > uint64(len(input)-8) {
		return nil, nil, nil, errors.New("invalid log key length")
	}
	pos := uint64(4) + keyLen
	valueLen := uint64(binary.LittleEndian.Uint32(input[pos:]))
	pos += 4
	if valueLen > uint64(len(input))-pos {
		return nil, nil, nil, errors.New("invalid log value length")
	}
	return input[4 : 4+keyLen], input[pos : pos+valueLen], input[pos+valueLen:], nil
}

func decodeMVCCKey(encoded []byte) ([]byte, uint64, error) {
	var raw []byte
	pos := 0
	for {
		if len(encoded)-pos < 9 {
			return nil, 0, errors.New("invalid memcomparable MVCC key")
		}
		group, marker := encoded[pos:pos+8], encoded[pos+8]
		pos += 9
		pad := int(0xff - marker)
		if pad < 0 || pad > 8 {
			return nil, 0, errors.New("invalid memcomparable key marker")
		}
		if pad != 0 {
			for _, b := range group[8-pad:] {
				if b != 0 {
					return nil, 0, errors.New("invalid memcomparable key padding")
				}
			}
			raw = append(raw, group[:8-pad]...)
			break
		}
		raw = append(raw, group...)
	}
	if len(encoded)-pos != 8 {
		return nil, 0, errors.New("MVCC key has trailing bytes")
	}
	ts := ^binary.BigEndian.Uint64(encoded[pos:])
	if ts == 0 {
		return nil, 0, errors.New("MVCC key has zero TSO")
	}
	return raw, ts, nil
}

func decodeWriteValue(value []byte) (byte, uint64, []byte, error) {
	if len(value) < 2 {
		return 0, 0, nil, errors.New("invalid write-CF value")
	}
	kind := value[0]
	if kind != 'P' && kind != 'D' && kind != 'R' && kind != 'L' {
		return 0, 0, nil, errors.New("unknown write-CF type")
	}
	startTS, n := binary.Uvarint(value[1:])
	if n <= 0 || startTS == 0 {
		return 0, 0, nil, errors.New("invalid write-CF start TSO")
	}
	rest := value[1+n:]
	var short []byte
	for len(rest) != 0 {
		switch rest[0] {
		case 'v':
			if len(rest) < 2 || int(rest[1]) > len(rest)-2 {
				return 0, 0, nil, errors.New("invalid short write value")
			}
			short = append([]byte(nil), rest[2:2+int(rest[1])]...)
			rest = rest[2+int(rest[1]):]
		case 'R':
			rest = rest[1:]
		case 'F':
			if len(rest) < 9 {
				return 0, 0, nil, errors.New("invalid GC fence")
			}
			rest = rest[9:]
		case 'l':
			if len(rest) < 9 {
				return 0, 0, nil, errors.New("invalid last-change metadata")
			}
			rest = rest[9:]
			_, n = binary.Uvarint(rest)
			if n <= 0 {
				return 0, 0, nil, errors.New("invalid last-change count")
			}
			rest = rest[n:]
		case 'S':
			_, n = binary.Uvarint(rest[1:])
			if n <= 0 {
				return 0, 0, nil, errors.New("invalid txn source")
			}
			rest = rest[1+n:]
		default:
			rest = nil
		}
	}
	return kind, startTS, short, nil
}

func replayValueKey(key []byte, ts uint64) string {
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], ts)
	return string(key) + string(suffix[:])
}
