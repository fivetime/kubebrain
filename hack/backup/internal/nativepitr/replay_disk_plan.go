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

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
	bolt "go.etcd.io/bbolt"
)

var (
	replayCandidateBucket = []byte("write-candidates")
	replayStartTSBucket   = []byte("start-ts")
	replayPlanBucket      = []byte("canonical-mutations")
)

const (
	replayPlanRecordVersion = byte(2)
	replayPlanRecordHeader  = 2 + 8 + sha256.Size
)

// ReplayDiskPlan owns a rebuildable, canonical mutation plan in scratch
// storage. Call Close when replay finishes; no returned key or value aliases
// the bbolt mmap.
type ReplayDiskPlan struct {
	db       *bolt.DB
	path     string
	Manifest ReplayManifest
}

type replayCandidateStore struct {
	db           *bolt.DB
	path         string
	tx           *bolt.Tx
	candidates   *bolt.Bucket
	starts       *bolt.Bucket
	pendingBytes uint64
}

func newReplayScratchDB(scratchDir, pattern string, buckets ...[]byte) (*bolt.DB, string, error) {
	file, err := os.CreateTemp(scratchDir, pattern)
	if err != nil {
		return nil, "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return nil, "", errors.Join(err, os.Remove(path))
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{NoSync: true, NoFreelistSync: true})
	if err != nil {
		return nil, "", errors.Join(err, os.Remove(path))
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, "", errors.Join(err, db.Close(), os.Remove(path))
	}
	return db, path, nil
}

func newReplayCandidateStore(scratchDir string) (*replayCandidateStore, error) {
	db, path, err := newReplayScratchDB(scratchDir, ".kubebrain-native-pitr-candidates-*.db", replayCandidateBucket, replayStartTSBucket)
	if err != nil {
		return nil, err
	}
	return &replayCandidateStore{db: db, path: path}, nil
}

func (s *replayCandidateStore) begin() error {
	if s.tx != nil {
		return nil
	}
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	s.tx = tx
	s.candidates = tx.Bucket(replayCandidateBucket)
	s.starts = tx.Bucket(replayStartTSBucket)
	if s.candidates == nil || s.starts == nil {
		_ = tx.Rollback()
		s.tx, s.candidates, s.starts = nil, nil, nil
		return errors.New("native PITR replay candidate scratch bucket is missing")
	}
	return nil
}

func replayCandidateKey(commitTS uint64, key []byte) []byte {
	encoded := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(encoded, commitTS)
	copy(encoded[8:], key)
	return encoded
}

func replayCandidateValue(startTS uint64, kind byte, short []byte) []byte {
	encoded := make([]byte, 10+len(short))
	binary.BigEndian.PutUint64(encoded, startTS)
	encoded[8] = kind
	if short != nil {
		encoded[9] = 1
		copy(encoded[10:], short)
	}
	return encoded
}

func decodeReplayCandidate(key, value []byte) (ReplayMutation, error) {
	if len(key) <= 8 || len(value) < 10 || value[9] > 1 {
		return ReplayMutation{}, errors.New("native PITR replay candidate scratch record is corrupt")
	}
	short := []byte(nil)
	if value[9] == 1 {
		short = append([]byte{}, value[10:]...)
	} else if len(value) != 10 {
		return ReplayMutation{}, errors.New("native PITR replay candidate scratch record is corrupt")
	}
	kind := value[8]
	if kind != 'P' && kind != 'D' {
		return ReplayMutation{}, errors.New("native PITR replay candidate scratch record has invalid write kind")
	}
	return ReplayMutation{CommitTS: binary.BigEndian.Uint64(key), StartTS: binary.BigEndian.Uint64(value), Key: append([]byte(nil), key[8:]...), Value: short, Delete: kind == 'D', sourceWriteKind: kind}, nil
}

func (s *replayCandidateStore) Put(commitTS, startTS uint64, key []byte, kind byte, short []byte) error {
	if err := s.begin(); err != nil {
		return err
	}
	candidateKey, candidateValue := replayCandidateKey(commitTS, key), replayCandidateValue(startTS, kind, short)
	if existing := s.candidates.Get(candidateKey); existing != nil {
		if !bytes.Equal(existing, candidateValue) {
			return errors.New("stream log contains conflicting write-CF entries")
		}
		return nil
	}
	startKey := make([]byte, 8)
	commitValue := make([]byte, 8)
	binary.BigEndian.PutUint64(startKey, startTS)
	binary.BigEndian.PutUint64(commitValue, commitTS)
	if existing := s.starts.Get(startKey); existing != nil && !bytes.Equal(existing, commitValue) {
		return errors.New("source transaction has multiple commit TSOs")
	}
	if err := s.starts.Put(startKey, commitValue); err != nil {
		return err
	}
	if err := s.candidates.Put(candidateKey, candidateValue); err != nil {
		return err
	}
	s.pendingBytes += uint64(len(candidateKey) + len(candidateValue) + len(startKey) + len(commitValue))
	if s.pendingBytes >= replayDefaultStoreBatchBytes {
		return s.Flush()
	}
	return nil
}

func (s *replayCandidateStore) Flush() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Commit()
	s.tx, s.candidates, s.starts, s.pendingBytes = nil, nil, nil, 0
	if err != nil {
		return fmt.Errorf("commit native PITR replay candidate scratch index: %w", err)
	}
	return nil
}

func (s *replayCandidateStore) Close() error {
	var rollbackErr error
	if s.tx != nil {
		rollbackErr = s.tx.Rollback()
		s.tx, s.candidates, s.starts = nil, nil, nil
	}
	return errors.Join(rollbackErr, s.db.Close(), os.Remove(s.path))
}

func newReplayDiskPlan(scratchDir string) (*ReplayDiskPlan, error) {
	db, path, err := newReplayScratchDB(scratchDir, ".kubebrain-native-pitr-plan-*.db", replayPlanBucket)
	if err != nil {
		return nil, err
	}
	return &ReplayDiskPlan{db: db, path: path}, nil
}

func replayPlanKey(m ReplayMutation) []byte {
	encoded := make([]byte, 16+len(m.Key))
	binary.BigEndian.PutUint64(encoded, m.CommitTS)
	binary.BigEndian.PutUint64(encoded[8:], m.StartTS)
	copy(encoded[16:], m.Key)
	return encoded
}

func replayPlanValue(key []byte, m ReplayMutation, ordinal uint64) []byte {
	encoded := make([]byte, replayPlanRecordHeader+len(m.Value))
	encoded[0] = replayPlanRecordVersion
	if m.Delete {
		encoded[1] = 1
	} else {
		copy(encoded[replayPlanRecordHeader:], m.Value)
	}
	binary.BigEndian.PutUint64(encoded[2:10], ordinal)
	digest := sha256.New()
	_, _ = digest.Write(key)
	_, _ = digest.Write(encoded[:10])
	_, _ = digest.Write(encoded[replayPlanRecordHeader:])
	copy(encoded[10:replayPlanRecordHeader], digest.Sum(nil))
	return encoded
}

func decodeReplayPlanRecord(key, value []byte) (ReplayMutation, uint64, error) {
	if len(key) <= 16 || len(value) < replayPlanRecordHeader || value[0] != replayPlanRecordVersion || value[1] > 1 || (value[1] == 1 && len(value) != replayPlanRecordHeader) {
		return ReplayMutation{}, 0, errors.New("native PITR replay disk plan record is corrupt")
	}
	digest := sha256.New()
	_, _ = digest.Write(key)
	_, _ = digest.Write(value[:10])
	_, _ = digest.Write(value[replayPlanRecordHeader:])
	if !bytes.Equal(value[10:replayPlanRecordHeader], digest.Sum(nil)) {
		return ReplayMutation{}, 0, errors.New("native PITR replay disk plan record checksum does not match key, ordinal, and value")
	}
	mutation := ReplayMutation{CommitTS: binary.BigEndian.Uint64(key), StartTS: binary.BigEndian.Uint64(key[8:]), Key: append([]byte(nil), key[16:]...), Delete: value[1] == 1}
	if !mutation.Delete {
		mutation.Value = append([]byte{}, value[replayPlanRecordHeader:]...)
	}
	return mutation, binary.BigEndian.Uint64(value[2:10]), nil
}

func decodeReplayPlanMutation(key, value []byte) (ReplayMutation, error) {
	mutation, _, err := decodeReplayPlanRecord(key, value)
	return mutation, err
}

func (p *ReplayDiskPlan) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	err := errors.Join(p.db.Close(), os.Remove(p.path))
	p.db = nil
	return err
}

func (p *ReplayDiskPlan) forEach(fn func(int, ReplayMutation) error) error {
	if p == nil || p.db == nil {
		return errors.New("native PITR replay disk plan is closed")
	}
	return p.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(replayPlanBucket)
		if bucket == nil {
			return errors.New("native PITR replay disk plan bucket is missing")
		}
		index := 0
		err := bucket.ForEach(func(key, value []byte) error {
			mutation, ordinal, err := decodeReplayPlanRecord(key, value)
			if err != nil {
				return err
			}
			if ordinal != uint64(index) {
				return errors.New("native PITR replay disk plan record ordinal is not contiguous")
			}
			if err := fn(index, mutation); err != nil {
				return err
			}
			index++
			return nil
		})
		if err != nil {
			return err
		}
		if p.Manifest.Format == ReplayManifestFormat && index != p.Manifest.MutationCount {
			return errors.New("native PITR replay disk plan record count does not match manifest")
		}
		return nil
	})
}

func numberReplayDiskPlan(plan *ReplayDiskPlan) error {
	return numberReplayDiskPlanWithBatchBytes(plan, replayDefaultStoreBatchBytes)
}

func numberReplayDiskPlanWithBatchBytes(plan *ReplayDiskPlan, maxBatchBytes uint64) error {
	if maxBatchBytes == 0 {
		return errors.New("native PITR replay disk plan numbering batch must be positive")
	}
	var nextKey []byte
	ordinal := uint64(0)
	for {
		done := true
		err := plan.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(replayPlanBucket)
			if bucket == nil {
				return errors.New("native PITR replay disk plan bucket is missing")
			}
			cursor := bucket.Cursor()
			key, value := cursor.First()
			if nextKey != nil {
				key, value = cursor.Seek(nextKey)
			}
			batchBytes := uint64(0)
			for key != nil {
				mutation, err := decodeReplayPlanMutation(key, value)
				if err != nil {
					return err
				}
				numbered := replayPlanValue(key, mutation, ordinal)
				if err := bucket.Put(key, numbered); err != nil {
					return err
				}
				ordinal++
				batchBytes += uint64(len(key) + len(numbered))
				key, value = cursor.Next()
				if batchBytes >= maxBatchBytes && key != nil {
					nextKey = append(nextKey[:0], key...)
					done = false
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func digestReplayDiskPlan(plan *ReplayDiskPlan) (string, error) {
	digest := sha256.New()
	if plan.Manifest.MutationCount == 0 {
		if err := writeReplayJSONString(digest, "null"); err != nil {
			return "", err
		}
		return hex.EncodeToString(digest.Sum(nil)), nil
	}
	if err := writeReplayJSONString(digest, "["); err != nil {
		return "", err
	}
	err := plan.forEach(func(index int, mutation ReplayMutation) error {
		if index != 0 {
			if err := writeReplayJSONString(digest, ","); err != nil {
				return err
			}
		}
		encoded, err := json.Marshal(mutation)
		if err != nil {
			return err
		}
		return writeReplayJSONBytes(digest, encoded)
	})
	if err != nil {
		return "", err
	}
	if err := writeReplayJSONString(digest, "]"); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func validateReplayDiskPlan(plan *ReplayDiskPlan, maxTransactionResidentBytes uint64) error {
	if plan == nil || plan.db == nil {
		return errors.New("native PITR replay disk plan is closed")
	}
	manifest := plan.Manifest
	if err := manifest.Validate(); err != nil {
		return err
	}
	if maxTransactionResidentBytes == 0 {
		return errors.New("native PITR replay transaction memory limit must be positive")
	}
	digest, err := digestReplayDiskPlan(plan)
	if err != nil {
		return err
	}
	if digest != manifest.MutationsSHA256 {
		return errors.New("native PITR replay disk plan does not match manifest")
	}
	ks, err := coder.NewKeyspace(manifest.Keyspace)
	if err != nil {
		return err
	}
	start, end := ks.ObjectKeyspaceStart(), ks.ObjectKeyspaceEnd()
	mutations, transactions, puts, deletes := 0, 0, 0, 0
	var previous ReplayMutation
	var transactionBytes uint64
	var firstCommitTS, lastCommitTS uint64
	err = plan.forEach(func(index int, mutation ReplayMutation) error {
		if index != mutations || mutation.StartTS == 0 || mutation.StartTS >= mutation.CommitTS || mutation.CommitTS <= manifest.StartExclusiveTS || mutation.CommitTS > manifest.RestoreTS || len(mutation.Key) == 0 || bytes.Compare(mutation.Key, start) < 0 || bytes.Compare(mutation.Key, end) >= 0 || (mutation.Delete && mutation.Value != nil) || (!mutation.Delete && mutation.Value == nil) || (index > 0 && !replayMutationLess(previous, mutation)) {
			return errors.New("replay disk plan mutations are not canonical, bounded, or complete")
		}
		newTransaction := index == 0 || mutation.CommitTS != previous.CommitTS || mutation.StartTS != previous.StartTS
		if newTransaction {
			transactions++
			transactionBytes = 0
		}
		charge := replayMutationResidentOverhead
		for _, size := range []uint64{uint64(len(mutation.Key)), uint64(len(mutation.Value))} {
			if size > ^uint64(0)-charge {
				return errors.New("native PITR replay transaction resident size overflows uint64")
			}
			charge += size
		}
		if charge > maxTransactionResidentBytes-transactionBytes {
			return fmt.Errorf("native PITR replay source transaction exceeds %d-byte resident memory limit", maxTransactionResidentBytes)
		}
		transactionBytes += charge
		if mutation.Delete {
			deletes++
		} else {
			puts++
		}
		if index == 0 {
			firstCommitTS = mutation.CommitTS
		}
		lastCommitTS = mutation.CommitTS
		mutations++
		previous = mutation
		return nil
	})
	if err != nil {
		return err
	}
	if mutations != manifest.MutationCount || transactions != manifest.TransactionCount || puts != manifest.PutCount || deletes != manifest.DeleteCount || firstCommitTS != manifest.FirstCommitTS || lastCommitTS != manifest.LastCommitTS {
		return errors.New("replay disk plan statistics do not match manifest")
	}
	return nil
}

// ApplyReplayDiskPlan validates the complete immutable disk plan before
// touching target storage, then retains at most one source transaction while
// atomically advancing the same v2 checkpoint used by ApplyReplay.
func ApplyReplayDiskPlan(ctx context.Context, target storage.KvStorage, planSHA string, plan *ReplayDiskPlan, maxTransactionResidentBytes uint64) (ReplayApplyResult, error) {
	if err := validateReplayDiskPlan(plan, maxTransactionResidentBytes); err != nil {
		return ReplayApplyResult{}, err
	}
	manifest := plan.Manifest
	if !sha256RE.MatchString(planSHA) {
		return ReplayApplyResult{}, errors.New("invalid replay plan digest")
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
		if json.Unmarshal(currentBytes, &current) != nil || current.Format != replayCheckpointFormat || current.PlanSHA256 != planSHA || current.MutationsSHA256 != manifest.MutationsSHA256 || current.RestoreTS != manifest.RestoreTS || current.AppliedMutations < 0 || current.AppliedMutations > manifest.MutationCount || (current.AppliedMutations == 0) != (current.LastCommitTS == 0 && current.LastStartTS == 0) {
			return ReplayApplyResult{}, errors.New("target replay checkpoint does not match this exact plan")
		}
		var before, after *ReplayMutation
		if err := plan.forEach(func(index int, mutation ReplayMutation) error {
			if index == current.AppliedMutations-1 {
				copy := mutation
				before = &copy
			}
			if index == current.AppliedMutations {
				copy := mutation
				after = &copy
			}
			return nil
		}); err != nil {
			return ReplayApplyResult{}, err
		}
		if current.AppliedMutations > 0 && (before == nil || before.CommitTS != current.LastCommitTS || before.StartTS != current.LastStartTS) {
			return ReplayApplyResult{}, errors.New("target replay checkpoint is not on a transaction boundary")
		}
		if after != nil && after.CommitTS == current.LastCommitTS && after.StartTS == current.LastStartTS {
			return ReplayApplyResult{}, errors.New("target replay checkpoint splits a source transaction")
		}
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return ReplayApplyResult{}, err
	}
	checkpointTransactions := 0
	var checkpointPrevious ReplayMutation
	if err := plan.forEach(func(index int, mutation ReplayMutation) error {
		if index >= current.AppliedMutations {
			return nil
		}
		if index == 0 || mutation.CommitTS != checkpointPrevious.CommitTS || mutation.StartTS != checkpointPrevious.StartTS {
			checkpointTransactions++
		}
		checkpointPrevious = mutation
		return nil
	}); err != nil {
		return ReplayApplyResult{}, err
	}
	result := ReplayApplyResult{CheckpointMutationsBefore: current.AppliedMutations, CheckpointTransactionsBefore: checkpointTransactions, Resumed: resumed, LastCommitTS: current.LastCommitTS, LastStartTS: current.LastStartTS}
	if manifest.MutationCount == 0 && !resumed {
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
		return result, nil
	}

	commitTransaction := func(end int, mutations []ReplayMutation) error {
		if len(mutations) == 0 {
			return nil
		}
		next := replayCheckpoint{Format: replayCheckpointFormat, PlanSHA256: planSHA, MutationsSHA256: manifest.MutationsSHA256, RestoreTS: manifest.RestoreTS, AppliedMutations: end, LastCommitTS: mutations[0].CommitTS, LastStartTS: mutations[0].StartTS}
		nextBytes, err := json.Marshal(next)
		if err != nil {
			return err
		}
		batch := target.BeginBatchWrite()
		for _, mutation := range mutations {
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
			return fmt.Errorf("commit replay source TSO %d: %w", mutations[0].CommitTS, err)
		}
		current, currentBytes = next, nextBytes
		result.AppliedMutations += len(mutations)
		result.AppliedTransactions++
		result.LastCommitTS, result.LastStartTS = next.LastCommitTS, next.LastStartTS
		clear(mutations)
		return nil
	}
	transactionStart := current.AppliedMutations
	var transaction []ReplayMutation
	err = plan.forEach(func(index int, mutation ReplayMutation) error {
		if index < current.AppliedMutations {
			return nil
		}
		if len(transaction) != 0 && (mutation.CommitTS != transaction[0].CommitTS || mutation.StartTS != transaction[0].StartTS) {
			if err := commitTransaction(index, transaction); err != nil {
				return err
			}
			transaction = transaction[:0]
			transactionStart = index
		}
		transaction = append(transaction, mutation)
		return nil
	})
	if err != nil {
		return result, err
	}
	if err := commitTransaction(transactionStart+len(transaction), transaction); err != nil {
		return result, err
	}
	return result, nil
}

// MaterializeReplayDiskPlanWithScratchDir builds the exact canonical replay
// order in rebuildable scratch storage. Physical write candidates, duplicate
// detection, source-transaction consistency, and final ordering never require
// an O(all output) Go slice.
func MaterializeReplayDiskPlanWithScratchDir(logs LogArtifactReceipt, logsSHA, root, scratchDir string, startExclusive, restoreTS uint64) (_ *ReplayDiskPlan, retErr error) {
	if err := logs.Validate(); err != nil {
		return nil, err
	}
	if !sha256RE.MatchString(logsSHA) || root == "" || startExclusive == 0 || restoreTS <= startExclusive || restoreTS > logs.GlobalCheckpointTS {
		return nil, errors.New("invalid replay receipt digest, root, or TSO window")
	}
	for _, object := range logs.Objects {
		path := filepath.Join(root, filepath.FromSlash(object.Name))
		if err := verifyReplayObject(path, object); err != nil {
			return nil, fmt.Errorf("recheck log object %q: %w", object.Name, err)
		}
	}
	expected, err := replaySegments(root, logs.Objects)
	if err != nil {
		return nil, err
	}
	ks, err := coder.NewKeyspace(logs.Keyspace)
	if err != nil {
		return nil, err
	}
	start, end := ks.ObjectKeyspaceStart(), ks.ObjectKeyspaceEnd()
	defaults, err := newReplayDefaultStore(scratchDir)
	if err != nil {
		return nil, fmt.Errorf("create replay default-CF index: %w", err)
	}
	candidates, err := newReplayCandidateStore(scratchDir)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create replay write-candidate index: %w", err), defaults.Close())
	}
	plan, err := newReplayDiskPlan(scratchDir)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create replay canonical disk plan: %w", err), candidates.Close(), defaults.Close())
	}
	succeeded := false
	defer func() {
		retErr = errors.Join(retErr, candidates.Close(), defaults.Close())
		if !succeeded || retErr != nil {
			retErr = errors.Join(retErr, plan.Close())
		}
	}()

	for _, object := range expected {
		for _, segment := range object.segments {
			entries := int64(0)
			err := walkReplaySegment(root, object.path, segment, func(encodedKey, value []byte) error {
				entries++
				rawKey, ts, err := decodeMVCCKey(encodedKey)
				if err != nil {
					return err
				}
				if bytes.Compare(rawKey, start) < 0 || bytes.Compare(rawKey, end) >= 0 {
					return errors.New("stream log contains a key outside the plan tenant range")
				}
				if len(value) == 0 {
					return nil
				}
				switch segment.Cf {
				case "default":
					return defaults.Put(replayValueKey(rawKey, ts), value)
				case "write":
					kind, startTS, short, err := decodeWriteValue(value)
					if err != nil {
						return err
					}
					if startTS >= ts {
						return errors.New("write-CF commit TSO does not follow start TSO")
					}
					if ts > startExclusive && ts <= restoreTS && (kind == 'P' || kind == 'D') {
						return candidates.Put(ts, startTS, rawKey, kind, short)
					}
					return nil
				default:
					return fmt.Errorf("unsupported stream column family %q", segment.Cf)
				}
			})
			if err != nil {
				return nil, fmt.Errorf("decode %s log entry: %w", segment.Cf, err)
			}
			if entries != segment.NumberOfEntries {
				return nil, errors.New("decoded log entry count does not match stream metadata")
			}
		}
	}
	if err := errors.Join(defaults.Flush(), candidates.Flush()); err != nil {
		return nil, err
	}

	var planTx *bolt.Tx
	var planBucket *bolt.Bucket
	var pendingBytes uint64
	flushPlan := func() error {
		if planTx == nil {
			return nil
		}
		err := planTx.Commit()
		planTx, planBucket, pendingBytes = nil, nil, 0
		return err
	}
	putPlan := func(mutation ReplayMutation) error {
		if planTx == nil {
			var err error
			planTx, err = plan.db.Begin(true)
			if err != nil {
				return err
			}
			planBucket = planTx.Bucket(replayPlanBucket)
			if planBucket == nil {
				return errors.New("native PITR replay disk plan bucket is missing")
			}
		}
		key := replayPlanKey(mutation)
		value := replayPlanValue(key, mutation, 0)
		if err := planBucket.Put(key, value); err != nil {
			return err
		}
		pendingBytes += uint64(len(key) + len(value))
		if pendingBytes >= replayDefaultStoreBatchBytes {
			return flushPlan()
		}
		return nil
	}
	defer func() {
		if planTx != nil {
			retErr = errors.Join(retErr, planTx.Rollback())
		}
	}()

	manifest := ReplayManifest{Format: ReplayManifestFormat, LogArtifactReceiptSHA256: logsSHA, ArtifactManifestSHA256: logs.ManifestSHA256, Keyspace: logs.Keyspace, StartExclusiveTS: startExclusive, RestoreTS: restoreTS, AllEntriesInTenantRange: true, AllPutsResolved: true, ExactLocalMirrorRechecked: true}
	err = candidates.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(replayCandidateBucket)
		if bucket == nil {
			return errors.New("native PITR replay candidate scratch bucket is missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			mutation, err := decodeReplayCandidate(key, value)
			if err != nil {
				return err
			}
			if mutation.Delete {
				mutation.Value = nil
			} else {
				if mutation.Value == nil {
					resolved, ok, err := defaults.Get(replayValueKey(mutation.Key, mutation.StartTS))
					if err != nil {
						return err
					}
					if !ok {
						return fmt.Errorf("PUT at commit TSO %d lacks its default-CF value", mutation.CommitTS)
					}
					mutation.Value = resolved
				}
			}
			mutation.sourceWriteKind = 0
			return putPlan(mutation)
		})
	})
	if err != nil {
		return nil, err
	}
	if err := flushPlan(); err != nil {
		return nil, fmt.Errorf("commit native PITR replay disk plan: %w", err)
	}
	if err := numberReplayDiskPlan(plan); err != nil {
		return nil, fmt.Errorf("number native PITR replay disk plan: %w", err)
	}
	var previousCommit, previousStart uint64
	err = plan.forEach(func(index int, mutation ReplayMutation) error {
		if index == 0 {
			manifest.FirstCommitTS = mutation.CommitTS
		}
		manifest.LastCommitTS = mutation.CommitTS
		manifest.MutationCount++
		if index == 0 || mutation.CommitTS != previousCommit || mutation.StartTS != previousStart {
			manifest.TransactionCount++
			previousCommit, previousStart = mutation.CommitTS, mutation.StartTS
		}
		if mutation.Delete {
			manifest.DeleteCount++
		} else {
			manifest.PutCount++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	plan.Manifest = manifest
	manifest.MutationsSHA256, err = digestReplayDiskPlan(plan)
	if err != nil {
		return nil, err
	}
	plan.Manifest = manifest
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	for _, object := range logs.Objects {
		path := filepath.Join(root, filepath.FromSlash(object.Name))
		if err := verifyReplayObject(path, object); err != nil {
			return nil, fmt.Errorf("post-materialization log object %q: %w", object.Name, err)
		}
	}
	succeeded = true
	return plan, nil
}

var _ io.Closer = (*ReplayDiskPlan)(nil)
