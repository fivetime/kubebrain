// Copyright 2022 ByteDance and/or its affiliates
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

package election

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// ResourceLockManager is the manager provide resource lock based on common storage
type ResourceLockManager interface {
	// GetResourceLock should base on storage.KvStorage
	GetResourceLock() resourcelock.Interface
}

func getElectionKey(prefix string) []byte {
	return []byte(fmt.Sprintf("%s/election", prefix))
}

const storageFenceShardCount = 256

func getStorageFenceKey(prefix string, shard uint64) []byte {
	return []byte(fmt.Sprintf("%s/election-fence/%02x", prefix, shard%storageFenceShardCount))
}

// StorageFenceTokenProvider exposes one shard of the process ownership token
// installed atomically with leader acquisition. User transactions CAS a shard
// so a successor acquisition and an old-leader commit cannot both succeed.
type StorageFenceTokenProvider interface {
	StorageFenceToken(shard uint64) (key, expected []byte, ok bool)
}

type Config struct {
	Prefix   string
	Identity string
	Timeout  time.Duration
}

// NewResourceLockManager build a manager for resource lock
func NewResourceLockManager(config Config, store storage.KvStorage) ResourceLockManager {
	fenceKeys := make([][]byte, storageFenceShardCount)
	for shard := range fenceKeys {
		fenceKeys[shard] = getStorageFenceKey(config.Prefix, uint64(shard))
	}
	return &resourceLockManager{
		resourceLock: &resourceLock{
			store: store,
			lockConfig: resourcelock.ResourceLockConfig{
				Identity: config.Identity,
			},
			electionKey: getElectionKey(config.Prefix),
			fenceKeys:   fenceKeys,
			timeout:     config.Timeout,
		},
	}
}

type resourceLockManager struct {
	*resourceLock
}

// GetResourceLock implements ResourceLockManager interface
func (r *resourceLockManager) GetResourceLock() resourcelock.Interface {
	return r.resourceLock
}

type resourceLock struct {
	store      storage.KvStorage
	lockConfig resourcelock.ResourceLockConfig
	// mu guards the mutable election state (record/lastVal/tso). It is written by
	// the single leader-election goroutine (Get/Create/Update) and read
	// concurrently by RPC goroutines via Describe (#60/#68). Storage I/O is done
	// outside the lock; only the field access is guarded.
	mu          sync.Mutex
	record      resourcelock.LeaderElectionRecord
	lastVal     []byte
	tso         uint64
	electionKey []byte
	fenceKeys   [][]byte
	timeout     time.Duration
	// fenceToken is unique to this process's current/most-recent ownership.
	// fenceInstalled becomes false after observing/releasing another holder so
	// the next acquisition rotates all shards atomically with the election CAS.
	fenceToken     []byte
	fenceInstalled bool
}

// Get implements resourcelock.Interface. The returned []byte is the raw stored
// record the leaderelection loop feeds back as the CAS old-value on the next
// Update (client-go v0.20+ interface).
func (r *resourceLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	klog.V(8).Info("[resource lock] get lock")

	if err := r.getRecord(ctx); err != nil {
		return nil, nil, err
	}

	if err := r.getTso(ctx); err != nil {
		return nil, nil, err
	}

	// Return a snapshot copy so the caller never reads the guarded field while
	// a later Get/Update rewrites it. LeaderElectionRecord has only value fields,
	// so this shallow copy is independent.
	r.mu.Lock()
	recordCopy := r.record
	rawCopy := append([]byte(nil), r.lastVal...)
	r.mu.Unlock()
	return &recordCopy, rawCopy, nil
}

func (r *resourceLock) getRecord(parent context.Context) (err error) {
	ctx, cancel := r.genContext(parent)
	defer cancel()
	val, err := r.store.Get(ctx, r.electionKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return apierrors.NewNotFound(schema.GroupResource{}, string(r.electionKey))
		}
		return err
	}
	record, err := decodeLeaderElectionRecord(val)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.lastVal = val
	r.record = record
	if record.HolderIdentity != r.lockConfig.Identity {
		r.fenceInstalled = false
	}
	r.mu.Unlock()
	return nil
}

func decodeLeaderElectionRecord(raw []byte) (resourcelock.LeaderElectionRecord, error) {
	var record resourcelock.LeaderElectionRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return resourcelock.LeaderElectionRecord{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return resourcelock.LeaderElectionRecord{}, errors.New("leader election record contains trailing JSON")
	}
	return record, nil
}

func (r *resourceLock) getTso(parent context.Context) (err error) {
	ctx, cancel := r.genContext(parent)
	defer cancel()
	tso, err := r.store.GetTimestampOracle(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.tso = tso
	r.mu.Unlock()
	return nil
}

// Create implements resourcelock.Interface
func (r *resourceLock) Create(parent context.Context, ler resourcelock.LeaderElectionRecord) error {
	lerBytes, err := json.Marshal(ler)
	if err != nil {
		return err
	}
	batch := r.store.BeginBatchWrite()
	batch.PutIfNotExist(r.electionKey, lerBytes, 0)
	var fenceToken []byte
	if ler.HolderIdentity == r.lockConfig.Identity {
		fenceToken = []byte(uuid.NewString())
		for _, key := range r.fenceKeys {
			batch.Put(key, fenceToken, 0)
		}
	}
	ctx, cancel := r.genContext(parent)
	defer cancel()
	err = batch.Commit(ctx)
	if err != nil {
		return err
	}
	// Bound the TSO read with the same election-timeout ctx as the commit rather
	// than an unbounded Background, so a wedged storage call cannot stall the
	// leader-election loop (audit E10).
	tso, err := r.store.GetTimestampOracle(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.lastVal = lerBytes
	r.tso = tso
	r.record = ler
	if len(fenceToken) > 0 {
		r.fenceToken = fenceToken
		r.fenceInstalled = true
	}
	r.mu.Unlock()
	return nil
}

// Update implements resourcelock.Interface
func (r *resourceLock) Update(parent context.Context, ler resourcelock.LeaderElectionRecord) error {
	klog.V(8).Info("[resource lock] update lock")
	r.mu.Lock()
	tso := r.tso
	lastVal := r.lastVal
	installFence := ler.HolderIdentity == r.lockConfig.Identity && !r.fenceInstalled
	r.mu.Unlock()
	if tso == 0 {
		return errors.New("endpoint not initialized, call get or create first")
	}

	recordBytes, err := json.Marshal(ler)
	if err != nil {
		return err
	}

	batch := r.store.BeginBatchWrite()
	batch.CAS(r.electionKey, recordBytes, lastVal, 0)
	var fenceToken []byte
	if installFence {
		fenceToken = []byte(uuid.NewString())
		for _, key := range r.fenceKeys {
			batch.Put(key, fenceToken, 0)
		}
	}
	ctx, cancel := r.genContext(parent)
	defer cancel()
	err = batch.Commit(ctx)
	if err != nil {
		return err
	}
	// The ownership token became durable with the election record even if the
	// following diagnostic TSO read fails. Retain it for in-flight write fences.
	r.mu.Lock()
	if len(fenceToken) > 0 {
		r.fenceToken = fenceToken
		r.fenceInstalled = true
	} else if ler.HolderIdentity != r.lockConfig.Identity {
		r.fenceInstalled = false
	}
	r.mu.Unlock()

	// Bound with the election-timeout ctx, not Background (audit E10).
	newTso, err := r.store.GetTimestampOracle(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.tso = newTso
	r.lastVal = recordBytes
	r.record = ler
	r.mu.Unlock()
	return nil
}

func (r *resourceLock) StorageFenceToken(shard uint64) (key, expected []byte, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.fenceToken) == 0 || len(r.fenceKeys) == 0 {
		return nil, nil, false
	}
	key = append([]byte(nil), r.fenceKeys[shard%uint64(len(r.fenceKeys))]...)
	expected = append([]byte(nil), r.fenceToken...)
	return key, expected, true
}

// RecordEvent implements resourcelock.Interface
func (r *resourceLock) RecordEvent(s string) {
	klog.InfoS("record event", "identity", r.lockConfig.Identity, "events", s)
}

// Identity implements resourcelock.Interface
func (r *resourceLock) Identity() string {
	return r.lockConfig.Identity
}

// NoLeader is the holder identity Describe() reports when the lock has no
// holder. Callers deciding "is a leader currently known" should use
// IsLeaderKnown rather than string-matching this sentinel (and "") by hand.
const NoLeader = "empty"

// IsLeaderKnown reports whether addr names a real current leader (not the
// no-holder sentinel and not an empty string).
func IsLeaderKnown(addr string) bool {
	return addr != "" && addr != NoLeader
}

func (r *resourceLock) Describe() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.record.HolderIdentity) > 0 {
		return fmt.Sprintf("%s,%d", r.record.HolderIdentity, r.tso)
	}
	return fmt.Sprintf("%s,%d", NoLeader, r.tso)
}

func (r *resourceLock) genContext(ctx context.Context) (newCtx context.Context, cancel func()) {
	return context.WithTimeout(ctx, r.timeout)
}
