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

package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/util"
)

const (
	latestMetadataBackfillQueueCapacity = 1024
	latestMetadataBackfillQueueMaxBytes = 8 << 20
	latestMetadataBackfillTaskOverhead  = 128
)

const (
	latestMetadataBackfillOutcomeHealed       = "healed"
	latestMetadataBackfillOutcomeConcurrent   = "concurrent"
	latestMetadataBackfillOutcomeFenced       = "fenced"
	latestMetadataBackfillOutcomeFailed       = "failed"
	latestMetadataBackfillOutcomeDropped      = "dropped"
	latestMetadataBackfillOutcomeDeduplicated = "deduplicated"
)

var latestMetadataBackfillOutcomes = []string{
	latestMetadataBackfillOutcomeHealed,
	latestMetadataBackfillOutcomeConcurrent,
	latestMetadataBackfillOutcomeFenced,
	latestMetadataBackfillOutcomeFailed,
	latestMetadataBackfillOutcomeDropped,
	latestMetadataBackfillOutcomeDeduplicated,
}

type latestMetadataBackfillID struct {
	keyHash  [sha256.Size]byte
	revision uint64
}

type latestMetadataBackfillTask struct {
	key                   []byte
	revision              uint64
	metadata              EtcdMetadata
	expectedMetadata      []byte
	expectedMetadataFound bool
}

type latestMetadataBackfillQueue struct {
	ch      chan latestMetadataBackfillTask
	mu      sync.Mutex
	pending map[latestMetadataBackfillID]struct{}
	bytes   int
}

func newLatestMetadataBackfillQueue() *latestMetadataBackfillQueue {
	return &latestMetadataBackfillQueue{
		ch:      make(chan latestMetadataBackfillTask, latestMetadataBackfillQueueCapacity),
		pending: make(map[latestMetadataBackfillID]struct{}, latestMetadataBackfillQueueCapacity),
	}
}

func initLatestMetadataBackfillMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, outcome := range latestMetadataBackfillOutcomes {
		_ = metricCli.EmitCounter("backend.latest_metadata.backfill_outcome", int64(0), metrics.Tag("outcome", outcome))
	}
}

func emitLatestMetadataBackfillOutcome(metricCli metrics.Metrics, outcome string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("backend.latest_metadata.backfill_outcome", 1, metrics.Tag("outcome", outcome))
}

// queueLatestMetadataBackfill schedules an auxiliary, revision-neutral repair.
// At most one task per key/revision can be pending, and readers never block when
// the bounded queue is saturated.
func (b *backend) queueLatestMetadataBackfill(task latestMetadataBackfillTask) {
	queue := b.latestMetadataBackfill
	if queue == nil {
		return
	}
	task.key = append([]byte(nil), task.key...)
	task.expectedMetadata = append([]byte(nil), task.expectedMetadata...)
	id := latestMetadataBackfillTaskID(task)
	taskBytes := latestMetadataBackfillTaskSize(task)

	queue.mu.Lock()
	if _, exists := queue.pending[id]; exists {
		queue.mu.Unlock()
		emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeDeduplicated)
		return
	}
	if taskBytes > latestMetadataBackfillQueueMaxBytes-queue.bytes {
		queue.mu.Unlock()
		emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeDropped)
		return
	}
	queue.pending[id] = struct{}{}
	queue.bytes += taskBytes
	select {
	case queue.ch <- task:
		queue.mu.Unlock()
	default:
		delete(queue.pending, id)
		queue.bytes -= taskBytes
		queue.mu.Unlock()
		emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeDropped)
	}
}

func latestMetadataBackfillTaskID(task latestMetadataBackfillTask) latestMetadataBackfillID {
	return latestMetadataBackfillID{keyHash: sha256.Sum256(task.key), revision: task.revision}
}

func latestMetadataBackfillTaskSize(task latestMetadataBackfillTask) int {
	return len(task.key) + len(task.expectedMetadata) + latestMetadataBackfillTaskOverhead
}

func (b *backend) runLatestMetadataBackfill(ctx context.Context) {
	queue := b.latestMetadataBackfill
	if queue == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-queue.ch:
			err := b.applyLatestMetadataBackfill(ctx, task)
			id := latestMetadataBackfillTaskID(task)
			queue.mu.Lock()
			delete(queue.pending, id)
			queue.bytes -= latestMetadataBackfillTaskSize(task)
			queue.mu.Unlock()

			switch {
			case err == nil:
				emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeHealed)
			case errors.Is(err, storage.ErrCASFailed):
				emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeConcurrent)
			case isCorruptAlarmFenceError(err), errors.Is(err, ErrRestorationFenced),
				errors.Is(err, ErrLeadershipFenced):
				emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeFenced)
			default:
				emitLatestMetadataBackfillOutcome(b.metricCli, latestMetadataBackfillOutcomeFailed)
				if ctx.Err() == nil {
					klog.ErrorS(err, "failed to backfill latest metadata", "key", util.LoggedKey(task.key), "revision", task.revision)
				}
			}
		}
	}
}

// applyLatestMetadataBackfill commits only if the live revision index and the
// observed metadata row are unchanged. The no-op revision CAS conflicts with a
// concurrent Put/Delete; the metadata CAS/PutIfNotExist prevents an old reader
// from replacing a repair or a newer writer's row. The corrupt-alarm guard
// makes the auxiliary mutation share the same safety boundary as user writes.
func (b *backend) applyLatestMetadataBackfill(ctx context.Context, task latestMetadataBackfillTask) error {
	if task.revision == 0 {
		return fmt.Errorf("latest metadata backfill revision is zero")
	}
	desired := latestMetadata{ModRevision: task.revision, Metadata: task.metadata}
	if err := ValidateEtcdMetadataAtRevision(task.metadata, task.revision, "latest metadata backfill"); err != nil {
		return err
	}
	members, corruptGenerationRaw, corruptGenerationExists, corruptGuard, err := b.readCorruptAlarmCommitState(ctx)
	if err != nil {
		return err
	}
	if len(members) != 0 {
		return ErrCorruptAlarmActive
	}

	batch := b.kv.BeginBatchWrite()
	revisionValue := uint64ToBytes(task.revision)
	batch.CAS(b.coder.EncodeRevisionKey(task.key), revisionValue, revisionValue, 0)
	metadataKey := b.ks.EncodeLatestMetadataKey(task.key)
	if task.expectedMetadataFound {
		batch.CAS(metadataKey, encodeLatestMetadata(desired), task.expectedMetadata, 0)
	} else {
		batch.PutIfNotExist(metadataKey, encodeLatestMetadata(desired), 0)
	}
	stageCorruptAlarmCommitGuard(batch, b.ks.EncodeInternalKey(corruptGuard.key), corruptGuard)
	commitErr := batch.Commit(ctx)
	if !errors.Is(commitErr, storage.ErrCASFailed) {
		return commitErr
	}
	// A failed metadata/revision compare is an ordinary concurrent repair or
	// user update. Re-read only on that slow path so an alarm generation change
	// is reported as a safety fence rather than being hidden as contention.
	currentMembers, currentGenerationRaw, currentGenerationExists, alarmErr := b.readStableCorruptAlarmState(ctx)
	if alarmErr != nil {
		return alarmErr
	}
	if len(currentMembers) != 0 {
		return ErrCorruptAlarmActive
	}
	if currentGenerationExists != corruptGenerationExists ||
		!bytes.Equal(currentGenerationRaw, corruptGenerationRaw) {
		return ErrCorruptAlarmChanged
	}
	return commitErr
}
