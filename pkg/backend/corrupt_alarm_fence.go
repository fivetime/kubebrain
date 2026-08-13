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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

const corruptAlarmFenceShardCount = 256

var corruptAlarmFenceControlKey = []byte("alarms/corrupt-fence-version")

// corruptAlarmFenceShardOffset gives each backend identity a stable place in
// the shard ring. Successive leaders therefore do not all restart on shard 00
// and manufacture avoidable TiKV write conflicts during rolling restarts.
// Empty identities retain the historical zero offset for embedded callers.
func corruptAlarmFenceShardOffset(identity string) uint64 {
	if identity == "" {
		return 0
	}
	digest := sha256.Sum256([]byte(identity))
	return binary.BigEndian.Uint64(digest[:8]) % corruptAlarmFenceShardCount
}

func corruptAlarmFenceShardKey(shard uint64) []byte {
	return []byte(fmt.Sprintf("alarms/corrupt-fence/%02x", shard%corruptAlarmFenceShardCount))
}

type corruptAlarmCommitGuard struct {
	key      []byte
	expected []byte
	exists   bool
}

// corruptAlarmCommitGuardFor selects one write-conflict shard. Legacy tenants
// have no control key; their transactions guard that key's absence until the
// first Arm/Disarm initializes all shards atomically.
func (b *backend) corruptAlarmCommitGuardFor(ctx context.Context, generationRaw []byte, generationExists bool) (corruptAlarmCommitGuard, error) {
	control, err := b.InternalGet(ctx, corruptAlarmFenceControlKey)
	if err != nil && !errorsIsKeyNotFound(err) {
		return corruptAlarmCommitGuard{}, err
	}
	if errorsIsKeyNotFound(err) {
		return corruptAlarmCommitGuard{key: corruptAlarmFenceControlKey}, nil
	}
	if !bytes.Equal(control, []byte{1}) {
		return corruptAlarmCommitGuard{}, invalidAlarmMetadataf("corrupt alarm fence version is %x", control)
	}
	if !generationExists {
		return corruptAlarmCommitGuard{}, invalidAlarmMetadataf("corrupt alarm fence exists without generation")
	}
	shard := b.corruptAlarmFenceShard.Add(1) - 1
	key := corruptAlarmFenceShardKey(shard)
	raw, err := b.InternalGet(ctx, key)
	if err != nil {
		if errorsIsKeyNotFound(err) {
			return corruptAlarmCommitGuard{}, invalidAlarmMetadataf("corrupt alarm fence shard %02x is missing", shard%corruptAlarmFenceShardCount)
		}
		return corruptAlarmCommitGuard{}, err
	}
	if !bytes.Equal(raw, generationRaw) {
		_, currentGenerationRaw, currentGenerationExists, generationErr := b.readCorruptAlarmGeneration(ctx)
		if generationErr != nil {
			return corruptAlarmCommitGuard{}, generationErr
		}
		if currentGenerationExists != generationExists || !bytes.Equal(currentGenerationRaw, generationRaw) {
			return corruptAlarmCommitGuard{}, ErrCorruptAlarmChanged
		}
		return corruptAlarmCommitGuard{}, invalidAlarmMetadataf("corrupt alarm fence shard %02x generation mismatch", shard%corruptAlarmFenceShardCount)
	}
	return corruptAlarmCommitGuard{key: key, expected: raw, exists: true}, nil
}

func stageCorruptAlarmCommitGuard(batch storage.BatchWrite, encodedKey []byte, guard corruptAlarmCommitGuard) {
	if guard.exists {
		batch.CAS(encodedKey, guard.expected, guard.expected, 0)
		return
	}
	batch.PutIfNotExist(encodedKey, []byte{0}, 0)
	batch.Del(encodedKey)
}

func (b *backend) appendCorruptAlarmFenceOps(ctx context.Context, ops []InternalCASOp, nextGeneration []byte, generationRaw []byte, generationExists bool) ([]InternalCASOp, error) {
	control, err := b.InternalGet(ctx, corruptAlarmFenceControlKey)
	legacy := errorsIsKeyNotFound(err)
	if err != nil && !legacy {
		return nil, err
	}
	if !legacy && !bytes.Equal(control, []byte{1}) {
		return nil, invalidAlarmMetadataf("corrupt alarm fence version is %x", control)
	}
	if !legacy && !generationExists {
		return nil, invalidAlarmMetadataf("corrupt alarm fence exists without generation")
	}
	if legacy {
		ops = append(ops, InternalCASOp{Key: corruptAlarmFenceControlKey, Value: []byte{1}})
	}
	for shard := uint64(0); shard < corruptAlarmFenceShardCount; shard++ {
		op := InternalCASOp{Key: corruptAlarmFenceShardKey(shard), Value: nextGeneration}
		if !legacy {
			op.Expected = generationRaw
			op.ExpectedExists = true
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// ValidateCorruptAlarmMetadata performs the expensive all-shard validation
// used by leadership publication and logical snapshots. Hot request paths read
// only their selected shard.
func (b *backend) ValidateCorruptAlarmMetadata(ctx context.Context) error {
	_, generationRaw, generationExists, err := b.readStableCorruptAlarmState(ctx)
	if err != nil {
		return err
	}
	control, err := b.InternalGet(ctx, corruptAlarmFenceControlKey)
	legacy := errors.Is(err, storage.ErrKeyNotFound)
	if err != nil && !legacy {
		return err
	}
	if legacy {
		// Without the control key no hot transaction trusts a shard: it guards
		// control-key absence instead. A first Arm/Disarm creates control and
		// overwrites every shard atomically, so stale orphan rows are harmless.
		return nil
	}
	shards, err := b.InternalRange(ctx, []byte("alarms/corrupt-fence/"))
	if err != nil {
		return err
	}
	if !bytes.Equal(control, []byte{1}) {
		return invalidAlarmMetadataf("corrupt alarm fence version is %x", control)
	}
	if !generationExists {
		return invalidAlarmMetadataf("corrupt alarm fence exists without generation")
	}
	if len(shards) != corruptAlarmFenceShardCount {
		return invalidAlarmMetadataf("corrupt alarm fence has %d shards, want %d", len(shards), corruptAlarmFenceShardCount)
	}
	for shard := uint64(0); shard < corruptAlarmFenceShardCount; shard++ {
		key := string(corruptAlarmFenceShardKey(shard))
		if !bytes.Equal(shards[key], generationRaw) {
			return invalidAlarmMetadataf("corrupt alarm fence shard %02x generation mismatch", shard)
		}
	}
	return nil
}

// Keep the storage sentinel check local so the fence helper remains readable.
func errorsIsKeyNotFound(err error) bool { return errors.Is(err, storage.ErrKeyNotFound) }
