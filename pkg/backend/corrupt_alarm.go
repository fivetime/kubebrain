package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type corruptAlarmCommitGuardContextKey struct{}

// WithCorruptAlarmCommitGuard requires an internal CAS mutation to share the
// same atomic CORRUPT-alarm boundary as an etcd applier mutation.
func WithCorruptAlarmCommitGuard(ctx context.Context) context.Context {
	return context.WithValue(ctx, corruptAlarmCommitGuardContextKey{}, true)
}

func corruptAlarmCommitGuardRequired(ctx context.Context) bool {
	required, _ := ctx.Value(corruptAlarmCommitGuardContextKey{}).(bool)
	return required
}

var (
	corruptAlarmKey           = []byte("alarms/corrupt")
	corruptAlarmGenerationKey = []byte("alarms/corrupt-generation")

	// ErrCorruptAlarmChanged means another replica activated or changed the
	// alarm while a disarm was validating durable evidence. Requiring a fresh
	// operator request prevents that new signal from being silently consumed.
	ErrCorruptAlarmChanged = errors.New("corrupt alarm changed during disarm")
	// ErrCorruptAlarmActive rejects a logical mutation whose commit-time alarm
	// snapshot contains at least one CORRUPT owner.
	ErrCorruptAlarmActive = errors.New("corrupt alarm is active")
)

func (b *backend) ArmCorrupt(ctx context.Context, memberID uint64) error {
	ctx, unlock := b.lockCorruptAlarm(ctx)
	defer unlock()
	for {
		members, raw, exists, err := b.readCorruptAlarms(ctx)
		if err != nil {
			return err
		}
		generation, generationRaw, generationExists, err := b.readCorruptAlarmGeneration(ctx)
		if err != nil {
			return err
		}
		index := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
		value := raw
		if index == len(members) || members[index] != memberID {
			members = append(members, 0)
			copy(members[index+1:], members[index:])
			members[index] = memberID
			value, err = json.Marshal(members)
			if err != nil {
				return err
			}
		}
		nextGeneration, err := encodeNextCorruptAlarmGeneration(generation)
		if err != nil {
			return err
		}
		ops := []InternalCASOp{
			{Key: corruptAlarmKey, Value: value, Expected: raw, ExpectedExists: exists},
			{Key: corruptAlarmGenerationKey, Value: nextGeneration, Expected: generationRaw, ExpectedExists: generationExists},
		}
		ops, err = b.appendCorruptAlarmFenceOps(ctx, ops, nextGeneration, generationRaw, generationExists)
		if err != nil {
			return err
		}
		err = b.InternalCAS(ctx, ops)
		if errors.Is(err, storage.ErrCASFailed) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		return err
	}
}

func (b *backend) CorruptAlarms(ctx context.Context) ([]uint64, error) {
	members, _, _, err := b.readStableCorruptAlarmState(ctx)
	return members, err
}

// readStableCorruptAlarmState reads one batch snapshot when available, or uses
// the generation as a seqlock around the separately-read member set. Arm/Disarm
// update both keys atomically and always advance the generation, so equal
// before/after generations certify one logical alarm state in the fallback.
func (b *backend) readStableCorruptAlarmState(ctx context.Context) ([]uint64, []byte, bool, error) {
	// One snapshot certifies the same atomic member/generation state without
	// three serial reads. Keep explicit historical snapshots on InternalGet's
	// timestamp-aware path; BatchGetter alone cannot promise that timestamp.
	if _, pinned := storage.SnapshotTimestampFromContext(ctx); !pinned {
		if getter, ok := storage.FindCapability[storage.BatchGetter](b.kv); ok {
			keys := [][]byte{b.ks.EncodeInternalKey(corruptAlarmGenerationKey), b.ks.EncodeInternalKey(corruptAlarmKey)}
			values, err := getter.BatchGet(ctx, keys)
			if err != nil {
				return nil, nil, false, err
			}
			generation, exists := values[string(keys[0])]
			// Preserve the fallback's generation-before-members validation order.
			if exists {
				if _, err := decodeCorruptAlarmGeneration(generation); err != nil {
					return nil, nil, false, err
				}
			}
			var members []uint64
			if raw, present := values[string(keys[1])]; present {
				members, err = decodeOrderedCorruptAlarmMembers(raw)
				if err != nil {
					return nil, nil, false, err
				}
			}
			return members, generation, exists, nil
		}
	}
	for {
		_, beforeRaw, beforeExists, err := b.readCorruptAlarmGeneration(ctx)
		if err != nil {
			return nil, nil, false, err
		}
		members, _, _, err := b.readCorruptAlarms(ctx)
		if err != nil {
			return nil, nil, false, err
		}
		_, afterRaw, afterExists, err := b.readCorruptAlarmGeneration(ctx)
		if err != nil {
			return nil, nil, false, err
		}
		if beforeExists == afterExists && bytes.Equal(beforeRaw, afterRaw) {
			return members, afterRaw, afterExists, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
	}
}

// readCorruptAlarmCommitState reads the complete hot-path write fence from one
// storage snapshot when the backend supports batch reads. Arm/Disarm mutate the
// member set, generation, control key, and every shard atomically, so one
// BatchGet is already a stable logical state and avoids the five serial TiKV
// transactions previously paid by every user write. Decorated or legacy
// storages without BatchGetter retain the seqlock-based fallback.
func (b *backend) readCorruptAlarmCommitState(ctx context.Context) ([]uint64, []byte, bool, corruptAlarmCommitGuard, error) {
	members, generation, exists, guard, _, err := b.readCorruptAlarmCommitStateWithPrefetch(ctx, nil)
	return members, generation, exists, guard, err
}

// Extra keys share the alarm snapshot; they remain protected by the caller's
// atomic comparisons at commit. A nil prefetch result requests ordinary reads.
func (b *backend) readCorruptAlarmCommitStateWithPrefetch(ctx context.Context, extraKeys [][]byte) ([]uint64, []byte, bool, corruptAlarmCommitGuard, map[string][]byte, error) {
	var prefetched map[string][]byte
	batchGetter, ok := storage.FindCapability[storage.BatchGetter](b.kv)
	if !ok {
		members, generationRaw, generationExists, err := b.readStableCorruptAlarmState(ctx)
		if err != nil {
			return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, err
		}
		guard, err := b.corruptAlarmCommitGuardFor(ctx, generationRaw, generationExists)
		return members, generationRaw, generationExists, guard, prefetched, err
	}

	shard := b.corruptAlarmFenceShard.Add(1) - 1
	shardKey := corruptAlarmFenceShardKey(shard)
	keys := [][]byte{
		b.ks.EncodeInternalKey(corruptAlarmKey),
		b.ks.EncodeInternalKey(corruptAlarmGenerationKey),
		b.ks.EncodeInternalKey(corruptAlarmFenceControlKey),
		b.ks.EncodeInternalKey(shardKey),
	}
	keys = append(keys, extraKeys...)
	values, err := batchGetter.BatchGet(ctx, keys)
	if err != nil {
		return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, err
	}

	if len(extraKeys) > 0 {
		prefetched = values
		if prefetched == nil {
			prefetched = make(map[string][]byte)
		}
	}

	alarmRaw, alarmExists := values[string(keys[0])]
	members := []uint64(nil)
	if alarmExists {
		members, err = decodeCorruptAlarmMembers(alarmRaw)
		if err != nil {
			return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, fmt.Errorf("decode corrupt alarm metadata: %w", err)
		}
		for i := 1; i < len(members); i++ {
			if members[i-1] >= members[i] {
				return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, invalidAlarmMetadataf("corrupt alarm metadata is not strictly ordered")
			}
		}
	}

	generationRaw, generationExists := values[string(keys[1])]
	if generationExists {
		if _, err := decodeCorruptAlarmGeneration(generationRaw); err != nil {
			return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, err
		}
	}
	control, controlExists := values[string(keys[2])]
	if !controlExists {
		return members, generationRaw, generationExists, corruptAlarmCommitGuard{key: corruptAlarmFenceControlKey}, prefetched, nil
	}
	if !bytes.Equal(control, []byte{1}) {
		return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, invalidAlarmMetadataf("corrupt alarm fence version is %x", control)
	}
	if !generationExists {
		return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, invalidAlarmMetadataf("corrupt alarm fence exists without generation")
	}
	shardRaw, shardExists := values[string(keys[3])]
	if !shardExists {
		return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, invalidAlarmMetadataf("corrupt alarm fence shard %02x is missing", shard%corruptAlarmFenceShardCount)
	}
	if !bytes.Equal(shardRaw, generationRaw) {
		return nil, nil, false, corruptAlarmCommitGuard{}, prefetched, invalidAlarmMetadataf("corrupt alarm fence shard %02x generation mismatch", shard%corruptAlarmFenceShardCount)
	}
	return members, generationRaw, generationExists, corruptAlarmCommitGuard{key: shardKey, expected: shardRaw, exists: true}, prefetched, nil
}

func (b *backend) DisarmCorrupt(ctx context.Context, memberID uint64) (bool, error) {
	// Drain this leader's in-flight logical writes before validating evidence.
	// A write that already passed the server alarm gate can otherwise become
	// uncertain and reassert the same alarm between validation and deletion.
	if err := b.logicalWriteMu.LockContext(ctx); err != nil {
		return false, err
	}
	defer b.logicalWriteMu.Unlock()
	ctx = b.withLogicalWriteOwnership(ctx)
	ctx, unlock := b.lockCorruptAlarm(ctx)
	defer unlock()
	members, raw, exists, err := b.readCorruptAlarms(ctx)
	if err != nil {
		return false, err
	}
	generation, generationRaw, generationExists, err := b.readCorruptAlarmGeneration(ctx)
	if err != nil {
		return false, err
	}
	index := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
	if index == len(members) || members[index] != memberID {
		return false, nil
	}
	// A restart witness is durable evidence that a transaction's event set
	// or one of its referenced object versions may be incomplete. Do not let
	// an operator reopen writes while that evidence still fails validation.
	if err := b.validatePersistedTxnWitnesses(ctx, true); err != nil {
		return false, err
	}
	members = append(members[:index], members[index+1:]...)
	op := InternalCASOp{Key: corruptAlarmKey, Expected: raw, ExpectedExists: exists}
	if len(members) == 0 {
		op.Delete = true
	} else {
		op.Value, err = json.Marshal(members)
		if err != nil {
			return false, err
		}
	}
	nextGeneration, err := encodeNextCorruptAlarmGeneration(generation)
	if err != nil {
		return false, err
	}
	ops := []InternalCASOp{
		op,
		{Key: corruptAlarmGenerationKey, Value: nextGeneration, Expected: generationRaw, ExpectedExists: generationExists},
	}
	ops, err = b.appendCorruptAlarmFenceOps(ctx, ops, nextGeneration, generationRaw, generationExists)
	if err != nil {
		return false, err
	}
	err = b.InternalCAS(ctx, ops)
	if errors.Is(err, storage.ErrCASFailed) {
		return false, ErrCorruptAlarmChanged
	}
	return err == nil, err
}

func (b *backend) readCorruptAlarmGeneration(ctx context.Context) (uint64, []byte, bool, error) {
	raw, err := b.InternalGet(ctx, corruptAlarmGenerationKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	generation, err := decodeCorruptAlarmGeneration(raw)
	if err != nil {
		return 0, nil, false, err
	}
	return generation, raw, true, nil
}

func decodeCorruptAlarmGeneration(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, invalidAlarmMetadataf("corrupt alarm generation has length %d", len(raw))
	}
	generation := binary.BigEndian.Uint64(raw)
	if generation == 0 {
		return 0, invalidAlarmMetadataf("corrupt alarm generation is zero")
	}
	if generation == ^uint64(0) {
		return 0, invalidAlarmMetadataf("corrupt alarm generation is exhausted")
	}
	return generation, nil
}

func encodeNextCorruptAlarmGeneration(current uint64) ([]byte, error) {
	if current == ^uint64(0) {
		return nil, invalidAlarmMetadataf("corrupt alarm generation overflow")
	}
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, current+1)
	return raw, nil
}

type corruptAlarmOwnerKey struct{}

func (b *backend) lockCorruptAlarm(ctx context.Context) (context.Context, func()) {
	if owner, _ := ctx.Value(corruptAlarmOwnerKey{}).(*backend); owner == b {
		return ctx, func() {}
	}
	b.corruptAlarmMu.Lock()
	return context.WithValue(ctx, corruptAlarmOwnerKey{}, b), b.corruptAlarmMu.Unlock
}

func (b *backend) readCorruptAlarms(ctx context.Context) ([]uint64, []byte, bool, error) {
	raw, err := b.InternalGet(ctx, corruptAlarmKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	members, err := decodeOrderedCorruptAlarmMembers(raw)
	if err != nil {
		return nil, nil, false, err
	}
	return members, raw, true, nil
}

func decodeOrderedCorruptAlarmMembers(raw []byte) ([]uint64, error) {
	members, err := decodeCorruptAlarmMembers(raw)
	if err != nil {
		return nil, fmt.Errorf("decode corrupt alarm metadata: %w", err)
	}
	for i := 1; i < len(members); i++ {
		if members[i-1] >= members[i] {
			return nil, invalidAlarmMetadataf("corrupt alarm metadata is not strictly ordered")
		}
	}
	return members, nil
}

func decodeCorruptAlarmMembers(raw []byte) ([]uint64, error) {
	var members []uint64
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&members); err != nil {
		return nil, invalidAlarmMetadataf("%v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, invalidAlarmMetadataf("corrupt alarm metadata contains trailing JSON")
	}
	if members == nil {
		return nil, invalidAlarmMetadataf("corrupt alarm metadata must be a JSON array")
	}
	return members, nil
}
