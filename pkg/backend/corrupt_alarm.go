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

var (
	corruptAlarmKey           = []byte("alarms/corrupt")
	corruptAlarmGenerationKey = []byte("alarms/corrupt-generation")

	// ErrCorruptAlarmChanged means another replica activated or changed the
	// alarm while a disarm was validating durable evidence. Requiring a fresh
	// operator request prevents that new signal from being silently consumed.
	ErrCorruptAlarmChanged = errors.New("corrupt alarm changed during disarm")
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
		err = b.InternalCAS(ctx, []InternalCASOp{
			{Key: corruptAlarmKey, Value: value, Expected: raw, ExpectedExists: exists},
			{Key: corruptAlarmGenerationKey, Value: nextGeneration, Expected: generationRaw, ExpectedExists: generationExists},
		})
		if errors.Is(err, storage.ErrCASFailed) {
			continue
		}
		return err
	}
}

func (b *backend) CorruptAlarms(ctx context.Context) ([]uint64, error) {
	members, _, _, err := b.readCorruptAlarms(ctx)
	if err != nil {
		return nil, err
	}
	// The generation participates in every future Arm/Disarm CAS. Treat it as
	// part of the alarm's readable integrity envelope so health, write gates and
	// logical snapshots cannot certify state that cannot be mutated safely.
	if _, _, _, err = b.readCorruptAlarmGeneration(ctx); err != nil {
		return nil, err
	}
	return members, nil
}

func (b *backend) DisarmCorrupt(ctx context.Context, memberID uint64) (bool, error) {
	// Drain this leader's in-flight logical writes before validating evidence.
	// A write that already passed the server alarm gate can otherwise become
	// uncertain and reassert the same alarm between validation and deletion.
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()
	ctx = b.withLogicalWriteOwnership(ctx)
	ctx, unlock := b.lockCorruptAlarm(ctx)
	defer unlock()
	for {
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
		// may be incomplete. Do not let an operator reopen writes merely by
		// clearing CORRUPT while that evidence still fails validation.
		if err := b.validatePersistedTxnWitnesses(ctx); err != nil {
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
		err = b.InternalCAS(ctx, []InternalCASOp{
			op,
			{Key: corruptAlarmGenerationKey, Value: nextGeneration, Expected: generationRaw, ExpectedExists: generationExists},
		})
		if errors.Is(err, storage.ErrCASFailed) {
			return false, ErrCorruptAlarmChanged
		}
		return err == nil, err
	}
}

func (b *backend) readCorruptAlarmGeneration(ctx context.Context) (uint64, []byte, bool, error) {
	raw, err := b.InternalGet(ctx, corruptAlarmGenerationKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	if len(raw) != 8 {
		return 0, nil, false, invalidAlarmMetadataf("corrupt alarm generation has length %d", len(raw))
	}
	generation := binary.BigEndian.Uint64(raw)
	if generation == 0 {
		return 0, nil, false, invalidAlarmMetadataf("corrupt alarm generation is zero")
	}
	if generation == ^uint64(0) {
		return 0, nil, false, invalidAlarmMetadataf("corrupt alarm generation is exhausted")
	}
	return generation, raw, true, nil
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
	members, err := decodeCorruptAlarmMembers(raw)
	if err != nil {
		return nil, nil, false, fmt.Errorf("decode corrupt alarm metadata: %w", err)
	}
	for i := 1; i < len(members); i++ {
		if members[i-1] >= members[i] {
			return nil, nil, false, invalidAlarmMetadataf("corrupt alarm metadata is not strictly ordered")
		}
	}
	return members, raw, true, nil
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
