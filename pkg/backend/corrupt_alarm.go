package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var corruptAlarmKey = []byte("alarms/corrupt")

func (b *backend) ArmCorrupt(ctx context.Context, memberID uint64) error {
	for {
		members, raw, exists, err := b.readCorruptAlarms(ctx)
		if err != nil {
			return err
		}
		index := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
		if index < len(members) && members[index] == memberID {
			return nil
		}
		members = append(members, 0)
		copy(members[index+1:], members[index:])
		members[index] = memberID
		value, err := json.Marshal(members)
		if err != nil {
			return err
		}
		err = b.InternalCAS(ctx, []InternalCASOp{{
			Key: corruptAlarmKey, Value: value, Expected: raw, ExpectedExists: exists,
		}})
		if errors.Is(err, storage.ErrCASFailed) {
			continue
		}
		return err
	}
}

func (b *backend) CorruptAlarms(ctx context.Context) ([]uint64, error) {
	members, _, _, err := b.readCorruptAlarms(ctx)
	return members, err
}

func (b *backend) DisarmCorrupt(ctx context.Context, memberID uint64) (bool, error) {
	for {
		members, raw, exists, err := b.readCorruptAlarms(ctx)
		if err != nil {
			return false, err
		}
		index := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
		if index == len(members) || members[index] != memberID {
			return false, nil
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
		err = b.InternalCAS(ctx, []InternalCASOp{op})
		if errors.Is(err, storage.ErrCASFailed) {
			continue
		}
		return err == nil, err
	}
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
			return nil, nil, false, errors.New("corrupt alarm metadata is not strictly ordered")
		}
	}
	return members, raw, true, nil
}

func decodeCorruptAlarmMembers(raw []byte) ([]uint64, error) {
	var members []uint64
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&members); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("corrupt alarm metadata contains trailing JSON")
	}
	if members == nil {
		return nil, errors.New("corrupt alarm metadata must be a JSON array")
	}
	return members, nil
}
