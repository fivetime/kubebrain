// Copyright 2026 ByteDance and/or its affiliates
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

package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var genericAlarmKey = []byte("alarms/generic")

type genericAlarmEntry struct {
	Alarm   int32    `json:"alarm"`
	Members []uint64 `json:"members"`
}

func (s *RPCServer) mutateGenericAlarm(
	ctx context.Context,
	alarm etcdserverpb.AlarmType,
	memberID uint64,
	activate bool,
) (bool, error) {
	for {
		entries, raw, exists, err := s.readGenericAlarmState(ctx)
		if err != nil {
			return false, err
		}
		alarmValue := int32(alarm)
		entryIndex := sort.Search(len(entries), func(i int) bool { return entries[i].Alarm >= alarmValue })
		if entryIndex == len(entries) || entries[entryIndex].Alarm != alarmValue {
			if !activate {
				return false, nil
			}
			entries = append(entries, genericAlarmEntry{})
			copy(entries[entryIndex+1:], entries[entryIndex:])
			entries[entryIndex] = genericAlarmEntry{Alarm: alarmValue, Members: []uint64{memberID}}
		} else {
			members := entries[entryIndex].Members
			memberIndex := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
			if memberIndex < len(members) && members[memberIndex] == memberID {
				if activate {
					return true, nil
				}
			} else {
				if !activate {
					return false, nil
				}
				members = append(members, 0)
				copy(members[memberIndex+1:], members[memberIndex:])
				members[memberIndex] = memberID
				entries[entryIndex].Members = members
			}
		}

		if !activate {
			members := entries[entryIndex].Members
			memberIndex := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
			members = append(members[:memberIndex], members[memberIndex+1:]...)
			if len(members) == 0 {
				entries = append(entries[:entryIndex], entries[entryIndex+1:]...)
			} else {
				entries[entryIndex].Members = members
			}
		}

		op := backend.InternalCASOp{Key: genericAlarmKey, Expected: raw, ExpectedExists: exists}
		if len(entries) == 0 {
			op.Delete = true
		} else {
			op.Value, err = json.Marshal(entries)
			if err != nil {
				return false, err
			}
		}
		err = s.backend.InternalCAS(ctx, []backend.InternalCASOp{op})
		if errors.Is(err, storage.ErrCASFailed) {
			continue
		}
		return err == nil, err
	}
}

func (s *RPCServer) genericAlarms(
	ctx context.Context,
	filter etcdserverpb.AlarmType,
) ([]*etcdserverpb.AlarmMember, error) {
	entries, _, _, err := s.readGenericAlarmState(ctx)
	if err != nil {
		return nil, err
	}
	alarms := make([]*etcdserverpb.AlarmMember, 0)
	for _, entry := range entries {
		if filter != etcdserverpb.AlarmType_NONE && entry.Alarm != int32(filter) {
			continue
		}
		for _, memberID := range entry.Members {
			alarms = append(alarms, &etcdserverpb.AlarmMember{
				MemberID: memberID,
				Alarm:    etcdserverpb.AlarmType(entry.Alarm),
			})
		}
	}
	return alarms, nil
}

// GenericAlarms returns every persisted non-NOSPACE/non-CORRUPT alarm. The
// top-level HTTP server uses this to mirror etcd's legacy /health alarm check.
func (s *RPCServer) GenericAlarms(ctx context.Context) ([]*etcdserverpb.AlarmMember, error) {
	return s.genericAlarms(ctx, etcdserverpb.AlarmType_NONE)
}

func (s *RPCServer) readGenericAlarmState(
	ctx context.Context,
) ([]genericAlarmEntry, []byte, bool, error) {
	raw, err := s.backend.InternalGet(ctx, genericAlarmKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	var entries []genericAlarmEntry
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&entries); err != nil {
		return nil, nil, false, fmt.Errorf("decode generic alarm metadata: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, nil, false, errors.New("generic alarm metadata contains trailing JSON")
	}
	if entries == nil {
		return nil, nil, false, errors.New("generic alarm metadata must be a JSON array")
	}
	for i, entry := range entries {
		if entry.Alarm == int32(etcdserverpb.AlarmType_NONE) ||
			entry.Alarm == int32(etcdserverpb.AlarmType_NOSPACE) ||
			entry.Alarm == int32(etcdserverpb.AlarmType_CORRUPT) {
			return nil, nil, false, fmt.Errorf("generic alarm metadata contains reserved type %d", entry.Alarm)
		}
		if i > 0 && entries[i-1].Alarm >= entry.Alarm {
			return nil, nil, false, errors.New("generic alarm metadata types are not strictly ordered")
		}
		if len(entry.Members) == 0 {
			return nil, nil, false, errors.New("generic alarm metadata contains an empty member set")
		}
		for memberIndex := 1; memberIndex < len(entry.Members); memberIndex++ {
			if entry.Members[memberIndex-1] >= entry.Members[memberIndex] {
				return nil, nil, false, errors.New("generic alarm metadata members are not strictly ordered")
			}
		}
	}
	return entries, raw, true, nil
}
