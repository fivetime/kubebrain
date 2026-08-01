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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestGenericAlarmConcurrentCASPreservesTypesAndMembers(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	types := []etcdserverpb.AlarmType{etcdserverpb.AlarmType(127), etcdserverpb.AlarmType(-7)}

	const membersPerType = 32
	var wg sync.WaitGroup
	errs := make(chan error, len(types)*membersPerType)
	for _, alarm := range types {
		for memberID := uint64(1); memberID <= membersPerType; memberID++ {
			wg.Add(1)
			go func(alarm etcdserverpb.AlarmType, memberID uint64) {
				defer wg.Done()
				_, err := server.mutateGenericAlarm(ctx, alarm, memberID, true)
				errs <- err
			}(alarm, memberID)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	for _, alarm := range types {
		active, err := server.genericAlarms(ctx, alarm)
		require.NoError(t, err)
		require.Len(t, active, membersPerType)
		for index, member := range active {
			require.Equal(t, alarm, member.Alarm)
			require.Equal(t, uint64(index+1), member.MemberID)
		}
	}

	errs = make(chan error, len(types)*membersPerType/2)
	for _, alarm := range types {
		for memberID := uint64(2); memberID <= membersPerType; memberID += 2 {
			wg.Add(1)
			go func(alarm etcdserverpb.AlarmType, memberID uint64) {
				defer wg.Done()
				removed, err := server.mutateGenericAlarm(ctx, alarm, memberID, false)
				if err == nil && !removed {
					err = fmt.Errorf("member %d was not removed", memberID)
				}
				errs <- err
			}(alarm, memberID)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for _, alarm := range types {
		active, err := server.genericAlarms(ctx, alarm)
		require.NoError(t, err)
		require.Len(t, active, membersPerType/2)
		for index, member := range active {
			require.Equal(t, alarm, member.Alarm)
			require.Equal(t, uint64(index*2+1), member.MemberID)
		}
	}
}

func TestGenericAlarmRejectsMalformedMetadata(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{name: "null", raw: `null`, err: "must be a JSON array"},
		{name: "reserved", raw: `[{"alarm":1,"members":[1]}]`, err: "reserved type 1"},
		{name: "empty members", raw: `[{"alarm":127,"members":[]}]`, err: "empty member set"},
		{name: "duplicate members", raw: `[{"alarm":127,"members":[1,1]}]`, err: "members are not strictly ordered"},
		{name: "unordered types", raw: `[{"alarm":127,"members":[1]},{"alarm":12,"members":[2]}]`, err: "types are not strictly ordered"},
		{name: "trailing", raw: `[] {}`, err: "contains trailing JSON"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			require.NoError(t, server.backend.InternalPut(context.Background(), genericAlarmKey, []byte(test.raw)))
			_, err := server.genericAlarms(context.Background(), etcdserverpb.AlarmType_NONE)
			require.ErrorContains(t, err, test.err, fmt.Sprintf("raw metadata %q", test.raw))
		})
	}
}
