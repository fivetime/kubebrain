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

package election

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestResourceLockRejectsMalformedElectionMetadata(t *testing.T) {
	base, err := json.Marshal(resourcelock.LeaderElectionRecord{
		HolderIdentity:       "peer-a",
		LeaseDurationSeconds: 10,
		LeaderTransitions:    1,
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		raw  []byte
		want string
	}{
		{
			name: "unknown field",
			raw:  append(base[:len(base)-1:len(base)-1], []byte(`,"unexpected":true}`)...),
			want: `json: unknown field "unexpected"`,
		},
		{
			name: "trailing json",
			raw:  append(append([]byte(nil), base...), []byte(` {}`)...),
			want: "leader election record contains trailing JSON",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			prefix := "/registry/kubebrain"
			batch := kv.BeginBatchWrite()
			batch.Put(getElectionKey(prefix), tc.raw, 0)
			require.NoError(t, batch.Commit(context.Background()))

			lock := NewResourceLockManager(Config{
				Prefix:   prefix,
				Identity: "peer-b",
				Timeout:  time.Second,
			}, kv).GetResourceLock()
			_, _, err := lock.Get(context.Background())
			require.EqualError(t, err, tc.want)
		})
	}
}
