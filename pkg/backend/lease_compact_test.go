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

package backend

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestCompactRetiresLeaseKeyspaceVersions verifies the lease half of #15: lease
// records live under the reserved \x00kubebrain/ namespace and are rewritten on
// every keepalive, so without compaction their MVCC versions grow without bound.
// Folding \x00kubebrain/ into the compaction borders retires the superseded
// versions while the latest record stays readable.
func TestCompactRetiresLeaseKeyspaceVersions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	// The etcd layer writes lease records under \x00kubebrain/leases/<id>; model
	// a grant (Create) followed by keepalive rewrites (Update) of one record.
	leaseKey := append(append([]byte(nil), internalKeyspacePrefix...), []byte("leases/42")...)
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: leaseKey, Value: []byte("record-1")})
	require.NoError(t, err)
	last := cr.Header.Revision
	const keepalives = 5
	for i := 2; i <= keepalives; i++ {
		u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: leaseKey, Value: []byte(fmt.Sprintf("record-%d", i)), Revision: last}})
		require.NoError(t, err)
		require.True(t, u.Succeeded)
		last = u.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= last }, 5*time.Second, 2*time.Millisecond)

	// Count object-key versions (rev != 0) for the lease key in storage.
	countVersions := func() int {
		iter, err := b.kv.Iter(ctx,
			b.coder.EncodeObjectKey(leaseKey, ^uint64(0)),
			b.coder.EncodeObjectKey(leaseKey, 0), 0, 0)
		require.NoError(t, err)
		defer iter.Close()
		n := 0
		for {
			if err := iter.Next(ctx); err != nil {
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
			}
			_, rev, derr := b.coder.Decode(iter.Key())
			require.NoError(t, derr)
			if rev != 0 { // skip the revision key
				n++
			}
		}
		return n
	}

	require.Equal(t, keepalives, countVersions(), "each keepalive appends a new lease-record version before compact")

	_, err = b.Compact(ctx, last)
	require.NoError(t, err)

	require.Equal(t, 1, countVersions(), "compaction must retire superseded lease-record versions, keeping only the latest")

	// The latest lease record is still readable after compaction.
	got, err := b.Get(ctx, &proto.GetRequest{Key: leaseKey})
	require.NoError(t, err)
	require.NotNil(t, got.Kv)
	require.Equal(t, "record-5", string(StripInlineValue(got.Kv.Value)))
}
