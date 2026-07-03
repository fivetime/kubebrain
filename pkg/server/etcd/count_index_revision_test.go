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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestRevisionedCountOnlyMatchesRangeAcrossHistory pins that a revisioned
// CountOnly (served from the count index without materializing the range) returns
// the same count as a full revisioned Range at each historical revision — and
// tracks the count as keys are added and deleted over time.
func TestRevisionedCountOnlyMatchesRangeAcrossHistory(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const prefix = "/registry/svc/"
	end := prefixEnd([]byte(prefix))

	countAt := func(rev int64) int64 {
		co, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, CountOnly: true, Revision: rev})
		require.NoError(t, err)
		require.Empty(t, co.Kvs)
		// Cross-check against a full range at the same revision.
		full, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, Revision: rev})
		require.NoError(t, err)
		require.Equal(t, int64(len(full.Kvs)), co.Count, "CountOnly must equal the materialized range size at rev %d", rev)
		return co.Count
	}

	// Add three keys, recording the revision after each.
	var revs []int64
	for i := 0; i < 3; i++ {
		p, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(fmt.Sprintf("%ss%d", prefix, i)), Value: []byte("v")})
		require.NoError(t, err)
		revs = append(revs, p.Header.Revision)
	}
	// Delete one, recording the revision.
	d, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "s0")})
	require.NoError(t, err)
	delRev := d.Header.Revision
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(delRev) }, 5*time.Second, 2*time.Millisecond)

	// Historical counts track the keyspace as of each revision.
	require.Equal(t, int64(1), countAt(revs[0]))
	require.Equal(t, int64(2), countAt(revs[1]))
	require.Equal(t, int64(3), countAt(revs[2]))
	require.Equal(t, int64(2), countAt(delRev)) // one deleted
	require.Equal(t, int64(2), countAt(0))      // current
}
