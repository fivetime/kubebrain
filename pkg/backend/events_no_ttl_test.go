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
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// TestCompactDoesNotExpireKeysByEventsName pins #16: the backend no longer stamps
// a hardcoded 3600s TTL on keys whose path merely contains "/events/", and the
// compaction scanner no longer name-expires them. Key expiry is now driven solely
// by the lease attached at the etcd layer (honoring the actual granted TTL); the
// backend only reclaims superseded versions/tombstones. Previously
// bytes.Contains(key, "/events/") gave any such key a fixed 3600s TTL and the
// scanner physically deleted it — mis-expiring real events (ignoring their real
// granted TTL and keepalive renewal) and, worse, unrelated keys that merely
// contained "/events/" (silent data loss).
//
// Runs on the memkv-backed suite, which reports SupportTTL()==false and therefore
// exercised the scanner's events-TTL GC path that this change removes.
func TestCompactDoesNotExpireKeysByEventsName(t *testing.T) {
	s, c := newTestSuites(t, tiKvStorage)
	defer c()
	ast := assert.New(t)
	ctx := context.Background()

	base := path.Join(prefix, "events-ttl-16")
	keys := []string{
		path.Join(base, "events", "ns", "evt-0"),        // real k8s-style event path
		path.Join(base, "configmaps", "events", "cm-0"), // unrelated key that merely contains /events/
		path.Join(base, "pods", "p-0"),                  // plain key
	}
	var rev uint64
	for _, k := range keys {
		resp, err := s.backend.Create(ctx, &proto.CreateRequest{Key: []byte(k), Value: []byte("v")})
		ast.NoError(err)
		ast.True(resp.Succeeded)
		rev = resp.Header.Revision
	}

	// Compact several cycles (advancing the revision each time via a bump key kept
	// outside base). Under the old events-TTL GC an /events/ key would eventually
	// be physically removed; with the heuristic gone, every key survives every
	// compaction.
	bumpPrefix := path.Join(prefix, "bump-16")
	for i := 0; i < 3; i++ {
		waitUntilRevisionEqualOrTimeout(s.backend, rev)
		_, err := s.backend.Compact(ctx, rev)
		ast.NoError(err)

		resp, err := s.backend.List(ctx, &proto.RangeRequest{
			Key: []byte(base),
			End: PrefixEnd([]byte(base)),
		})
		ast.NoError(err)
		ast.Equal(len(keys), len(resp.Kvs),
			"no key may be expired by name; expiry is the lease manager's job")

		bump, err := s.backend.Create(ctx, &proto.CreateRequest{
			Key:   []byte(path.Join(bumpPrefix, string(rune('a'+i)))),
			Value: []byte("b"),
		})
		ast.NoError(err)
		rev = bump.Header.Revision
	}
}
