// Copyright 2022 ByteDance and/or its affiliates
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

package scanner

import (
	"context"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// Scanner is used to control concurrent range in partitions for list or compact
type Scanner interface {

	// Range run scan in partitions concurrently
	Range(ctx context.Context, start []byte, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error)

	// RangeStream run scan in partitions concurrently and returns value by stream
	RangeStream(ctx context.Context, start []byte, end []byte, revision uint64, keysOnly bool) chan *proto.StreamRangeResponse

	// Count run scan in partitions concurrently and returns the count of user key
	Count(ctx context.Context, start []byte, end []byte, revision uint64) (int, error)

	// Compact reclaims superseded versions and tombstones across all the given
	// [start,end) border pairs. It returns the first border scan error (after the
	// per-worker retries) so the caller can surface a failed physical GC instead
	// of silently reporting success while garbage accumulates. Key expiry (TTL) is
	// not handled here — it is driven by the lease manager, which deletes expired
	// keys whose tombstones are then reclaimed by this compaction like any other.
	Compact(ctx context.Context, borders [][]byte, revision uint64) error

	// CompactKeys runs the same version-GC as Compact but only over the version
	// ranges of the given user keys (the incremental physical GC path: the keys
	// touched since the last completed pass, derived from the event log). GC
	// semantics per row are identical to Compact — each key's range is scanned by
	// the same worker machinery — so correctness does not depend on the caller's
	// key set being minimal, only on it covering every key written since the
	// baseline. Any error means the round must not advance the incremental
	// baseline (the caller falls back to a full scan).
	CompactKeys(ctx context.Context, userKeys [][]byte, revision uint64) error
}
